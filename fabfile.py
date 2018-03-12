#!/usr/bin/python
# coding: utf-8

# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

#
# fabfile.py
#

#
# A Python fabric file for setting up and managing the AWS ECS POV-Ray worker demo.
# This file assumes python2 is installed on the system as the default.
#

# Imports
from fabric.api import local, quiet, env, run, put, cd
from ConfigParser import ConfigParser
import boto3
import botocore
from zipfile import ZipFile, ZIP_DEFLATED
import os
import json
import time
from urllib2 import unquote
from cStringIO import StringIO

# Constants (User configurable), imported from config.py

from config import *

##Support Functions ##
def hello_world():
    print("HI!")

def update_dependencies():
    local('pip2 install -r requirements.txt')
    #local('cd ' + LAMBDA_FUNCTION_NAME + '; npm install ' + LAMBDA_FUNCTION_DEPENDENCIES)

def get_aws_credentials():
    config = ConfigParser()
    config.read(AWS_CONFIG_FILE_NAME)
    config.read(AWS_CREDENTIAL_FILE_NAME)
    return config.get(AWS_PROFILE, 'aws_access_key_id'), config.get(AWS_PROFILE, 'aws_secret_access_key')

def get_session():
    dev = boto3.session.Session(profile_name='buchanam')
    return dev

def get_iam_connection():
  dev = get_session()
  return dev.client('iam')

#Create the application user
#def create_application_user():
#    get_iam_connection()

#***
#Create a ssh key for ecs

#
# def set_application_user_roles():

#Create the application user
# def create_application_user():
#
# def set_application_user_roles():

## update_bucket
def get_s3_connection():
    dev = get_session()
    return dev.resource('s3')

def get_lambda_connection():
    dev = get_session()
    return dev.client('lambda')

def get_sqs_connection():
    dev = get_session()
    return dev.client('sqs')

def get_autoscale_connection():
    dev = get_session()
    return dev.resource('autoscaling')

def get_ec2_connection():
    dev = get_session()
    return dev.client('ec2')

def get_create_keypair():
    ec2 = get_ec2_connection()
    SshFile = '/home/' + USERNAME + '/.ssh/' + EC2_KEYNAME
    print(SshFile)
    if (os.path.isfile(SshFile)):
        try:
            ec2.describe_key_pairs(KeyNames=[EC2_KEYNAME])
            print('Awesome, looks like that key pair exists in AWS and locally at:'+SshFile)
        except:
            print('you have a copy of the key but it does not exist in AWS, loading it up there!')
            try:
                with open(SshFile+'.pub', 'r') as f:
                    ec2.import_key_pair(KeyName=EC2_KEYNAME,PublicKeyMaterial=f.read())
            except:
                print('you have no public/private key pair with this name. Create one and try again, or and recreate with this function.')
    else:
        try:
            ec2.describe_key_pairs(KeyNames=[EC2_KEYNAME])
        except:
            print('The key exists in AWS, but you do not have a copy. Creating a new key...')
    ec2.delete_key_pair(KeyName=EC2_KEYNAME)
    response = ec2.create_key_pair(KeyName=EC2_KEYNAME)
    print(response)
    with open(SshFile, 'w') as f:
        print >> f, response['KeyMaterial']
        os.chmod(SshFile, 0600)
        print('Created the ' + EC2_KEYNAME + ' key pair and saved it to '+ SshFile)
    return response


def get_or_create_bucket():
    s3 = get_s3_connection()
    b = s3.Bucket(UPLOAD_BUCKET)
    if b not in s3.buckets.all():
        print('Trying to create bucket: ' + UPLOAD_BUCKET + ' in region: ' + AWS_REGION + '...')
        LOCATION = AWS_REGION
        b = s3.create_bucket(Bucket=UPLOAD_BUCKET,
        CreateBucketConfiguration={
            'LocationConstraint':LOCATION
            }
        )
    else:
        print('Found bucket: ' + UPLOAD_BUCKET + '.')
    return b

# Create the dynamodb
def get_dynamo_connection():
    dev = get_session()
    return dev.resource('dynamodb')

def get_or_create_dynamodb():
    dynamodb = get_dynamo_connection()
    try:
        table = dynamodb.create_table(
            TableName = DYNAMODB_UPLOAD_TABLE,
            KeySchema = DYNAMO_KEYSCHEMA,
            AttributeDefinitions = DYNAMO_ATTRIBUTES,
            ProvisionedThroughput = DYNAMO_THROUGHPUT
            )
    except:
        print('Table ' + DYNAMODB_UPLOAD_TABLE + ' already exists.')
        table = dynamodb.Table(DYNAMODB_UPLOAD_TABLE)
    return table

#update_queue
def get_queue_url():
    sqs = get_sqs_connection()
    try:
        response = sqs.get_queue_url(
            QueueName = SQS_QUEUE_NAME,
            QueueOwnerAWSAccountId = ACCOUNT_ID,
            )
        QueueUrl = response['QueueUrl']
        print(QueueUrl)
    except:
        QueueUrl = None
        print('The queue does not exist.')
    return QueueUrl

## finish queue business
def get_or_create_queue():
    QueueUrl = get_queue_url()
    if QueueUrl is None:
        print('Creating the ' + SQS_QUEUE_NAME + '...')
        sqs = get_sqs_connection()
        queue = sqs.create_queue(
            QueueName=SQS_QUEUE_NAME,
            Attributes={
                'MessageRetentionPeriod': '1209600',
	            'VisibilityTimeout': '360'
                    }
                )
        print('Created the ' + SQS_QUEUE_NAME + '!')
        print(queue)
        return QueueUrl
    else:
        return QueueUrl

#test send a queue message
# def test_queue():
#     key = 'test'
#     QueueUrl = get_queue_url()
#     sqs = get_sqs_connection()
#     response = queue.send_message(QueueUrl=MessageBody=key)



#update_lambda_function
def dump_lambda_function_configuration():
    print('Writing config for Lambda function...')
    lambda_function_config = LAMBDA_FUNCTION_CONFIG.copy()
    lambda_function_config['queue'] = get_queue_url()
    with open(LAMBDA_FUNCTION_CONFIG_PATH, 'w') as fp:
        fp.write(json.dumps(lambda_function_config))

def create_lambda_deployment_package():
    print('Creating ZIP file: ' + ZIPFILE_NAME + '...')
    with ZipFile(ZIPFILE_NAME, 'w', ZIP_DEFLATED) as z:
        saved_dir = os.getcwd()
        os.chdir(LAMBDA_FUNCTION_NAME)
        try:
            os.remove(ZIPFILE_NAME)
        except OSError:
            pass
        for root, dirs, files in os.walk('.'):
            for basename in files:
                filename = os.path.join(root, basename)
                print('Adding: ' + filename + '...')
                z.write(filename)
        os.chdir(saved_dir + '/cowenaws/lib/python2.7/site-packages/')
        for root, dirs, files in os.walk('.'):
            for basename in files:
                filename = os.path.join(root, basename)
                print('Adding: ' + filename + '...')
                z.write(filename)
        os.chdir(saved_dir)
        z.close()

def get_or_create_lambda_execution_role():
    iam = get_iam_connection()
    target_policy = json.dumps(LAMBDA_EXECUTION_ROLE_TRUST_POLICY, sort_keys=True)
    try:
        result = iam.get_role(
        RoleName=LAMBDA_EXECUTION_ROLE_NAME
            )
        print('Found role: ' + LAMBDA_EXECUTION_ROLE_NAME + '.')
    except:
    #if result is None:
        print(LAMBDA_EXECUTION_ROLE_NAME + ' not found!')
        print('Creating role: ' + LAMBDA_EXECUTION_ROLE_NAME + '...')
        result = iam.create_role(
            RoleName=LAMBDA_EXECUTION_ROLE_NAME,
            AssumeRolePolicyDocument=target_policy
            )
    policy = result['Role']['AssumeRolePolicyDocument']
    if (
        policy is not None and
        json.dumps(policy, sort_keys=True) == target_policy
     ): print('Assume role policy for: ' + LAMBDA_EXECUTION_ROLE_NAME + ' verified.')
    else:
        print('Updating assume role policy for: ' + LAMBDA_EXECUTION_ROLE_NAME + '.')
        iam.update_assume_role_policy(LAMBDA_EXECUTION_ROLE_NAME, target_policy)
        time.sleep(WAIT_TIME)
    role_arn = result['Role']['Arn']
    print(role_arn)
    return role_arn

def check_lambda_execution_role_policies():
    iam = get_iam_connection()

    response = iam.list_role_policies(
        RoleName=LAMBDA_EXECUTION_ROLE_NAME
            )
    print(response)
    policy_names = response['PolicyNames']

    found = False
    for p in policy_names:
        found = (p == LAMBDA_EXECUTION_ROLE_POLICY_NAME)
        if found:
            print('Found policy: ' + LAMBDA_EXECUTION_ROLE_POLICY_NAME + '.')
            break

    if not found:
        print('Attaching policy: ' + LAMBDA_EXECUTION_ROLE_POLICY_NAME + '.')
        iam.put_role_policy(
            RoleName = LAMBDA_EXECUTION_ROLE_NAME,
            PolicyName = 'AWSLambdaExecute',
            PolicyDocument = json.dumps(LAMBDA_EXECUTION_ROLE_POLICY)
        )

    return

def get_lambda_function_arn():
    awslambda = get_lambda_connection()
    try:
        response = awslambda.get_function(
            FunctionName=LAMBDA_FUNCTION_NAME
            )
        LambdaArn = response['Configuration']['FunctionArn']
        print('Found ' + LAMBDA_FUNCTION_NAME + ' Arn!')
        return LambdaArn
    except:
        print('Function '+ LAMBDA_FUNCTION_NAME +' found!')
        return None

## needed??
def check_bucket_permissions():
    try:
        awslambda = get_lambda_connection()
        result = awslambda.get_policy(
            FunctionName=LAMBDA_FUNCTION_NAME
        )
    except:
        result = ''
    if result is None or result == '':
        return False

    result_decoded = json.loads(result)
    if not isinstance(result_decoded, dict):
        return False

    policy = json.loads(result_decoded.get('Policy', '{}'))
    if not isinstance(policy, dict):
        return False

    statements = policy.get('Statement', [])
    for s in statements:
        if s.get('Sid', '') == BUCKET_PERMISSION_SID:
            return True

    return False

def create_lambda_function():
    with open(ZIPFILE_NAME, 'rb') as ZipFile:
        awslambda = get_lambda_connection()
        role_arn = get_or_create_lambda_execution_role()

        if get_lambda_function_arn() is not None:
            print('Deleting existing Lambda function ' + LAMBDA_FUNCTION_NAME + '.')
            delete_lambda_function()
        response = awslambda.create_function(
        FunctionName=LAMBDA_FUNCTION_NAME,
        Runtime = 'python2.7',
        Role = role_arn,
        Handler = LAMBDA_FUNCTION_NAME + '.handler' ,
        Code={'ZipFile': ZipFile.read()},
        Description = LAMBDA_FUNCTION_NAME
        )
        return response

def update_lambda_function():
    dump_lambda_function_configuration()
    create_lambda_deployment_package()
    get_or_create_lambda_execution_role()
    check_lambda_execution_role_policies()
    create_lambda_function()

def update_bucket_permissions():
    get_or_create_bucket()
    if check_bucket_permissions():
        print('Lambda invocation permission for bucket: ' + UPLOAD_BUCKET + ' is set.')
    else:
        print('Setting Lambda invocation permission for bucket: ' + UPLOAD_BUCKET + '.')
        awslambda = get_lambda_connection()
        awslambda.add_permission(
            FunctionName = LAMBDA_FUNCTION_NAME,
            StatementId = BUCKET_PERMISSION_SID,
            Action = 'lambda:InvokeFunction',
            Principal = 's3.amazonaws.com',
            SourceArn = 'arn:aws:s3:::' + UPLOAD_BUCKET
        )
    return True

def delete_lambda_function():
    awslambda = get_lambda_connection()
    awslambda.delete_function(
        FunctionName = LAMBDA_FUNCTION_NAME,
    )
    print('Deleted the function: ' + LAMBDA_FUNCTION_NAME)

def check_bucket_notifications():
    s3 = get_s3_connection()
    try:
        result = s3.get_bucket_notification_configuration(UPLOAD_BUCKET)
    except:
        print('No bucket notification configuration set.')
        result = None
    if result is None or result == '':
        return False
    result_decoded = json.loads(result)
    if not isinstance(result_decoded, dict):
        return False
    else:
        print('Bucket permissions exist!')
        return True

def setup_bucket_notifications():
    # Pre-Reqs
    #update_lambda_function()
    #update_bucket_permissions()
    notification_configuration = BUCKET_NOTIFICATION_CONFIGURATION.copy()
    lambda_function_arn = get_lambda_function_arn()
    notification_configuration['LambdaFunctionConfigurations'][0]['LambdaFunctionArn'] = lambda_function_arn
    if check_bucket_notifications():
        print('Bucket notification configuration for bucket: ' + UPLOAD_BUCKET + ' is set.')
    else:
        print('Setting bucket notification configuration for bucket: ' + UPLOAD_BUCKET + '.')
        dev = get_session()
        s3 = dev.client('s3')
        s3.put_bucket_notification_configuration(
            Bucket = UPLOAD_BUCKET,
            NotificationConfiguration = notification_configuration
        )
    return True

def get_or_create_spot_fleet_execution_role():
    iam = get_iam_connection()
    target_policy = json.dumps(SPOT_FLEET_ROLE_TRUST_POLICY, sort_keys=True)
    try:
        result = iam.get_role(
        RoleName=SPOT_FLEET_ROLE_NAME
            )
        print('Found role: ' + SPOT_FLEET_ROLE_NAME + '.')
    except:
    #if result is None:
        print(SPOT_FLEET_ROLE_NAME + ' not found!')
        print('Creating role: ' + SPOT_FLEET_ROLE_NAME + '...')
        result = iam.create_role(
            RoleName=SPOT_FLEET_ROLE_NAME,
            AssumeRolePolicyDocument=target_policy
            )
    policy = result['Role']['AssumeRolePolicyDocument']
    if (
        policy is not None and
        json.dumps(policy, sort_keys=True) == target_policy
     ): print('Assume role policy for: ' + SPOT_FLEET_ROLE_NAME + ' verified.')
    else:
        print('Updating assume role policy for: ' + SPOT_FLEET_ROLE_NAME + '.')
        iam.update_assume_role_policy(
            RoleName=SPOT_FLEET_ROLE_NAME,
            PolicyDocument=target_policy)
        time.sleep(WAIT_TIME)
    role_arn = result['Role']['Arn']
    print(role_arn)
    return role_arn

def check_spot_fleet_execution_role_policies():
    iam = get_iam_connection()

    response = iam.list_role_policies(
        RoleName=SPOT_FLEET_ROLE_NAME
            )
    print(response)
    policy_names = response['PolicyNames']

    found = False
    for p in policy_names:
        found = (p == SPOT_FLEET_ROLE_POLICY_NAME)
        if found:
            print('Found policy: ' + SPOT_FLEET_ROLE_POLICY_NAME + '.')
            break

    if not found:
        print('Attaching policy: ' + SPOT_FLEET_ROLE_POLICY_NAME + '.')
        iam.put_role_policy(
            RoleName = SPOT_FLEET_ROLE_NAME,
            PolicyName = 'AWSSpotFleetRole',
            PolicyDocument = json.dumps(SPOT_FLEET_ROLE_POLICY)
        )

    return

def describe_spot_fleet():
    ec2 = get_ec2_connection()

    response = ec2.describe_spot_fleet_requests(
    DryRun=False,
    MaxResults=10
    )
    return response

def get_or_create_spotfleet():
    ec2 = get_ec2_connection()
    creds = get_aws_credentials()
    role_arn = get_or_create_spot_fleet_execution_role()
    user_data = base64.b64encode((SEGMENTATION_USER_DATA_SCRIPT % (creds[0],creds[1])).encode()).decode("ascii")
    spotfleet_request_config = SPOTFLEET_REQUEST_CONFIG.copy()
    spotfleet_request_config['IamFleetRole'] = role_arn
    spotfleet_request_config['LaunchSpecifications'][0]['UserData'] = user_data
    print(spotfleet_request_config)
    response = ec2.request_spot_fleet(
    DryRun=False,
    SpotFleetRequestConfig=spotfleet_request_config
    )
    print(response)

def destroy_spotfleet():
    ec2 = get_ec2_connection()
    spotfleet_request = describe_spot_fleet()
    spotfleet_id = spotfleet_request.get('SpotFleetRequestConfigs')[0].get('SpotFleetRequestId')
    print(spotfleet_id)
    response = ec2.cancel_spot_fleet_requests(
    DryRun=False,
    SpotFleetRequestIds=[
        spotfleet_id,
    ],
    TerminateInstances=True
    )
    print(response)

def get_instance_ip_from_id(instance_id):
    result = json.loads(local(
        'aws ec2 describe-instances' +
        '    --instance ' + instance_id +
        '    --query Reservations[0].Instances[0].PublicIpAddress' +
        AWS_CLI_STANDARD_OPTIONS,
        capture=True
    ))
    print ('IP address for instance ' + instance_id + ' is: ' + result)
    return result


def get_container_instances():
    result = json.loads(local(
        'aws ecs list-container-instances' +
        '    --query containerInstanceArns' +
        '    --cluster ' + ECS_CLUSTER +
        AWS_CLI_STANDARD_OPTIONS,
        capture=True
    ))
    print('Container instances: ' + ','.join(result))
    return result


def get_first_ecs_instance():
    container_instances = get_container_instances()

    result = json.loads(local(
        'aws ecs describe-container-instances' +
        '    --cluster ' + ECS_CLUSTER +
        '    --container-instances ' + container_instances[0] +
        '    --query containerInstances[0].ec2InstanceId' +
        AWS_CLI_STANDARD_OPTIONS,
        capture=True
    ))
    print('First container instance: ' + result)
    return result

# def create_autoscaling_config():
#     get_ec2_connection()

## Primary Functions
def update_bucket():
    get_or_create_bucket()

def update_queue():
    get_or_create_queue()

# def update_dynamo():
#     get_or_create_dynamodb()

def update_lambda():
    update_lambda_function()
    update_bucket_permissions()
    setup_bucket_notifications()

# def update_autoscaling_group()
#     get_create_launch_configuration()
#     get_create_autoscaling_group()

def setup():
    update_bucket()
    update_queue()
    update_dynamo()
    update_lambda()
#def update_spot_fleet():
