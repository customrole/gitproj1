import boto3
import logging
from logging.config import dictConfig

def get_asg_for_cluster(cluster_name, asg_client):
    """
    Gets the details of Auto-Scaling Group for the cluster
    :param cluster_name: the name of the cluster
    :param asg_client: an AWS Auto-Scaling client
    :return: an Auto-Scaling Group's details
    """
    paginator = asg_client.get_paginator('describe_auto_scaling_groups')
    print paginator
    page_iterator = paginator.paginate(
    )
    print page_iterator
    filtered_asgs = page_iterator.search(
        'AutoScalingGroups[] | [?contains(Tags[?Key==`{}`].Value, `{}`)]'.format(
            'Name', cluster_name)
    )
    print filtered_asgs
    asgs = []
    for asg in filtered_asgs:
        asgs.append(asg['AutoScalingGroupName'])
    if len(asgs) == 0:
        return None
    elif len(asgs) == 1:
        return asgs[0]
    else:
        raise Exception("should find exactly one asg for cluster " + cluster_name)

def get_scale_clusters(dry_run=True):
    """
    Gets the details of clusters, how they are scaled based on desired capacity
    :param dry_run:
    """
    ecs_client = boto3.client('ecs')
    asg_client = boto3.client('autoscaling')
    clusters = ecs_client.list_clusters()
    for cluster_arn in clusters['clusterArns']:
        cluster_name = cluster_arn.partition('/')[2]
        logging.info('cluster = %s', cluster_name)
        asg = get_asg_for_cluster('ECS-' + cluster_name, asg_client)
        if asg:
            logging.info(' ASG = %s', asg)
            desc_asgs = asg_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg],
            )
            existing_desired_capacity = desc_asgs['AutoScalingGroups'][0]['DesiredCapacity']
            logging.info(' existingdesiredcapacity %s ' , existing_desired_capacity)
            tasks = ecs_client.list_tasks(
                cluster=cluster_name,
                )
        container_instances = get_container_instances(ecs_client, cluster_name, inst_state)
        if container_instances:
            current_instance_state, running_tasks_count = get_container_instances_state(container_instances)
            container_instance_resources = get_container_instance_resources(tasks, cluster_name, ecs_client, asg_client, running_tasks_count, existing_desired_capacity, container_instances)
            task_definition_resources = list_task_definition_resources(tasks, cluster_name, ecs_client, running_tasks_count, asg_client, existing_desired_capacity, container_instance_resources)
            highest_cpu, highest_memory, highest_memory_reservation = get_highest_cpu_memory(task_definition_resources, ecs_client, running_tasks_count, asg_client, container_instance_resources, existing_desired_capacity)
            get_desired_capacity(asg_client, container_instance_resources, existing_desired_capacity, running_tasks_count, highest_cpu, highest_memory, highest_memory_reservation, dry_run)
        return get_desired_capacity

def get_desired_capacity(asg_client, container_instance_resources, existing_desired_capacity, running_tasks_count, highest_cpu, highest_memory, highest_memory_reservation, dry_run):
    """
    Gets the desired capacity instance value in an Auto-Scaling Group
    param:
    param:
    """
    desired_capacity = get_asg_desired_instances(container_instance_resources, existing_desired_capacity, running_tasks_count, asg_client, highest_memory, highest_cpu, highest_memory_reservation)
    if desired_capacity != existing_desired_capacity:
        terminate_idle_instances(asg_client, container_instance_resources, running_tasks_count, dry_run=dry_run)
    list_low_capacity_instances(container_instance_resources, dry_run=dry_run)

    #Gives the list of the container instances running in the cluster.

def get_container_instances(ecs_client, cluster_name, inst_state):
    """
    Gets the details on container instaces running in a cluster
    :param ecs_client: an AWS ECS client
    :param cluster_name: the name of the cluster
    :return: container instance details
    """
    container_instances = ecs_client.list_container_instances(cluster=cluster_name)
    if not container_instances['containerInstanceArns']:
        logging.info('No container instances exist for cluster %s', cluster_name)
        return None
    container_instances = ecs_client.describe_container_instances(
        cluster=cluster_name,
        containerInstances=container_instances['containerInstanceArns']
    )
    return container_instances

def get_container_instances_state(container_instances):
    """
    Get the details of container instance state
    :param container_instances: gets the container instance in a cluster
    """
    current_instance_state = None
    for inst_state in container_instances['containerInstances']:
        current_instance_state = inst_state['status']
        logging.info(' current instance state %s', current_instance_state)
        get_running_tasks_count(container_inst)
        return current_instance_state

def get_running_tasks_count(container_inst):
    """
    Gets the value of running task running on the cluster
    :param container_instaces: gets the container instance in a cluster
    :return: container instance state and running task count
    """
    running_tasks_count = 0
    for running_tasks in container_instances['containerInstances']:
        running_tasks_count = running_tasks['runningTasksCount']
        logging.info('runningtaskscount %s', running_tasks_count)
    return running_tasks_count

def get_container_instance_resources(tasks, cluster_name, ecs_client, asg_client, running_tasks_count, existing_desired_capacity, container_instances):
    """
    Gets the details of the container instance resources
    :param tasks: give the details of the tasks on ecs cluster
    :param cluster_name: the name of the cluster
    :param ecs_client: an AWS ECS client
    :param asg_client: an AWS Auto-Scaling client
    :param running_task_count: give the details of how many tasks are running
    :param existing_desired_capacity:
    :param container_instances: gets the container instance in a cluster
    :return: container instance details
    """
    container_instance_resources = []
    for container_instance in container_instances['containerInstances']:
        container_instance_resource = {
            'instance_id': container_instance ['ec2InstanceId']
        }
        for registered_resource in container_instance ['registeredResources']:
            if registered_resource['name'] == 'CPU':
                container_instance_resource['cpu_registered'] = registered_resource['integerValue']
            if registered_resource['name'] == 'MEMORY':
                container_instance_resource['memory_registered'] = registered_resource['integerValue']
        for remaining_resource in container_instance ['remainingResources']:
            if remaining_resource['name'] == 'CPU':
                container_instance_resource['cpu_free'] = remaining_resource['integerValue']
            if remaining_resource['name'] == 'MEMORY':
                container_instance_resource['memory_free'] = remaining_resource['integerValue']
        cpu_registered = container_instance_resource['cpu_registered']
        memory_registered = container_instance_resource['memory_registered']
        cpu_free = container_instance_resource['cpu_free']
        memory_free = container_instance_resource['memory_free']
        cpu_used = cpu_registered - cpu_free
        memory_used = memory_registered - memory_free
        container_instance_resource['cpu_capacity'] = float(cpu_used) / cpu_registered
        container_instance_resource['memory_capacity'] = float(memory_used)/memory_registered
        container_instance_resources.append(container_instance_resource)
    logging.info('container instances %s', container_instance_resources)
    return container_instance_resources

    # Gives the list of task-definition details like memory and cpu.

def list_task_definition_resources(tasks, cluster_name, ecs_client, running_tasks_count, asg_client, existing_desired_capacity, container_instance_resources):
    """
    Gets the details of the container instance resources
    :param tasks: give the details of the tasks on ecs cluster
    :param cluster_name: the name of the cluster
    :param ecs_client: an AWS ECS client
    :param asg_client: an AWS Auto-Scaling client
    :param running_task_count: give the details of how many tasks are running
    :param existing_desired_capacity: gives the details of existing desired instances on an AutoScalingGroup
    :param container_instances: gets the container instance in a cluster
    :return: container instance details
    """
    task_definition_resources = {}
    for task in tasks['taskArns']:
        task_desc = ecs_client.describe_tasks(
           cluster=cluster_name,tasks=[task.rpartition('/')[2]]
        )
        task_defin = ecs_client.describe_task_definition(
            taskDefinition=task_desc['tasks'][0]['taskDefinitionArn']
        )
        memory = 0
        cpu = 0
        memory_reservation = 0
        for container_definition in task_defin['taskDefinition']['containerDefinitions']:
            cpu += container_definition['cpu']
            memory_reservation += container_definition['memoryReservation']
            memory += container_definition['memory']
        task_definition_resources[task_desc['tasks'][0]['taskDefinitionArn']] = {
            'memory': memory,
            'cpu': cpu,
            'memory_reservation': memory_reservation
        }
    return task_definition_resources

def get_highest_cpu_memory(task_definition_resources, ecs_client, running_tasks_count, asg_client, container_instance_resources, existing_desired_capacity):
    """
    Gets the details of the highest cpu, memory and memory reservation
    :param task_definition_resources: give the details of the tasks definition resource on ecs cluster
    :param ecs_client: an AWS ECS client
    :param asg_client: an AWS Auto-Scaling client
    :param running_task_count: give the details of how many tasks are running
    :param existing_desired_capacity: gives the details of existing desired instances on an AutoScalingGroup
    :param container_instances_resources: gets the container instance resources in a cluster
    :return: gives the values of highest cpu, higest memory and highest memory reservation
    """
    highest_cpu = 0
    highest_memory = 0
    highest_memory_reservation = 0
    for task_definition_arn in task_definition_resources:
        if task_definition_resources[task_definition_arn]['cpu'] > highest_cpu:
            highest_cpu = task_definition_resources[task_definition_arn]['cpu']
        if task_definition_resources[task_definition_arn]['memory_reservation'] > highest_memory_reservation:
            highest_memory_reservation = task_definition_resources[task_definition_arn]['memory_reservation']
        logging.info(' Highest CPU = %s' , highest_cpu)
        logging.info(' Highest MEMORY = %s', highest_memory)
        logging.info(' Highest Memory Reservation = %s', highest_memory_reservation)
    return highest_cpu, highest_memory, highest_memory_reservation


def get_asg_desired_instances(container_instance_resources, existing_desired_capacity, running_tasks_count, asg_client, highest_memory, highest_cpu, highest_memory_reservation):
    """
    Gets the details of the Autoscaling group desired instances
    :param container_instances_resources: gets the container instance resources in a cluster
    :param existing_desired_capacity: gives the details of existing desired instances on an AutoScalingGroup
    :param running_task_count: give the details of how many tasks are running
    :param asg_client: an AWS Auto-Scaling client
    :param highest_cpu: gives the details of the highest cpu
    :param highest_memory: gives the details of the highest memory
    :param highest_memory_reservation: gives the details of the highest memory reservation
    :return: gives the value of desired capacity
    """
    add_instances = True
    desired_capacity = existing_desired_capacity
    for container_instance_resource in container_instance_resources:
        logging.info('    %s', container_instance_resource)
        if (highest_memory <= container_instance_resource['memory_free']) and
           (highest_cpu <= container_instance_resource['cpu_free']) and
           (highest_memory_reservation <= container_instance_resource['memory_free']):
            desired_capacity += 1
        logging.info('The new desired capacity %s', desired_capacity)
    return desired_capacity

# now we have the instances which are running on low capacity set them to draining

def list_low_capacity_instances(container_instance_resources, dry_run):
    """
    Gets the list of low capacity instances
    :param container_instances_resources: gets the container instance resources in a cluster
    :param dry_run:
    """
    low_capacity_instances = []
    for container_instance_resource in container_instance_resources:
        logging.info('cpu capaccity %s', container_instance_resource['cpu_capacity'])
        logging.info('memory capacity %s', container_instance_resource['memory_capacity'])
        if container_instance_resource['memory_capacity'] <= 0.5 and container_instance_resource['cpu_capacity'] <= 0.5:
           low_capacity_instances.append(container_instance_resource['instance_id'])
    # logging.info('low capacity instances %s', low_capacity_instances)
    if len(low_capacity_instances) >= 2:
        instances_to_drain_count = int(len(low_capacity_instances) / 2)
        if dry_run:
            logging.info('terminating low capacity instances %s', low_capacity_instances[:instances_to_drain_count])
        else:
            for instance_id in low_capacity_instances[:instances_to_drain_count]:
                response = client.update_container_instances_state(
                    cluster=cluster_name,
                    containerInstances=low_capacity_instances[:instances_to_drain_count],
                    status='DRAINING'
                )

def terminate_idle_instances(asg_client, container_instance_resources, running_tasks_count, dry_run):
    """
    terminates the idle instances in the Autoscaling group
    :param container_instances_resources: gets the container instance resources in a cluster
    :param asg_client: an AWS Auto-Scaling client
    :param running_task_count: give the details of how many tasks are running
    :param dry_run:
    """
    for container_instance_resource in container_instance_resources:
        if running_tasks_count == 0:
            if dry_run:
                logging.info('terminating instance %s', container_instance_resource['instance_id'])
            else:
                asg_client.terminate_instance_in_auto_scaling_group(
                    InstanceId=container_instance_resource['instance_id'],
                    ShouldDecrementDesiredCapacity=True
                )

def get_parameters():
    """
    Gets all the parameters from EC2 Parameter Store
    :return: a dictionary of parameters
    """
    ssm_client = boto3.client('ssm')
    response = ssm_client.get_parameters_by_path(Path='/devops/devops-service-scaler/')
    parameters = {}
    for parameter in response['Parameters']:
        key = parameter['Name'].rpartition('/')[2]
        parameters[key] = parameter['Value']
    logging.info('parameters: %s', parameters)
    return parameters


def set_up_logging_config(logging_type='console'):
    """
    Sets up the logging configuration
    :param logging_type: if 'console', output is streamed to stdout. if 'logzio', output is sent to Logz.io in addition to being streamed to stdout.
    :return: None
    """
    if logging_type == 'logzio':
        parameters = get_parameters()
        logging_config = {
            'version': 1,
            # 'disable_existing_loggers': False,
            'formatters': {
                'verbose': {
                    'format': '%(asctime)s %(levelname)-5s %(filename)s:%(lineno)d %(message)s'
                },
                'logzioFormat': {
                    'format': '{"additional_field": "value"}'
                }
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': parameters['log_level'],
                    'formatter': 'verbose'
                },
                'logzio': {
                    'class': 'logzio.handler.LogzioHandler',
                    'level': parameters['log_level'],
                    'formatter': 'logzioFormat',
                    'token': parameters['logzio_token'],
                    'logzio_type': "devops-service-scaler",
                    'logs_drain_timeout': 3,
                    'url': 'https://listener.logz.io:8071',
                    'debug': True
                },
            },
            'root': {
                'handlers': ['console', 'logzio'],
                'level': parameters['log_level']
            }
        }
    elif logging_type == 'console':
        logging_config = {
            'version': 1,
            'formatters': {
                'f': {
                    'format': '%(asctime)s %(levelname)-5s %(filename)s:%(lineno)d %(message)s'
                }
            },
            'handlers': {
                'h': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'f',
                    'level': 'INFO'
                }
            },
            'root': {
                'handlers': ['h'],
                'level': 'INFO'
            }
        }
    else:
        raise IOError('Invalid logging type: %s' % logging_type)

    dictConfig(logging_config)


def lambda_handler(event, context):
    """
    Entry point intended for AWS Lambda execution
    :param event: the event which triggered this execution
    :param context: the context of this execution
    :return: None
    """
    set_up_logging_config('logzio')
    get_scale_clusters()

def main():
    """
    Entry point intended for local execution
    :return: None
    """
    set_up_logging_config()

    from argparse import ArgumentParser
    parser = ArgumentParser(description='ECS SCALING TOOL to scale in and scale out the desired capacity of an Auto-Scaling Group')
    parser.add_argument('--dry-run', dest='dry_run', default=True, type=bool, help='the name of the Auto-Scaling Group which is scaled based on the memory and cpu usage')

    args = parser.parse_args()
    asg_client = boto3.client('autoscaling')
    print  get_asg_for_cluster('ECS-' + cluster_name, asg_client)

    #get_scale_clusters(args.dry_run)


if __name__ == '__main__':
    main()
