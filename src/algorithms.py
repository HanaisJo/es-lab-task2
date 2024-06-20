"""
This module contains the scheduling algorithms used in the scheduling API.

It provides implementations for both Least Deadline First (LDF) and Earliest Deadline First (EDF) scheduling strategies, applicable in single-core and multi-core processor environments. Functions within are designed to be called with specific application and platform data structures.

Functions:
- ldf_singlecore: Schedules tasks on a single-core processor using LDF.
- edf_singlecore: Schedules tasks on a single-core processor using EDF.
- rms_singlecore: Schedules tasks on a single-core processor using RMS.
- ll_singlecore: Schedules tasks on a single-core processor using LL.
- ldf_multicore: Schedules tasks on multiple cores using LDF.
- edf_multicore: Schedules tasks on multiple cores using EDF.
"""

__author__ = "Priya Nagar"
__version__ = "1.0.0"

from collections import defaultdict, deque
import networkx as nx

# just an eample for the structure of the schedule to be returned and to check the frontend and backend connection
example_schedule = [
    {
        "task_id": "3",
        "node_id": 0,
        "end_time": 20,
        "deadline": 256,
        "start_time": 0,
    },
    {
        "task_id": "2",
        "node_id": 0,
        "end_time": 40,
        "deadline": 300,
        "start_time": 20,
    },
    {
        "task_id": "1",
        "node_id": 0,
        "end_time": 60,
        "deadline": 250,
        "start_time": 40,
    },
    {
        "task_id": "0",
        "node_id": 0,
        "end_time": 80,
        "deadline": 250,
        "start_time": 60,
    },
]


def ldf_single_node(application_data):
    """
    Schedule jobs on a single node using the Latest Deadline First (LDF) strategy.

    This function schedules jobs based on their latest deadlines after sorting them and considering dependencies through a directed graph representation.

    .. todo:: Implement Latest Dealine First Scheduling (LDF) algorithm for single compute node.


    Args:
        application_data (dict): Contains jobs and messages that indicate dependencies among jobs.

    Returns:
        list of dict: Scheduling results with each job's details, including execution time, node assignment,
                      and start/end times relative to other jobs.
    """
    tasks = application_data["tasks"]
    messages = application_data["messages"]
    graph = defaultdict(list)
    in_degree = {task['id']: 0 for task in tasks}

    for message in messages:
        sender = message['sender']
        receiver = message['receiver']
        graph[sender].append(receiver)
        in_degree[receiver] += 1

    tasks.sort(key=lambda task: task['deadline'], reverse=True)
    current_time = 0
    ldf_schedule=[]
    ready_queue = deque([task for task in tasks if in_degree[task['id']] == 0])

    # Sort the ready queue by latest deadlines
    ready_queue = deque(sorted(ready_queue, key=lambda task: task['deadline'], reverse=True))
    
    while ready_queue:
        task = ready_queue.popleft()
        task_id = task['id']
        start_time = current_time
        end_time = start_time + task['wcet']
        
        ldf_schedule.append({
            'task_id': task_id,
            'node_id': 1,
            'start_time': start_time,
            'end_time': end_time,
            'deadline': task['deadline']
        })
        
        current_time = end_time
        
        # Reduce the in-degree of the dependent tasks
        for dependent in graph[task_id]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                # Find the job details for the dependent task
                dependent_task = next(t for t in tasks if t['id'] == dependent)
                ready_queue.append(dependent_task)
        
        # Sort the ready queue again by latest deadlines
        ready_queue = deque(sorted(ready_queue, key=lambda task: task['deadline'], reverse=True))
   
    return {"schedule": ldf_schedule, "name": "LDF Single Node"} #return {"schedule": example_schedule}


def edf_single_node(application_data):
    """
    Schedule jobs on single node using the Earliest Deadline First (EDF) strategy.

    This function processes application data to schedule jobs based on the earliest
    deadlines. It builds a dependency graph and schedules accordingly, ensuring that jobs with no predecessors are
    scheduled first, and subsequent jobs are scheduled based on the minimum deadline of available nodes.

    .. todo:: Implement Earliest Deadline First Scheduling (EDF) algorithm for single compute node.

    Args:
        application_data (dict): Job data including dependencies represented by messages between jobs.

    Returns:
        list of dict: Contains the scheduled job details, each entry detailing the node assigned, start and end times,
                      and the job's deadline.
    """
    tasks = application_data["tasks"]
    messages = application_data["messages"]
    graph = defaultdict(list)
    in_degree = {task['id']: 0 for task in tasks}

    for message in messages:
        sender = message['sender']
        receiver = message['receiver']
        graph[sender].append(receiver)
        in_degree[receiver] += 1

    tasks.sort(key=lambda task: task['deadline'])
    
    current_time = 0
    edf_schedule = []
    ready_queue = deque([task for task in tasks if in_degree[task['id']] == 0])

    # Sort the ready queue by deadlines
    ready_queue = deque(sorted(ready_queue, key=lambda task: task['deadline']))
    
    while ready_queue:
        task = ready_queue.popleft()
        task_id = task['id']
        start_time = current_time
        end_time = start_time + task['wcet']
        
        edf_schedule.append({
            'task_id': task_id,
            'node_id': 1,
            'start_time': start_time,
            'end_time': end_time,
            'deadline': task['deadline']
        })
        
        current_time = end_time
        
        # Reduce the in-degree of the dependent tasks
        for dependent in graph[task_id]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                # Find the job details for the dependent task
                dependent_task = next(t for t in tasks if t['id'] == dependent)
                ready_queue.append(dependent_task)
        
        # Sort the ready queue again by deadlines
        ready_queue = deque(sorted(ready_queue, key=lambda task: task['deadline']))
        


    return {"schedule": edf_schedule, "name": "EDF Single Node"}


def ll_multinode(application_data, platform_data):
    """
    Schedule jobs on a distributed system with multiple compute nodes using the Least Laxity (LL) strategy.
    This function schedules jobs based on their laxity, with the job having the least laxity being scheduled first.

    .. todo:: Implement Least Laxity (LL) algorithm to schedule jobs on multiple node in a distributed system.

    Args:
        application_data (dict): Job data including dependencies represented by messages between jobs.

    Returns:
        list of dict: Contains the scheduled job details, each entry detailing the node assigned, start and end times,
                      and the job's deadline.

    """
    """
 # Initialize nodes as lists
    tasks = application_data
    nodes = platform_data
    # Initialize nodes as lists to hold scheduled tasks
    node_queues = {node['id']: [] for node in nodes}
    current_time = 0
    all_tasks_completed = False

    # Initialize task remaining execution time
    for task in tasks:
        task['remaining_wcet'] = task['wcet']

    while not all_tasks_completed:
        # Calculate laxity and sort tasks by laxity (ascending order)
        tasks.sort(key=lambda t: t['deadline'] - t['remaining_wcet'] - current_time)

        # Schedule tasks on nodes based on LL algorithm
        for task in tasks:
            scheduled = False
            for node_id in node_queues:
                if task['remaining_wcet'] > 0:
                    node_queues[node_id].append(task)
                    scheduled = True
                    break
            if scheduled:
                break

        # Execute one unit of time for tasks on each node
        for node_id in node_queues:
            if node_queues[node_id]:
                node_queues[node_id][0]['remaining_wcet'] -= 1

        # Check if all tasks are completed
        all_tasks_completed = all(task['remaining_wcet'] == 0 for task in tasks)

        current_time += 1

    # Gather the schedule for visualization
    schedule = []
    for node_id, node_queue in node_queues.items():
        schedule.append({
            'node': node_id,
            'tasks': [task['id'] for task in node_queue]
        })

"""
    return {"schedule": example_schedule, "name": "LL Multi Node"}


def ldf_multinode(application_data, platform_data):
    """
    Schedule jobs on a distributed system with multiple compute nodes using the Latest Deadline First(LDF) strategy.
    This function schedules jobs based on their periods and deadlines, with the shortest period job being scheduled first.

    .. todo:: Implement Latest Deadline First(LDF) algorithm to schedule jobs on multiple nodes in a distributed system.

    Args:
        application_data (dict): Job data including dependencies represented by messages between jobs.
        platform_data (dict): Contains information about the platform, nodes and their types, the links between the nodes and the associated link delay.

    Returns:
        list of dict: Contains the scheduled job details, each entry detailing the node assigned, start and end times,
                      and the job's deadline.

    """

    return {"schedule": example_schedule, "name": "LDF Multi Node"}

def find_link_delay(links, start_node, end_node):
    for link in links:
        if link['start_node'] == start_node and link['end_node'] == end_node:
            return link['link_delay']
    return 0

def edf_multinode(application_data, platform_data):
    """
    Schedule jobs on a distributed system with multiple compute nodes using the Earliest Deadline First (EDF) strategy.
    This function processes application data to schedule jobs based on the earliest
    deadlines.

    .. todo:: Implement Earliest Deadline First(EDF) algorithm to schedule jobs on multiple nodes in a distributed system.

    Args:
        application_data (dict): Job data including dependencies represented by messages between jobs.
        platform_data (dict): Contains information about the platform, nodes and their types, the links between the nodes and the associated link delay.

    Returns:
        list of dict: Contains the scheduled job details, each entry detailing the node assigned, start and end times,
                      and the job's deadline.

    """
    
    tasks = application_data["tasks"]
    messages = application_data["messages"]
    nodes = platform_data['nodes']
    links = platform_data['links']
    graph = defaultdict(list)
    in_degree = {task['id']: 0 for task in tasks}

    for message in messages:
        sender = message['sender']
        receiver = message['receiver']
        graph[sender].append(receiver)
        in_degree[receiver] += 1
    # Sort tasks based on their deadlines initially
    tasks.sort(key=lambda task: task['deadline'])
    
    # Initialize the scheduling data structures
    node_time = {node['id']: 0 for node in nodes}
    schedule = []
    ready_queue = deque([task for task in tasks if in_degree[task['id']] == 0])

    # Sort the ready queue by deadlines
    ready_queue = deque(sorted(ready_queue, key=lambda task: task['deadline']))
    
    while ready_queue:
        task = ready_queue.popleft()
        task_id = task['id']
        
        # Assign the task to the node that will be free the earliest
        node_id = min(node_time, key=node_time.get)
        start_time = node_time[node_id]
        end_time = start_time + task['wcet']
        
        schedule.append({
            'task_id': task_id,
            'node_id': node_id,
            'start_time': start_time,
            'end_time': end_time,
            'deadline': task['deadline']
        })
        
        node_time[node_id] = end_time
        
        # Reduce the in-degree of the dependent tasks
        for dependent in graph[task_id]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                # Find the job details for the dependent task
                dependent_task = next(t for t in tasks if t['id'] == dependent)
                
                # Calculate link delay if tasks are on different nodes
                if dependent_task.get('node_id') and dependent_task['node_id'] != node_id:
                    link_delay = find_link_delay(links, node_id, dependent_task['node_id'])
                    node_time[dependent_task['node_id']] = max(node_time[dependent_task['node_id']], node_time[node_id] + link_delay)
                
                ready_queue.append(dependent_task)
        
        # Sort the ready queue again by deadlines
        ready_queue = deque(sorted(ready_queue, key=lambda task: task['deadline']))

    return {"schedule": schedule, "name": "EDF Multi Node"}
