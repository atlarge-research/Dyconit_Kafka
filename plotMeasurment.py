import matplotlib.pyplot as plt
import os
import subprocess
import argparse
import numpy as np

def ensure_path_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Path '{path}' created.")
    else:
        print(f"Path '{path}' already exists.")

def copy_docker_files(name, prodCount):
    try:
        ensure_path_exists(f'./measurments/{name}/')
        # Run the docker cp command
        result = subprocess.run(
            ["docker", "cp", "manager:/app/m_reports/", f"./measurments/{name}/"],
            check=True,   
            capture_output=True,   
            text=True   
        )
        for i in range(prodCount):
            result = subprocess.run(
                ["docker", "cp", f"producer_{i+1}:/app/p{i+1}_reports/", f"./measurments/{name}"],
                check=True,   
                capture_output=True,   
                text=True   
            )
        for i in range(4):
            result = subprocess.run(
                ["docker", "cp", f"consumer_{i+1}:/app/c{i+1}_reports/", f"./measurments/{name}"],
                check=True,   
                capture_output=True,   
                text=True   
            )
        ensure_path_exists(f"./measurments/{name}/confings")
        for i in range(4):
            result = subprocess.run(
                ["docker", "cp", f"consumer_{i+1}:app/appsettings.json", f"./measurments/{name}/confings/c{i+1}"],
                check=True,   
                capture_output=True,   
                text=True   
            )
        for i in range(prodCount):
            result = subprocess.run(
                ["docker", "cp", f"producer_{i+1}:app/appsettings.json", f"./measurments/{name}/confings/p{i+1}"],
                check=True,   
                capture_output=True,   
                text=True   
            )
        result = subprocess.run(
            ["docker", "cp", f"manager:app/appsettings.json", f"./measurments/{name}/confings/m"],
            check=True,   
            capture_output=True,   
            text=True   
            )
        print("Command output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error occurred:", e)
        print("Command output:", e.stdout)
        print("Command error output:", e.stderr)


def read_numbers_from_file(filename):
    numbers_array = []
    with open(filename, 'r') as file:
        idx = -1
        for line in file:
            try:
                number = float(line)
                numbers_array[idx].append(number)
            except ValueError:
                numbers_array.append([])
                idx += 1 
    
    return numbers_array

def normalize(lists):
    last_array = lists[-1]
    last_array_len = len(last_array)
    last_array_last_value = last_array[-1]
    for i in range(len(lists)):
        lists[i] = [
            min(val, last_array[idx] if idx < last_array_len else last_array_last_value)
            for idx, val in enumerate(lists[i])
        ]
        
def combine_plots(lists, labels, name, yaxis):
    fig, ax = plt.subplots()

    for lst, label in zip(lists, labels):
        x = np.arange(0, len(lst) * 5, 5)  # Generate x-values based on 5-second intervals, modify manualy for different report rates
        ax.plot(x, lst, label=label)

    ax.set_xlabel('Time [seconds]')
    ax.set_ylabel(yaxis)
    ax.set_title(name)

    ax.grid(True)
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

    plt.tight_layout()

def get_horizontal_diff(consumer, topic, index):
    if consumer[index] == 0:
        return 0
    tmpIdx = index
    steps = 0
    tmpIdx = min(tmpIdx, len(topic) - 2)
    if topic[tmpIdx] == consumer[index]:
        return 0
    while topic[tmpIdx] > consumer[index]:
        tmpIdx -= 1
        steps += 1
    return (steps + ( 1 - ((consumer[index] - topic[tmpIdx]) / (topic[tmpIdx + 1] - topic[tmpIdx])))) * 5

def get_stale_error(data, con):
    sem = []
    for i in range(con):
        se = []
        for j in range(len(data[i])):
            se.append(get_horizontal_diff(data[i], data[con], j))
        sem.append(se)
    return sem

def get_throughput(data):
    result = [] 
    for list in data:
        throughput = []
        for i in range(1, len(list)):
            throughput.append((list[i] - list[i-1]) / 5)
        result.append(throughput)
    return result

def get_num_error(data, con):
    sem = []
    for i in range(con):
        se = []
        for j in range(len(data[i])):
            tmp = min(j, len(data[con]) - 1)
            se.append(max(data[con][tmp] - data[i][j],0))
        sem.append(se)
    return sem

def writeErrors(name, array, num_to_plot, topic):
    idx = 1
    ensure_path_exists(name + '/se/' + topic)
    for list in get_stale_error(array, num_to_plot):
        with open(f'{name}/se/{topic}/{idx}.txt', 'w') as file:
            for ele in list:
                file.write(f"{ele}\n")
        idx += 1
    
    idx = 1
    ensure_path_exists(name + '/ne/' + topic)
    for list in get_num_error(array, num_to_plot):
        with open(f'{name}/ne/{topic}/{idx}.txt', 'w') as file:
            for ele in list:
                file.write(f"{ele}\n")
        idx += 1   

def writeThroughput(name, array, topic):
    idx = 1
    ensure_path_exists(f'{name[:-1]}/{name}/through/{topic}/')
    for list in array:
        with open(f'{name[:-1]}/{name}/through/{topic}/{idx}.txt', 'w') as file:
            for ele in list:
                file.write(f"{ele}\n")
        idx += 1

parser = argparse.ArgumentParser(description="take measurments")
parser.add_argument('name', type=str, help='name of measurment')
parser.add_argument('--skip', action='store_true', help="skip the copy call -> plot/save already copied data")
parser.add_argument('--no_write', action='store_true', help="do not store resutls")
parser.add_argument('--no_plot', action='store_true', help="dont plot")
parser.add_argument('--number', type=int, default=4, help='number of consumer to plot, only change if --no_write')
parser.add_argument('--prod', type=int, default=3, help='number of consumer to plot, only change if --no_write')

args = parser.parse_args()

if not args.skip:
    copy_docker_files(args.name, args.prod)

c_reports = []

for i in range(4):
    path_cpu = f'measurments/{args.name}/c{i+1}_reports/cpu.txt'
    with open(path_cpu, 'r') as f:
        content = [float(x) for x in f.read().split()]
        c_reports.append(content)


p_reports = []
producers = f'measurments/{args.name}/m_reports/producers'

files = os.listdir(producers)


for file in files:
    with open(os.path.join(producers, file), 'r') as f:
        content = [float(x) for x in f.read().split()]
        p_reports.append(content)

names = []
if args.number == 4:
    names = ['Dyconit consumer 1','Dyconit consumer 2','Kafka consumer 1', 'Kafka consumer 2', 'Overall Produced Topic Messages']
if args.number == 2:
    names = ['Dyconit consumer', 'Kafka consumer', 'Overall Produced Topic Messages']


combine_plots(c_reports, names, 'Consumer CPU usage', 'CPU usage [%]')
combine_plots(p_reports, ['High Priority Topic','Medium Priority Topic','Low Priority Topic'], 'Topic Production Rates', 'Production rate [messages per second]')

msg_tn = []
msg_tp = []
msg_tl = []

nep = []
nen = []
nel = []

sep = []
sen = []
sel = []


for i in range( (int)(0 + (2 - args.number / 2)), (int)(4 - (2 - args.number / 2))):
    path_normal = f'measurments/{args.name}/c{i+1}_reports/msgcount_topic_normal.txt'
    path_prio = f'measurments/{args.name}/c{i+1}_reports/msgcount_topic_priority.txt'
    path_low = f'measurments/{args.name}/c{i+1}_reports/msgcount_topic_low.txt'
        
    with open(path_normal, 'r') as f:
        content = [int(x) for x in f.read().split()]
        msg_tn.append(content)
    
    with open(path_prio, 'r') as f:
        content = [int(x) for x in f.read().split()]
        msg_tp.append(content)
        
    if (args.prod > 2):
        with open(path_low, 'r') as f:
            content = [int(x) for x in f.read().split()]
            msg_tl.append(content)        

path_prio = f'measurments/{args.name}/p1_reports/msgcount.txt'
path_normal = f'measurments/{args.name}/p2_reports/msgcount.txt'
path_low = f'measurments/{args.name}/p3_reports/msgcount.txt'
    
with open(path_prio, 'r') as f:
    content = [int(x) for x in f.read().split()]
    msg_tp.append(content)

    
with open(path_normal, 'r') as f:
    content = [int(x) for x in f.read().split()]
    msg_tn.append(content)
if (args.prod > 2):
    with open(path_low, 'r') as f:
        content = [int(x) for x in f.read().split()]
        msg_tl.append(content)

normalize(msg_tn)
if (args.prod > 2):
    normalize(msg_tl)
normalize(msg_tp)


combine_plots(msg_tp,names, 'High Priority Topic Messages Consumed', 'Message count')
combine_plots(msg_tn,names, 'Medium Priority Topic Messages Consumed', 'Message count')
combine_plots(get_throughput(msg_tp),names, 'High Priority Topic Message Throughput', 'Throughput [messages per second]')
combine_plots(get_throughput(msg_tn),names, 'Medium Priority Topic Message Throughput', 'Throughput [messages per second]')
if (args.prod > 2):
    combine_plots(msg_tl,names, 'Low Priority Topic Messages Consumed', 'Message count')
    combine_plots(get_throughput(msg_tl),names, 'Low Priority Topic Message Throughput', 'Throughput [messages per second]')
    
if not args.no_write:
    writeThroughput(args.name, get_throughput(msg_tl), 'tl')
    writeThroughput(args.name, get_throughput(msg_tn), 'tn')
    writeThroughput(args.name, get_throughput(msg_tp), 'tp')


combine_plots(get_stale_error(msg_tp, args.number),names, 'High Priority Topic Staleness Error', 'Staleness Error [seconds]')
combine_plots(get_num_error(msg_tp, args.number),names, 'High Priority Topic Numerical Error', 'Numerical Error [seconds]')

combine_plots(get_stale_error(msg_tn, args.number),names, 'Medium Priority Topic Staleness Error', 'Staleness Error [seconds]')
combine_plots(get_num_error(msg_tn, args.number),names, 'Medium Priority Topic Numerical Error', 'Numerical Error [seconds]')

combine_plots(get_stale_error(msg_tl, args.number),names, 'Low Priority Topic Staleness Error', 'Staleness Error [seconds]')
combine_plots(get_num_error(msg_tl, args.number),names, 'Low Priority Topic Numerical Error', 'Numerical Error [seconds]')


if not args.no_write:
    
    ensure_path_exists(args.name[:-1] + '/' + args.name + '/errors/' + '/se')
    ensure_path_exists(args.name[:-1] + '/' + args.name + '/errors/' + '/ne')
    writeErrors(args.name[:-1] + '/' + args.name + '/errors/', msg_tp, args.number, 'tp')
    writeErrors(args.name[:-1] + '/' + args.name + '/errors/', msg_tn, args.number, 'tn')
    writeErrors(args.name[:-1] + '/' + args.name + '/errors/', msg_tl, args.number, 'tl')

if not args.no_plot:
    plt.show()