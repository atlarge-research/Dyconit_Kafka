import argparse
import os
import matplotlib.pyplot as plt
import numpy as np

"NAMING SCHEME FOR MEASURMENTS:"
"choose a name and add a single digit number"
"for example, 'first1' and 'first2' but not 'first12'"

def ensure_path_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Path '{path}' created.")
    else:
        print(f"Path '{path}' already exists.")

def saveData(list, name):
    global experiment
    ensure_path_exists(experiment.replace('/','') + '_res/' + f'{name}.txt', 'a')
    with open(experiment.replace('/','') + '_res/' + f'{name}.txt', 'a') as file:
        for ele in list:
            file.write(f"{ele}\n")
        file.write(f"\n")
        
def saveData2d(data, name):
        for list in data:
           saveData(list, name)
def compareError(data1, data2, title, yaxis, xlabels, topicIdx, colors):
    type = []
    content1 = []
    content2 = []
    for j in range(len(data1)):
        if j % 3 != topicIdx:
            continue
        content1 += data1[j]
        content2 += data2[j]
    type.append(content1)
    type.append(content2)
    plotbox(type, title, yaxis, xlabels, 'Consumer', colors = colors)
    
def plotError(data, title, yaxis, xlabels, write):
    type = []
    for i in range(len(topicNames)):
        content = []
        for j in range(len(data)):
            if j % len(topicNames) != i:
                continue
            content += data[j]
        type.append(content)
    if write:
        saveData2d(data, title.replace(" ", ""))
    plotbox(type, title, yaxis, xlabels, 'Topic')

def plotbox(data, title, yaxis, xlabels, xlabel, topbar = 1.02, colors = ['lightblue', 'lightcoral', 'lightgreen']):
    bplot = plt.boxplot(data, patch_artist=True, showmeans=True, meanline=True, boxprops=dict(alpha=1),
)

    # Add labels and title
    plt.xlabel(xlabel)
    plt.ylabel(yaxis)
    plt.title(title)
    plt.xticks(ticks=range(1, len(xlabels) + 1), labels=xlabels)

    for median in bplot['medians']:
        median.set_visible(False)
        
    for mean in bplot['means']:
        mean.set(color='purple', linewidth=2)

    # Customize colors
    for patch, color in zip(bplot['boxes'], colors):
        patch.set_facecolor(color)

    plt.ylim(bottom=0)
    plt.ylim(0, np.max(data) * topbar)

    # Add grid
    plt.grid(True)

# base = 'backup_'
base = ''

parser = argparse.ArgumentParser(description="take measurments")
parser.add_argument('--write', action='store_true', help="write error vals.")
parser.add_argument('name', type=str, help='name of experiment')
args = parser.parse_args()

experiment = args.name + '/'


directory = base + experiment
topicNames = ['High priority', 'Medium priority', 'Low priority']

d_avg_ne = []
d_avg_se = []

d_max_ne = []
d_max_se = []

k_avg_ne = []
k_avg_se = []

k_max_ne = []
k_max_se = []

topics =['tp', 'tn', 'tl']



for dir in os.listdir(directory):
    filepath = directory + '/' + dir + '/errors/'
    num_err_path = os.path.join(filepath, 'ne')
    ste_err_path = os.path.join(filepath, 'se')
    
    for topic in topics:
        d_avg_ne_topic = []
        d_avg_se_topic = []

        d_max_ne_topic = []
        d_max_se_topic = []

        k_avg_ne_topic = []
        k_avg_se_topic = []

        k_max_ne_topic = []
        k_max_se_topic = []
        for i in range(2):
            with open(num_err_path + f'/{topic}/{i+1}.txt', 'r') as f:
                content = [int(x) for x in f.read().split()]
                d_avg_ne_topic.append(np.mean(content))
                d_max_ne_topic.append(np.max(content))
            with open(ste_err_path + f'/{topic}/{i+1}.txt', 'r') as f:
                content = [float(x) for x in f.read().split()]
                d_avg_se_topic.append(np.mean(content))
                d_max_se_topic.append(np.max(content))
        
        for i in range(2):
            with open(num_err_path + f'/{topic}/{4 - i}.txt', 'r') as f:
                content = [int(x) for x in f.read().split()]
                k_avg_ne_topic.append(np.mean(content))
                k_max_ne_topic.append(np.max(content))
            with open(ste_err_path + f'/{topic}/{4 - i}.txt', 'r') as f:
                content = [float(x) for x in f.read().split()]
                k_avg_se_topic.append(np.mean(content))
                k_max_se_topic.append(np.max(content))

        d_avg_ne.append(d_avg_ne_topic)
        d_avg_se.append(d_avg_se_topic)

        d_max_ne.append(d_max_ne_topic)
        d_max_se.append(d_max_se_topic)

        k_avg_ne.append(k_avg_ne_topic)
        k_avg_se.append(k_avg_se_topic)

        k_max_ne.append(k_max_ne_topic)
        k_max_se.append(k_max_se_topic)        

colors = ['lightblue', 'lightcoral', 'lightgreen', 'blue', 'red', 'green']


########################################
# # plotting error boxes
compareError(d_avg_ne, k_avg_ne, 'Consumer Numerical Error for medium priority topic', 'Messages', ['Dyconit', 'Kafka'], 1, ['lightcoral', 'lightcoral', 'lightcoral'])
compareError(d_avg_ne, k_avg_ne, 'Consumer Numerical Error for high priority topic', 'Messages', ['Dyconit', 'Kafka'], 0, ['lightblue', 'lightblue', 'lightblue'])
compareError(d_avg_ne, k_avg_ne, 'Average consumer numerical error topic', 'Messages', ['Dyconit', 'Kafka'], 2, ['lightgreen', 'lightgreen', 'lightgreen'])
legend_labels = ['topic 1 average', 'topic 2 average', 'topic 3 average', 'topic 1 max', 'topic 2 max', 'topic 3 max']
legend_patches = [plt.Line2D([0], [0], color='w', marker='s', markersize=10, markerfacecolor=color, alpha=1) for color in colors]
plt.legend(legend_patches, legend_labels, loc='upper right')
compareError(d_max_ne, k_max_ne, 'Consumer Numerical Error for medium priority topic', 'Messages', ['Dyconit', 'Kafka'], 1, ['red', 'red', 'red'])
compareError(d_max_ne, k_max_ne, 'Consumer Numerical Error for high priority topic', 'Messages', ['Dyconit', 'Kafka'], 0, ['blue', 'blue', 'blue'])
compareError(d_max_ne, k_max_ne, 'Consumer Numerical Error', 'Messages', ['Dyconit', 'Kafka'], 2, ['green', 'green', 'green'])
plt.show()

compareError(d_avg_se, k_avg_se, 'Consumer Staleness Error for medium priority topic', 'Messages', ['Dyconit', 'Kafka'], 1, ['lightcoral', 'lightcoral', 'lightcoral'])
compareError(d_avg_se, k_avg_se, 'Consumer Staleness Error for high priority topic', 'Messages', ['Dyconit', 'Kafka'], 0, ['lightblue', 'lightblue', 'lightblue'])
compareError(d_avg_se, k_avg_se, 'Average cStalenessumerical error topic', 'Messages', ['Dyconit', 'Kafka'], 2, ['lightgreen', 'lightgreen', 'lightgreen'])
compareError(d_max_se, k_max_se, 'Consumer Staleness Error for medium priority topic', 'Messages', ['Dyconit', 'Kafka'], 1, ['red', 'red', 'red'])
compareError(d_max_se, k_max_se, 'Consumer Staleness Error for high priority topic', 'Messages', ['Dyconit', 'Kafka'], 0, ['blue', 'blue', 'blue'])
compareError(d_max_se, k_max_se, 'Consumer Staleness Error', 'Time [seconds]', ['Dyconit', 'Kafka'], 2, ['green', 'green', 'green'])
legend_labels = ['topic 1 average', 'topic 2 average', 'topic 3 average', 'topic 1 max', 'topic 2 max', 'topic 3 max']
legend_patches = [plt.Line2D([0], [0], color='w', marker='s', markersize=10, markerfacecolor=color, alpha=1) for color in colors]
plt.legend(legend_patches, legend_labels, loc='upper right')
plt.show()

# plotting error boxes
###########################################

########################################
# plotting cpu
cpu_us_avg = []
cpu_max = []
d_avg = []
k_avg = []
d_max = []
k_max = []


for dir in os.listdir('measurments/'):
    if dir[:-1] != experiment[:-1]:
        continue
    path = f'measurments/' + dir
    for j in range(1,3):
        f_path = path + f'/c{j}_reports/cpu.txt'
        with open(f_path, 'r') as f:
            content = [float(x) for x in f.read().split()]
            d_avg.append(np.mean(content))
            d_max.append(np.max(content))
    for j in range(3,5):
        f_path = path + f'/c{j}_reports/cpu.txt'
        with open(f_path, 'r') as f:
            content = [float(x) for x in f.read().split()]
            k_avg.append(np.mean(content))
            k_max.append(np.max(content))

cpu_us_avg.append(d_avg)
cpu_us_avg.append(k_avg)
cpu_max.append(d_max)
cpu_max.append(k_max)
if args.write:
    saveData(d_avg, f'dyconit_cpu_avg')
    saveData(k_avg, f'kafka_cpu_avg')
    saveData(k_max, f'kafka_cpu_max')
    saveData(d_max, f'dyconit_cpu_max')
plotbox(cpu_us_avg, 'Average CPU usage', 'CPU usage [%]',['Dyconit', 'Kafka'], 'Consumer')
# plt.show()
plotbox(cpu_max, 'Consumer CPU usage', 'CPU usage [%]',['Dyconit', 'Kafka'], 'Consumer')
plt.show()
# plotting cpu
###########################################

########################################
# plotting throughput
overall = []   
d_avg = []
k_avg = []  

temp = [0,0,0,0]


for dir in os.listdir(directory):
    filepath = directory + '/' + dir + '/through/'
    for topic in topics:
        path = filepath + topic
        for j in range(1,3):
            f_path = path + f'/{j}.txt'
            with open(f_path, 'r') as f:
                content = [float(x) for x in f.read().split()]
                temp[j-1] += np.mean(content)
        for j in range(3,5):
            f_path = path + f'/{j}.txt'
            with open(f_path, 'r') as f:
                content = [float(x) for x in f.read().split()]
                temp[j-1] += np.mean(content)
    d_avg.append(temp[0])
    d_avg.append(temp[1])
    k_avg.append(temp[2])
    k_avg.append(temp[3])
    
    temp = [0,0,0,0,]
 
if args.write:
    saveData(d_avg, f'dyconit_through')
    saveData(k_avg, f'kafka_through')       
    
overall.append(d_avg)
overall.append(k_avg)
plotbox(overall, 'Consumer throughput', 'Throughput [messages / second]',['Dyconit', 'Kafka'], 'Consumer', 1.2)
plt.show()
# plotting throughput
###########################################


