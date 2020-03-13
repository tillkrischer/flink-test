import numpy as np
import time
import urllib.request
import urllib.parse
import json
import matplotlib.pyplot as plt

jobmanagermetrics = [
    "Status.JVM.Memory.Mapped.TotalCapacity",
    "taskSlotsAvailable",
    "Status.JVM.Memory.Mapped.MemoryUsed",
    "taskSlotsTotal",
    "Status.JVM.CPU.Time",
    "Status.JVM.Threads.Count",
    "Status.JVM.Memory.Heap.Committed",
    "Status.JVM.Memory.Direct.MemoryUsed",
    "numRunningJobs",
    "Status.JVM.Memory.Mapped.Count",
    "Status.JVM.CPU.Load",
    "Status.JVM.Memory.Heap.Max",
    "Status.JVM.Memory.Heap.Used"
]
taskmanagermetrics = [
    "Status.Network.AvailableMemorySegments",
    "Status.JVM.Memory.Mapped.TotalCapacity",
    "Status.Network.TotalMemorySegments",
    "Status.JVM.Memory.Mapped.MemoryUsed",
    "Status.JVM.CPU.Time",
    "Status.JVM.Threads.Count",
    "Status.JVM.Memory.Heap.Committed",
    "Status.JVM.Memory.Direct.Count",
    "Status.JVM.Memory.NonHeap.Max",
    "Status.JVM.Memory.NonHeap.Committed",
    "Status.JVM.Memory.NonHeap.Used",
    "Status.JVM.Memory.Direct.MemoryUsed",
    "Status.JVM.Memory.Direct.TotalCapacity",
    "Status.JVM.Memory.Mapped.Count",
    "Status.JVM.CPU.Load",
    "Status.JVM.Memory.Heap.Max",
    "Status.JVM.Memory.Heap.Used"
]


def parseinput(filename):
    with open(filename) as f:
        # skip first line
        f.readline()

        events = []
        tsx = []
        tsy = []
        line = f.readline()
        while line:
            time, value = line.split(",")
            time = float(time)
            # round down the value
            value = int(float(value) // 1)

            tsx += [time]
            tsy += [value]

            # generate value many events using a normal distribution around
            # the mean given by time
            for _ in range(value):
                event = np.random.normal(time, 1)
                events += [event]

            line = f.readline()

        events.sort()
        return events, tsx, tsy


def submitjob(jobarg):
    jobid = "96e38cda-179a-4b5a-a6c1-7106137f73db_piestimation-1.0-SNAPSHOT.jar"
    url = f'http://localhost:8081/jars/{jobid}/run?programArg={jobarg}'

    req = urllib.request.Request(url, method="POST")

    urllib.request.urlopen(req)


def querymetrics():
    jobmanagerurl = f"http://localhost:8081/jobmanager/metrics?get={','.join(jobmanagermetrics)}"
    taskmanagerurl = f"http://localhost:8081/taskmanagers/metrics?get={','.join(taskmanagermetrics)}"

    metrics = {}

    with urllib.request.urlopen(jobmanagerurl) as f:
        metrics["jobsmanager"] = json.loads(f.read().decode())

    with urllib.request.urlopen(taskmanagerurl) as f:
        metrics["taskmanager"] = json.loads(f.read().decode())

    return metrics


dataA = "dataA.csv"
dataB = "dataC.csv"
# dataA_weight = 100000
dataA_weight = 1000000
dataB_weight = 300000

eventsA, tsxA, tsyA = parseinput(dataA)
eventsB, tsxB, tsyB = parseinput(dataB)

start = time.time()
lastsecond = 0

metrics = {}

while len(eventsA) > 0 or len(eventsB) > 0:
    elapsedtime = time.time() - start

    # A
    if len(eventsA) > 0 and elapsedtime > eventsA[0]:
        print("submitting job type A")
        submitjob(dataA_weight)
        eventsA.pop(0)

    # B
    if len(eventsB) > 0 and elapsedtime > eventsB[0]:
        print("submitting job type B")
        submitjob(dataB_weight)
        eventsB.pop(0)

    # metrics
    elapsedseconds = int(elapsedtime)
    if elapsedseconds > lastsecond:
        print(elapsedseconds)
        m = querymetrics()
        metrics[elapsedseconds] = m
        lastsecond = elapsedseconds

    time.sleep(0.01)

jmmet = {}
tmmet = {}
for met in jobmanagermetrics:
    jmmet[met] = []

for met in taskmanagermetrics:
    tmmet[met] = []

i = 1
while i in metrics:
    for o in metrics[i]["jobsmanager"]:
        value = ""
        if "value" in o:
            value = o["value"]
        elif "avg" in o:
            value = o["avg"]
        jmmet[o["id"]] += [float(value)]
    for o in metrics[i]["taskmanager"]:
        value = ""
        if "value" in o:
            value = o["value"]
        elif "avg" in o:
            value = o["avg"]
        tmmet[o["id"]] += [float(value)]
    i += 1


def showplot(tsxA, tsyA, metrics, name):
    _, ax1 = plt.subplots()
    # ax1
    color = 'tab:blue'
    ax1.set_xlabel('time (s)')
    ax1.set_ylabel('generated', color=color)
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.plot(tsxA, tsyA, color=color)
    # ax1.plot(tsxB, tsyB, color=color)

    # ax2
    ax2 = ax1.twinx()
    color = 'tab:red'
    ax2.set_ylabel(name, color=color)
    ax2.plot(metrics, color=color)
    ax2.tick_params(axis='y', labelcolor=color)
    plt.show()


for met in jmmet:
    showplot(tsxA, tsyA, jmmet[met], met)

for met in tmmet:
    showplot(tsxA, tsyA, tmmet[met], met)
