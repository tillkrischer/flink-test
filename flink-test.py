import numpy as np
import time
import urllib.request
import urllib.parse


def parseinput(filename):
    with open('data.csv') as f:
        # skip first line
        f.readline()

        events = []
        line = f.readline()
        while line:
            time, value = line.split(",")
            time = float(time)
            # round down the value
            value = int(float(value) // 1)

            # generate value many events using a normal distribution around
            # the mean given by time
            for _ in range(value):
                event = np.random.normal(time, 1)
                events += [event]

            line = f.readline()

        events.sort()
        return events


def submitjob(jobarg):
    jobid = "fc0ca690-2780-4922-b1c6-5d9a263b263a_piestimation-1.0-SNAPSHOT.jar"
    url = f'http://localhost:8081/jars/{jobid}/run?programArg={jobarg}'

    req = urllib.request.Request(url, method="POST")

    urllib.request.urlopen(req)


def querymetrics():
    jobmanagerurl = "http://localhost:8081/jobmanager/metrics?get=taskSlotsAvailable,Status.JVM.CPU.Time"
    taskmanagerurl = "http://localhost:8081/taskmanagers/metrics?get=Status.JVM.CPU.Load"

    metrics = {}

    with urllib.request.urlopen(jobmanagerurl) as f:
        metrics["jobsmanager"] = f.read().decode()

    with urllib.request.urlopen(taskmanagerurl) as f:
        metrics["taskmanager"] = f.read().decode()

    return metrics


eventsA = parseinput("data.csv")

start = time.time()
lastsecond = 0

metrics = {}

while len(eventsA) > 0:
    elapsedtime = time.time() - start
    if elapsedtime > eventsA[0]:
        print("submitting job")
        submitjob(10000)
        eventsA.pop(0)
    elapsedseconds = elapsedtime // 1
    if elapsedseconds > lastsecond:
        print(elapsedseconds)
        m = querymetrics()
        metrics[elapsedseconds] = m
        lastsecond = elapsedseconds

    time.sleep(0.01)

print(metrics)
