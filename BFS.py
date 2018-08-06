from pyspark import SparkConf, SparkContext

UNPROCESSED = 'UNPROCESSED'
PROCESSED = 'PROCESSED'
START = 'START'

startId = 1
targetId = 100
hitCounter = sc.accumulator(0)

conf = SparkConf().setMaster("local").setAppName("BreadthFirstSearch")
sc = SparkContext(conf = conf)
inputFile = sc.textFile("test-data/data.txt")
BFSrdd = inputFile.map(convertToBFS)

def convertToBFS(line):
    fields = line.split()
    id = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = UNPROCESSED
    distance = 9999

    if (id == startId):
        color = PROCESSED
        distance = 0

    return (id, (connections, distance, color))
