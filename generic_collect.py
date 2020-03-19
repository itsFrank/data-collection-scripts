import sys
import subprocess
import csv
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('out_file', action="store", help="The file to which the output is written to")
parser.add_argument('-t', action="store_true", default=False, dest="test", help="Enable test mode; real function printed to console and dummy function is executed, output is filled with dummy output plus config")
parser.add_argument('-a', action="store_true", default=False, dest="append", help="Append to outfile rather than overwriting")
parser.add_argument('-all', action="store_true", default=False, dest="all", help="Run All apps")
parser.add_argument('-bfs', action="store_true", default=False, dest="bfs", help="Run BFS")
parser.add_argument('-sssp', action="store_true", default=False, dest="sssp", help="Run SSSP")
parser.add_argument('-pr', action="store_true", default=False, dest="pr", help="Run PR")

cli = parser.parse_args()

TEST_CMD = ["printf", "Running time : 8.27\nhi:hello\nbye:farewell\ngreet:howdy"]
test_mode = cli.test

def arrToRunPairsWitOpt(arr, opt):
    return [runPair(a, opt + ' ' + str(a)) for a in arr]

def arrPairsToRunPairs(arr):
    return [runPair(a[0], a[1]) for a in arr]

def arrPairToRunPairs(names, cmds):
    return [runPair(names[i], cmds[i]) for i in range(len(names))]

def runPair(name, cmd):
    return {"cmd": cmd.split(" "), "name": name}

def makeDataTuple(str_pair):
    first = str_pair[0]
    second = str_pair[1]
    try:
        second = float(second)
    except:
        pass
    return [first, second]

def pipedPrint(*args):
    print(*args)
    sys.stdout.flush()

def blankRes(lines):
    return {}

def execToLines(cmd, timeout=None, quiet=False):
    if not quiet:
        pipedPrint("Executing:", ' '.join(cmd))
    
    if test_mode:
        cmd = TEST_CMD

    try:
        lines = subprocess.check_output(cmd, timeout=timeout).decode(sys.stdout.encoding).split('\n')
    except:
        return None
    return lines

RUN_ERR_CONTINUE = 1
RUN_ERR_FAIL = 2
RUN_ERR_RETRY = 3

def run(config, order, idxMatch={}, subConf={}, extractRes=blankRes, writer=None, timeout=None, quiet=False, errorBehavior=RUN_ERR_CONTINUE):
    idx = {}
    for key in order:
        idx[key] = 0
    
    done = False
    while (not done):
        # Make command
        confDict = {}
        cmd = []
        for key in order:
            conf = {}
            confArr = config[key]
            if key in subConf:
                confArr = config[key][config[subConf[key]][idx[subConf[key]]]['name']]

            if key in idxMatch:
                matchIdx = idx[idxMatch[key]]
                conf = confArr[matchIdx]
            else:
                conf = confArr[idx[key]]
            
            if (isinstance(conf, str)):
                cmd += conf.split(' ')
            else:
                cmd += conf['cmd']
                confDict[key] = conf['name']
                

        # exec command
        lines = None
        while lines == None:
            lines = execToLines(cmd, timeout=timeout, quiet=quiet)
            if not lines and errorBehavior == RUN_ERR_CONTINUE:
                lines=[""]
        execDict = extractRes(lines)

        resDict = {**confDict, **execDict}

        if writer:
            writer.writeRow(resDict)

        # next indexes
        for key in reversed(order):
            if key in idxMatch:
                continue
            num_entries = len(config[key])
            if key in subConf:
                num_entries = len(config[key][config[subConf[key]][idx[subConf[key]]]['name']])
            if (idx[key] < num_entries - 1):
                idx[key] += 1
                break
            else:
                idx[key] = 0
                if (key == order[0]):
                    done = True

class CSVWriter:
    def __init__(self, outPath, append=False):
        self.outPath = outPath
        self.append = append
        self.writer = None
        self.outFile = None
    
    def _initWriter(self, keys=None):
        if self.writer:
            return
            
        self.outFile = open(self.outPath, 'a+' if self.append else 'w+')
        if (keys):
            self.writer = csv.DictWriter(self.outFile, keys)
            if not self.append:
                self.writer.writeheader()

    def writeRows(self, dicts):
        self._initWriter(dicts[0].keys())
        self.writer.writerows(dicts)
        self.outFile.flush()

    def writeRow(self, dict):
        self.writeRows([dict])

"""
BFS
    graph
        threads
            start vtx - averaged
"""

BASE_GRAPH_PATH = "/homes/obrienfr/ligra/inputs/"
UNWEIGHTED_SUFFIX = ".g"
WEIGHTED_SUFFIX = ".w.g"
UNWEIGHTED_DIR = ""
WEIGHTED_DIR = ""

GRAPH_NAMES = ["S23-E8", "S25-E8", "S23-E16", "S25-E16", "S23-E32", "Higgs", "Pokec", "StackOverflow", "Skitter", "LiveJournal", "Orkut"]#, "S25-E32", "Twitter"]
GRAPH_FILES = ["s23_e8", "s25_e8", "s23_e16", "s25_e16", "s23_e32", "higgs-social_network", "soc-pokec-relationships", "sx-stackoverflow", "as-skitter", "soc-LiveJournal1", "com-orkut"]#, "s25_e32", "twitter"]

UNWEIGHTED_GRAPH_FILES = [BASE_GRAPH_PATH + UNWEIGHTED_DIR + g + UNWEIGHTED_SUFFIX for g in GRAPH_FILES]
WEIGHTED_GRAPH_FILES = [BASE_GRAPH_PATH + WEIGHTED_DIR + g + WEIGHTED_SUFFIX for g in GRAPH_FILES]


UNWEIGHTED_GRAPHS = arrPairsToRunPairs([ [GRAPH_NAMES[i], UNWEIGHTED_GRAPH_FILES[i]] for i in range(len(GRAPH_NAMES)) ])
WEIGHTED_GRAPHS = arrPairsToRunPairs([ [GRAPH_NAMES[i], WEIGHTED_GRAPH_FILES[i]] for i in range(len(GRAPH_NAMES)) ])
THREADS = arrPairsToRunPairs([[1, '-w 1'], [2, '-w 2'], [4, '-w 4'], [8, '-w 8'], [16, '-w 16']])

ALL_BASE = '-rounds 1'
PR_BASE = ALL_BASE + ' -maxiters 1'

BFS_APP = [runPair("bfs", "./BFS")]
SSSP_APP = [runPair("sssp", "./BellmanFord")]
PR_APP = [runPair("pr", "./PageRank")]

BFS_START_VTX = {
    "S23-E8": arrToRunPairsWitOpt([7414634, 2118656, 6381226, 4881611, 1666101, 2890779], '-r'),
    "S25-E8": arrToRunPairsWitOpt([19409568, 7837389, 3866742, 29117763, 24206166, 6196134], '-r'),
    "S23-E16": arrToRunPairsWitOpt([3194636, 7151391, 6119304, 4402409, 2106228, 3761509], '-r'),
    "S25-E16": arrToRunPairsWitOpt([22315294, 24060767, 7806671, 18751491, 9072263, 9240140], '-r'),
    "S23-E32": arrToRunPairsWitOpt([8158011, 4277437, 2994540, 591891, 5064012, 7976987], '-r'),
    "S25-E32": arrToRunPairsWitOpt([29380095, 2137252, 22779219, 23263437, 30134112, 18424607], '-r'),
    "Higgs": arrToRunPairsWitOpt([165486, 15147, 288568, 17220, 127341, 328483], '-r'),
    "Pokec": arrToRunPairsWitOpt([858951, 438160, 1385063, 793905, 310461, 300989], '-r'),
    "StackOverflow": arrToRunPairsWitOpt([5515818, 3554183, 2622510, 200094, 1323299, 1166567], '-r'),
    "Skitter": arrToRunPairsWitOpt([878248, 1093773, 1040066, 1529161, 1105468, 1543502], '-r'),
    "LiveJournal": arrToRunPairsWitOpt([3903641, 4158378, 1486101, 467386, 1875102, 1966836], '-r'),
    "Orkut": arrToRunPairsWitOpt([2062367, 767779, 1805450, 1060076, 424425, 641114], '-r'),
    "Twitter": arrToRunPairsWitOpt([15917685, 20295785, 29849673, 38108011, 19986669, 40645677], '-r')
}
SSSP_START_VTX = {
    "S23-E8": arrToRunPairsWitOpt([4240417, 3665634, 3793479, 7755544, 4578522, 565759], '-r'),
    "S25-E8": arrToRunPairsWitOpt([4262764, 15267576, 6749823, 25237631, 27553639, 8694173], '-r'),
    "S23-E16": arrToRunPairsWitOpt([3686426, 4045917, 2257506, 7399604, 6822104, 3743497], '-r'),
    "S25-E16": arrToRunPairsWitOpt([3486228, 6900011, 10908673, 16984563, 21682423, 31440624], '-r'),
    "S23-E32": arrToRunPairsWitOpt([2219513, 5165283, 621779, 4581322, 8384500, 1587501], '-r'),
    "S25-E32": arrToRunPairsWitOpt([27611571, 13755958, 17606434, 21230608, 21802641, 20865646], '-r'),
    "Higgs": arrToRunPairsWitOpt([132279, 206656, 418509, 197172, 213192, 11003], '-r'),
    "Pokec": arrToRunPairsWitOpt([247882, 246033, 1092414, 720811, 255164, 139421], '-r'),
    "StackOverflow": arrToRunPairsWitOpt([3214351, 2446213, 3198986, 1313602, 32228, 824045], '-r'),
    "Skitter": arrToRunPairsWitOpt([1687785, 774220, 1672775, 324212, 411760, 554802], '-r'),
    "LiveJournal": arrToRunPairsWitOpt([2885369, 669594, 848203, 679576, 2735354, 2016861], '-r'),
    "Orkut": arrToRunPairsWitOpt([376633, 2503156, 1941442, 742190, 1461468, 2082824], '-r'),
    "Twitter": arrToRunPairsWitOpt([33258104, 28111708, 42057675, 26764230, 33362701, 58768415], '-r'),
}

bfs_config = {
    'graphs': UNWEIGHTED_GRAPHS,
    'app': BFS_APP,
    'threads': THREADS,
    'base': [ALL_BASE],
    'start_vtx': BFS_START_VTX
}

sssp_config = {
    'graphs': WEIGHTED_GRAPHS,
    'app': SSSP_APP,
    'threads': THREADS,
    'base': [ALL_BASE],
    'start_vtx': SSSP_START_VTX
}

pr_config = {
    'graphs': UNWEIGHTED_GRAPHS,
    'app': PR_APP,
    'threads': THREADS,
    'base': [PR_BASE]
}

csvWriter = CSVWriter(cli.out_file, cli.append)

def extractData(lines):
    for line in lines:
        vals = line.strip().split(":")
        if vals[0].strip() == "Running time":
            return {'runtime': vals[1].strip()}
    return {'runtime': '---'}

if cli.bfs or cli.all:
    run(bfs_config, ['app', 'base', 'threads', 'start_vtx', 'graphs'], {}, {'start_vtx': 'graphs'}, extractData, csvWriter)
if cli.sssp or cli.all:
    run(sssp_config, ['app', 'base', 'threads', 'start_vtx', 'graphs'], {}, {'start_vtx': 'graphs'}, extractData, csvWriter)
if cli.pr or cli.all:
    run(pr_config, ['app', 'base', 'threads', 'graphs'], {}, {}, extractData, csvWriter)