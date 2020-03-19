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
parser.add_argument('-pr32', action="store_true", default=False, dest="pr32", help="Run PR")
parser.add_argument('-cl_count', action="store_true", default=False, dest="pr", help="count Cls")

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

BASE_GRAPH_PATH = "/homes/obrienfr/mattgraphs/BINARY/"

def buildGraphConfig(g_names, g_files, prefix, suffix):
    return [ runPair(g_names[i], prefix + g_files[i] + suffix) for i in range(len(g_names)) ]

def extractData(extract):
    def fn(lines):
        data = {}
        for line in lines:
            vals = line.strip().split(":")
            vals[0] = vals[0].strip()
            if vals[0] in extract:
                data[vals[0]] = vals[1].strip()
        
        for key in extract:
            if key not in data:
                data[key] = "---"
        return data
    return fn

UNWEIGHTED_SUFFIX = ".g"
WEIGHTED_SUFFIX = ".w.g"
UNWEIGHTED_DIR = ""
WEIGHTED_DIR = ""

GRAPH_NAMES = ["S23-E8", "S25-E8", "S23-E16", "S25-E16", "S23-E32", "Higgs", "Pokec", "StackOverflow", "Skitter", "LiveJournal", "Orkut", "S25-E32", "Twitter"]
GRAPH_FILES = ["s23_e8", "s25_e8", "s23_e16", "s25_e16", "s23_e32", "higgs-social_network", "soc-pokec-relationships", "sx-stackoverflow", "as-skitter", "soc-LiveJournal1", "com-orkut", "s25_e32", "twitter"]

ALL_BASE = '-b -q -script -automemo -st 1 -gt 16 -clcount -cpu -r 10'
EXEC = "./graph_analytics"

bfs_config = {
    'exec': [EXEC],
    'graphs': buildGraphConfig(GRAPH_NAMES, GRAPH_FILES, BASE_GRAPH_PATH + 'bfs/', '_bfs.b'),
    'app': [runPair('bfs', '-bfs')],
    'base': [ALL_BASE]
}

sssp_config = {
    'exec': [EXEC],
    'graphs': buildGraphConfig(GRAPH_NAMES, GRAPH_FILES, BASE_GRAPH_PATH + 'sssp/', '_sssp.b'),
    'app': [runPair('sssp', '-sssp')],
    'base': [ALL_BASE]
}

pr_config = {
    'exec': [EXEC],
    'graphs': buildGraphConfig(GRAPH_NAMES, GRAPH_FILES, BASE_GRAPH_PATH + 'pr32/', '_pr.b'),
    'app': [runPair('pr32', '-pr32')],
    'base': [ALL_BASE]
}

EXTRACT_VALS = ["VTX_CLs", "EDG_CLs", "Boolbuf_EDG_CLs", "UPD_CLs", "MIN_EDG_CLs"]

csvWriter = CSVWriter(cli.out_file, cli.append)

if cli.bfs or cli.all:
    run(bfs_config, ['exec', 'graphs', 'app', 'base'], {}, {'start_vtx': 'graphs'}, extractData(EXTRACT_VALS), csvWriter)
if cli.sssp or cli.all:
    run(sssp_config, ['exec', 'graphs', 'app', 'base'], {}, {'start_vtx': 'graphs'}, extractData(EXTRACT_VALS), csvWriter)
if cli.pr32 or cli.all:
    run(pr_config, ['exec', 'graphs', 'app', 'base'], {}, {}, extractData(EXTRACT_VALS), csvWriter)