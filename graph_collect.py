import sys
import subprocess
import csv
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('out_file', action="store", help="The file to which the output is written to")
parser.add_argument('-t', action="store_true", default=False, dest="test", help="Enable test mode; real function printed to console and dummy function is executed, output is filled with dummy output plus config")
parser.add_argument('-a', action="store_true", default=False, dest="append", help="Append to outfile rather than overwriting")
parser.add_argument('-all', action="store_true", default=False, help="Execute all BFS, SSSP, and PR")
parser.add_argument('-bfs', action="store_true", default=False, help="Execute BFS only")
parser.add_argument('-sssp', action="store_true", default=False, help="Execute SSSP only")
parser.add_argument('-pr', action="store_true", default=False, help="Execute PR only")
parser.add_argument('-pr64', action="store_true", default=False, help="Execute PR only")

cli = parser.parse_args()

TEST_CMD = ["printf", "hi:hello\nbye:farewell\ngreet:howdy"]
test_mode = cli.t

def pipedPrint(*args):
    print(*args)
    sys.stdout.flush()

def makeDataTuple(str_pair):
    first = str_pair[0]
    second = str_pair[1]
    try:
        second = float(second)
    except:
        pass
    return [first, second]

def exec(cmd, timeout=60, quiet=False):
    if not quiet:
        pipedPrint("Executing:", ' '.join(cmd))
    
    if test_mode:
        cmd = TEST_CMD

    try:
        subprocess.check_output(cmd, timeout=timeout)
    except subprocess.TimeoutExpired:
        return False
    return True

def execToDict(cmd, timeout=60, quiet=False):
    if not quiet:
        pipedPrint("Executing:", ' '.join(cmd))
    
    if test_mode:
        cmd = TEST_CMD

    try:
        lines = subprocess.check_output(cmd, timeout=timeout).decode(sys.stdout.encoding).split('\n')
    except subprocess.TimeoutExpired:
        return None
    
    return dict(makeDataTuple(line.strip().split(":")) for line in lines if line!='')

def cleanGraphName(graph):
    if graph[0] == 'd':
        return graph[2:9]
    else:
        return graph.split("_")[0]

class Config:
    def __init__(self, options, info):
        self.options = options
        self.info = info

class CSVWriter:
    def __init__(self, outPath, append=False):
        self.outPath = outPath
        self.append = append
        self.writer = None
        self.outFile = None
    
    def _initWriter(self, keys):
        if self.writer:
            return
            
        self.outFile = open(self.outPath, 'a+' if self.append else 'w+')
        self.writer = csv.DictWriter(self.outFile, keys)
        
        if not self.append:
            self.writer.writeheader()

    def writeRows(self, dicts):
        self._initWriter(dicts[0].keys())
        self.writer.writerows(dicts)
        self.outFile.flush()

    def writeRow(self, dict):
        self.writeRows([dict])

class RunCollect:
    def __init__(self, app, graphPath, graphs, baseOptions=[]):
        self.app = app
        self.baseOptions = baseOptions
        self.graphPath = graphPath
        self.graphs = graphs
        self.configs = []
        self._runDicts = []
        self._dict_prune = None
        self.writer = None
    
    def addConfig(self, config):
        self.configs.append(config)

    def addConfigs(self, configs):
        self.configs += configs

    def run(self, exec, timeout=60, quiet=False):
        for graph in self.graphs:
            for config in self.configs:
                cmd  = [exec, self.graphPath + graph] + self.baseOptions + config.options
                
                runDict = {}
                runDict["app"] = self.app
                runDict["graph"] = cleanGraphName(graph)
                
                resultDict = None
                while resultDict == None:
                    resultDict = execToDict(cmd, timeout=timeout, quiet=quiet)
                    if not resultDict and not quiet:
                        pipedPrint("\tTimed-out, retrying...")

                if self._dict_prune:
                    self._dict_prune(resultDict)

                runDict = {**runDict, **config.info, **resultDict}

                if self.writer:
                    self.writer.writeRow(runDict)

                self._runDicts.append(runDict)
    
    def runAvg(self, exec, avgOpts, avgVal, timeout=60, quiet=False):
        for graph in self.graphs:
            for config in self.configs:
                cmd  = [exec] + self.baseOptions + config.options + [self.graphPath + graph]
                
                runDict = {}
                runDict["app"] = self.app
                runDict["graph"] = cleanGraphName(graph)
                
                resultDict = None
                while resultDict == None:
                    resultDict = execToDict(cmd, timeout=timeout, quiet=quiet)
                    if not resultDict and not quiet:
                        pipedPrint("\tTimed-out, retrying...")

                if self._dict_prune:
                    self._dict_prune(resultDict)

                runDict = {**runDict, **config.info, **resultDict}

                if self.writer:
                    self.writer.writeRow(runDict)

                self._runDicts.append(runDict)

    def setWriter(self, writer):
        self.writer = writer

    def setResultPruneFn(self, fn):
        self._dict_prune = fn

    #order results for prop
    def getResults(self):
        return self._runDicts


EXEC = "./graph_analytics"
BASE_GRAPH_PATH = "/homes/obrienfr/mattgraphs/BINARY"
BFS_GRAPH_PATH = BASE_GRAPH_PATH + "/bfs/"
SSSP_GRAPH_PATH = BASE_GRAPH_PATH + "/sssp/"
PR_GRAPH_PATH = BASE_GRAPH_PATH + "/pr32/"
PR64_GRAPH_PATH = BASE_GRAPH_PATH + "/pr64/"

BASE_OPTIONS = ["-b", "-q", "-script", "-colbw", "-gt", "16", "-automemo", "-nox2u"]
FPGA_CONFIG = Config(BASE_OPTIONS + [ "-fpga" ], {"processor": "fpga"})
CPU_CONFIG = Config(BASE_OPTIONS + [ "-cpu" ], {"processor": "cpu"})

# BFS_GRAPHS = ["d_s23_e32_bfs.b", "d_s24_e32_bfs.b", "d_s25_e32_bfs.b", "as-skitter_bfs.b", "soc-LiveJournal1_bfs.b", "twitter_bfs.b"]
# SSSP_GRAPHS = ["d_s23_e32_sssp.b", "d_s24_e32_sssp.b", "d_s25_e32_sssp.b", "as-skitter_sssp.b", "soc-LiveJournal1_sssp.b", "twitter_sssp.b"]
# PR_GRAPHS = ["d_s23_e32_pr.b", "d_s24_e32_pr.b", "d_s25_e32_pr.b", "as-skitter_pr.b", "soc-LiveJournal1_pr.b", "twitter_pr.b"]
# PR64_GRAPHS = ["d_s23_e32_pr64.b", "d_s24_e32_pr64.b", "d_s25_e32_pr64.b", "as-skitter_pr64.b", "soc-LiveJournal1_pr64.b", "twitter_pr64.b"]

# GRAPHS = ["d_s23_e8", "d_s24_e8", "d_s25_e8", "d_s23_e16", "d_s24_e16", "d_s25_e16", "d_s23_e32", "d_s24_e32", "d_s25_e32", "higgs-social_network", "soc-pokec-relationships", "sx-stackoverflow", "as-skitter", "soc-LiveJournal1", "com-orkut", "twitter"]
GRAPHS = ["d_s23_e8", "d_s25_e8", "d_s23_e16", "d_s25_e16", "d_s23_e32", "d_s25_e32", "higgs-social_network", "soc-pokec-relationships", "sx-stackoverflow", "as-skitter", "soc-LiveJournal1", "com-orkut", "twitter"]

BFS_SUFFIX = "_bfs.b"
SSSP_SUFFIX = "_sssp.b"
PR_SUFFIX = "_pr.b"
PR64_SUFFIX = "_pr64.b"

BFS_BITSTREAM = "/homes/obrienfr/bitstreams/bfs/ws_bfs_dedup_291.gbs"
SSSP_BITSTREAM = "/homes/obrienfr/bitstreams/sssp/ws_sssp_dedup_286.gbs"
PR_BITSTREAM = "/homes/obrienfr/bitstreams/pr/ws_pr32_dedup_235.gbs"
PR64_BITSTREAM = "/homes/obrienfr/bitstreams/pr64/ws_pr64_dedup_239.gbs"

csvWriter = CSVWriter(cli.out_file, cli.append)

BFS_GRAPHS = [s + BFS_SUFFIX for s in GRAPHS]
bfs_run = RunCollect("bfs", BFS_GRAPH_PATH, BFS_GRAPHS, ["-bfs"])
bfs_run.addConfigs([FPGA_CONFIG, CPU_CONFIG])
bfs_run.setWriter(csvWriter)

SSSP_GRAPHS = [s + SSSP_SUFFIX for s in GRAPHS]
sssp_run = RunCollect("sssp", SSSP_GRAPH_PATH, SSSP_GRAPHS, ["-sssp"])
sssp_run.addConfigs([FPGA_CONFIG, CPU_CONFIG])
sssp_run.setWriter(csvWriter)

PR_GRAPHS = [s + PR_SUFFIX for s in GRAPHS]
pr_run = RunCollect("pr32", PR_GRAPH_PATH, PR_GRAPHS, ["-pr32"])
pr_run.addConfigs([FPGA_CONFIG, CPU_CONFIG])
pr_run.setWriter(csvWriter)

PR64_GRAPHS = [s + PR64_SUFFIX for s in GRAPHS]
pr64_run = RunCollect("pr64", PR64_GRAPH_PATH, PR64_GRAPHS, ["-pr"])
pr64_run.addConfigs([FPGA_CONFIG, CPU_CONFIG])
pr64_run.setWriter(csvWriter)

if cli.bfs or cli.all:
    exec(["fpgaconf", BFS_BITSTREAM])
    bfs_run.run("./graph_analytics", timeout=900)

if cli.sssp or cli.all:
    exec(["fpgaconf", SSSP_BITSTREAM])
    sssp_run.run("./graph_analytics", timeout=900)

if cli.pr or cli.all:
    exec(["fpgaconf", PR_BITSTREAM])
    pr_run.run("./graph_analytics", timeout=900)

if cli.pr64 or cli.all:
    exec(["fpgaconf", PR64_BITSTREAM])
    pr64_run.run("./graph_analytics", timeout=900)

