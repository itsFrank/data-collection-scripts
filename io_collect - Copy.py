import os 
import sys

if len(sys.argv) < 2:
    print('No outfile')
    quit()

out_file = sys.argv[1]
cli_app = ''
 
if len(sys.argv) >= 3:
    cli_app = sys.argv[2] if sys.argv[2] != '-t' else ''

test_en = False
for arg in sys.argv:
    if arg == '-t':
        test_en = True

def runCommand(command):
    if test_en:
        print(command)
    else:
        os.system(command)

col_op = {
    '': 'tot',
    '-dr': 'wr',
    '-dw': 'rd'
}

def collectData(app, op_list, extract, optionalArgs = []):
    for op in op_list:
            optional = ' '.join(optionalArgs)
            print(f'Running {app} with {op} and {optional} ...')

            runCommand(f'{app} -d {op} {optional} > tmp_out')
            runCommand(f'awk \'$1 == "{extract}" {{print $3}}\' tmp_out > tmp_out2')
            runCommand(f'echo -n "{col_op[optional]},{op}," | cat - tmp_out2 >> {out_file}')

scale_low = 15
scale_high = 24
app_command = './interface_debug'
collectData(app_command, [i for i in range(scale_low, scale_high+1)], 'Tot' , [''])
collectData(app_command, [i for i in range(scale_low, scale_high+1)], 'Wr' , ['-dr'])
collectData(app_command, [i for i in range(scale_low, scale_high+1)], 'Rd' , ['-dw'])
