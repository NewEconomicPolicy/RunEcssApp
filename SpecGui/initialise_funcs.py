"""
#-------------------------------------------------------------------------------
# Name:        initialise_funcs.py
# Purpose:     script to read read and write the setup and configuration files
# Author:      Mike Martin
# Created:     31/07/2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------
"""

__prog__ = 'initialise_funcs.py'
__version__ = '0.0.0'

# Version history
# ---------------
# 
from os.path import exists, normpath, isfile, isdir, join
from os import getcwd, getenv
from json import dump as json_dump, load as json_load
from time import sleep
from multiprocessing import cpu_count
from input_output_funcs import read_study_definition

PROGRAM_ID = 'spatial_ecosse'
ERROR_STR = '*** Error *** '
WARNING_STR = '*** Warning *** '
sleepTime = 5

def initiation(form):
    """
    this function is called to initiate the programme to process non-GUI settings.
    NB logging not required for SpecGui which is just a front end to spec_run.py
    """

    # retrieve settings
    # =================
    form.settings = _read_setup_file()
    return

def _read_setup_file():
    """
    # read settings used for programme from the setup file, if it exists,
    # or create setup file using default values if file does not exist
    """
    func_name =  __prog__ +  ' _read_setup_file'

    # look for setup file here...
    setup_file = join(getcwd(), PROGRAM_ID + '_setup.json')

    if exists(setup_file):
        try:
            with open(setup_file, 'r') as fsetup:
                setup = json_load(fsetup)
        except (OSError, IOError) as err:
                print(ERROR_STR + str(err))
                sleep(sleepTime)
                exit(0)
    else:
        setup = _write_default_setup_file(setup_file)

    # initialise vars
    # ===============
    settings = setup['setup']
    settings_list = ['config_dir', 'fname_png', 'log_dir', 'python_exe', 'spec_run_py']
    for key in settings_list:
        if key not in settings:
            print(ERROR_STR + 'setting {} is required in setup file {} '.format(key, setup_file))
            sleep(sleepTime)
            exit(0)

    fname_png =  settings['fname_png']
    python_exe = settings['python_exe']
    spec_run_py = settings['spec_run_py']
    log_dir = settings['log_dir']
    config_dir = settings['config_dir']

    error_flag = False

    if isdir(log_dir):
        print('\tlog file directory: ' + log_dir)
    else:
        print(ERROR_STR + 'log_dir in setup file: ' + log_dir + ' must be a directory')
        error_flag = True

    if isdir(config_dir):
        print('\tconfiguration file directory: ' + config_dir)
    else:
        print(ERROR_STR + 'config_dir in setup file: ' + config_dir + ' must be a directory')
        error_flag = True

    if not isfile(fname_png):
        print(WARNING_STR + 'Could not find logo file ' + fname_png)

    # make sure all components of the command string exist
    # ====================================================
    errmess = ' does not exist - cannot run Ecosse'
    if not exists(python_exe):
        print(ERROR_STR + 'Python programme ' + python_exe + errmess)
        error_flag = True

    if not exists(spec_run_py):
        print(ERROR_STR + 'Run script ' + spec_run_py + errmess)
        error_flag = True

    if error_flag:
        sleep(sleepTime)
        exit(0)

    settings['config_file'] = join(config_dir, PROGRAM_ID + '_config.json')  # configuration file

    # required for GUI:
    # =================
    try:
        maxcpus = cpu_count()
    except:
        maxcpus = None

    settings['maxcpus'] = maxcpus

    return settings

def _write_default_setup_file(setup_file):
    """
    #  stanza if setup_file needs to be created
    """
    if getenv('USERNAME') == 'mmartin':
        root_dir = 'C:\\AbUniv\\'
        root_dir_scripts = root_dir
        python_dir = 'E:\\Python38\\'
    else:
        root_dir = 'E:\\'
        root_dir_scripts = 'H:\\'
        python_dir = 'E:\\Python38\\'

    _default_setup = {
        'setup': {
            'config_dir': root_dir + 'GlobalEcosseSuite\\config',
            'fname_png'  : join(root_dir + 'GlobalEcosseSuite\\Images', 'World_small.PNG'),
            'log_dir': root_dir + 'GlobalEcosseOutputs\\logs',
            'python_exe' : python_dir + 'python.exe',
            'root_dir': root_dir,
            'spec_run_py': join(root_dir_scripts + 'DevSpec', 'spec_run.py')
        },
        'run_settings': {
            'completed_max' : 5000000000,
            'guiMode'       : True
        }
    }
    # if setup file does not exist then create it...
    with open(setup_file, 'w') as fsetup:
        json_dump(_default_setup, fsetup, indent=2, sort_keys=True)
        return _default_setup

def _write_default_config_file(config_file, maxcpus):
    """
    # only required if the config_file needs to be created
    """
    _default_config = {
        'General': {
                'config_check_interval': 60
            },
        'Simulations': {
            'output_dir': 'C:\\',
            'sims_dir': 'C:\\',
            'exepath': 'C:\\',
            'delete_sim_dirs': False,
            'resume_frm_prev': False,
            'output_variables': [],
            'timeout': 240
            },
        'Speed': {
            'max_cpus': maxcpus,
            'fast': 1,
            'slow': 0.5,
            'workdays': [],
            'start_work': '09:10',
            'end_work': '17:00',
            },
        'Logging': {
            'logfile': 'speclog.txt',
            'level': 'INFO'
            }
    }
    # if config file does not exist then create it...
    with open(config_file, 'w') as fconfig:
        json_dump(_default_config, fconfig, indent=2, sort_keys=True)

    return _default_config

def read_config_file(form):
    """
    # read widget settings used in the previous programme session from the config file, if it exists,
    # or create config file using default settings if config file does not exist
    """
    func_name =  __prog__ +  ' read_config_file'

    config_file = form.settings['config_file']
    form.w_max_cpus.setText('Max CPUs: ' + str(form.settings['maxcpus']))

    if exists(config_file):
        try:
            with open(config_file, 'r') as fconfig:
                config = json_load(fconfig)
        except (OSError, IOError) as err:
                print(err)
                return False
    else:
        config = _write_default_config_file(config_file, form.settings['maxcpus'])

    settings = form.settings

    # these attributes must be present TODO: does not check for 'cropName' in 'General' group
    # ================================
    required_vars = {'General':['config_check_interval', 'cropName'],
                'Simulations':['exepath', 'output_dir', 'output_variables', 'resume_frm_prev', 'sims_dir', 'timeout'],
                'Logging':['level', 'log_dir'],
                'Speed':['use_cpus', 'start_work', 'end_work']}

    for grp in required_vars:
        if grp in config:
            for key in required_vars[grp]:
                if key in config[grp]:

                    # log directory is specified in setup file therefore do not permit overwrite if invalid
                    # =====================================================================================
                    if key == 'log_dir':
                        log_dir = config[grp][key]
                        if isdir(log_dir):
                            settings[key] = config[grp][key]
                    else:
                        settings[key] = config[grp][key]
                else:
                    print(ERROR_STR + 'group {} requires setting {} in config file {}'.format(grp, key, config_file))
                    return False
        else:
            print(ERROR_STR + 'group {} is required in configuration file {}'.format(grp, config_file))
            return False

    # display inputs
    # ==============
    check_interval = config['General']['config_check_interval']
    form.w_chck_int.setText(str(check_interval))

    if settings['resume_frm_prev']:
        form.w_resume.setCheckState(2)
    else:
        form.w_resume.setCheckState(0)

    form.w_tim_out.setText(str(settings['timeout']))
    form.w_lbl02.setText(settings['exepath'])

    sims_dir = normpath(settings['sims_dir'])   # path to simulations
    form.w_lbl03.setText(sims_dir)
    form.study_defn = read_study_definition(sims_dir)

    form.w_strt_wrk.setText(settings['start_work'])
    form.w_end_wrk.setText(settings['end_work'])
    form.w_use_cpus.setText(str(settings['use_cpus']))
    form.settings = settings

    return True

def write_config_file(form):
    """
    write current selections to config file
    """
    if form.study_defn is None:
        crop_name = 'limited_data'
        print(WARNING_STR + 'study definition object not defined - will assume ' + crop_name)
        crop_name = 'limited_data'
    else:
        crop_name = form.study_defn['cropName']

    config_file = form.settings['config_file']
    sims_dir = form.w_lbl03.text()
    exe_path = form.w_lbl02.text()

    config = {
        'General': {
                'config_check_interval': int(form.w_chck_int.text()),
                'cropName': crop_name
            },
        'Simulations': {
            'output_dir': '',
            'sims_dir': sims_dir,
            'exepath': exe_path,
            'delete_sim_dirs': False,
            'resume_frm_prev': form.w_resume.isChecked(),
            'output_variables': [],
            'timeout': int(form.w_tim_out.text())
            },
        'Speed': {
            'use_cpus': int(form.w_use_cpus.text()),
            'fast': 1,
            'slow': 0.75,
            'workdays': [],
            'start_work': '09:10',
            'end_work': '17:00',
            },
        'Logging': {
            'log_dir': form.settings['log_dir'],
            'level': 'INFO'
            }
    }
    with open(config_file, 'w') as fconfig:
        json_dump(config, fconfig, indent=2, sort_keys=True)

    print('\nWrote configuration file: ' + config_file)

    return
