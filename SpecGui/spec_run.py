#-------------------------------------------------------------------------------
# Name:        spec_run
# Purpose:     use multiprocessing to run Fortran Ecosse
# Author:      Mike Martin, based on module written by Mark Richards
# Created:     4 August 2017
# Description:  standard script for use in Global Ecosse
#-------------------------------------------------------------------------------
#
__author__ = 'soi698'
__prog__ = 'spec_run'
__version__ = '0.0'

from argparse import ArgumentParser
from datetime import datetime
from json import load as json_load
import math
from os.path import abspath, expanduser, expandvars, normpath, join, isfile, split, isdir
from os import getcwd, walk, chdir

from subprocess import Popen, PIPE, STDOUT
from sys import stdout, exit
from time import time, sleep
from multiprocessing import cpu_count

from socket import socket, AF_INET, SOCK_STREAM, gethostname
from copy import copy, deepcopy

from set_up_logging import set_up_logging

sleepTime = 5
WARN_STR = '*** Warning *** '
PROGRAM_ID = 'spec_run'
ERROR_STR = '*** Error *** '

CONFIG_RQRD_ATTRIBS = {'General': ['config_check_interval', 'cropName'],
                     'Simulations': ['delete_sim_dirs', 'exepath', 'output_variables', 'resume_frm_prev', 'sims_dir',
                                     'timeout'],
                     'Speed': ['end_work', 'fast', 'slow', 'start_work', 'use_cpus', 'workdays']}

class Instance(object):
    """
    Class to store info about a subprocess/instance of ECOSSE     
    """
    def __init__(self, inst, num, sim_dir, stdout_path, lat_id, lon_id, soil_id, start_time):
            """

            """
            self.inst = inst
            self.num = num      # instance number
            self.sim_dir = sim_dir
            self.stdout_path = stdout_path
            self.lat_id = lat_id
            self.lon_id = lon_id
            self.soil_id = soil_id
            self.start_time = start_time
            self.finished = False
            self.successful = None

class RunSites(object):
    """
    SPatial ECosse.
    """
    daynums = {'mon': 0, 'tue': 1, 'wed': 2, 'thu': 3, 'fri': 4, 'sat': 5, 'sun': 6}

    def __init__(self, configfile):

        if not isfile(configfile):
            print('Config file <{}> does not exist'.format(configfile))
            sleep(sleepTime)
            exit(0)

        self.configfile = configfile

        try:
            self.maxcpus = cpu_count()
        except:
            self.maxcpus = None

        self.start_time = None
        self.client = None

        TIMEOUT = 60
        HOST = gethostname()
        PORT = 65432  # the same port as used by the server
        with socket(AF_INET, SOCK_STREAM) as client:
            client.settimeout(TIMEOUT)
            try:
                client.connect((HOST, PORT))
            except ConnectionRefusedError as err:
                print(WARN_STR + str(err))
            else:
                client.sendall(b'Hello, world')
                try:
                    data = client.recv(1024)
                except (ConnectionAbortedError, ConnectionResetError) as err:
                    print(str(err))
                else:
                    print('Received', repr(data))

                self.client = client

        self._get_config()

    def _check_subprocs(self, instances):
        """
        Loops through all instances in the instances list.
        The return code for each instance is checked:
            If it is non-zero a warning is issued.
            If a zero (success) return code is issued the redirected ECOSSE output file for the instance is checked
                                                                    to see if the simulation completed successfully.
        """
        # Wait until the ecosse subprocesses have finished before proceeding
        # ==================================================================
        for inst in instances:
            retcode = inst.inst.poll()
            if retcode is not None:     # Process has finished.
                if retcode != 0:
                    self.lgr.error('Instance failed giving return code: {} (instance {}) ({}) '
                                                            .format(retcode, inst.num, inst.sim_dir))
                    inst.successful = False
                elif not self._sim_successful(inst):
                    self.lgr.error('Instance failed: (instance {}) ({}). Please check {} for details'
                                                    .format(inst.num, inst.sim_dir, inst.stdout_path))
                    inst.successful = False
                else:
                    self.lgr.info('Simulation sucessful: {} (instance {})'.format(inst.sim_dir, inst.num))
                    inst.successful = True
                inst.finished = True

    def _create_inst(self, instances, inst_num, sim_dir, ref_sys_flag):
        """

        """
        retcode = 1

        # Set the working directory for the ECOSSE exe
        # ============================================
        old_dir = getcwd()
        chdir(sim_dir)
        try:
            stdout_path = join(sim_dir, 'stdout.txt')
            new_inst = Popen(self.exe_path, shell = False, stdin = PIPE, stdout = open(stdout_path, 'w'),
                                                                                                stderr = STDOUT)
            # Provide the user input to ECOSSE
            # ================================
            if new_inst.stdin is not None:
                new_inst.stdin.write(bytes(self.cmd,"ascii"))
                new_inst.stdin.close()
            else:
                self.lgr.error('Instance is None')
        except OSError as err:
            self.lgr.error('Instance {} ({}) could not be launched: {}: {}'.format(inst_num, sim_dir, self.cmd, err))
            retcode = 0  # non-fatal error
        else:
            # deconstruct directory name to give unique identifiers
            # =====================================================
            directory = split(sim_dir)[1]
            parts = directory.split('_')
            
            # ============================
            if ref_sys_flag == 'WGS84':
                lat_id = parts[0].strip('lat')
                lon_id = parts[1].strip('lon')
    
                # Get rid of leading zeros
                # ========================
                lat_id = str(int(lat_id))
                lon_id = str(int(lon_id))

                soil_id = parts[3].lstrip('s')
                soil_id = soil_id.lstrip('0')
    
                instance = Instance(new_inst, inst_num, sim_dir, stdout_path, lat_id, lon_id, soil_id, time())
            else:
                grid_ref = parts[0]
                soil_id = parts[1].lstrip('s')
                soil_id = soil_id.lstrip('0')

                instance = Instance(new_inst, inst_num, sim_dir, stdout_path, grid_ref, grid_ref, soil_id, time())
            instances.append(instance)

        chdir(old_dir)
        return retcode

    def _check_simulations_performed(self, subdirs, num_sims):
        """ checks for simulations already performed """
        new_subdirs = []
        for subdir in subdirs:
            sim_dir_full = join(self.run_dir, subdir)
            summary_out = join(sim_dir_full, 'SUMMARY.OUT')
            if not isfile(summary_out):
                new_subdirs.append(subdir)

        num_new = len(new_subdirs)
        if num_new == 0:
            print('Simulations are complete: {} SUMMARY.OUT files exist - nothing to do'.format(num_sims))
        else:
            print('Number of simulation subdirectories before: {}\tafter: {}'.format(num_sims, num_new))

        return new_subdirs, num_new

    def _display_headers(self):
        """ Writes a header to the screen and logfile """
        print('')
        print('spec {0}'.format(__version__))
        print('')
        print('   ####   ###      #   #  ###  #####      ### #      ###   #### #####   # #')
        print('   #   # #   #     ##  # #   #   #       #    #     #   # #     #       # #')
        print('   #   # #   #     # # # #   #   #      #     #     #   #  ###  #####   # #')
        print('   #   # #   #     #  ## #   #   #       #    #     #   #     # #')
        print('   ####   ###      #   #  ###    #        ### #####  ###  ####  #####   # #')
        print('')
        self.lgr.info('Starting simulations')

    def _check_attribs(self, cfg, grp, cnfg_fn):
        """
        checks JSON config file conformance
        """
        for attrib in CONFIG_RQRD_ATTRIBS[grp]:
            if attrib not in cfg[grp]:
                mess = ERROR_STR + 'attribute {} required for group {} in config file {}'.format(attrib, grp, cnfg_fn)
                print(mess)
                self.lgr.critical(mess)
                exit(0)

    def _get_config(self, critical = True):
        """
        Reads settings from the config file
        Args:
        critical -  True if reading the config file is critically important e.g. first time it is being read)
                    False if failure to read can be tolerated e.g. when config file is being checked for updates
        """
        self.last_config_check = time()
        cnfg_fn = self.configfile

        try:
            with open(cnfg_fn, 'r') as fobj:
                self.config = json_load(fobj)
        except (OSError, IOError, NameError) as err:
            if critical:
                raise Exception(err)
            else:
                return False

        cfg = self.config

        # Logging settings - only required once
        # =====================================
        if critical:
            grp = 'Logging'
            if 'log_dir' in cfg[grp]:
                log_dir = cfg[grp]['log_dir']
            else:
                log_dir = getcwd()

            print('Logs will be written to: ' + log_dir)
            self.settings = {'log_dir': log_dir}
            set_up_logging(self, PROGRAM_ID)

        # General - this section determines Ecosse run mode
        # =================================================
        grp = 'General'
        self._check_attribs(cfg, grp, cnfg_fn)

        self.config_check_interval = cfg[grp]['config_check_interval']
        if cfg[grp]['cropName'] == 'limited_data':
            self.cmd = '{}\n\n{}\n2\n\n'.format(3, 'input.txt')
        else:
            self.cmd = '1\n\n\n'

        # Simulations settings
        # ====================
        grp = 'Simulations'
        self._check_attribs(cfg, grp, cnfg_fn)

        self.varnames = cfg[grp]['output_variables']

        self.exe_path = abspath(normpath(expanduser(expandvars(cfg[grp]['exepath']))))
        if not isfile(self.exe_path):
            mess = 'ECOSSE exe path does not exist: {}'.format(cfg[grp]['exepath'])
            self.lgr.critical(mess)
            exit(0)

        self.run_dir = abspath(normpath(expanduser(expandvars(cfg[grp]['sims_dir']))))
        if not isdir(self.run_dir):
            mess = 'Simulation directory does not exist: {}'.format(cfg[grp]['sims_dir'])
            self.lgr.critical(mess)
            sleep(sleepTime)
            exit(0)

        self.timeout = cfg[grp]['timeout']
        self.del_sim_dirs = cfg[grp]['delete_sim_dirs']
        self.resume_frm_prev = cfg[grp]['resume_frm_prev']

        # Speed settings
        # ==============
        grp = 'Speed'
        self._check_attribs(cfg, grp, cnfg_fn)

        self.requested_cpus = cfg[grp]['use_cpus']
        if self.maxcpus:
            if self.requested_cpus < self.maxcpus:
                self.cpus = self.requested_cpus
            else:
                self.cpus = self.maxcpus
        else:
            self.cpus = self.requested_cpus  # For better or for worse!

        self.fast = cfg[grp]['fast']
        self.fast = int(math.ceil(self.cpus * self.fast))

        self.slow = cfg[grp]['slow']
        self.slow = int(math.ceil(self.cpus * self.slow))

        self.workdays = cfg[grp]['workdays']
        self.workday_nums = [self.daynums[wrkday.lower()] for wrkday in self.workdays]
        if len(self.workday_nums) == 0:
            self.workday_nums = [-999]  # i.e. no days are workdays

        self.workstart = cfg[grp]['start_work'].split(':')
        self.workstart = [int(ival) for ival in self.workstart]

        self.workend = cfg[grp]['end_work'].split(':')
        self.workend = [int(ival) for ival in self.workend]

        return True

    def _get_max_inst(self):
        """
        return slow or fast operation
        """
        now = datetime.now()
        if now.weekday() not in self.workday_nums:
            max_inst = self.fast
        elif self._within_times(now, self.workstart[0], self.workstart[1], self.workend[0], self.workend[1]):
            max_inst = self.slow
        else:  # Must be outside working hours
            max_inst = self.fast
        return max_inst

    def _s2hms(self, seconds):
        """
        Converts time period in seconds to hours, minutes and seconds.
        """
        hours = int(seconds / 3600)
        seconds -= hours * 3600
        mins = int(seconds / 60)
        secs = seconds % 60
        return hours, mins, secs

    def _sim_successful(self, inst):
        """
        Searches the ecosse redirected output file for the phrase "simulation of cells completed"
        to check whether ECOSSE ran OK.
        """
        success = False
        # Read in ecosse redirected output
        try:
            with open(inst.stdout_path, "r") as outfile:
                for line in outfile:
                    if line.find('SIMULATION SUCCESSFULLY COMPLETED') != -1:
                        success = True
                        break
        except (OSError, IOError) as err:
            self.lgr.error('Unable to open ECOSSE redirection file. Cannot '
                           'determine if simulation was successful. {0}.'.format(err))
        return success

    def _update_config(self):
        """
        check to see if configuration file needs to be reread
        """
        if time() - self.last_config_check > self.config_check_interval:
            success = self._get_config(critical=False)
            self.last_config_check = time()

    def _update_progress(self, last_time, num_sims, instances, max_inst):
        """
        Update progress bar - all times in seconds
        """
        from datetime import timedelta
        if time() - last_time > 1.0:
            sec_elapsed = int(time() - self.start_time)
            time_elpsd = str(timedelta(seconds=sec_elapsed))

            ncomplete = self.completed
            pc_complete = max(float(ncomplete) / float(num_sims), 0.0000001)
            prcnt = round(pc_complete * 100.0, 1)

            t_left = int(sec_elapsed / pc_complete - sec_elapsed)
            time_left = str(timedelta(seconds=t_left))
            stdout.flush()

            line_frag = 'Done: {} ({}%)\t Fail: {}\tWarn: {} '.format(ncomplete, prcnt, self.failed, self.warn_count)

            line = ('\r' + line_frag +  'Taken: {}\tLeft: {}\tCPUs: {}'.format(time_elpsd, time_left, max_inst))
            padding = ' ' * (79 - len(line))
            line += padding
            stdout.write(line)
            last_time = time()

            # send message to parent
            # ======================
            if self.client is not None:
                bs = bytes(line_frag, 'utf-8')
                try:
                    self.client.sendall(bs)
                except OSError as err:
                    # print(str(err))
                    pass

        return last_time

    def _within_times(self, dt, starthour, startminute, endhour, endminute):
        """
        Determines if the time is within the specified boundaries
        dt    - [datetime object] the time to be checked
        """
        within = False
        if dt.hour > starthour and dt.hour < endhour:
            within = True
        elif dt.hour == starthour:
            if dt.minute >= startminute:
                within = True
        elif dt.hour == endhour and dt.minute <= endminute:
            within = True
        return within

    def run_ecosse(self):
        """

        """
        self._display_headers()
        sim_num = 0         # No. of sims that have run & are currently running
        self.completed = 0  # No. of sims that have completed successfully
        self.failed = 0     # No. of sims that failed to complete due to error
        self.warn_count = 0   # No. of warnings
        instances = []      # List containing a dict about each subprocess
        self.start_time = time()

        last_time = time()
        for directory, subdirs_raw, files in walk(self.run_dir):
            num_sims = len(subdirs_raw)
            max_isim = num_sims - 1
            break

        if num_sims == 0:
            print(ERROR_STR + 'no sub-directories under path ' + self.run_dir)
            return

        del directory
        del files

        # filter weather directories to leave only simulation directories
        # ===============================================================
        subdirs, subdirs_osgb = [], []
        for subdir in subdirs_raw:
            if subdir[0:5] == 'lat00':
                subdirs.append(subdir)
            elif subdir[:2].isupper():
                subdirs_osgb.append(subdir)
            else:
                # make sure directory is valid
                # ============================
                if subdir.find('_') >= 0:
                    eastng, nrthng = subdir.split('_')
                    subdirs_osgb.append(subdir)

        num_sims = len(subdirs)
        if num_sims == 0:
            num_sims = len(subdirs_osgb)
            if num_sims == 0:
                print(ERROR_STR + 'no lat/lon or OSGB sub-directories under path ' + self.run_dir)
                return

            ref_sys_flag = 'OSGB'
            subdirs = subdirs_osgb
        else:
            ref_sys_flag = 'WGS84'

        # skip simulations already performed if requested
        # ===============================================
        if self.resume_frm_prev:
            subdirs, num_sims = self._check_simulations_performed(subdirs, num_sims)
            if num_sims == 0:
                print(ERROR_STR + 'no simulations to be processed under path ' + self.run_dir)
                return

        print('Number of simulation subdirectories: {}'.format(num_sims))
        max_isim = num_sims - 1
        while True:
            self._update_config()
            max_inst = self._get_max_inst()
            last_time = self._update_progress(last_time, num_sims, instances, max_inst)

            # loop to check instances
            # =======================
            self._check_subprocs(instances)
            for inst in instances:
                if inst.finished:
                    if inst.successful:
                        pass
                    else:
                        self.lgr.error('Simulation failed: {0}'.format(inst.sim_dir))
                        self.failed += 1
                    instances.remove(inst)
                    self.completed += 1

                elif time() - inst.start_time > self.timeout:
                    # ECOSSE has probably hung trying to spin-up
                    # ==========================================
                    self.lgr.error('Simulation timed out: {}'.format(sim_dir))
                    if inst.inst.stdout is not None:
                        inst.inst.stdout.close()
                    inst.inst.terminate()
                    self.failed += 1
                    self.completed += 1
                    instances.remove(inst)

            if max_inst - len(instances) > 0:
                for i in range(max_inst - len(instances)):
                    if sim_num > max_isim:
                        break
                    sim_dir = join(self.run_dir, subdirs[sim_num])
                    self._create_inst(instances, sim_num, sim_dir, ref_sys_flag)
                    sim_num += 1
            else:
                sleep(0.05)
            if len(instances) == 0:
                break

        # Wait for the last remaining simulations to finish TODO: duplicate code
        # =================================================
        while len(instances) > 0:
            self._check_subprocs(instances)
            for inst in instances:
                if inst.finished:
                    if inst.successful:
                        pass
                    else:
                        self.failed += 1
                    self.completed += 1
                    instances.remove(inst)
                else:
                    if time() - inst.start_time > self.timeout:
                        # ECOSSE has probably hung trying to spin-up
                        # ==========================================
                        self.lgr.error('Simulation timed out: {}.'.format(sim_dir))
                        if inst.inst.stdout is not None:
                            inst.inst.stdout.close()
                        inst.inst.terminate()
                        self.failed += 1
                        self.completed += 1
                        instances.remove(inst)
            sleep(0.05)
            last_time = self._update_progress(last_time, num_sims, instances, max_inst)

        sleep(0.75) # delay so that result is reported
        last_time = self._update_progress(self.start_time, num_sims, instances, max_inst)
        self.lgr.info('\nSimulations completed.')

        if self.client is not None:
            self.client.close()

def main():
    """
    Entry point
    """
    argparser = ArgumentParser( prog = __prog__,
            description = 'Run ECOSSE in parallel for spatial simulations.',
            usage = '{} configfile'.format(__prog__))

    argparser.add_argument('configfile', help = 'Full path of the config file.')
    argparser.add_argument('--version', action = 'version', version = '{} {}'.format(__prog__, __version__),
                                                                        help = 'Display the version number.')
    args = argparser.parse_args()

    args.configfile = abspath(normpath(expanduser(expandvars(args.configfile))))

    sim = RunSites(args.configfile)
    sim.run_ecosse()

if __name__ == '__main__':
    main()

