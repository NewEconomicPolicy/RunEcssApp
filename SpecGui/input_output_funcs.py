#-------------------------------------------------------------------------------
# Name:
# Purpose:     read and write functions for processing ECOSSE results
# Author:      Mike Martin
# Created:     16/11/2020
# Licence:     <your licence>
#
#-------------------------------------------------------------------------------
#!/usr/bin/env python

__prog__ = 'input_output_funcs.py'
__version__ = '0.0.1'
__author__ = 's03mm5'

from json.decoder import JSONDecodeError
from json import load as json_load
from os.path import join, isfile, split

cropName =  'cropName'
REQUIRED_KEYS = list(['bbox', 'climScnr', cropName, 'resolution', 'futEndYr', 'futStrtYr', 'land_use', 'study'])

def _check_study_defn(study_defn_fname, study_defn):
    """
    validate study definition file contents
    TODO:   for a site specific simulation cropName attribute is present in study definition file
            this attribute is missing in limited data simulation therefore cropName is set to "limited_data"
    """
    for key in REQUIRED_KEYS:
        if key in study_defn:
            val = study_defn[key]
            if key == 'futEndYr' or key == 'futStrtYr':
                study_defn[key] = int(val)

            elif key == cropName:
                mess = '\n' + cropName + ' key is set to: ' + val + ' in study definition file - '
                if val == 'Unknown':
                    print(mess + 'will assume limited data simulation')
                    study_defn[cropName] = 'limited_data'
                else:
                    print(mess + 'will assume site specific simulation')

            elif key == 'resolution':
                if val == '' or val is None:
                    study_defn[key] = 0.0
                else:
                    try:
                        study_defn[key] = float(val)
                    except TypeError as err:
                        print(str(err) + ' invalid study resolution: ' + str(val))
                        return None
        else:
            if key == cropName:
                print(cropName + ' not in study definition file - will assume limited data simulation')
                study_defn[cropName] = 'limited_data'
            else:
                print(key + ' not in study definition - please check ' + study_defn_fname)
                return None

    return study_defn

def read_study_definition(sims_dir):

    func_name = __prog__ + ' read_study_definition'

    # identify the study definition file
    # ==================================
    base_dir, study = split(sims_dir)
    study_defn_file = join(base_dir, study + '_study_definition.txt')
    if not isfile(study_defn_file):
        print('Study definition file ' + study_defn_file + ' does not exist')
        return None

    # read
    # ====
    try:
        with open(study_defn_file, 'r') as fstudy:
            study_defn_raw = json_load(fstudy)

    except (JSONDecodeError, OSError, IOError) as err:
        print(err)
        return None

    grp = 'studyDefn'
    study_defn = _check_study_defn(study_defn_file, study_defn_raw[grp])

    return study_defn
