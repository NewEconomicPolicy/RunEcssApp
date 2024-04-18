#-------------------------------------------------------------------------------
# Name:
# Purpose:     Creates a GUI to run limited data files for Ecosse
# Author:      Mike Martin
# Created:     25/01/2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------
'''
Labels are as follows:
    w_lbl02 - ECOSSE exe
    w_lbl03 - simulations path
'''
#!/usr/bin/env python

__prog__ = 'SpecGui.py'
__version__ = '0.0.1'
__author__ = 's03mm5'

from os.path import normpath, isfile, isdir
from os import system
import sys

from PyQt5.QtCore import Qt
from PyQt5.QtGui import QPixmap
from PyQt5.QtWidgets import QLabel, QWidget, QApplication, QHBoxLayout, QVBoxLayout, QGridLayout, QLineEdit, \
                                                                                QPushButton, QCheckBox, QFileDialog

from initialise_funcs import read_config_file, write_config_file, initiation
from input_output_funcs import read_study_definition

STD_FLD_SIZE = 60

class Form(QWidget):

    def __init__(self, parent=None):

        super(Form, self).__init__(parent)

        # read settings
        initiation(self)

        # define two vertical boxes, in LH vertical box put the painter and in RH put the grid
        # define horizon box to put LH and RH vertical boxes in
        hbox = QHBoxLayout()
        hbox.setSpacing(10)

        # left hand vertical box consists of png image
        # ============================================
        lh_vbox = QVBoxLayout()

        # LH vertical box contains image only
        lbl20 = QLabel()
        pixmap = QPixmap(self.settings['fname_png'])
        lbl20.setPixmap(pixmap)

        lh_vbox.addWidget(lbl20)

        # add LH vertical box to horizontal box
        hbox.addLayout(lh_vbox)

        # right hand box consists of combo boxes, labels and buttons
        # ==========================================================
        rh_vbox = QVBoxLayout()

        # The layout is done with the QGridLayout
        grid = QGridLayout()
        grid.setSpacing(10)	# set spacing between widgets

        # line 2 - ECOSSE executable
        # ==========================
        irow = 2
        w_exe_file = QPushButton('Ecosse exe')
        helpText = 'Option to enable user to select an Ecosse .exe file'
        w_exe_file.setToolTip(helpText)
        grid.addWidget(w_exe_file, irow, 0)
        w_exe_file.clicked.connect(self.fetchExeFile)

        w_lbl02 = QLabel()
        grid.addWidget(w_lbl02, irow, 1, 1, 5)
        self.w_lbl02 = w_lbl02

        # path for simulations
        # ====================
        irow += 1
        w_sims_dir = QPushButton('Simulations dir')
        helpText = 'Option to enable user to select a spec simulations directory'
        w_sims_dir.setToolTip(helpText)
        grid.addWidget(w_sims_dir, irow, 0)
        w_sims_dir.clicked.connect(self.fetchSimsDir)

        w_lbl03 = QLabel()
        grid.addWidget(w_lbl03, irow, 1, 1, 5)
        self.w_lbl03 = w_lbl03

        irow += 2
        lbl05 = QLabel()
        grid.addWidget(lbl05, irow, 1)     # cosmetic

        # line 8:
        # ======
        irow += 3
        lbl08 = QLabel('Check Interval (secs)')
        lbl08.setAlignment(Qt.AlignRight)
        helpText = 'Configuration check interval (seconds), typically 60'
        lbl08.setToolTip(helpText)
        grid.addWidget(lbl08, irow, 0)

        w_chck_int = QLineEdit()
        w_chck_int.setFixedWidth(STD_FLD_SIZE)
        grid.addWidget(w_chck_int, irow, 1)    # row, column, rowSpan, columnSpan
        self.w_chck_int = w_chck_int

        lbl12 = QLabel('Timeout')
        lbl12.setAlignment(Qt.AlignRight)
        helpText = 'Time given to ECOSSE before cancelling a simulation ' + \
                        'e.g. Ecosse process can become hung trying to spin-up'
        lbl12.setToolTip(helpText)
        grid.addWidget(lbl12, irow, 2)

        w_tim_out = QLineEdit()
        w_tim_out.setFixedWidth(STD_FLD_SIZE)
        self.w_tim_out = w_tim_out
        grid.addWidget(w_tim_out, irow, 3)

        lbl14 = QLabel('Use CPUs')
        lbl14.setAlignment(Qt.AlignRight)
        helpText = 'Maximum number of CPUs to be used'
        lbl14.setToolTip(helpText)
        grid.addWidget(lbl14, irow, 4)

        w_use_cpus = QLineEdit('')
        w_use_cpus.setFixedWidth(STD_FLD_SIZE)
        grid.addWidget(w_use_cpus, irow, 5)
        self.w_use_cpus = w_use_cpus

        w_max_cpus = QLabel()
        grid.addWidget(w_max_cpus, irow, 6)
        self.w_max_cpus = w_max_cpus

        # Start and end work
        # ==================
        irow += 2
        lbl18a = QLabel('Start work')
        lbl18a.setAlignment(Qt.AlignRight)
        helpText = 'Time at which processing starts'
        lbl18a.setToolTip(helpText)
        grid.addWidget(lbl18a, irow, 0)

        w_strt_wrk = QLineEdit()
        w_strt_wrk.setFixedWidth(STD_FLD_SIZE)
        grid.addWidget(w_strt_wrk, irow, 1)
        self.w_strt_wrk = w_strt_wrk

        lbl18b = QLabel('End work')
        lbl18b.setAlignment(Qt.AlignRight)
        helpText = 'Time after which processing ceases'
        lbl18b.setToolTip(helpText)
        grid.addWidget(lbl18b, irow, 2)

        w_end_wrk = QLineEdit()
        w_end_wrk.setFixedWidth(STD_FLD_SIZE)
        grid.addWidget(w_end_wrk, irow, 3)
        self.w_end_wrk = w_end_wrk

        irow += 2
        lbl11 = QLabel()
        grid.addWidget(lbl11, 11, 1)     # cosmetic

        # row for action push buttons
        # ===========================
        irow += 2
        w_run_ecosse = QPushButton('Run Ecosse')
        helpText = 'Will create a configuration file for the spec_ltd_data.py script and run it.\n' \
                                                        + 'The spec_ltd_data.py script runs the ECOSSE program'
        w_run_ecosse.setToolTip(helpText)
        grid.addWidget(w_run_ecosse, 19, 0)
        w_run_ecosse.clicked.connect(self.runEcosse)

        w_resume = QCheckBox('Resume from previous run')
        helpText = 'Check this box if previous ECOSSE run was interrupted. The new ECOSSE run will generate a \n' + \
                   'SUMMARY.OUT file for simulation directories only where there is no existing SUMMARY.OUT\n' + \
                   'Leave unchecked to generate a SUMMARY.OUT for all simulation directories\n'
        w_resume.setToolTip(helpText)
        grid.addWidget(w_resume, 19, 1, 1, 2)
        self.w_resume = w_resume

        w_save = QPushButton('Save', self)
        w_save.setToolTip('save the GUI settings')
        grid.addWidget(w_save, irow, 5)
        w_save.clicked.connect(self.saveClicked)

        w_exit = QPushButton('Exit', self)
        grid.addWidget(w_exit, irow, 6)
        w_exit.clicked.connect(self.exitClicked)

        # add grid to RH vertical box
        rh_vbox.addLayout(grid)

        # vertical box goes into horizontal box
        hbox.addLayout(rh_vbox)

        # the horizontal box fits inside the window
        self.setLayout(hbox)

        # posx, posy, width, height
        self.setGeometry(300, 300, 690, 250)
        self.setWindowTitle('Global Ecosse - Run ECOSSE programme')

        # read and set values from last run
        # =================================
        if not read_config_file(self):
            print('Bad configuration file')
            self.close()
            sys.exit()

    def runEcosse(self):

        func_name =  __prog__ + ' runEcosse'

        #  make sure config settings are saved
        # ====================================
        write_config_file(self)

        # run the make simulations script
        # ===============================
        cmd_str = self.settings['python_exe'] + ' ' + self.settings['spec_run_py'] + ' ' + self.settings['config_file']
        system(cmd_str)

    def saveClicked(self):

        write_config_file(self)   # write last GUI selections

    def exitClicked(self):

        write_config_file(self)   # write last GUI selections
        self.close()

    def fetchExeFile(self):
        '''
        identify the ECOSSE executable to be used to generate the simulations
        '''
        fname = self.w_lbl02.text()
        fname, dummy = QFileDialog.getOpenFileName(self, 'Select exe', fname, 'ECOSSE .exe file (*.exe)')
        fname = normpath(fname)
        if fname != '':
            # TODO: check permissions
            if isfile(fname):
                self.w_lbl02.setText(fname)

    def fetchSimsDir(self):
        '''
        select the directory under which the directories containing the ECOSSE simulation files are to be found
        '''
        dirname = self.w_lbl03.text()
        dirname = QFileDialog.getExistingDirectory(self, 'Select directory', dirname)
        if dirname != '':
            # TODO: need to check this is a directory with read permissions
            if isdir(dirname):
                sims_dir = dirname
                self.settings['sims_dir'] = sims_dir
                self.study_defn = read_study_definition(sims_dir)
                self.w_lbl03.setText(normpath(sims_dir))

def main():

    app = QApplication(sys.argv)  # create QApplication object
    form = Form()     # instantiate form
    form.show()       # paint form
    sys.exit(app.exec_())   # start event loop

if __name__ == '__main__':
    main()
