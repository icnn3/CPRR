import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from functools import reduce
import os
import openpyxl
from pathlib import Path
import requests
from requests_ntlm import HttpNtlmAuth
import webbrowser
import sys
import pwinput
import yaml
import time
import threading
import cursor
import string

requests.packages.urllib3.disable_warnings()
pd.options.mode.chained_assignment = None  # default='warn'

# settings to display all columns if working in jupyter notebook and want to view the data in the UI
pd.set_option("display.max_columns", None)
#pd.set_option("display.max_rows", None)

class Spinner:
    busy = False
    delay = 0.1
    @staticmethod
    def spinning_cursor():
        while 1:
            for cursor in '|/-\\': yield cursor

    def __init__(self, delay=None):
        self.spinner_generator = self.spinning_cursor()
        if delay and float(delay): self.delay = delay

    def spinner_task(self):
        while self.busy:
            sys.stdout.write(next(self.spinner_generator))
            sys.stdout.flush()
            time.sleep(self.delay)
            sys.stdout.write('\b')
            sys.stdout.flush()

    def __enter__(self):
        self.busy = True
        threading.Thread(target=self.spinner_task).start()

    def __exit__(self, exception, value, tb):
        self.busy = False
        time.sleep(self.delay)
        if exception is not None:
            return False

def load_previous_rpt():

    #Pull previous DTI Bi-Weely Metrics report to get any existing comments
    # Save all .xlsx files paths and modification time into paths
    print("\n========================= Previous Bi-Weekly Report =======================")

    paths = [(p.stat().st_mtime, p) for p in Path(unc_easp_reports).iterdir() if p.name.startswith("DTI Bi-Weekly")]

    # Sort them by the modification time
    paths = sorted(paths, key=lambda x: x[0], reverse=True)

    #print (paths)
    # Get the last modified file
    last = paths[0][1]
    print("-----> using last report data from " + last.name)

    # reading latest DTI Bi-Weekly Metrics ASC Comments
    print("-----> reading last Bi-Weekly report....", end="")
    with Spinner():
        vulnPrevious = pd.read_excel(last, sheet_name='All EASP Defects', engine='openpyxl')

        vulnPrevious = vulnPrevious[[
            'UniqueID',
            'ASC Comments'
        ]]

    print("done")

    return vulnPrevious


def load_totaldefects_data():
    print("\n========================= Total Defects =========================")
    # check if file already exists, if not - download it
    fname = "D-TotalDefects-Detail_" + datetime.today().strftime('%m%d') + ".csv"
    total_defects_file = unc_easp_files + fname
    if not os.path.isfile(total_defects_file):
        dnld_total_defects(total_defects_file)
    else:
        print("-----> using existing TotalDefects file " + fname )

    print("-----> reading TotalDefects data...", end="")
    with Spinner():
        vuln = pd.read_csv(total_defects_file, low_memory=False)
#        vuln = vuln.drop(['Hover', 'Total Results'], axis = 1)
        vuln = vuln[(vuln['Defect Status'] != "Resolved or Exception - No Action Required") | (vuln['Exception Number'].notnull())]

        #convert datatypes to datetime to be able to work with them and do analysis later
        vuln['Compliance Requirement'] = vuln['Compliance Requirement'].str.replace('Pending deployment date',"")
        vuln['Compliance Requirement'] = vuln['Compliance Requirement'].astype('datetime64[ns]')

        vuln['Exception Expiration Date'] = vuln['Exception Expiration Date'].fillna("")# replacing NaN values
        vuln['Exception Expiration Date'] = vuln['Exception Expiration Date'].astype('datetime64[ns]')

        vuln['Open Time'] = vuln['Open Time'].astype('datetime64[ns]')
        # vuln['Open Time'] = vuln['Open Time'].astype('string')
        # vuln['Open Time'] = vuln.apply(change_time, axis=1)

        # print(vuln['Open Time'].astype('datetime64[ns]'))
        # print(vuln['Open Time'])
        vuln['Close Time'] = vuln['Close Time'].astype('datetime64[ns]')
        vuln['MaxTimeToFix'] = vuln['MaxTimeToFix'].astype('datetime64[ns]')

        vuln.rename(columns={
            'Application Name (AppOne)':'Application Name',
            'Application Status (AppOne)': "Application Status"
        }, inplace=True)

        vuln['Severity'] = vuln['Severity'].str.upper()
        vuln['Application Name'] = vuln['Application Name'].str.upper()
        vuln['App Name'] = vuln['App Name'].str.upper()

    print("done")
    return vuln


def load_cedl_data():
    # Get latest TIG CM Enforcement Status to join on UniqueID
    # Download if file is not there
    print("\n=========================== CEDL Data =========================")
    cedl_file = unc_easp_files + "CEDL-MasterView_" + datetime.today().strftime('%m%d') + ".csv"
    if not os.path.isfile(cedl_file):
        dnld_cedl_data(cedl_file)

    print("-----> reading CEDL data...", end="")
    with Spinner():
        consequence = pd.read_csv(cedl_file, low_memory=False)
        consequence.columns = [c.strip().replace('  ', ' ') for c in consequence.columns]

        consequence = consequence[(consequence['Status'] != "Cleared") & (consequence['Compliance Area'] == 'Vulnerabilities')]

        consequence.rename(columns = {
            'Findings Detail': "UniqueID",
            'Service Ticket': "TIG Service Ticket"
        }, inplace=True)

        consequence = consequence[[
            'UniqueID',
            'Status',
            'TIG Service Ticket',
        ]]

    print("done")

    return consequence


def load_apps_latest_data():
    # Latest Digital Apps supported list
    # Only data unique is the manually added "ASC" (from Spencer) column, other data is duplicate.  Can update in UI.

    print("\n========================= Digital Apps LATEST =========================")
    print("-----> reading Digital Apps_LATEST.xlsx...", end="")
    asc = pd.read_excel(unc_easp_files + "\\Digital Apps_LATEST.xlsx", sheet_name='All Digital EASP Apps', engine='openpyxl')

    asc['Application Name'] = asc['Application Name'].str.upper()

    asc = asc[[
        'ASC',
        'Application Name'
    ]]

    print("done")

    return asc


def dnld_total_defects(dfilename):
    # no changes, it's a csv file, endpoint call returns no data
    print("-----> downloading total defects from tableau...", end="")
    with Spinner():
        vulns = "D-TotalDefects-Detail.csv"
        #vulnsurl = "https://tableau.wellsfargo.com/views/ExecDash_SecureCode-Defects-BAU_Extract_WAM_PROD_New_0/D-TotalDefects-Detail/U542612@ENT.WFB.BANK.CORP/Chintan-Apps.csv"
        vulnsurl = user_prefs["TotalDefect Tableau URL"]
        file_dnld(vulnsurl, dfilename)

    print("done")
    #TODO: file is 41mb, reduce the report size


def dnld_cedl_data(filename):
    # csv file, Endpoint access works
    # NOTE: this worked the 2nd time, 1st time it returned quickly - can check if filesize is < 5k and retry
    cedlurl = "https://tableau.wellsfargo.com/views/TIGOMasterView/CEDL-MasterView.csv"
    print("-----> downloading CEDL data from tableau...", end="")
    with Spinner():
        file_dnld(cedlurl, filename)

    print("done")

def dnld_threadfix_data(filename):
    versionurl = "https://tableau.wellsfargo.com/views/CFSR-Consolidated_Extract_PROD/VersionData.csv"

    print("-----> downloading Threadfix data from tableau...", end="")
    with Spinner():
        file_dnld(versionurl, filename)

    print("done")

def dnld_servicenow_data(filename):
    # for now we're using the webbrowser for this, requires two calls 1) authenticates 2) do the download

    snURL = 'https://wellsfargoprod.servicenowservices.com'
    serviceNowUrl = "https://wellsfargoprod.servicenowservices.com/sys_report_template.do?CSV&jvar_report_id=" + user_prefs["ServiceNow Report ID"]

    # first open the service now page to all for authentication
    webbrowser.open(snURL, new=2)
    print("-----> waiting 30s for ServiceNow site to open")
    time.sleep(25) # wait for authen to complete

    # open download url for csv file
    print("-----> attempting to down csv file, save file as 'change_request_" + datetime.today().strftime('%m%d') + ".csv'")
    webbrowser.open(serviceNowUrl, new=2)
    print("-----> if failed to download, use the link below to manually download")
    print("-----> " + serviceNowUrl)
    print("-----> *********")
    cursor.show()
    input("Press [ENTER] to continue AFTER download completes")
    cursor.hide()


def file_dnld(src_url, dest):
    file_request = requests.get(src_url, auth=HttpNtlmAuth('AD-ENT\\' + username, password), verify=False, allow_redirects=True)

    if file_request.status_code == 200:
        open(dest, 'wb').write(file_request.content)
    else:
        print("-----> download failed - status code " + str(file_request.status_code))


def load_ssat_data():
    # TODO: need a direct download from tableau.  Ryan is working with Scot Reichert.
    # link we have is https://tableau.wellsfargo.com/#/views/EASPAdherenceModel/EASPAdherenceModel but not the same data
    # that a weekly report sent by Scot called the -  "Secure Coding/Weekly CM Summary 9/20/2022" we rename for our py

    # make sure file exists
    print("\n===================== Weekly CM Summary - SSAT ====================")
    ssat_file = unc_easp_files + 'Weekly CM summary - SSAT.xlsx'
    if not os.path.isfile(ssat_file):
        print("-----> missing SSAT data file: " + ssat_file)
        print("-----> this file is emailed from Scot Reichert")
        exit(-1)

    print("-----> reading Weekly CM summary - SSAT.xlsx...", end="")
    ssat = pd.read_excel(ssat_file, sheet_name="Vuln Details", engine='openpyxl')

    #ssat = ssat[(ssat['Compliance Area'] == "Vulnerabilities")]

    ssat['Distributed App ID (TF)'] = ssat['Distributed App ID (TF)'].astype('str')
    #ssat['App ID'] = ssat['App ID'].astype('str')

    #ssat['Vulnerability Id'] = ssat['Vulnerability Id'].astype('int')

    ssat['Vulnerability Id'] = ssat['Vulnerability Id'].astype('str')

    # ssat['UniqueID'] = ssat['App ID'] + "-" + ssat['Vulnerability Id']
    ssat['UniqueID'] = ssat['Distributed App ID (TF)'] + "-" + ssat['Vulnerability Id']

    ssat.rename(columns={'Comments. ': 'Comments'}, inplace=True)

    ssat = ssat[[
        'UniqueID',
    #    'Primary ASC',
        # 'Comments. '
        'Comments'
    ]]

    print("done")

    return ssat

    #ssat['Primary ASC'] = ssat['Primary ASC'].str.split('[').str[0]


def load_ffiec_blast_data():
    # Get latest SCR/FFIEC/BLAST Tracking file
    # RYAN - IGNORE THIS ONE...NO LONGER USED
    scrFfiecBlast = pd.read_excel(unc_easp_files + "\\SCR_FFIEC_BLAST_Tracking.xlsx", engine='openpyxl')

    scrFfiecBlast.rename(columns={
        'Status': 'SCR/FFIEC/BLAST Status',
        'Remediation': 'SCR/FFIEC/BLAST Remediation',
        'Artifacts': 'SCR/FFIEC/BLAST Artifacts',
        'Date Submitted': 'Date Submitted SCR/FFIEC/BLAST Pre-Validation',
        'JIRA': 'AISE Team JIRA',
        'Notes': 'SCR/FFIEC/BLAST Notes'
    }, inplace=True)

    scrFfiecBlast = scrFfiecBlast[[
        'UniqueID',
        'SCR/FFIEC/BLAST Status',
        'SCR/FFIEC/BLAST Remediation',
        'SCR/FFIEC/BLAST Artifacts',
        'Date Submitted SCR/FFIEC/BLAST Pre-Validation',
        'AISE Team JIRA',
        'SCR/FFIEC/BLAST Notes'
    ]]

    print("load_ffiec_blast_data() done!")

    return scrFfiecBlast


def load_dast_retest_data():
    # Get latest DAST retesting Tracking file
    # RYAN - data comes from JIRA DAST ticketing - get a walkthrough from him

    print("\n========================== DAST Retest Data =========================")
    print("-----> reading DAST_Retest_Tracking.xlsx...", end="")
    dast = pd.read_excel(unc_easp_files + "\\DAST_Retest_Tracking.xlsx", engine='openpyxl')

    dast.rename(columns={
        'JIRA Ticket': 'DAST JIRA',
        'Status': 'DAST Retest Status',
        'Start Date': 'DAST Retest Start Date',
        'End Date': 'DAST Retest End Date'
    }, inplace=True)

    print("done")

    return dast


def load_exception_data():
    #Get the latest exception in progress data that team in tracking via sharepoint/xlsx Files
    # manual from Kelly Hargrove, only for in progress requests

    print("\n======================== DTI Security Exceptions ======================")
    print("-----> reading DTI_Security_Exceptions_Primary_Tracker.xlsx...", end="")
    exceptions = pd.read_excel(unc_easp_files + "\\DTI_Security_Exceptions_Primary_Tracker.xlsx", engine='openpyxl')

    # This file can be empty, code for null case
    count = exceptions.count()["UniqueID"]
    if count == 0:
        print("done\n\t ---> exception file contains NO data <-----")
        print(exceptions.count())
        print("-----> processing exception data...", end="")
        # insert dummy row to prevent errors later
        exceptions.loc[len(exceptions)] = ["N/A", "N/A", "N/A", '1/1/2100', 0, "N/A", "N/A", "N/A", "N/A", "N/A"]

    exceptions['Expiration Date'] = exceptions['Expiration Date'].fillna("")
    exceptions['Expiration Date'] = exceptions['Expiration Date'].astype('datetime64[ns]')

    #Filter exceptions in progress
    excInProgress = exceptions[(exceptions['Overall Exception Status'] != "Approved") & (exceptions['Overall Exception Status'] != "Completed") & (exceptions['Overall Exception Status'] != "Withdrawn") & (exceptions['Overall Exception Status'] != "Awaiting Closure in PGP")]

    excInProgress.rename(columns = {
        "Exception Number":"Exception In Progress",
        "Overall Exception Status": "Exception In Progress Status",
        "Expiration Date": "Exception In Progress Expiration Date"
    }, inplace=True)

    excInProgress = excInProgress[[
        "UniqueID",
        "Exception In Progress",
        "Exception In Progress Status",
        "Exception In Progress Expiration Date",
    ]]

    excApproved = exceptions[(exceptions['Overall Exception Status'] == "Approved")] #Filter approved exceptions

    excApproved.rename(columns = {
        'Overall Exception Status': "Exception Status"
    }, inplace=True)

    excApproved = excApproved[[
        'Exception Number',
        'Exception Status',
        'Expiration Date',
        'UniqueID',
        'Extension Filed?',
        'Extension Status',
        'Notes'
    ]]

    print("done")

    return excInProgress, excApproved


def load_threadfix_data():
    print("\n======================== Threadfix (VersionData) =====================")
    # Download if file is not there
    tf_file = unc_easp_files + "VersionData_" + datetime.today().strftime('%m%d') + ".csv"
    if not os.path.isfile(tf_file):
        dnld_threadfix_data(tf_file)

    print("-----> reading Threadfix data - VersionData.csv...", end="")
    with Spinner():
        version = pd.read_csv(tf_file, low_memory=False)

        # print(version.columns)
        version.rename(columns = {
            'Threadfix Repo ID': "App Name",
            'CR Number': "CHG",
            'Version Date (CST)': "Version Date"
        }, inplace=True)

        version['App Name'] = version['App Name'].str.upper()

        # Get the latest managed version
        version['Version Date2'] = pd.to_datetime(version['Version Date'], errors = 'coerce') # Forces out of bounds datetime to be accepted
        #version[version['Version Date2'].isna()]

        version = version[[
            'CHG',
            'App Name',
            'Version Date2'
        ]]

        idx = version.groupby(['App Name'])['Version Date2'].transform(max) == version['Version Date2']
        version = version[idx]

        version = version.groupby(["App Name"])["CHG"].last().reset_index()

    print("done")

    return version


def remove_non_ascii(a_str):
    ascii_chars = set(string.printable)

    return ''.join(
        filter(lambda x: x in ascii_chars, a_str)
    )


def load_servicenow_data():
    # Get latest ServiceNow CHG Data
    # possible alternative https://tableau.wellsfargo.com/#/views/SDLCComplianceScoreCard/SDLCScoreCardDetails.csv

    print("\n========================= ServiceNow Data ===========================")
    sn_file = unc_easp_files + "change_request_" + datetime.today().strftime('%m%d') + ".csv"
    if not os.path.isfile(sn_file):
        dnld_servicenow_data(sn_file)

    print("-----> reading ServiceNow change data...", end="")
    with Spinner():
        serviceNow = pd.read_csv(sn_file, encoding='cp1252', low_memory=False)

        # strip whitespace from all columns
        serviceNow = serviceNow.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)

        if 'ï»¿number' in serviceNow.columns:
            serviceNow.rename(columns={'ï»¿number': "number"}, inplace=True)

        # print(serviceNow.columns)

        serviceNow.rename(columns={
            "number": "CHG",
            "start_date": "Installation Start D/T",
            "end_date": "Installation End D/T",
            "state": "CHG State",
            "type": "CHG Type",
            "close_code": 'Close Code'
        }, inplace=True)

        serviceNow = serviceNow[[
            'CHG',
            'Installation Start D/T',
            'Installation End D/T',
            'CHG State',
            'CHG Type',
            'Close Code'
        ]]

        serviceNow['Installation Start D/T'] = serviceNow['Installation Start D/T'].astype('datetime64[ns]')
        serviceNow['Installation End D/T'] = serviceNow['Installation End D/T'].astype('datetime64[ns]')

        serviceNow = serviceNow.drop_duplicates()

    print("done")

    return serviceNow


def load_app_families_data():
    # App families list
    # Data doesn't look unique, may all be duplicates and not needed. No updates since March

    print("\n=========================== AppFamilies Data =========================")
    print("-----> reading AppFamilies_LATEST.csv...", end="")
    appFamilies = pd.read_csv(unc_easp_files + "\\AppFamilies_LATEST.csv")

    appFamilies['Application Name'] = appFamilies['Application Name'].str.upper()

    appFamilies = appFamilies[[
        'AppFamily',
        'Application Name'
    ]]

    print("done")

    return appFamilies


def chg(row):
    # Create new column "CHG" that uses if-else statement to get the CR/CHG from TF (SCR/FFIEC/BLAST) or
    # latest TF version CR/CHG

    if row['SCR Fix CR'] != "":
        val = row['SCR Fix CR']
    else:
        val = row['CHG']
    return val

def dueDate(row):
    # Logic calculates a due date based on Compliance Requirement,
    # Exception Expiration date or MaxTimeToFix if compliance requirement = "Pending deployment date"

    if pd.notna(row['Exception Expiration Date']):
        val = row['Exception Expiration Date']
    elif row['Compliance Requirement'] == 'Pending deployment date':
        val = row['MaxTimeToFix']
    else:
        val = row['Compliance Requirement']
    return val

def dueDateStatus(row):
    # Logic to determine if the Due Date is from Exception Expiration Date, MaxTimeToFix or Compliance Requirement

    if pd.notna(row['Exception Expiration Date']):
        val = "Exception Expiration Date"
    elif row['Compliance Requirement'] == 'Pending deployment date':
        val = "MaxTimeToFix"
    else:
        val = "Compliance Requirement"
    return val

# Logic to group coming due ranges based on Due Date column
def comingDue(row):
    if row['Days Until Due'] <= 30:
        val = "<=30"
    elif row['Days Until Due'] < 60:
        val = "31-60"
    elif row['Days Until Due'] < 90:
        val = "61-90"
    elif row['Days Until Due'] < 180:
        val = "91-180"
    else:
        val = ">180"
    return val


def upcomingChg(row):
    # Logic to find upcoming CHG information based on latest Threadfix managed version for vulnerabilities
    # Fixed in Pre-PROD but not in PROD ####

    if ((row['Installation Start D/T']) >= (row['Close Time'])) & (row['CHG State'] != "Canceled" ) & (row['CHG State'] != "Closed"):
        val = row['CHG']
    else:
        val = ""
    return val


def change_time(row):
    val = ''
    # if row['Open Time']:
    print(row['Open Time'])
    datestr = str(row['Open Time']).split('/')
    val = datestr[2] + '/' + datestr[0] + '/' + datestr[1]
    print(val)
    # ndate = datetime.strptime(datestr, '%m/%d/%y')
    # val = ndate.strftime('%y/%m/%d')

    return val

def upcomingChgStart(row):

    if ((row['Installation Start D/T']) >= (row['Close Time'])) & (row['CHG State'] != "Canceled" ) & (row['CHG State'] != "Closed"):
        val = row['Installation Start D/T']
    else:
        val = ""
    return val


def upcomingChgEnd(row):
    if (row['Installation Start D/T'] >= row['Close Time']) & (row['CHG State'] != "Canceled") & (row['CHG State'] != "Closed"):
        val = row['Installation End D/T']
    else:
        val = ""

    return val


def upcomingChgState(row):

    if ((row['Installation Start D/T']) >= (row['Close Time'])) & (row['CHG State'] != "Canceled" ) & (row['CHG State'] != "Closed"):
        val = row['CHG State']
    else:
        val = ""

    return val


def upcomingChgType(row):
    if ((row['Installation Start D/T']) >= (row['Close Time'])) & (row['CHG State'] != "Canceled" ) & (row['CHG State'] != "Closed"):
        val = row['CHG Type']
    else:
        val = ""
    return val


def upcomingChgCloseCode(row):
    if ((row['Installation Start D/T']) >= (row['Close Time'])) & (row['CHG State'] != "Canceled" ) & (row['CHG State'] != "Closed"):
        val = row['Close Code']
    else:
        val = ""
    return val


def actionRequired(row):
    # Identifies what action is needed by the Application Team based on previous logic

    if (row['Application Status'] == "Being Assembled"):
        val = "ASC team to submit attestation to TIG for vulnerability closed in Threadfix but Application status is 'Being Assembled'"
    elif ((row['Installation Start D/T']) >= (row['Close Time'])) & (row['CHG State'] != "Canceled" ) & ((row['Close Code'] != "Unsuccessful (Abandoned, not Attempted)") & (row['Close Code'] != "Unsuccessful (Rollback Performed)")):
        val = "No Action Required - Pending CHG Install"
    else:
        val = "Application Team needs to add a new Threadfix Managed Version with a CHG that installs AFTER the Close Time"
    return val


def remediationPlan(row):
    # States specific remediation plan information
    try:
        if ((row['Upcoming CHG Installation Start D/T']) <= (row['Compliance Requirement'])):
            val = "Pending " + (row['Upcoming CHG']) + " to install start on " + (row['Upcoming CHG Installation Start D/T'].strftime('%m/%d/%y %I:%M %p')) + " and install end on " + (row['Upcoming CHG Installation End D/T'].strftime('%m/%d/%y %I:%M %p'))
        elif ((row['Upcoming CHG Installation Start D/T']) >= (row['Compliance Requirement'])):
            val = "Pending " + (row['Upcoming CHG']) + " to install start on " + (row['Upcoming CHG Installation Start D/T'].strftime('%m/%d/%y %I:%M %p')) + " and install end on " + (row['Upcoming CHG Installation End D/T'].strftime('%m/%d/%y %I:%M %p'))
        else:
            val = "No remediation plan from Application Team"
    except ValueError as err:
        val = "No remediation plan from Application Team"

    return val


def actionRequiredOpenSs(row):
    if (pd.notna(row['Exception In Progress'])):
        val = "No Action Required - Policy exception in progress, not yet approved"
    else:
        val = "Application Team needs to: \n1) Run " + str(row['Scan Type']) + " scan to close in Threadfix and add new managed version.\n3) Deploy fix to production and close in ServiceNow before " + (row['Compliance Requirement'].strftime('%m/%d/%y'))
    return val


def remediationPlanSs(row):
    if (pd.notna(row['Exception In Progress'])):
        val = "Policy exception " + (row['Exception In Progress']) + " with exception status " + (row['Exception In Progress Status'])
    else:
        val = "No remediation plan from Application Team"
    return val


def actionRequiredDast(row):
    if (pd.notna(row['Exception In Progress'])):
        val = "Pending - Policy exception in progress, not yet approved"
    elif (pd.notna(row['DAST JIRA'])):
        val = "Pending DAST Retest"
    elif ((row['Severity'] == 'SEV-1') | (row['Severity'] == 'SEV-2')):
        val =  "Application Team needs to submit ticket to ASC Service Desk for DAST Retest"
    elif ((row['Severity'] == 'SEV-3') | (row['Severity'] == 'SEV-4')):
        val = "Application Team needs to submit ticket to ASC Service Desk for ASC review and Closure"
    else:
          val = "Application Team needs to submit ticket to ASC Service Desk for ASC review"
    return val


def remediationPlanDast(row):
    if (pd.notna(row['Exception In Progress'])):
        val = "Policy exception " + (row['Exception In Progress']) + " with exception status " + (row['Exception In Progress Status'])
    elif (pd.notna(row['DAST JIRA'])):
        val = "Pending - " + row['DAST JIRA'] + " to start on " + row['DAST Retest Start Date'].strftime('%m/%d/y') + " and ends on " + row['DAST Retest End Date'].strftime('%m/%d/%y')
    else:
        val = "No remediation plan from Application Team"
    return val


def actionRequiredScrFfBl(row):
    # NOT USED - from old Blast data that's no longer included

    if (pd.notna(row['Exception In Progress'])):
        val = "Policy exception in progress, not yet approved"
    elif (pd.notna(row['Date Submitted SCR/FFIEC/BLAST Pre-Validation'])):
        val = "Pending - Pre-validation/Credible Challenge request submitted to AISE Team"
    else:
        val = "Application needs to submit Pre-validation/Credible Challenge request to AISE Team Service Desk"
    return val


def remediationPlanScrFfBl(row):
    if (pd.notna(row['Exception In Progress'])):
        val = "Policy exception " + (row['Exception In Progress']) + " with exception status " + (row['Exception In Progress Status'])
    elif (pd.notna(row['Date Submitted SCR/FFIEC/BLAST Pre-Validation'])):
        val = "Pre-validation/Credible Challenge request submitted to AISE Team on " + row['Date Submitted SCR/FFIEC/BLAST Pre-Validation'].strftime('%m/%d/%y') + "; " + (str(row['AISE Team JIRA']))
    else:
        val = "No remediation plan from Application Team"
    return val


def actionRequiredExc(row):
    if (row['Days Until Due'] <= 60) & (row['Extension Filed?'] == "Yes"):
        val = "Exception expiring <= 60 days and Extension filed status: " + (row['Extension Status'])
    elif (row['Days Until Due'] <= 60) & (row['Extension Filed?'] == "No"):
        val = "Exception is expiring <= 60 days and no Extension filed. Application needs to file a policy Extension or confirm vulnerability has been remediated and the fix is in PROD."
    else:
        val = "No Action Required - Approved policy exception and expiration date > 60 days"
    return val


def remediationPlanExc(row):
    if (row['Days Until Due'] <= 60) & (row['Extension Filed?'] == "Yes"):
        val = row['Exception Number_x'] + " expiring and extension filed status: " + (row['Extension Status'])
    elif (row['Days Until Due'] <= 60) & (row['Extension Filed?'] == "No"):
        val = "No remediation plan from Application Team"
    else:
        val = "Approved Policy Exception"
    return val


def ascComments(row):
    # Function to merge SSAST comments

    if (pd.notna(row['Comments'])):
        val = row['Comments']
    elif (pd.notna(row['ASC Comments'])):
        val = row['ASC Comments']
    else:
        val = ""
    return val


# Function to merge SSAT ASC's
def assignedAsc(row):
    if (pd.notna(row['Primary ASC'])):
        val = row['Primary ASC']
    elif (pd.notna(row['ASC'])):
        val = row['ASC']
    else:
        val = ""
    return val


def dastAiseJira(row):
    # Function to pull in DAST/AISE jira ticket numbers
    #

    if (pd.notna(row['DAST JIRA'])):
        val = row['DAST JIRA']
    elif (pd.notna(row['AISE Team JIRA'])):
        val = row['AISE Team JIRA']
    else:
        val = ""
    return val


def merge_vuln_asc_appfamilies(vuln, asc, app_families):
    # Merge previously vulnerability data with asc assignments and app families
    vulns = reduce(lambda left, right: pd.merge(left, right, on='Application Name', how='left'),
           [vuln, asc, app_families])

    vulns['ASC Comments'] = vulns.apply(ascComments, axis=1)

    # vulnerabilities['ASC'] = vulnerabilities.apply(assignedAsc, axis=1)

    # NOT USED - Part of the old FFIEC_BLAST data
    #vulns['AISE/DAST JIRA'] = vulns.apply(dastAiseJira, axis=1)

    # Data cleanup
    vulns.rename(columns={
        'Exception Number_x': "Exception Number",
        'Distributed AppID (AppOne)': "Dist ID",
        'Primary Technical Manager (AppOne)': "Primary Technical Manager",
        'Level 4 App Port Ownr (AppOne)': "Level 4 App Port Ownr",
        'Level 3 CTO (AppOne)': "Level 3 CTO",
        'CIO_level 2 (AppOne)': "CIO_level 2",
        'CIO_level 1 (AppOne)': "CIO_level 1",
        'Asset Type (AppOne)': "Asset Type",
        'Remedy CI ID (AppOne)': "Remedy CI ID"
    }, inplace=True)

    vulns = vulns[[
        'UniqueID',
        'Status',
        'ASC Group',
        'ASC',
        'Application Name',
        'Dist ID',
        'App Name',
        'Scan Type',
        'Severity',
        'Security Defect Category',
        'Defect Status',
        'Open Time',
        'Close Time',
        'Compliance Requirement',
        'Remediation Plan',
        'Action Required',
        'Upcoming CHG',
        'Upcoming CHG Installation Start D/T',
        'Upcoming CHG Installation End D/T',
        'Upcoming CHG State',
        'Upcoming CHG Type',
        'Upcoming CHG Close Code',
        'Exception Number',
        'Exception Expiration Date',
        'Extension Filed?',
        'Extension Status',
        'Notes',
        'Exception In Progress',
        'Exception In Progress Status',
        'Exception In Progress Expiration Date',
        'TIG Service Ticket',
       # 'AISE/DAST JIRA',
        'ASC Comments',
        'Days Until CHG',
        'Due Date',
        'Due Date Status',
        'Days Until Due',
        'Coming Due',
        #'SCR/FFIEC/BLAST Status',
        #'SCR/FFIEC/BLAST Remediation',
        #'SCR/FFIEC/BLAST Artifacts',
        #'Date Submitted SCR/FFIEC/BLAST Pre-Validation',
        #'SCR/FFIEC/BLAST Notes',
        'DAST Retest Status',
        'DAST Retest Start Date',
        'DAST Retest End Date',
        'Fix Deploy Details',
        'Compliant',
        'Not Compliant Status',
        'Bug Bar Category',
        'Defect Deploy Details',
        'Primary Technical Manager',
        'Level 4 App Port Ownr',
        'Level 3 CTO',
        'CIO_level 2',
        'CIO_level 1',
        'App Type',
        'Application Status',
        'Asset Type',
        'MaxTimeToFix',
        'Remedy CI ID',
        'WF GUID',
        'link',
        'AppFamily'
    ]]

    return vulns

def merge_all(l_defects, l_cedl, l_ssat, l_eip, l_ea, l_dast_retest, l_prev_rpt, l_tf_data, l_servicenow):

    print("\n\nmerging data....", end="")

    # Merging the dataframes that can be merged by UniqueID
    l_vuln2 = reduce(lambda left, right: pd.merge(left, right, on='UniqueID', how='left'), [l_defects, l_cedl, l_ssat,
                                                                                          l_eip, l_ea,
                                                                                          l_dast_retest, l_prev_rpt])

    # Merge previously merged vulnerability data with latest Threadfix version on App Name
    l_vuln3 = reduce(lambda left, right: pd.merge(left, right, on='App Name', how='left'), [l_vuln2, l_tf_data])

    # Create new column to parse Threadfix Comment for SCR/FFIEC/BLAST to get the upcoming CHG#
    l_vuln3['SCR Fix CR'] = np.where((l_vuln3['Scan Type'] == "SCR") & (l_vuln3['Not Compliant Status'] == "Fixed in code"),
                                   l_vuln3['Fix Deploy Details'].str[:10], "")

    # search for CHG# and assign it
    l_vuln3['CHG'] = l_vuln3.apply(chg, axis=1)

    # Merge previously merged vulnerability data with ServiceNow CHG data
    l_vuln4 = reduce(lambda left, right: pd.merge(left, right, on='CHG', how='left'),
                   [l_vuln3, l_servicenow])

    l_vuln4['Due Date'] = l_vuln4.apply(dueDate, axis=1)

    l_vuln4['Due Date Status'] = l_vuln4.apply(dueDateStatus, axis=1)  # Reference column

    l_vuln4['Days Until Due'] = (l_vuln4['Due Date'] - datetime.now()).dt.days  # Based on Due Date Column calculation

    l_vuln4['Coming Due'] = l_vuln4.apply(comingDue, axis=1)

    #print("done")

    return l_vuln4

def get_exeception_in_progress(vulns):

    eip = vulns[(vulns['Exception In Progress'].notnull())]

    eip = eip[[
        'UniqueID',
        'Status',
        'ASC Group',
        'ASC',
        'Application Name',
        'Dist ID',
        'App Name',
        'Scan Type',
        'Severity',
        'Security Defect Category',
        'Defect Status',
        'Open Time',
        'Close Time',
        'Compliance Requirement',
        'Remediation Plan',
        'Action Required',
        #    'Upcoming CHG',
        #    'Upcoming CHG Installation Start D/T',
        #    'Upcoming CHG Installation End D/T',
        #    'Upcoming CHG State',
        #    'Upcoming CHG Type',
        #    'Upcoming CHG Close Code',
        'Exception Number',
        'Exception Expiration Date',
        'Extension Filed?',
        'Extension Status',
        'Notes',
        'Exception In Progress',
        'Exception In Progress Status',
        'Exception In Progress Expiration Date',
        'TIG Service Ticket',
        #    'AISE/DAST JIRA',
        'ASC Comments',
        #    'Days Until CHG',
        'Due Date',
        'Due Date Status',
        'Days Until Due',
        'Coming Due',
        #    'SCR/FFIEC/BLAST Status',
        #    'SCR/FFIEC/BLAST Remediation',
        #    'SCR/FFIEC/BLAST Artifacts',
        #    'Date Submitted SCR/FFIEC/BLAST Pre-Validation',
        #    'SCR/FFIEC/BLAST Notes',
        #    'DAST Retest Status',
        #    'DAST Retest Start Date',
        #    'DAST Retest End Date',
        'Fix Deploy Details',
        'Compliant',
        'Not Compliant Status',
        'Bug Bar Category',
        'Defect Deploy Details',
        'Primary Technical Manager',
        #    'Level 4 App Port Ownr',
        #    'Level 3 CTO',
        #    'CIO_level 2',
        #    'CIO_level 1',
        #    'App Type',
        'Application Status',
        'Asset Type',
        'MaxTimeToFix',
        #    'Remedy CI ID',
        #    'WF GUID',
        'link'
        #    'AppFamily'
    ]]

    return eip


def get_coming_due(vulns):
    due = vulns[(vulns['Days Until Due'] <= 30)]

    due = due[[
       'UniqueID',
       'Status',
       'ASC Group',
       'ASC',
       'Application Name',
       'Dist ID',
       'App Name',
       'Scan Type',
       'Severity',
       'Security Defect Category',
       'Defect Status',
       'Open Time',
       'Close Time',
       'Compliance Requirement',
       'Remediation Plan',
       'Action Required',
       'Upcoming CHG',
       'Upcoming CHG Installation Start D/T',
       'Upcoming CHG Installation End D/T',
       'Upcoming CHG State',
       'Upcoming CHG Type',
       'Upcoming CHG Close Code',
       'Exception Number',
       'Exception Expiration Date',
       'Extension Filed?',
       'Extension Status',
       'Notes',
       'Exception In Progress',
       'Exception In Progress Status',
       'Exception In Progress Expiration Date',
       'TIG Service Ticket',
      # 'AISE/DAST JIRA',
       'ASC Comments',
       'Days Until CHG',
       'Due Date',
       'Due Date Status',
       'Days Until Due',
       'Coming Due',
       #    'SCR/FFIEC/BLAST Status',
       #    'SCR/FFIEC/BLAST Remediation',
       #    'SCR/FFIEC/BLAST Artifacts',
       #    'Date Submitted SCR/FFIEC/BLAST Pre-Validation',
       #    'SCR/FFIEC/BLAST Notes',
       #    'DAST Retest Status',
       #    'DAST Retest Start Date',
       #    'DAST Retest End Date',
       'Fix Deploy Details',
       'Compliant',
       'Not Compliant Status',
       'Bug Bar Category',
       'Defect Deploy Details',
       'Primary Technical Manager',
       #    'Level 4 App Port Ownr',
       #    'Level 3 CTO',
       #    'CIO_level 2',
       #    'CIO_level 1',
       #    'App Type',
       'Application Status',
       'Asset Type',
       'MaxTimeToFix',
       #    'Remedy CI ID',
       #    'WF GUID',
       'link'
       #    'AppFamily'
    ]]

    return due


def get_all_easp(vulns):

    easp = vulns[[
        'UniqueID',
        'Status',
        'ASC Group',
        'ASC',
        'Application Name',
        'Dist ID',
        'App Name',
        'Scan Type',
        'Severity',
        'Security Defect Category',
        'Defect Status',
        'Open Time',
        'Close Time',
        'Compliance Requirement',
        'Remediation Plan',
        'Action Required',
        'Upcoming CHG',
        'Upcoming CHG Installation Start D/T',
        'Upcoming CHG Installation End D/T',
        'Upcoming CHG State',
        'Upcoming CHG Type',
        'Upcoming CHG Close Code',
        'Exception Number',
        'Exception Expiration Date',
        'Extension Filed?',
        'Extension Status',
        'Notes',
        'Exception In Progress',
        'Exception In Progress Status',
        'Exception In Progress Expiration Date',
        'TIG Service Ticket',
      #  'AISE/DAST JIRA',
        'ASC Comments',
        'Days Until CHG',
        'Due Date',
        'Due Date Status',
        'Days Until Due',
        'Coming Due',
        #'SCR/FFIEC/BLAST Status',
        #'SCR/FFIEC/BLAST Remediation',
        #'SCR/FFIEC/BLAST Artifacts',
        #'Date Submitted SCR/FFIEC/BLAST Pre-Validation',
        #'SCR/FFIEC/BLAST Notes',
        'DAST Retest Status',
        'DAST Retest Start Date',
        'DAST Retest End Date',
        'Fix Deploy Details',
        'Compliant',
        'Not Compliant Status',
        'Bug Bar Category',
        'Defect Deploy Details',
        'Primary Technical Manager',
        'Level 4 App Port Ownr',
        'Level 3 CTO',
        'CIO_level 2',
        'CIO_level 1',
        'App Type',
        'Application Status',
        'Asset Type',
        'MaxTimeToFix',
        'Remedy CI ID',
        'WF GUID',
        'link',
        'AppFamily'
    ]]

    return easp


def extract_chg_dataframe(vuln_data):
    ## Starting Step by analysis and breaking into different dataframes ##

    l_closed = vuln_data[((vuln_data['Close Time'].notnull()) & (
            vuln_data['Exception Number_x'].isnull()))]  # Vulnerabilitis Fixed in Pre-PROD but not fixed in PROD

    l_closed['Upcoming CHG'] = l_closed.apply(upcomingChg, axis=1)

    l_closed['Upcoming CHG Installation Start D/T'] = l_closed.apply(upcomingChgStart, axis=1)

    # convert datatypes to datetime to be able to work with them and do analysis later
    l_closed['Upcoming CHG Installation Start D/T'] = l_closed['Upcoming CHG Installation Start D/T'].fillna(
        "")  # replacing NaN values
    l_closed['Upcoming CHG Installation Start D/T'] = l_closed['Upcoming CHG Installation Start D/T'].astype('datetime64[ns]')

    # To get around the "Future Warning..." messages about date, don't use 'apply'
    #l_closed['Upcoming CHG Installation End D/T'] = l_closed.apply(upcomingChgEnd, axis=1)
    for index, row in l_closed.iterrows():
        l_closed['Upcoming CHG Installation End D/T'] = upcomingChgEnd(row)
        #print(l_closed['Upcoming CHG Installation End D/T'])

        # convert datatypes to datetime to be able to work with them and do analysis later
    l_closed['Upcoming CHG Installation End D/T'] = l_closed['Upcoming CHG Installation End D/T'].fillna(
        "")  # replacing NaN values
    l_closed['Upcoming CHG Installation End D/T'] = l_closed['Upcoming CHG Installation End D/T'].astype('datetime64[ns]')
    l_closed['Upcoming CHG State'] = l_closed.apply(upcomingChgState, axis=1)
    l_closed['Upcoming CHG Type'] = l_closed.apply(upcomingChgType, axis=1)

    l_closed['Upcoming CHG Close Code'] = l_closed.apply(upcomingChgCloseCode, axis=1)

    l_closed['Days Until CHG'] = (l_closed['Due Date'] - l_closed[
        'Upcoming CHG Installation Start D/T']).dt.days  # if before compliance want # of days to be positive
    l_closed['Days Until CHG'] = l_closed['Days Until CHG'].fillna(0).astype(
        int)  # convert "Days Until CHG" from float to int and replace NaN values

    #### END Logic to find upcoming CHG information based on latest Threadfix managed version for vulnerabilities Fixed in Pre-PROD but not in PROD ####

    l_closed['Action Required'] = l_closed.apply(actionRequired, axis=1)

    l_closed['Remediation Plan'] = l_closed.apply(remediationPlan, axis=1)

    return l_closed


def merge_dast_sast_sca(vuln_data):
    #### START Logic for vulnerabilities Not Fixed in Pre-PROD AND not Fixed in PROD ####

    # Creates separate dataframe for SAST/SCA remediation logic

    l_openSastSca = vuln_data[(vuln_data['Close Time'].isnull()) & (vuln_data['Exception Number_x'].isnull()) & (
                (vuln_data['Scan Type'] == "SAST") | (vuln_data['Scan Type'] == "SCA"))]

    if not l_openSastSca.empty:
        l_openSastSca['Action Required'] = l_openSastSca.apply(actionRequiredOpenSs, axis=1)
        l_openSastSca['Remediation Plan'] = l_openSastSca.apply(remediationPlanSs, axis=1)

    # Creates separate dataframe for DAST remediation logic
    l_openDast = vuln_data[
        (vuln_data['Close Time'].isnull()) & (vuln_data['Exception Number_x'].isnull()) & (vuln_data['Scan Type'] == "DAST")]

    if not l_openDast.empty:
        l_openDast['Action Required'] = l_openDast.apply(actionRequiredDast, axis=1)
        l_openDast['Remediation Plan'] = l_openDast.apply(remediationPlanDast, axis=1)
    else:
        print("\n*** empty dataframe, no open DAST vulns in the TotalDefects data ***\n")

    # Creates separate dataframe for DAST remediation logic
    l_openScrFfBl = vuln_data[(vuln_data['Close Time'].isnull()) & (vuln_data['Exception Number_x'].isnull()) & (
                (vuln_data['Scan Type'] == "SCR") | (vuln_data['Scan Type'] == "FFIEC") | (vuln_data['Scan Type'] == "BLAST"))]

    # Create dataframe for ThreadModeling
    l_openTM = vuln_data[
        (vuln_data['Close Time'].isnull()) & (vuln_data['Exception Number_x'].isnull()) & (
                    vuln_data['Scan Type'] == "ThreatModeling")]

    #l_openScrFfBl['Action Required'] = l_openScrFfBl.apply(actionRequiredScrFfBl, axis=1)
    #l_openScrFfBl['Remediation Plan'] = l_openScrFfBl.apply(remediationPlanScrFfBl, axis=1)

    # l_openVuln = pd.concat([l_openSastSca, l_openDast, l_openScrFfBl])

    # need to remove any dup columns before trying to concat
    l_openSastSca = l_openSastSca.loc[:, ~l_openSastSca.columns.duplicated()].copy()
    l_openDast = l_openDast.loc[:, ~l_openDast.columns.duplicated()].copy()
    l_openScrFfBl = l_openScrFfBl.loc[:, ~l_openScrFfBl.columns.duplicated()].copy()
    l_openTM = l_openTM.loc[:, ~l_openTM.columns.duplicated()].copy()

    l_openVuln = pd.concat([l_openSastSca, l_openDast, l_openScrFfBl, l_openTM])

    l_openVuln['Upcoming CHG'] = ""
    l_openVuln['Upcoming CHG Installation Start D/T'] = ""
    l_openVuln['Upcoming CHG Installation End D/T'] = ""
    l_openVuln['Upcoming CHG Type'] = ""
    l_openVuln['Upcoming CHG State'] = ""
    l_openVuln['Upcoming CHG Close Code'] = ""
    l_openVuln['Days Until CHG'] = ""

    print("done")

    return l_openVuln


def extract_exceptions_df(vuln_data):
    # Creates separate dataframe for vulnerabilities with approved policy exceptions
    print("processing exception data...", end="")

    l_exc = vuln_data[(vuln_data['Exception Number_x'].notnull())]
    l_exc['Action Required'] = l_exc.apply(actionRequiredExc, axis=1)
    l_exc['Remediation Plan'] = l_exc.apply(remediationPlanExc, axis=1)

    l_exc['Upcoming CHG'] = ""
    l_exc['Upcoming CHG Installation Start D/T'] = ""
    l_exc['Upcoming CHG Installation End D/T'] = ""
    l_exc['Upcoming CHG Type'] = ""
    l_exc['Upcoming CHG State'] = ""
    l_exc['Upcoming CHG Close Code'] = ""
    l_exc['Days Until CHG'] = ""

    print("done")

    return l_exc


def write_xls_report(exceptions_in_progress, coming_due, all_easp):
    # Create a Pandas Excel writer using XlsxWriter as the engine.

    print("\n\n**************************************************\n")
    print("writing report " + dstfile + "...", end="")

    with Spinner():
        writer = pd.ExcelWriter(unc_easp_reports + dstfile,
                                engine='xlsxwriter',
                                datetime_format='yyyy/mm/dd',
                                # datetime_format='m/d/yyyy hh:mm:ss',
                                date_format="yyyy/mm/dd")
                                # date_format = "m/d/yyyy")

        # Convert the dataframe to an XlsxWriter Excel object.
        coming_due.to_excel(writer, sheet_name='Coming Due 30 Days', startrow=1, header=False, index=False)
        exceptions_in_progress.to_excel(writer, sheet_name='Exceptions In Progress', startrow=1, header=False, index=False)
        all_easp.to_excel(writer, sheet_name='All EASP Defects', startrow=1, header=False, index=False)

        # Get the xlsxwriter workbook and worksheet objects.
        workbook = writer.book
        worksheet1 = writer.sheets['Coming Due 30 Days']
        worksheet2 = writer.sheets['Exceptions In Progress']
        worksheet3 = writer.sheets['All EASP Defects']

        (max_row, max_col) = coming_due.shape
        worksheet1.autofilter(0, 0, max_row, max_col - 1)

        (max_row, max_col) = exceptions_in_progress.shape
        worksheet2.autofilter(0, 0, max_row, max_col - 1)

        (max_row, max_col) = all_easp.shape
        worksheet3.autofilter(0, 0, max_row, max_col - 1)

        # Add a header format.
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'center',
            'fg_color': '#9A89D9',
            'border': 1})

        # Write the column headers with the defined format.
        for col_num, value in enumerate(coming_due.columns.values):
            worksheet1.write(0, col_num, value, header_format)

        worksheet1.set_column('A:A', 15)
        worksheet1.set_column('B:B', 15)
        worksheet1.set_column('C:C', 10)
        worksheet1.set_column('D:D', 25)
        worksheet1.set_column('E:E', 40)
        worksheet1.set_column('F:F', 10)
        worksheet1.set_column('G:G', 30)
        worksheet1.set_column('H:H', 10)
        worksheet1.set_column('I:I', 10)
        worksheet1.set_column('J:J', 30)
        worksheet1.set_column('K:K', 35)
        worksheet1.set_column('L:L', 18)
        worksheet1.set_column('M:M', 18)
        worksheet1.set_column('N:N', 18)
        worksheet1.set_column('O:O', 45)
        worksheet1.set_column('P:P', 45)
        worksheet1.set_column('Q:Q', 15)
        worksheet1.set_column('R:R', 15)
        worksheet1.set_column('S:S', 12)
        worksheet1.set_column('T:T', 10)
        worksheet1.set_column('U:U', 10)
        worksheet1.set_column('V:V', 12)
        worksheet1.set_column('W:W', 15)
        worksheet1.set_column('X:X', 10)
        worksheet1.set_column('Y:Y', 10)
        worksheet1.set_column('Z:Z', 15)
        worksheet1.set_column('AA:AA', 25)
        worksheet1.set_column('AB:AB', 15)
        worksheet1.set_column('AC:AC', 15)
        worksheet1.set_column('AD:AD', 10)
        worksheet1.set_column('AE:AE', 12)
        worksheet1.set_column('AF:AF', 12)
        worksheet1.set_column('AG:AG', 12)
        worksheet1.set_column('AH:AH', 12)

        # Write the column headers with the defined format.
        for col_num, value in enumerate(exceptionsInProgress.columns.values):
            worksheet2.write(0, col_num, value, header_format)

        worksheet2.set_column('A:A', 15)
        worksheet2.set_column('B:B', 15)
        worksheet2.set_column('C:C', 10)
        worksheet2.set_column('D:D', 25)
        worksheet2.set_column('E:E', 40)
        worksheet2.set_column('F:F', 10)
        worksheet2.set_column('G:G', 30)
        worksheet2.set_column('H:H', 10)
        worksheet2.set_column('I:I', 10)
        worksheet2.set_column('J:J', 30)
        worksheet2.set_column('K:K', 35)
        worksheet2.set_column('L:L', 10)
        worksheet2.set_column('M:M', 10)
        worksheet2.set_column('N:N', 12)
        worksheet2.set_column('O:O', 45)
        worksheet2.set_column('P:P', 45)
        worksheet2.set_column('Q:Q', 15)
        worksheet2.set_column('R:R', 15)
        worksheet2.set_column('S:S', 12)
        worksheet2.set_column('T:T', 10)
        worksheet2.set_column('U:U', 10)
        worksheet2.set_column('V:V', 12)
        worksheet2.set_column('W:W', 15)
        worksheet2.set_column('X:X', 10)
        worksheet2.set_column('Y:Y', 10)
        worksheet2.set_column('Z:Z', 15)
        worksheet2.set_column('AA:AA', 25)
        worksheet2.set_column('AB:AB', 15)
        worksheet2.set_column('AC:AC', 15)
        worksheet2.set_column('AD:AD', 10)
        worksheet2.set_column('AE:AE', 12)
        worksheet2.set_column('AF:AF', 12)
        worksheet2.set_column('AG:AG', 12)
        worksheet2.set_column('AH:AH', 12)

        # Write the column headers with the defined format.
        for col_num, value in enumerate(allEasp.columns.values):
            worksheet3.write(0, col_num, value, header_format)

        worksheet3.set_column('A:A', 15)
        worksheet3.set_column('B:B', 15)
        worksheet3.set_column('C:C', 10)
        worksheet3.set_column('D:D', 25)
        worksheet3.set_column('E:E', 40)
        worksheet3.set_column('F:F', 10)
        worksheet3.set_column('G:G', 30)
        worksheet3.set_column('H:H', 10)
        worksheet3.set_column('I:I', 10)
        worksheet3.set_column('J:J', 30)
        worksheet3.set_column('K:K', 35)
        worksheet3.set_column('L:L', 10)
        worksheet3.set_column('M:M', 10)
        worksheet3.set_column('N:N', 12)
        worksheet3.set_column('O:O', 45)
        worksheet3.set_column('P:P', 45)
        worksheet3.set_column('Q:Q', 15)
        worksheet3.set_column('R:R', 15)
        worksheet3.set_column('S:S', 12)
        worksheet3.set_column('T:T', 10)
        worksheet3.set_column('U:U', 10)
        worksheet3.set_column('V:V', 12)
        worksheet3.set_column('W:W', 15)
        worksheet3.set_column('X:X', 10)
        worksheet3.set_column('Y:Y', 10)
        worksheet3.set_column('Z:Z', 15)
        worksheet3.set_column('AA:AA', 25)
        worksheet3.set_column('AB:AB', 15)
        worksheet3.set_column('AC:AC', 15)
        worksheet3.set_column('AD:AD', 10)
        worksheet3.set_column('AE:AE', 12)
        worksheet3.set_column('AF:AF', 12)
        worksheet3.set_column('AG:AG', 12)
        worksheet3.set_column('AH:AH', 12)

        # Close the Pandas Excel writer and output the Excel file.
        writer.close()
        # writer.save()

    print("done")
    print("\n******************************************************\n")


def get_credentials():
    # get user/pass and test authentication
    pw = pwinput.pwinput(prompt="Enter AD-ENT Password for %s: " % username, mask='*')

    # set environment var
    #print(f"http://{username}:{pw}@proxy.wellsfargo.com:8080")
    #os.environ['http_proxy'] = f"http://{username}:{pw}@proxy.wellsfargo.com:8080"
    #os.environ['https_proxy'] = f"http://{username}:{pw}@proxy.wellsfargo.com:8080"

    proxies = {"http": f"http://{username}:{pw}@proxy.wellsfargo.com:8080"}
               # "https": f"http://{username}:{pw}@proxy.wellsfargo.com:8080"}

    # Test the credentials
    wfhome = "https://portal.teamworks.wellsfargo.net/1/TechCentral/Pages/default.aspx"
    wfhome2 = "http://home.teamworks.wellsfargo.net/TECHNOLOGY"
    wfhome3 = "https://wellsfargo.sharepoint.com/sites/7000-0099/SitePages/Home.aspx"
    # response = requests.get(wfhome, auth=HttpNtlmAuth('AD-ENT\\' + username.strip(), pw.strip()), verify=False, allow_redirects=True)
    #response = requests.get(wfhome3, verify=False, allow_redirects=True, proxies=proxies)

    #return pw, response
    return pw, ""


def get_and_test_credentials():
    # Test Credentials - allow two tries
    passed = False
    pw1, r1 = get_credentials()
#    for i in range(0, 2):
#        pw1, r1 = get_credentials()
#        if r1.status_code != 200:
#            print("authentication failed, status code %s " % str(r1.status_code))
#        else:
#            print("Authentication Successful\n\n")
#            passed = True
#            break

#    if not passed:
#        exit(-1)

    return pw1


def load_prefs(host):
    # Load user settings or request them if not present
    pref_file = "userprefs.yml"
    current_prefs = {}

    if os.path.isfile(pref_file):
        with open(pref_file, 'r') as file:
            prefs = yaml.safe_load(file)

        if prefs != None and host in prefs:
            current_prefs = prefs[host]

    updated_prefs, prefs_changed = pref_check(current_prefs)

    if prefs_changed:
        current_prefs = updated_prefs
        # prefs = {host : updated_prefs}
        prefs[host] = updated_prefs
        with open(pref_file, 'w') as file:
            yaml.dump(prefs, file)

    return current_prefs


def pref_check(prefs):
    pref_updated = False

    pref_items_with_defaults = {
        "ServiceNow Report ID": "",
        "Outlook DSOP Folder Name": "DSOP",
        "TotalDefect Tableau URL":
            'https://tableau.wellsfargo.com/views/ExecDash_SecureCode-Defects-BAU_Extract_WAM_PROD_New_0/D-TotalDefects-Detail/srenner@ENT.WFB.BANK.CORP/Chintan-Apps.csv'}

    for key, val in pref_items_with_defaults.items():
        if key not in prefs:
            pref_updated = True
            answer = input("Enter " + key + " : ")
            prefs[key] = answer.strip()
            #print(answer)

    return prefs, pref_updated


def file_cleanup(ifiles, dir, msg, rdays):
    # remove any of the versioned files > 5 days old
    print(msg, end="")
    with Spinner():
        for p in Path(dir).iterdir():
            mtime = datetime.fromtimestamp(p.stat().st_mtime)
            daysold = (datetime.today() - mtime).days
            # print("\n" + p.name + " : " + str(daysold) + " days old")
            for i in ifiles:
                if p.name.startswith(i) and daysold > rdays:
                    os.remove(p)
                    # print("removing " + p.name + "\n")

    print("done")

# ============ Main - start ==========

# TODO: after downloads, check for expected colunns

#Set file names and variables
username = str(os.environ['USERNAME']).lower()
hostname = os.environ['COMPUTERNAME']
unc_easp_files = '//NCCCNSF701z1.wellsfargo.net/C_CFG_Groups/DTIASC/EASP Files/'
unc_easp_reports = '//NCCCNSF701z1.wellsfargo.net/C_CFG_Groups/DTIASC/EASP Reports/'
dstfile = "DTI Bi-Weekly Metrics " + datetime.today().strftime('%m%d%Y') + ".xlsx"

# Cleanup old files
cursor.hide()
dated_files = ["CEDL-MasterView", "change_request", "D-TotalDefects-Detail", "VersionData"]
rpt_files = ["DTI"]
file_cleanup(dated_files, unc_easp_files, "cleaning up old input files...", 1)
file_cleanup(rpt_files, unc_easp_reports, "cleaning up old reports...", 7)
cursor.show()

# Load user settings for DSOP, Tableau URL and ServiceNow report_id
user_prefs = load_prefs(hostname)

password = get_and_test_credentials()

# hide the cursor so the spinners work
cursor.hide()

# Getting current date and time using now()
start_time = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
print("Building DTI Bi-Weekly Metrics report, this will take a few minutes...: ", end = "")
print(start_time)


previous_rpt = load_previous_rpt() #old var is vulnPrevious
defects = load_totaldefects_data()  #old var is vuln
cedl = load_cedl_data() #old var consequence
ssat = load_ssat_data()

# RYAN - this is NOT needed or updated anymore
#ffiec = load_ffiec_blast_data() #old var is scrFfiecBlast

dast_retest = load_dast_retest_data() #old var is dast
exceptions_in_progress, exceptions_approved = load_exception_data() #old is excInProgress
app_latest = load_apps_latest_data() #old is asc

# No updates since March, may not be needed
app_families = load_app_families_data() #old is appFamilies

tf_data = load_threadfix_data()

service_now = load_servicenow_data()

# -------- data merging section ---------
merged_data = merge_all(defects, cedl, ssat, exceptions_in_progress,
                  exceptions_approved, dast_retest, previous_rpt, tf_data, service_now)

closed = extract_chg_dataframe(merged_data)
openVuln = merge_dast_sast_sca(merged_data)
exc = extract_exceptions_df(merged_data)

# need to remove any dup columns before trying to concat
closed = closed.loc[:,~closed.columns.duplicated()].copy()
openVuln = openVuln.loc[:,~openVuln.columns.duplicated()].copy()
exc = exc.loc[:,~exc.columns.duplicated()].copy()

vuln5 = pd.concat([closed, openVuln, exc])

vulnerabilities = merge_vuln_asc_appfamilies(vuln5, app_latest, app_families)
# ------- end merge section --------

# ------- tabs for the xls -------------
exceptionsInProgress = get_exeception_in_progress(vulnerabilities)
comingdue30 = get_coming_due(vulnerabilities)
allEasp = get_all_easp(vulnerabilities)
# -------- end tabs section ------------

write_xls_report(exceptionsInProgress, comingdue30, allEasp)

# Getting current date and time using now()
end_time = datetime.now().strftime('%m/%d/%Y %H:%M:%S')

print ("DTI Bi-Weekly Metrics report complete...: ", end = "")
print (end_time)
