import pandas as pd
import numpy as np
from datetime import datetime
import time
import os
import os.path
import webbrowser
import xlsxwriter
import openpyxl
from functools import reduce
import win32com.client as client
import yaml
import cursor
import threading
from tfs import TFSAPI
import sys
from pathlib import Path
import requests
from requests_ntlm import HttpNtlmAuth
import pwinput

import shutil
requests.packages.urllib3.disable_warnings()
pd.options.mode.chained_assignment = None  # default='warn'
pd.set_option("display.max_columns", None)

#Test comment for bi_weekly

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

def dnld_cedl_data(filename):
    # csv file, Endpoint access works
    # NOTE: this worked the 2nd time, 1st time it returned quickly - can check if filesize is < 5k and retry
    cedlurl = "https://tableau.wellsfargo.com/views/TIGOMasterView/CEDL-MasterView.csv"
    print("-----> downloading CEDL data from tableau...", end="")
    with Spinner():
        file_dnld(cedlurl, filename)

    print("done")

def file_dnld(src_url, dest):
    file_request = requests.get(src_url, auth=HttpNtlmAuth('AD-ENT\\' + username, password), verify=False, allow_redirects=True)

    if file_request.status_code == 200:
        open(dest, 'wb').write(file_request.content)
    else:
        print("-----> download failed - status code " + str(file_request.status_code))

def key(row):
    # used this function to establish a unique key for the consequence data

    if (row['Compliance Area'] == "Annual Recertification"):
        val = (row['Compliance Area']) + "-" + (row['Application Name'])
    elif (row['Compliance Area'] == "ECR/Expedited"):
        val = (row['Compliance Area']) + "-" + (row['Application Name']) + "-" + (row['Findings Detail'])
    else:
        val = (row['Compliance Area']) + "-" + (row['Application Name']) + "-" + (row['Findings Detail'])
    return val

def key2(row):
    if (row['FSR Type'] == "Annual Recertification"):
        val = (row['FSR Type']) + "-" + (row['Application Name'])
    elif (row['FSR Type'] == "ECR/Expedited"):
        val = (row['FSR Type']) + "-" + (row['Application Name']) + "-" + (row['CHG'])
    else:
        val = (row['FSR Type']) + "-" + (row['Application Name']) + "-" + (row['CHG'])
    return val

def subject(row):
    if row['Compliance Area'] == "ECR/Expedited":
        val = "ACTION REQUIRED: " + row['Dist ID'] + "-" + row['Compliance Area'] + " " + row['Findings Detail'] + "-" + row['Status '] + "-" + str(row['Compliance Date'])
    elif row['Compliance Area'] == "Annual Recertification":
        val = "ACTION REQUIRED: " + row['Dist ID'] + "-" + row['Compliance Area'] + "-" + row['Status '] + "-" + row['Compliance Date']
    elif row['Compliance Area'] == "SDE":
        val = "ACTION REQUIRED: " + row['Dist ID'] + "-" + "SDE Deferred Task(s)" + "-" + row['Status '] + "-" + row['Compliance Date']
    else:
        val = "ACTION REQUIRED: " + row['Dist ID'] + "-" + row['Compliance Area'] + "-" + row['Status '] + "-" + row['Compliance Date']
    return val

def comments(row):
    if (pd.notna(row['FSR Status'])):
        val = row['FSR Status']
    else:
        val = ""
    return val

def make_hyperlink(url, name):
    return '=HYPERLINK("{}", "{}")'.format(url, name)

def firstemail(row):
    if row['Minimum_Date'] == row['Maximum_Date']:
        val = row['Minimum_Date'].strftime('%#m/%d/%Y')
    else:
        val = row['Minimum_Date'].strftime('%#m/%d/%Y')
    return val

def secondemail(row):
    if row['Minimum_Date'] == row['Maximum_Date']:
        val = ""
    else:
        val = row['Maximum_Date'].strftime('%#m/%d/%Y')
    return val

def load_prefs(host):
    # Load user settings or request them if not present
    pref_file = "userprefs.yml"
    current_prefs = {}

    if os.path.isfile(pref_file):
        with open(pref_file, 'r') as file:
            prefs = yaml.safe_load(file)

        if host in prefs:
            current_prefs = prefs[host]

    updated_prefs, prefs_changed = pref_check(current_prefs)

    if prefs_changed:
        current_prefs = updated_prefs
        prefs = {host : updated_prefs}
        with open(pref_file, 'w') as file:
            yaml.dump(prefs, file)

    return current_prefs

def pref_check(prefs):
    pref_updated = False

    pref_items_with_defaults = {
        "Personal Access Token": ""
    }

    for key, val in pref_items_with_defaults.items():
        if key not in prefs:
            pref_updated = True
            answer = input("Enter " + key + " : ")
            prefs[key] = answer.strip()
            print(answer)

    return prefs, pref_updated

def file_cleanup(ifiles, dir, msg):
    # remove any of the versioned files > 5 days old
    print(msg, end="")
    with Spinner():
        for p in Path(dir).iterdir():
            mtime = datetime.fromtimestamp(p.stat().st_mtime)
            daysold = (datetime.today() - mtime).days
            # print("\n" + p.name + " : " + str(daysold) + " days old")
            for i in ifiles:
                if p.name.startswith(i) and daysold > 5:
                    os.remove(p)
                    # print("removing " + p.name + "\n")

    print("done")

def get_credentials():
    # get user/pass and test authentication
    pw = pwinput.pwinput(prompt="Enter AD-ENT Password for %s: " % username, mask='*')

    # Test the credentials
    wfhome = "https://portal.teamworks.wellsfargo.net/1/TechCentral/Pages/default.aspx"
    response = requests.get(wfhome, auth=HttpNtlmAuth('AD-ENT\\' + username, pw), verify=False, allow_redirects=True)

    return pw, response

def get_and_test_credentials():
    # Test Credentials - allow two tries
    passed = False
    for i in range(0, 2):
        pw1, r1 = get_credentials()
        if r1.status_code != 200:
            print("authentication failed, status code %s " % str(r1.status_code))
        else:
            print("Authentication Successful\n\n")
            passed = True
            break

    if not passed:
        exit(-1)

    return pw1

def get_sent_emails():
    # create an connection to outlook to pull notification emails from sent folder
    outlook = client.Dispatch("Outlook.Application").GetNamespace("MAPI").Folders['DTIAppSecSupport@wellsfargo.com']
    inbox = outlook.Folders.Item(4)  # "4" refers to 'Sent' folder
    messages = inbox.Items

    return messages

def get_cedl_data():
    # Get latest TIG CM Enforcement Status to join on UniqueID
    # Download if file is not there
    print("\n=========================== CEDL Data =========================")
    cedl_file = unc_easp_files + "CEDL-MasterView_" + datetime.today().strftime('%m%d') + ".csv"
    if not os.path.isfile(cedl_file):
        dnld_cedl_data(cedl_file)
    else:
        print("-----> using existing CEDL-MasterView_" + datetime.today().strftime('%m%d') + ".csv file")

    print("-----> reading CEDL data...", end="")
    with Spinner():
        consequence = pd.read_csv(cedl_file, low_memory=False)

        #print(consequence.keys())
        consequence = consequence[
            (consequence['L1 CIO'] == "Mehta, Chintan") & (consequence['Status '] != "Cleared") & (
                    consequence['Compliance Area'] != "Vulnerabilities") & (
                        consequence['Compliance Area'] != "DAST") & (
                    consequence['ASC Lead'] == 'Mangone, Christopher; Sanjeev, Mitra')]

        consequence.rename(columns={
            'App ID': "Dist ID"
        }, inplace=True)

        consequence['Dist ID'] = consequence['Dist ID'].str.upper()

        consequence['Compliance Date'] = consequence['Compliance Date'].astype('datetime64')
        consequence['Days Until Due'] = (consequence['Compliance Date'] - datetime.now()).dt.days

        # Remove column name 'Application Name' from TIG data due to inconsistencies
        consequence.drop(['App Name'], axis=1, inplace=True)

    print("done")
    #print(consequence.keys())
    #print(consequence)

    return consequence

def get_app_latest_data():

    print("\n========================= Digital Apps LATEST =========================")
    print("-----> reading Digital Apps_LATEST.xlsx...", end="")
    asc = pd.read_excel(unc_easp_files + "\\Digital Apps_LATEST.xlsx", sheet_name='All Digital EASP Apps',
                        engine='openpyxl')

    asc = asc[[
        'Application Name',
        'Dist ID',
        'ASC',
    ]]

    asc['Application Name'] = asc['Application Name'].str.upper()
    asc['Dist ID'] = asc['Dist ID'].str.upper()

    print("done\n")

    return asc


def get_fsr_status(token):
    # Get latest FSR Status
    # establish connection to TFS
    # path is server url
    path = "https://tfs.wellsfargo.net/tfs/cps_migration/"

    print("=========================== FSR Data =========================")
    print("-----> running All_FSR_Status_LATEST query...", end="")

    with Spinner():
        client = TFSAPI(path, pat=token, project="TCP11")

        # establish lists for workitems
        fsrId = []
        fsrType = []
        fsrStatus = []
        fsrCreatedDate = []
        CHG = []
        applicationName = []
        remedyId = []

        # Run query in selected folder
        query = client.run_query("My Queries/All_FSR_Status_LATEST")
        # result content raw data
        results = query.result
        results = query.workitems
        workitems = query.workitems

    print("done")

    print("-----> processing query data...", end="")
    with Spinner():
        # iterate through workitem results
        for item in workitems:
            fsrid = item.id
            fsrtype = item.fields["WF.TFS.Fields.FSRType"]
            fsrstatus = item.fields["WF.TFS.Fields.Status"]
            fsrcreateddate = item.fields["WF.TFS.Fields.FSRCreatedDate"]
            chg = item.fields["WF.TFS.Fields.CR"]
            applicationname = item["System.Title"]
            remedyid = item.fields["WF.TFS.Fields.EncodedRemedyID"]

            # iterate through and add to each list previously created
            fsrId.append(fsrid)
            fsrType.append(fsrtype)
            fsrStatus.append(fsrstatus)
            fsrCreatedDate.append(fsrcreateddate)
            CHG.append(chg)
            applicationName.append(applicationname)
            remedyId.append(remedyid)

        # pull appendeded list data into DataFrame
        data = {"FSR ID": fsrId,
                "FSR Type": fsrType,
                "FSR Status": fsrStatus,
                "FSR Created Date": fsrCreatedDate,
                "CHG": CHG,
                "Application Name": applicationName,
                "Remedy CI ID": remedyId}

        fsr = pd.DataFrame.from_dict(data)

        fsr['FSR Created Date'] = fsr['FSR Created Date'].astype('datetime64')

        fsr['Application Name'] = fsr['Application Name'].str.upper()

        fsr['FSR Type'] = fsr['FSR Type'].str.replace('Annual Review', "Annual Recertification")

        fsr['FSR Type'] = fsr['FSR Type'].str.replace('Emergency CR', "ECR/Expedited")

        maxFsr = fsr.groupby(["CHG", "Application Name", "FSR Type"])["FSR ID"].max().reset_index()  # get max/latest FSR

        fsrId = fsr[[
            "FSR ID",
            "FSR Status"
        ]]

        # get Max FSR status
        maxFsrId = reduce(lambda left, right: pd.merge(left, right, on=["FSR ID"], how="left"), [maxFsr, fsrId])

        maxFsrId['key'] = maxFsrId.apply(key2, axis=1)

        maxFsrId['FSR ID'] = maxFsrId['FSR ID'].astype(str)

        # create link to workitems
        maxFsrId["link"] = "https://tfs.wellsfargo.net/tfs/CPS_Migration/TCP11/_workitems/edit/" + maxFsrId[
            "FSR ID"].astype(str)

    print("done")
    return maxFsrId

def pull_sent_compliance_emails(messages):
    # pull sent folder information for adherence model emails notifications
    matches = ["ECR/Expedited", "Annual Recertification", "SDE"]  # compliance area strings we want to search for

    # create a list to store the email information
    subject = []
    senton = []

    print("\n=========================== EMail Data =========================")
    print("-----> searching for compliance emails...", end="")
    with Spinner():
        # iterate through the sent emails search for compliance area strings
        for message in messages:
            if any(x in str(message.subject) for x in matches):
                emailsubject = message.Subject
                emailsenton = message.SentOn
                dtime = pd.to_datetime(message.SentOn, utc=True).strftime('%#m/%d/%y')

                subject.append(emailsubject)
                #senton.append(emailsenton)
                senton.append(dtime)

        # create a dict of sent emails
        data = {
            "Subject": subject,
            "Sent On": senton
        }

        # convert dict to dataframe
        sent = pd.DataFrame.from_dict(data)

        #if sent['Sent On'] is not None:
        #    sent['Sent On'] = pd.to_datetime(sent['Sent On'], utc=True)
        #    sent['Sent On'] = sent['Sent On'].dt.strftime('%#m/%d/%y')

        # using groupby() function on Group column
        df = sent.groupby(['Subject'])

        # using agg() function on Date column
        df2 = df.agg(Minimum_Date=('Sent On', np.min), Maximum_Date=('Sent On', np.max)).reset_index()

        df2['Minimum_Date'] = df2['Minimum_Date'].astype('datetime64')

        df2['Maximum_Date'] = df2['Maximum_Date'].astype('datetime64')

        df2['1st Email Notification'] = df2.apply(firstemail, axis=1)

        df2['2nd Email Notification'] = df2.apply(secondemail, axis=1)

        df2 = df2[[
            'Subject',
            '1st Email Notification',
            '2nd Email Notification'
        ]]

    print("done")
    print(sent)

    return df2


def write_report(cq_data):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    print("\n\n**************************************************\n")
    print("writing report " + dstfile + "...", end="")

    with Spinner():
        writer = pd.ExcelWriter(unc_easp_reports + dstfile,
                                engine='xlsxwriter',
                                date_format="m/d/yyyy")

        # Convert the dataframe to an XlsxWriter Excel object.
        cq_data.to_excel(writer, sheet_name='Data', startrow=1, header=False, index=False)

        # Get the xlsxwriter workbook and worksheet objects.
        workbook = writer.book
        worksheet = writer.sheets['Data']

        (max_row, max_col) = cq_data.shape

        worksheet.autofilter(0, 0, max_row, max_col - 1)

        # Add a header format.
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'center',
            'fg_color': '#9A89D9',
            'border': 1})

        # Write the column headers with the defined format.
        for col_num, value in enumerate(cq_data.columns.values):
            worksheet.write(0, col_num, value, header_format)

        worksheet.set_column('A:A', 20)
        worksheet.set_column('B:B', 15)
        worksheet.set_column('C:C', 10)
        worksheet.set_column('D:D', 35)
        worksheet.set_column('E:E', 8)
        worksheet.set_column('F:F', 8)
        worksheet.set_column('G:G', 18)
        worksheet.set_column('H:H', 20)
        worksheet.set_column('I:I', 25)
        worksheet.set_column('J:J', 10)
        worksheet.set_column('K:K', 10)
        worksheet.set_column('L:L', 25)
        worksheet.set_column('M:M', 15)

        # Close the Pandas Excel writer and output the Excel file.
        writer.close()
        # writer.save()

    print("done")
    print("\n******************************************************\n")


# =================== Main =======================================
username = str(os.environ['USERNAME']).lower()
hostname = os.environ['COMPUTERNAME']
unc_easp_files = '//NCCCNSF701z1.wellsfargo.net/C_CFG_Groups/DTIASC/EASP Files/'
unc_easp_reports = '//NCCCNSF701z1.wellsfargo.net/C_CFG_Groups/DTIASC/EASP Reports/'
dstfile = "DTI Adherence Model " + datetime.today().strftime('%m%d%Y') + ".xlsx"

# Cleanup old files
cursor.hide()
file_cleanup(["DTI Adherence Model"], unc_easp_reports, "cleaning up old reports...")
cursor.show()

# Get user prefs and test password
userprefs = load_prefs(hostname)
password = get_and_test_credentials()

# settings to display all columns
#pd.set_option("display.max_rows", None)

# Getting current date and time using now()
start_time = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
print("Starting to build Adherence Model report :", end = "")
print(start_time)
cursor.hide()

# Merge previous consequence dataframe with Digital Apps
cq = get_cedl_data()
ac = get_app_latest_data()
consequence2 = reduce(lambda left,right: pd.merge(left,right,on=['Dist ID'], how='left'), [cq, ac])
consequence2['key'] = consequence2.apply(key, axis=1)

# Merge previous consequence dataframe with maxFsrID dataframe on CHG
fsr_frame = get_fsr_status(userprefs['Personal Access Token'])
consequence3 = reduce(lambda left,right: pd.merge(left,right,on=['key'], how='left'), [consequence2, fsr_frame])
consequence3['Comments'] = consequence3.apply(comments, axis=1)
consequence3.rename(columns={
    'Application Name_x': "Application Name"
}, inplace=True)
consequence3['Comments'] = consequence3.apply(lambda x: make_hyperlink(x['link'], x['Comments']), axis=1)
consequence3 = consequence3.sort_values(by='Compliance Date')
consequence3['Compliance Date'] = consequence3['Compliance Date'].dt.strftime('%#m/%d/%Y')
consequence3['Subject'] = consequence3.apply(subject, axis=1)


# Merge previous consequence dataframe with maxFsrID dataframe on CHG
msgs = get_sent_emails()
email_df = pull_sent_compliance_emails(msgs)
consequence4 = reduce(lambda left,right: pd.merge(left,right,on=['Subject'], how='left'), [consequence3, email_df])
consequence4 = consequence4[[
    'Compliance Area',
    'Status ',
    'Compliance Date',
    'Application Name',
    'Dist ID',
    'Days Until Due',
    'Findings Detail',
    'ASC',
    '1st Email Notification',
    '2nd Email Notification',
    'Comments',
    'Service Ticket'
]]
consequence4 = consequence4.replace(np.nan, " ")

# Write the report
write_report(consequence4)

# Getting current date and time using now()
end_time = datetime.now().strftime('%m/%d/%Y %H:%M:%S')

print("Adherence Model report complete: ", end = "")
print(end_time)
