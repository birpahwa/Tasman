import sqlite3
import smtplib
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
import math
import os
from distutils.sysconfig import get_python_lib
from email import encoders
import pandas as pd

BASE_URL = 'https://data.usajobs.gov/api/'
PAGE_LIMIT = 500
endpoint="search?"
absPath='/Users/birparkash/PycharmProjects/Tasman/'


headers ={
    'Host':'data.usajobs.gov',
    'User-Agent':'recruiting@tasman.ai',
    'Authorization-Key':'9MbHa87/i38Y36f7BjnF/HnyPGEsOUWXJdInME0B99E='}


def db_connect(db_name):
    """Connects to database and returns a database connection object. """
    try:
        connection = sqlite3.connect(db_name)
        return connection
    except sqlite3.Error as e:
        print("Error while connectiong to sqlite", e)

def get_api_call(endpoint, params, BASE_URL, PAGE_LIMIT):
    """
    Makes a GET request with appropriate parameters, authentication,
    while respecting page and rate limits, and paginating if needed.

    Returns a JSON API response object.
    """
    try:
        r = requests.get(BASE_URL+endpoint, headers=params).json()

        TotalNoOfRecords = r.get('SearchResult').get('SearchResultCountAll')
        TotalPages = math.ceil(TotalNoOfRecords / PAGE_LIMIT)
        dict={}
        for i in range(1, TotalPages+1):
            url = BASE_URL+endpoint+'Page='+str(i)+'&ResultsPerPage='+str(PAGE_LIMIT)
            tempDict = requests.get(url, headers=params).json()
            dict = dict | tempDict
        return dict
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)




def extract_positions(titles, keywords):
    """
    Makes API calls for titles and keywords, parses the responses.

    Returns the values ready to be loaded into database. """
    TitleParam='PositionTitle='
    for el in titles:
        if TitleParam[-1]=="=":
            TitleParam=TitleParam+el
        else:
            TitleParam = TitleParam +","+el
    KeywordParam='Keyword='

    for el in keywords:
        if KeywordParam[-1]=="=":
            KeywordParam=KeywordParam+el
        else:
            KeywordParam = KeywordParam +","+el

    ResponseDict=get_api_call(endpoint+TitleParam+KeywordParam,headers, BASE_URL, PAGE_LIMIT)
    return ResponseDict


def prep_database(dbName):
    """Connects to database
    Drop Tables first for daily Refresh
    and creates tables if necessary. """
    try:
        connection=db_connect(dbName)
        cursor = connection.cursor()
        cursor.execute('''DROP TABLE IF EXISTS UserArea''')
        cursor.execute('''DROP TABLE IF EXISTS Location''')
        cursor.execute('''DROP TABLE IF EXISTS JobPosting''')

        cursor.execute('''CREATE TABLE IF NOT EXISTS UserArea (MatchedObjectId INTEGER PRIMARY KEY, JobSummary BLOB NOT NULL ON CONFLICT REPLACE NULL, WhoMayApplyName TEXT NOT NULL ON CONFLICT REPLACE NULL,WhoMayApplyCode TEXT NOT NULL ON CONFLICT REPLACE NULL, LowGrade TEXT NOT NULL ON CONFLICT REPLACE NULL, HighGrade TEXT NOT NULL ON CONFLICT REPLACE NULL,PromotionPotential TEXT NOT NULL ON CONFLICT REPLACE NULL, OrganizationCodes TEXT NOT NULL ON CONFLICT REPLACE NULL, Relocation TEXT NOT NULL ON CONFLICT REPLACE NULL, HiringPath TEXT NOT NULL ON CONFLICT REPLACE NULL,TotalOpenings TEXT NOT NULL ON CONFLICT REPLACE NULL,AgencyMarketingStatement TEXT NOT NULL ON CONFLICT REPLACE NULL,TravelCode Text NOT NULL ON CONFLICT REPLACE NULL, DetailStatusUrl Text NOT NULL ON CONFLICT REPLACE NULL,MajorDuties TEXT NOT NULL ON CONFLICT REPLACE NULL,Education TEXT NOT NULL ON CONFLICT REPLACE NULL,Requirements TEXT NOT NULL ON CONFLICT REPLACE NULL,Evaluations TEXT NOT NULL ON CONFLICT REPLACE NULL, HowToApply TEXT NOT NULL ON CONFLICT REPLACE NULL,WhatToExpectNext TEXT NOT NULL ON CONFLICT REPLACE NULL,RequiredDocuments TEXT NOT NULL ON CONFLICT REPLACE NULL,Benefits TEXT NOT NULL ON CONFLICT REPLACE NULL,BenefitsUrl TEXT NOT NULL ON CONFLICT REPLACE NULL,BenefitsDisplayDefaultText TEXT NOT NULL ON CONFLICT REPLACE NULL,OtherInformation TEXT NOT NULL ON CONFLICT REPLACE NULL,KeyRequirements TEXT NOT NULL ON CONFLICT REPLACE NULL,WithinArea TEXT NOT NULL ON CONFLICT REPLACE NULL, CommuteDistance TEXT NOT NULL ON CONFLICT REPLACE NULL,ServiceType TEXT NOT NULL ON CONFLICT REPLACE NULL,AnnouncementClosingType TEXT NOT NULL ON CONFLICT REPLACE NULL,AgencyContactEmail TEXT NOT NULL ON CONFLICT REPLACE NULL, AgencyContactPhone TEXT NOT NULL ON CONFLICT REPLACE NULL, SecurityClearance TEXT NOT NULL ON CONFLICT REPLACE NULL,DrugTestRequired TEXT NOT NULL ON CONFLICT REPLACE NULL,AdjudicationType TEXT NOT NULL ON CONFLICT REPLACE NULL,TeleworkEligible TEXT NOT NULL ON CONFLICT REPLACE NULL, RemoteIndicator TEXT NOT NULL ON CONFLICT REPLACE NULL,IsRadialSearch TEXT NOT NULL ON CONFLICT REPLACE NULL)''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS Location (LocationName TEXT NOT NULL ON CONFLICT REPLACE NULL,CountryCode TEXT NOT NULL ON CONFLICT REPLACE NULL,CountrySubDivisionCode TEXT NOT NULL ON CONFLICT REPLACE NULL, CityName TEXT NOT NULL ON CONFLICT REPLACE NULL, Longitude TEXT NOT NULL ON CONFLICT REPLACE NULL, Latitude TEXT NOT NULL ON CONFLICT REPLACE NULL,Primary Key(LocationName) )''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS JobPosting (PositionID TEXT NOT NULL ON CONFLICT REPLACE NULL, PositionTitle TEXT NOT NULL ON CONFLICT REPLACE NULL,PositionURI TEXT NOT NULL ON CONFLICT REPLACE NULL,ApplyURI TEXT NOT NULL ON CONFLICT REPLACE NULL, PositionLocationDisplay TEXT NOT NULL ON CONFLICT REPLACE NULL, OrganizationName TEXT NOT NULL ON CONFLICT REPLACE NULL, DepartmentName TEXT NOT NULL ON CONFLICT REPLACE NULL,JobCategoryName TEXT NOT NULL ON CONFLICT REPLACE NULL,JobCategoryCode TEXT NOT NULL ON CONFLICT REPLACE NULL, JobGradeCode TEXT NOT NULL ON CONFLICT REPLACE NULL, PositionScheduleName TEXT NOT NULL ON CONFLICT REPLACE NULL,PositionScheduleCode TEXT NOT NULL ON CONFLICT REPLACE NULL, PositionOfferingTypeName TEXT NOT NULL ON CONFLICT REPLACE NULL,PositionOfferingTypeCode TEXT NOT NULL ON CONFLICT REPLACE NULL, QualificationSummary BLOB NOT NULL ON CONFLICT REPLACE NULL, MinimumRemunerationRange TEXT NOT NULL ON CONFLICT REPLACE NULL, MaximumRemunerationRange TEXT NOT NULL ON CONFLICT REPLACE NULL, RemunerationRateIntervalCode TEXT NOT NULL ON CONFLICT REPLACE NULL, PositionStartDate TEXT NOT NULL ON CONFLICT REPLACE NULL, PositionEndDate TEXT NOT NULL ON CONFLICT REPLACE NULL, PublicationStartDate TEXT NOT NULL ON CONFLICT REPLACE NULL,ApplicationCloseDate TEXT NOT NULL ON CONFLICT REPLACE NULL,PositionFormattedDescriptionLabel  TEXT NOT NULL ON CONFLICT REPLACE NULL,PositionFormattedDescriptionLabelDescription  TEXT NOT NULL ON CONFLICT REPLACE NULL, MatchedObjectId INTEGER NOT NULL ON CONFLICT REPLACE NULL, Foreign key(PositionLocationDisplay) References Location(LocationName),Foreign key(MatchedObjectId) References UserArea(MatchedObjectId),Primary Key(PositionID))''')
    except sqlite3.Error as e:
        print("Error while connectiong to sqlite", e)

    finally:
        if connection:
            connection.commit()
            connection.close()

def parse_positions_and_load_data(dbName,response_json):
    """
    Parses a response JSON for wanted fields.
    And Load data directly to respective tables
    """
    try:
        response= response_json
        tempVariable = response.get('SearchResult', "empty").get('SearchResultItems', "empty")

        for i in range(len(tempVariable)):
            matchedObjectId = tempVariable[i].get('MatchedObjectId', "empty")
            jobSummary = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details',"empty").get('JobSummary', "empty")
            whoMayApplyName = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('WhoMayApply', "empty").get('Name', "empty")
            whoMayApplyCode = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('WhoMayApply', "empty").get('Code', "empty")
            lowGrade = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details',"empty").get('LowGrade', "empty")
            highGrade = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details',"empty").get('HighGrade', "empty")
            promototionPotential = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('PromotionPotential', "empty")
            organizationCode = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('OrganizationCodes', "empty")
            relocation = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details',"empty").get('Relocation', "empty")
            hiringPath = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details',"empty").get('HiringPath', "empty")
            totalOpenings = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details',"empty").get('TotalOpenings', "empty")
            agencyMarketingStatement = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('AgencyMarketingStatement', "empty")
            travelCode = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details',"empty").get('TravelCode', "empty")
            detailStatusUrl = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('DetailStatusUrl', "empty")
            majorDuties = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details',"empty").get('MajorDuties', "empty")
            education = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details',"empty").get('Education', "empty")
            requirements = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details',"empty").get('Requirements', "empty")
            evaluations = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details',"empty").get('Evaluations', "empty")
            howToApply = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details',"empty").get('HowToApply', "empty")
            whatToExpectNext = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('WhatToExpectNext', "empty")
            requiredDocuments = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('RequiredDocuments', "empty")
            benefits = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details',"empty").get('Benefits', "empty")
            benefitsUrl = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details',"empty").get('BenefitsUrl', "empty")
            benefitsDisplayDefaultText = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea',"empty").get('Details',"empty").get('BenefitsDisplayDefaultText', "empty")
            otherInformation = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('OtherInformation', "empty")
            keyRequirements = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('KeyRequirements', "empty")
            withinArea = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details',"empty").get('WithinArea', "empty")
            commuteDistance = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('CommuteDistance', "empty")
            serviceType = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details',"empty").get('ServiceType', "empty")
            announcementClosingType = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('AnnouncementClosingType', "empty")
            agencyContactEmail = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('AgencyContactEmail', "empty")
            agencyContactPhone = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('AgencyContactPhone', "empty")
            securityClearance = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('SecurityClearance', "empty")
            drugTestRequired = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('DrugTestRequired', "empty")
            adjudicationType = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('AdjudicationType', "empty")
            teleworkEligible = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('TeleworkEligible', "empty")
            remoteIndicator = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('Details', "empty").get('RemoteIndicator', "empty")
            isRadialSearch = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('UserArea', "empty").get('IsRadialSearch', "empty")

            # print(tempVariable[i].get('MatchedObjectDescriptor',"empty").get('UserArea',"empty").get('Details',"empty").get('KeyRequirements',"empty"))
            rowUser = (str(matchedObjectId), str(jobSummary), str(whoMayApplyName), str(whoMayApplyCode), str(lowGrade),str(highGrade), str(promototionPotential), str(organizationCode), str(relocation), str(hiringPath),str(totalOpenings), str(agencyMarketingStatement), str(travelCode), str(detailStatusUrl),str(majorDuties), str(education), str(requirements), str(evaluations), str(howToApply),str(whatToExpectNext), str(requiredDocuments), str(benefits), str(benefitsUrl),str(benefitsDisplayDefaultText), str(otherInformation), str(keyRequirements), str(withinArea),str(commuteDistance), str(serviceType), str(announcementClosingType), str(agencyContactEmail),str(agencyContactPhone), str(securityClearance), str(drugTestRequired), str(adjudicationType),str(teleworkEligible), str(remoteIndicator), str(isRadialSearch))
            sqlUser = '''insert or replace into UserArea (MatchedObjectId,JobSummary, WhoMayApplyName,WhoMayApplyCode, LowGrade, HighGrade, PromotionPotential, OrganizationCodes, Relocation, HiringPath, TotalOpenings, AgencyMarketingStatement, TravelCode, DetailStatusUrl, MajorDuties, Education, Requirements, Evaluations, HowToApply, WhatToExpectNext, RequiredDocuments, Benefits, BenefitsUrl, BenefitsDisplayDefaultText, OtherInformation, KeyRequirements, WithinArea, CommuteDistance, ServiceType, AnnouncementClosingType, AgencyContactEmail, AgencyContactPhone, SecurityClearance, DrugTestRequired, AdjudicationType, TeleworkEligible, RemoteIndicator, IsRadialSearch) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
            connection = db_connect(dbName)
            cursor = connection.cursor()
            cursor.execute(sqlUser, rowUser)

            for i in range(len(tempVariable)):
                locationName = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionLocation', "empty")[0].get('LocationName', "empty")
                countryCode = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionLocation', "empty")[0].get('CountryCode', "empty")
                countrySubDivisionCode = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionLocation', "empty")[0].get('CountrySubDivisionCode', "empty")
                cityName = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionLocation', "empty")[0].get('CityName', "empty")
                longitude = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionLocation', "empty")[0].get('Longitude', "empty")
                latitude = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionLocation', "empty")[0].get('Latitude', "empty")

                rowLocation = (str(locationName), str(countryCode), str(countrySubDivisionCode), str(cityName), str(longitude),str(latitude))
                # print(tempVariable[i].get('MatchedObjectDescriptor',"empty").get('PositionLocation',"empty")[0].get('Latitude',"empty"))
                sqlLocation = '''insert or replace into Location(LocationName,CountryCode,CountrySubDivisionCode,CityName,Longitude,Latitude) values(?,?,?,?,?,?)'''
                cursor.execute(sqlLocation, rowLocation)

            for i in range(len(tempVariable)):
                positionID = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionID', "empty")
                positionTitle = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionTitle', "empty")
                positionURI = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionURI', "empty")
                applyURI = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('ApplyURI', "empty")
                positionLocationDisplay = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionLocationDisplay', "empty")
                organizationName = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('OrganizationName', "empty")
                departmentName = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('DepartmentName', "empty")
                jobCategoryName = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('JobCategory', "empty")[0].get('Name', "empty")
                jobCategoryCode = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('JobCategory', "empty")[0].get('Code', "empty")
                jobGradeCode = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('JobGrade', "empty")[0].get('Code', "empty")
                jobScheduleName = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionSchedule', "empty")[0].get('Name', "empty")
                jobScheduleCode = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionSchedule', "empty")[0].get('Code', "empty")
                positionOfferingTypeName = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionOfferingType', "empty")[0].get('Name',"empty")
                positionOfferingTypeCode = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionOfferingType', "empty")[0].get('Code',"empty")
                qualificationSummary = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('QualificationSummary',"empty")
                remunerationMinimumSalary = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionRemuneration', "empty")[0].get('MinimumRange', "empty")
                remunerationMaximumSalary = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionRemuneration', "empty")[0].get('MaximumRange', "empty")
                remunerationRateIntervalCode = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionRemuneration', "empty")[0].get('RateIntervalCode', "empty")
                positionStartDate = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionStartDate',"empty")
                positionEndDate = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionEndDate', "empty")
                publicationStartDate = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PublicationStartDate',"empty")
                applicationCloseDate = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('ApplicationCloseDate',"empty")
                positionFormattedDescriptionLabel = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionFormattedDescription', "empty")[0].get('Label', "empty")
                positionFormattedDescriptionLabelDescription = tempVariable[i].get('MatchedObjectDescriptor', "empty").get('PositionFormattedDescription', "empty")[0].get('LabelDescription', "empty")
                matchedObjectId = tempVariable[i].get('MatchedObjectId', "empty")

                rowJob = (str(positionID), str(positionTitle), str(positionURI), str(applyURI), str(positionLocationDisplay),str(organizationName), str(departmentName), str(jobCategoryName), str(jobCategoryCode),str(jobGradeCode), str(jobScheduleName), str(jobScheduleCode), str(positionOfferingTypeName),str(positionOfferingTypeCode), str(qualificationSummary), str(remunerationMinimumSalary),str(remunerationMaximumSalary), str(remunerationRateIntervalCode), str(positionStartDate),str(positionEndDate), str(publicationStartDate), str(applicationCloseDate),str(positionFormattedDescriptionLabel), str(positionFormattedDescriptionLabelDescription),str(matchedObjectId))
                sqlJob = '''insert or replace into JobPosting (PositionID , PositionTitle ,PositionURI ,ApplyURI , PositionLocationDisplay , OrganizationName , DepartmentName ,JobCategoryName ,JobCategoryCode , JobGradeCode , PositionScheduleName ,PositionScheduleCode , PositionOfferingTypeName ,PositionOfferingTypeCode , QualificationSummary , MinimumRemunerationRange , MaximumRemunerationRange , RemunerationRateIntervalCode , PositionStartDate , PositionEndDate , PublicationStartDate ,ApplicationCloseDate,PositionFormattedDescriptionLabel  ,PositionFormattedDescriptionLabelDescription  , MatchedObjectId) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
                cursor.execute(sqlJob, rowJob)

    except sqlite3.Error as e:
        print("Error while connectiong to sqlite", e)
        #rows=cursor.execute('''select * from JobPosting''').fetchall()
        #for row in rows:
           # print(row)
    finally:
        if connection:
            connection.commit()
            connection.close()

def export_queryResults_To_csv(query_result,headings,outputPath):
    '''

    :param query_result:   Output of the query to be loaded in to csv file
    :param headings:        Headings of the Query columns
    :return:                return CSV file on the outputPath which contains lcation along with filename
    '''

    df = pd.DataFrame(query_result,columns=headings)
    df.to_csv(outputPath, index=False, header=True)
    

def run_analysis(dbName):
    """
    Runs 3 SQL queries to obtain results that could answer the following questions:
    1. How do *monthly* starting salaries differ across positions with different titles and keywords?
    
    2. Do (filtered) positions for which 'United States Citizens' can apply have a higher average salary than those
       that 'Student/Internship Program Eligibles' can apply for? (by month)
    3. What are the organisations that have most open (filtered) positions?

    Exports results of queries into CSV files in the `output_path` directory.
    """
    try:
        connection = db_connect(dbName)
        cursor = connection.cursor()

        #1. How do *monthly* starting salaries differ across positions with different titles and keywords?
        salRows=cursor.execute('''select PositionTitle,round((MinimumRemunerationRange/12),2)as MonthlyMinimumSalary from JobPosting group by PositionTitle''').fetchall()
        salColumns=['Position Title','MonthlyMinimumSalary']
        export_queryResults_To_csv(salRows,salColumns,absPath+"PositionSalaryPerMonth.csv")

        #2. Do (filtered) positions for which 'United States Citizens' can apply have a higher average salary than those
        #that 'Student/Internship Program Eligibles' can apply for? (by month)
        posRows=cursor.execute('''select a.PositionTitle from (select * from JobPosting j join UserArea u on j.MatchedObjectId=u.MatchedObjectId) a join (select * from JobPosting j join UserArea u on j.MatchedObjectId=u.MatchedObjectId) b on a.PositionID=b.PositionID where a.whoMayApplyName like '%United States Citizens%'  and b.whoMayApplyName like '%Student/Internship Program Eligibles%' and (a.MinimumRemunerationRange)/12 > (b.MinimumRemunerationRange+b.MaximumRemunerationRange)/2*12''').fetchall()
        PosColumns=['Position Title']
        export_queryResults_To_csv(posRows,PosColumns,absPath+"TitlesSalaryForUSCitizens.csv")


        #3. What are the organisations that have most open (filtered) positions?
        orgRows=cursor.execute('''select OrganizationName, OpenPositions from ( select OrganizationName, count(*) as OpenPositions,rank() over(order by count(*) desc) as rank  from JobPosting where DATETIME(PositionEndDate) >DATETIME('now') group by OrganizationName order by count(*) desc)where rank =1  ''').fetchall()  #where rank =1 (Most Openings)
        orgColumns = ["OrganizationName", "OpenPositions"]
        export_queryResults_To_csv(orgRows,orgColumns,absPath+"MostOpeningOrganization.csv")

    except sqlite3.Error as e:
        print("Error while connectiong to sqlite", e)
    finally:
        if connection:
            connection.commit()
            connection.close()

def send_reports(attachment_path_list):
    """
    Loops through present CSV files in reports_path,
    and sends them via email to recipient.

    Returns None
    """
    msg = MIMEMultipart()

    msg['From'] = 'bir.parkash1992@gmail.com'
    msg['To'] = 'yuvi138@gmail.com'
    msg['Subject'] = "CSV Reports for Visualization"
    body="CSV Reports for Visualization extracted by query results"
    msg.attach(MIMEText(body, 'plain'))
    FROM ='bir.parkash1992@gmail.com'
    TO ='yuvi138@gmail.com'
    PASS='**********'
    SUBJECT='CSV Reports for Visualization'

    for file_path in attachment_path_list or []:
            with open(file_path, "rb") as fp:
                part = MIMEBase('application', "octet-stream")
                part.set_payload((fp).read())
                # Encoding payload is necessary if encoded (compressed) file has to be attached.
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', "attachment; filename= %s" % os.path.basename(file_path))
                msg.attach(part)

    server = smtplib.SMTP("smtp.gmail.com:587")
    server.starttls()
    server.login(FROM, PASS)
    text = msg.as_string()
    server.sendmail(FROM, TO, text)
    server.quit()
            

if __name__ == "__main__":
    """
    Puts it all together, and runs everything end-to-end. 

    Feel free to create additional functions that represent distinct functional units, 
    rather than putting it all in here. 

    Optionally, enable running this script as a CLI tool with arguments for position titles and keywords. 
    """


    connection=prep_database('jobData') # Creating DataBase in Sqlite
    position=['Data Engineer', 'Data Analyst', 'Data Scientist'] #Position list to be passed in API Endpoints
    keyword=['Data','Analyst','Analysis'] #Keyword list to be passed in API Endpoints
    response_json=extract_positions(position,keyword) #get json object from api using endpoints
    parse_positions_and_load_data('jobData',response_json) #Parse json Data and load in the respective tables
    run_analysis('jobData') #Analysis on the Data and extract the respective csv for each analysis
    link1=[absPath+"MostOpeningOrganization.csv",absPath+"PositionSalaryPerMonth.csv",absPath+"TitlesSalaryForUSCitizens.csv"] #FilePaths to be attached
    send_reports(link1) # Sending Email with attached csv reports

