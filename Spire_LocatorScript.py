# Project: Spire to Locator Contractor data compilation
# Create Date: 02/13/2020
# Last Updated: 04/01/2023
# Created by: [Removed Names]
# Purpose: To provide a clean set of MO East, MO West, and Alabama to locator company
# ArcGIS Version:   Pro 2.8
# Python Version:   3.6
# For a changelog of updates, visit the github at: [removed]
# -----------------------------------------------------------------------
# Import modules
import sys, arcpy, datetime, traceback
import os
import pandas
import geopandas
import shutil
import keyring
import requests
import base64
import regex as re
import urllib.request
from urllib.parse import urlparse
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.files.file import File
# This helps print statements generate as they are produced instead of at once.
class Unbuffered(object):
   def __init__(self, stream):
       self.stream = stream
   def write(self, data):
       self.stream.write(data)
       self.stream.flush()
   def writelines(self, datas):
       self.stream.writelines(datas)
       self.stream.flush()
   def __getattr__(self, attr):
       return getattr(self.stream, attr)
def arcgis_table_to_df(in_fc, input_fields=None, query=""):
    """Function will convert an arcgis table into a pandas dataframe with an object ID index, and the selected
    input fields using an arcpy.da.SearchCursor.
    :param - in_fc - input feature class or table to convert
    :param - input_fields - fields to input to a da search cursor for retrieval
    :param - query - sql query to grab appropriate values
    :returns - pandas.DataFrame"""
    # Set object id field name 
    OIDFieldName = arcpy.Describe(in_fc).OIDFieldName
    # If there are input fields entered...
    if input_fields:
        # add oid to input fields variable
        final_fields = [OIDFieldName] + input_fields
    # otherwise if no input fields supplied...
    else:
        # Utilize all fields
        final_fields = [field.name for field in arcpy.ListFields(in_fc)]
    # Iterate through rows to get a list of all data that is relevant
    data = [row for row in arcpy.da.SearchCursor(in_fc,final_fields,where_clause=query)]
    # Convert that list into a dataframe
    fc_dataframe = pandas.DataFrame(data,columns=final_fields)
    # Set an index based on the input object id
    fc_dataframe = fc_dataframe.set_index(OIDFieldName,drop=True)
    # Return a dataframe object
    return fc_dataframe
def copyFeature(shpName, sdeConnect, keepList, inputFC, sqlQ='#'):
    """
    This function creates a shapefile based on an input
    feature class within a Spire SDE while using a list of
    field names to keep.
    It optionally can take a SQL query to take only certain
    features from the original feature class.
    """
    # Set the workspace to the sdeConnect variable given. Get output is used as
    # a string to path avoids some issues.
    wsConnect = sdeConnect.getOutput(0)
    arcpy.env.workspace = wsConnect
    print("Set Environment to: {0}".format(wsConnect))
    # Empty field mapping object created
    fmap = arcpy.FieldMappings()
    # The input FC is added to the field mappings object
    fmap.addTable(inputFC)
    # Get all fields
    fields = {f.name: f for f in arcpy.ListFields(inputFC)}
    # Clean up field map based on keep list
    for fname, fld in fields.items():
        # Avoid required OID and Geometry fields as well as SHAPE field
        if fld.type not in ('OID', 'Geometry') and 'shape' not in fname.lower():
            # if the field name isn't something set to be kept...
          if fname not in keepList:
              # Delete it from the field mappings object
              fmap.removeFieldMap(fmap.findFieldMapIndex(fname))
    # A list of current Spire Directories and areas
    paths = ['SpireAL','MoEast','MoWest']
    # Set variable to the path declared in script
    # TO CHANGE WHAT DIRECTORY THIS CREATES THE BASE FILES IN, POINT THIS
    # VARIABLE TO DESIRED DIRECTORY.
    setPath = sdeTempPath
    # Global variables for the folder directories are created for use outside
    # the function
    global shpPath
    # Determine what SDE is being used and set shpPath to point to a matching
    # directory to store shapefiles in.
    if sdeConnect == sdeAL:
        print("Connecting to the {0} SDE.".format(paths[0]))
        shpPath = os.path.join(setPath, paths[0])
    elif sdeConnect == sdeMOE or sdeConnect == sdeMOEPoly:
        shpPath = os.path.join(setPath, paths[1])
        print("Connecting to the {0} SDE.".format(paths[1]))
    elif sdeConnect == sdeMOW or sdeConnect == sdeMOWPoly:
        shpPath = os.path.join(setPath, paths[2])
        print("Connecting to the {0} SDE.".format(paths[2]))
    # Prior to using the directory, test to see if it exists.
    # If it does not, create a new directory based on that name.
    if not os.path.exists(shpPath):
        os.mkdir(shpPath)
        print("No folder for shapefiles. A directory has been created at {0}.".format(shpPath))
    # Delete any existing shapefile to avoid overwrite issues
    doesEx = os.path.join(shpPath, shpName + ".shp")
    if arcpy.Exists(doesEx):
        arcpy.Delete_management(doesEx)
    # Create the new shapefile to be sent to locator company
    # newSHP is made global so it can be used in Distribution Main/Services/WO Polys
    global newSHP
    newSHP = arcpy.conversion.FeatureClassToFeatureClass(inputFC, shpPath, shpName,\
                                                sqlQ, fmap)
    print("Shapefile has been created in {0}.".format(newSHP))
    print("\n")
    # Delete inputs to prepare for next shapefile
    del shpName, keepList, inputFC, sdeConnect
    # return none
    return
def create_pdf(fc, path, fields):
    """
    Take input date, and feature class which contains the
    FIELDBOOKP path and copy the files using the FIELDBOOKP url to the
    designated path
    """
    # Get current date in proper format
    cur_date = datetime.date.today()
    # Perform time calc on date and get date without time
    change_date = (cur_date - datetime.timedelta(days=14))
    # Set SQL query for search
    query= fields[1] + ' <= date ' + "'" + str(cur_date) + "' AND " + fields[1] + '>= date ' + "'" + str(change_date) + "'"
    # Search for fieldbooks matching query
    with arcpy.da.SearchCursor(fc, fields, query) as cursor:
        # iterate through all rows
        for row in cursor:
            # If fieldbookp is not null and its url has a working file in it...
            if row[0] != 'None' and os.path.exists(row[0]):
                # Copy file to output folder
                shutil.copy2(row[0], path)
def get_fieldnote(feature, in_field, out_field):
    """
    Take an input shapefile with a FIELDBOOKP field. Create a new field called
    FieldNote and then enter just the pdf name from FIELDBOOKP into the 
    FieldNote path.
    """
    # Add a new field for holding the fieldnote
    arcpy.AddField_management(feature,out_field,"TEXT","#","#","200","#",\
                            "NULLABLE","NON_REQUIRED","#")
    # open a  cursor on the feature class
    with arcpy.da.UpdateCursor(feature, [in_field, out_field]) as cursor:
        # Iterate through all rows
        for row in cursor:
            # Identify if the row is null and needs revision
            if row[0] == " " or row[0] == "None" or row[0] is None:
                # If it is, set the row to a string for None
                row[1] = "None"
            # otherwise...
            else:
                # If the file name contains a forward slash...
                if "/" in row[0]:
                    # Replace all forward slashes with a backslash
                    field_replace = row[0].replace("/", "\\")
                    # Print it to be sure
                    # print(field_replace)
                    # Find the field name from the fixed path
                    row[1] = os.path.basename(field_replace)
                # Otherwise...
                else:
                    # Grab the file name
                    fieldnote_name = os.path.basename(row[0])
                    # Print it to be sure
                    # print(fieldnote_name)
                    # Assign it to the fieldNote
                    row[1] = fieldnote_name
            cursor.updateRow(row)
def isspatial(table_df, table_field, sp_df, location_field, region, output_path):
    """
    This function takes in a dataframe of service sketches from a table and
    service point feature class made into a geodataframe. It then finds sketches
    from the table that have a matching mxlocation to an existing service point
    and creates two text files based on the match. the _spatial file is created
    if there is a match found and the _nonspatial file is made for
    non matching records.
    
    Parameters
    ----------
    table_df : pandas dataframe
        A dataframe of a geodatabase table containing service information
    sp_df : geopandas dataframe
        A geodataframe from a service point feature class containing mxlocation
    region : String
        A string proving the region name. Valid uses are: SpireAL, MoEast, MoWest
    Returns Nothing
    """                
    # Create a new dataframe based on an intersecting selection of locations
    spatial_df = table_df[table_df[table_field].isin(sp_df[location_field])]
    # Send the new dataframe to output
    spatial_df.to_csv(os.path.join(output_path, "serviceinfo_spatial.txt"))
    # create a new dataframe based on opposite of intersection
    nospatial_df = table_df[~table_df[table_field].isin(sp_df[location_field])]
    # Send no spatial dataframe to csv
    nospatial_df.to_csv(os.path.join(output_path, "serviceinfo_nospatial.txt"))
    
def region_csv(input_df, service_fc, output_path, fieldname):
    """
    input_df is a cleaned pandas dataframe that will be joined with a target
    feature class from service_fc and then a csv of joined-only records
    will be output in the output_path directory.
    fieldname should be a string containing the MX location data
    """
    # Read the input feature class
    fc_df = geopandas.read_file(service_fc, usecols=[fieldname])
    # Merge
    df_merge = input_df[input_df['Location'].isin(fc_df[fieldname])]
    # df_merge = fc_df.merge(input_df, left_on=fieldname, right_on="Location")
    # Output to target location
    reg_csv = df_merge.to_csv(output_path, columns=['Location', 'Document', 'createdate'], index = False)
    # Return a csv path and object
    return reg_csv
def retrieve_url(path, url, location, filename, region, badcsv):
    """
    Go to a url and if it is a valid path, store the file
    with the filename parameter in the listed path and region
    folder. Otherwise, add the information to the badcsv file.
    """
    # Create the sharepoint connection
    sharepoint_pass = keyring.get_password("rjd_sharepointlogin", "email")
    credentials = UserCredential("email", sharepoint_pass)
    # Set path for writing new file
    filepath = os.path.join(path, region, filename)
    # If its a sharepoint url...
    if 'sharepoint' in url:
        # Try the following operation
        try:
            # Create a copy of the url file in target location
            with open(filepath, 'wb') as new_file:
                # print("Sharepoint file found at {0}. Storing file now.".format(url))
                File.from_url(url).with_credentials(credentials).download(new_file).execute_query()
        # If file/url not found...
        except Exception as e:
            print(str(e))
            print("Error on {0}".format(url))
            # Add its record to a csv of incorrect service cards
            with open(badcsv, 'a') as bad_file:
                bad_file.write(str(location) + "," + str(url) + "\n")
    # If the url is not a sharepoint url...
    else:
        # try to copy the file to the given path using the given filename
        try:
            urllib.request.urlretrieve(url, filepath)
        # If file can't be copied, print errors and send to bad csv
        except Exception as e:
            print("Error {0}.".format(e))
            print("URL location {0} not found.".format(url))
            #If the file can't be copied, add to the bad csv list
            with open(badcsv, 'a') as bad_file:
                bad_file.write(str(location) + "," + str(url))
def unsplit_service(feature, path, keepList):
    """
    This function takes in a feature class and a list of field names.
    It then creates a statisticis field list from the input list and
    unsplits the target feature class while applying the statistics field
    to keep needed field information.
    
    The function then cleans up field names so the output field names are
    the same as the input field names.
    """
    # Create an empty list to collect statistics fields within
    stat_list = []
    # Get a list of valid fields from input feature
    valid_flds = [f.name for f in arcpy.ListFields(feature)] 
    # Loop through the keepList provided list and create the statistics field list
    for fieldname in keepList:
        if fieldname in valid_flds:
            #Create the empty list to fill in
            fld_list = []
            # Append the field name to the list
            fld_list.append(fieldname)
            # Append the statistic type to list
            fld_list.append("MAX")
            # Append the list to the master list
            stat_list.append(fld_list)
    #path for unsplit service
    unsplit_path = os.path.join(path, "unsplit_service")
    # Create the unsplit service
    unsplit_fc = arcpy.management.UnsplitLine(feature, unsplit_path, None, stat_list) 
    # Loop through all field names
    for field in arcpy.ListFields(unsplit_fc):
        # If the field isn't required
        if not field.required:
            #Change the field name to the name minus any "MAX" found
            arcpy.AlterField_management(unsplit_fc, field.name, field.name.replace("MAX_", ""))
    return unsplit_fc
# Set unbuffered mode
sys.stdout = Unbuffered(sys.stdout)
# ignore geopandas warnings for chained assignment--its an intentional decision
pandas.options.mode.chained_assignment = None
try:
    # set datetime variable
    date = datetime.datetime.now()
    sdeTempPath = r""
    if not os.path.exists(sdeTempPath):
        os.mkdir(sdeTempPath)
        print("Temporary directory not found. A new directory has been " + \
              "created at {0}.".format(sdeTempPath))
    else:
        print("The temp directory already exists at {0} and will be used.".format(sdeTempPath))
    # open log file for holding errors
    logName = "Log.txt"
    logPath = os.path.join(r'', logName)
    log = open(logPath,"a+")
    log.write("----------------------------" + "\n")
    log.write("----------------------------" + "\n")
    # write datetime to log
    log.write("Log: " + str(date) + "\n")
    log.write("\n")
    ## -----------------------Variable setup---------------------------------------
    # set arcpy environment to allow overwriting
    arcpy.env.overwriteOutput=True
    # Set environment to transport subtype descriptions
    arcpy.env.transferDomains = True
    #--------------------Create SDE Connections------------------------------------
    print("Creating database connections...")
    # SDE connetion information removed for confidentiality
    sdeMOE = arcpy.CreateDatabaseConnection_management()
    ##Create the MO West SDE Connection
    sdeMOW = arcpy.CreateDatabaseConnection_management()
    sdeAL = arcpy.CreateDatabaseConnection_management()
    #Mo East WO Polygons are stored in a different SDE than gas facilities.
    # This SDE connection sets that up.
    sdeMOEPoly = arcpy.CreateDatabaseConnection_management()
    sdeMOWPoly = arcpy.CreateDatabaseConnection_management()
    
    #--------------------Sign into Maximo for Service Card Downloads-----------
    # Print statement
    print("Connecting to Maximo...")
    # Create sign in string from keyring
    # Elements removed for confidentiality
    signin_string = "b':" + str(keyring.get_password("Maximo_RD", ""))
    # Set maximo sign in credentials as bytes
    maxauth = base64.b64encode(str.encode(signin_string))
    # Set host url
    host = 'hostname'
    # Create maximo headers
    headers = {'maxauth': maxauth, # BASIC AUTH = LOGIN: PASSWORD}
               'Accept': "application/json",
               'Content-Type': "application/json",
               'Allow-Hidden': "true",
               }
    # Set up parameters
    params = { 'lean':1,
              }
    # Send in the maximo request to sign in during session
    r = requests.post(host, headers=headers, verify=False)
    ###----------------------------AL Setup------------------------------------
    ########---------ROW Lines-------------------------------------------------
    shpName = "Right of Way Lines"
    inputFC = sdeAL.getOutput(0) + 'location'
    keepList = []
    copyFeature(shpName,sdeAL,keepList,inputFC)
    #####---------Services----------------------------------------------------
    shpName = "Services"
    inputFC = sdeAL.getOutput(0) + 'location'
    keepList = ['INSTALLDATE','MEASUREDLENGTH','LENGTHSOURCE','COATINGTYPE',\
              'PIPETYPE','NOMINALPIPESIZE','PIPEGRADE','PRESSURECODE',\
              'MATERIALCODE','LABELTEXT','TRANSMISSION_FLAG',\
              'LOCATIONDESCRIPTION','HIGHDENSITYPLASTIC','PROJECTYEAR',\
              'PROJECTNUMBER','SERVICETYPE','MANUFACTURER','LENGTH604',\
              'STREETADDRESS','MAINMATERIAL','MXLOCATION']
    copyFeature(shpName,sdeAL,keepList,inputFC)
    # Assign variable to hold shapefile location
    alSvc_shp = str(newSHP)
    #------------------------Service Point------------------------- --------
    shpName = "ServicePoint"
    inputFC = sdeAL.getOutput(0) + 'location'
    keepList = ['CUSTOMERTYPE','SERVICEMXLOCATION','SERVICESTATUS','DISCLOCATION',\
                  'STREETADDRESS','METERLOCATIONDESC','METERLOCATION','MXSTATUS']
    copyFeature(shpName,sdeAL,keepList,inputFC)
    al_svc_pt = str(newSHP)
    #------Service point CSV creation------
    #Read text file of service lines, set dtypes and make createdate a date
    svc_df = pandas.read_csv(r"file.txt", 
                              usecols=['Location', 'URLName', 'createdate'],
                              dtype={'Location':'string', 'URLName':'string'},
                              parse_dates=['createdate'])
    # Fill null values as those can cause errors
    svc_df['Location'].fillna("No Location", inplace = True)
    svc_df['URLName'].fillna("No URL Found", inplace = True)
    svc_df['createdate'].fillna('01/01/1000', inplace = True)
    # Clean up csv urls that contain incorrect sections
    svc_df = svc_df.apply(lambda x: x.replace({'doclocation': 'server',
                                                '#': '%23', "FieldBook":"Field Book", "\\\\": r'/'},
                                              regex=True))
    # Further clean up the URLName field using re.escape to allow ** entries
    svc_df['URLName'] = svc_df['URLName'].str.split(re.escape('**')).str[0]       
    # Create new column based on just fieldbook name of last segment
    svc_df['Document'] = svc_df['URLName'].apply(lambda x: x[x.rfind('/')+1:])
    # Replace FieldNote where URLName contains 'servicecards' to account for duplicates
    svc_mask = (svc_df['URLName'].str.contains('servicecards'))
    #Create a dataframe based on the new mask
    svc_mask_df = svc_df.loc[svc_mask]
    # On the masked layer, set all FieldNotes to use the last two parts of the URLName
    svc_mask_df['Document'] = svc_mask_df['URLName'].apply(lambda x: '_'.join(urlparse(x).path[1:].split('/')[2:]))
    # Create an inverse mask
    svc_invert_mask = (~svc_df['URLName'].str.contains('servicecards'))
    # Create dataframe of inverted mask
    svc_invert_df = svc_df.loc[svc_invert_mask]
    # Concantenate the two masked dataframes into one
    mask_concat = pandas.concat([svc_invert_df, svc_mask_df])
    # Replacement list
    replace_list = ["OHBUpoad_", "MaximoDrawers123_Images_", "FY19_Maximo_Images_"]
    # Set replacement list into a regex string
    replace_pat = '|'.join(replace_list)
    # Use a mask to change only columns in Document containing replacement string and replace string with ''
    mask_concat.loc[mask_concat['Document'].str.contains(replace_pat, regex = True), 'Document'] = mask_concat['Document'].str.replace(replace_pat, '', regex = True)
    # Set the concat output path
    svc_tbl_csv = os.path.join(sdeTempPath, 'file.csv')
    # Create the new base CSV
    mask_concat.to_csv(svc_tbl_csv)
#     # Get the dates needed for subset date selection
    curdate = datetime.datetime.today()
    # After any testing, make sure b ackdate time is set to 7 days
    backdate = curdate - datetime.timedelta(days=7)
    # Create a boolean mask for the date range
    mask = (svc_df['createdate'] < curdate) & (svc_df['createdate'] >= backdate)
    # Detect the sub-dataframe and then assign to a new dataframe
    sel_df = svc_df.loc[mask]
    #Create a geodf from alabama services
    al_gdf = geopandas.read_file(alSvc_shp, usecols=['MXLOCATION'])
    # output the region-specific CSV for   locators
    region_csv(svc_df, alSvc_shp, os.path.join(sdeTempPath, 'SpireAL', 'file.txt'), 'MXLOCATION')
    # Join the service lines to the sketch file to get just AL services that
    # have been updated in last [backdate] days
    al_merge = al_gdf.merge(sel_df, left_on="MXLOCATION", right_on="Location")
    # Create bad service card csv path
    badcsv_loc = os.path.join(sdeTempPath, 'SpireAL', 'BadServiceCards.csv')
    # run the url thing
    for index, row in al_merge.iterrows():
        url = row['URLName']
        file_name = row['Document']
        # Ignore null or empty file names
        if file_name or file_name == 'No URL Found':
            #get the url
            retrieve_url(sdeTempPath, row['URLName'], row['Location'], row['Document'],
                          'SpireAL', badcsv_loc)
    # Create the _spatial and _nospatial text files
    #Read dataframe of service table created in other script
    al_serviceinfo = r"servicehistorylocation"
    # Convert it to a dataframe
    svcinfo_df = arcgis_table_to_df(al_serviceinfo)
    # Read the alabana service point file as a geodataframe
    al_mxfield = 'SERVICEMXL'
    # Create service point geodataframe
    al_gdf_sp = geopandas.read_file(str(al_svc_pt), usecols=[al_mxfield])
    # Create spatial and nospatial csvs
    isspatial(svcinfo_df, 'MXLOC', al_gdf_sp, 'SERVICEMXL',"Alabama", shpPath)
    
    #############################   MISSOURI EAST    #######################
    ##-------------------------Inspection-----------------------------------
    shpName = "Inspections"
    inputFC = sdeMOE.getOutput(0) + 'inspectionlocation'
    keepList = ['DATECREATED', 'SYMBOLROTATION', 'GLOBALID']
    copyFeature(shpName,sdeMOE,keepList,inputFC)
    # The Inspections FC contains pictures of markerball placement
    # that has been deemed important for the locators. This code segment takes those
    # pictures from an attachment table to the feature class.
    # Get needed variables
    inputTable = sdeMOE.getOutput(0) + 'file'
    mbPicPath = os.path.join(shpPath, "ElectronicMarkerPictures")
    # If path to save pictures does not exist, create it.
    if not os.path.exists(mbPicPath):
        os.mkdir(mbPicPath)
        print("No folder for Electronic Marker Pictures. A directory has been created at {0}.".format(mbPicPath))
    # Only recent pictures need to be sent
    # Create list to store GlobalIDs that match a certain date range
    globalList = []
    # set two date variables with no h/m/s to match shapefile format
    cur_date = datetime.date.today()
    # back date should be for 14 days. Currently set to 6000 to give all data in one go.
    # Change back following 12/25/2020
    backDate = datetime.date.today() - datetime.timedelta(days=7)
    # Set a sql query using dates
    query='"DATECREATE" <= date '+"'"+str(cur_date)+"' AND "+'"DATECREATE" >= date '+"'"+str(backDate)+"'"

    # Create search cursor in newly created Inspections shapefile
    # only search where inspection created date is from last two weeks
    with arcpy.da.SearchCursor(newSHP, ['DATECREATE', 'GLOBALID'], query) as cursor:
        # Iterate through each row and append the global id's found to the empty list
        for row in cursor:
            idList = row[1]
            globalList.append(idList)
    # With a list of global ids marking pictures from the last 14 days, search
    # through the attached table of pictures
    with arcpy.da.SearchCursor(inputTable, ['DATA', 'ATT_NAME', 'ATTACHMENTID', 'REL_GLOBALID', 'CONTENT_TYPE']) as cursor:
        for row in cursor:
            # Only iterate through attachments where the global ids match the
            # Ids of inspections from the last 14 days
            # Note: Someone once attached a file with a CONTENT_TYPE of application.
            # This generated an error. As such, code filters for only images.
            if row[3] in globalList and row[4] == 'image/jpeg':
                # Set the attachment variable which contains the data
                attachment = row[0]
                # Store the name of the file
                filename = str(row[3]) + ".jpg"
                # open the path desired and then write the found attachment to that location
                # using the filename variable to name it.
                print("The attachment {0} has been copied to {1}.".format(filename, mbPicPath))
                open(mbPicPath + os.sep + filename, 'wb').write(attachment.tobytes())
                # Print statement to check files are copied

##------------------------Service Points--------------------------- ------
    shpName = "ServicePointMoEast"
    inputFC = sdeMOE.getOutput(0) + 'location'
    keepList = ['CUSTOMERTYPE','SERVICEMXLOCATION','SERVICESTATUS','DISCLOCATION',\
              'STREETADDRESS','METERLOCATIONDESC','METERLOCATION','MXSTATUS']
    copyFeature(shpName,sdeMOE,keepList,inputFC)
    # Add missing addresses
    # Empty dictionary to fill with service line addresses & mxlocations
    svcDict = {}
    # Fields to fill dictionary
    svcFields = ['MXLOCATION','STREETADDRESS']
    # service line feature class to use for search
    searchFC = sdeMOE.getOutput(0) + 'location'
    distMainFC = sdeMOE.getOutput(0) + 'location'
    # Insert search cursor
    with arcpy.da.SearchCursor(searchFC, svcFields) as Searchcursor:
    # For each row in cursor, get data for dictionary
        for row in Searchcursor:
            # store the row data in variables
            loc = row[0]
            newAddr = row[1]
            # Only add addresses that are not blank
            if row[1] != None:
                # add address in format of key(mxlocation) and value (streetaddress)
                svcDict[loc] = newAddr
# Use update cursor to update service point shp
    with arcpy.da.UpdateCursor(newSHP, ['SERVICEMXL','STREETADDR']) as cur:
        #For each row in cursor, iterate
        for row in cur:
            #Set variables for serviceMXL and streetaddr in the row
            mxLoc = row[0]
            oldAddr = row[1]
        #If the mx location is found in the dictionary, update
            if mxLoc in svcDict:
                # update if old address is blank and the dictionary address isn't null
                if oldAddr == ' ' and svcDict[mxLoc] != None:
                    #Change the street address to the street address matching the mxLocation in dict
                    row[1] = svcDict[mxLoc]
                    #update the row
                    cur.updateRow(row)
  ### The section below is to create service points from services that only have
    #a service line fc and no service point in the data. Locator uses service points
    # to look at whether a service exists so creating phantom ones avoids issues.
    # Create feature layers from service shp and service line and dist main
    ws = arcpy.env.workspace = sdeTempPath
    arcpy.MakeFeatureLayer_management(newSHP, "point_lyr")
    arcpy.MakeFeatureLayer_management(searchFC, "svcLine_lyr")
    arcpy.MakeFeatureLayer_management(distMainFC, "distMain_lyr")
    # Select feature layers by location, lines that intersect service point shp
    # can use INVERT to invert selection
    arcpy.SelectLayerByLocation_management(in_layer="svcLine_lyr",\
                                          overlap_type="INTERSECT",\
                                          select_features="point_lyr",\
                                          search_distance="",\
                                          selection_type="NEW_SELECTION",\
                                          invert_spatial_relationship="INVERT")
    # Copy features to in memory fc
    memLine = "in_memory" + "\\" + "svcLine_lyr"
    arcpy.CopyFeatures_management("svcLine_lyr", memLine)
    print("Service line copied.")
    # Generate points along lines for END_POINTS
    # Use FeatureVerticesToPoints_management using BOTH_ENDS
    memPoints = "in_memory" + "\\" + "vertice_points"
    arcpy.FeatureVerticesToPoints_management(memLine, memPoints, "BOTH_ENDS")
    arcpy.MakeFeatureLayer_management(memPoints, "memPoint_lyr")
    print("Vertice Points created.")
    # Select newly generated points that intersect with dist main, invert
    arcpy.SelectLayerByLocation_management(in_layer="memPoint_lyr",\
                                        overlap_type="INTERSECT",\
                                        select_features="distMain_lyr",\
                                        search_distance="",\
                                        selection_type="NEW_SELECTION",\
                                        invert_spatial_relationship="INVERT")
    # Copy selected features to in memory
    memPointsNew = "in_memory" + "\\" + "notdMain_points"
    arcpy.CopyFeatures_management("memPoint_lyr", memPointsNew)
    fieldsLi = arcpy.ListFields(memPointsNew)
    # Append in memory features to shapefile using No_Test
    appendLayer = memPointsNew
    target_layer = newSHP
    # Set field mappings object var
    fieldMappings = arcpy.FieldMappings()
    # Add tables for layers to be used
    fieldMappings.addTable(target_layer)
    fieldMappings.addTable(appendLayer)
    # Create list for map fields and add the fields wanted
    listMapFields = []
    # List goes append layer(1) ,target layer (2)
    listMapFields.append(('MXLOCATION','SERVICEMXL'))
    listMapFields.append(('STREETADDRESS','STREETADDR'))
  # Iterate through the fields
    for field_map in listMapFields:
      # Add fields to a field map index with target layer
      fieldToMapIndex = fieldMappings.findFieldMapIndex(field_map[1])
      # Get map var
      fieldToMap = fieldMappings.getFieldMap(fieldToMapIndex)
      # Add append layer to field map to match target layer
      fieldToMap.addInputField(appendLayer, field_map[0])
      # Replace original index with updated one
      fieldMappings.replaceFieldMap(fieldToMapIndex, fieldToMap)
    # Create append layer as a list
    inData = [appendLayer]
    # Append the newly created points to the target shapefile
    arcpy.Append_management(inData, target_layer, "NO_TEST", fieldMappings)
    # Assign service points to variable for later use
    moe_svc_pt = str(newSHP)
###--------------Add Service Sketches----------------------------------------
### This section sends over service sketches to based on the last 7 days
    # Read the cleaned csv
    clean_df = pandas.read_csv(svc_tbl_csv,
                                usecols=['Location', 'URLName', 'createdate', 'Document'],
                                dtype={'Location':'string', 'URLName':'string', 'Document': 'string'},
                                parse_dates=['createdate'])
    # Create a mask using dates made in Al section on the cleaned file
    mask = (clean_df['createdate'] < curdate) & (clean_df['createdate'] >= backdate)
    # Detect the sub-dataframe and then assign to a new dataframe
    sel_df = clean_df.loc[mask]
    #Create a geodf from mo east services
    moe_mxfield = 'SERVICEMXL'
    moe_gdf = geopandas.read_file(str(moe_svc_pt), usecols=[moe_mxfield])
    # output the region-specific CSV for locator
    region_csv(clean_df, str(moe_svc_pt),
                os.path.join(sdeTempPath, 'MOEast', 'location.txt'),
                moe_mxfield)
    # Join the service lines to the sketch file to get just AL services that
    # have been updated in last [backdate] days
    moe_merge = moe_gdf.merge(sel_df, left_on=moe_mxfield, right_on="Location")
    # Create bad service card csv path
    badcsv_loc = os.path.join(sdeTempPath, 'MOEast', 'BadServiceCards.csv')
    # grab url names that aren't good so they can be added to a bad csv file
    for index, row in moe_merge.iterrows():
        url = row['URLName']
        file_name = row['Document']
        # Ignore null or empty file names
        if file_name or file_name == 'No URL Found':
            #get the url
            retrieve_url(sdeTempPath, row['URLName'], row['Location'], row['Document'],
                          'MOEast', badcsv_loc)
    # service info table created in another script
    moe_serviceinfo = r"serviceinfotablelocation"
    # Convert it to a dataframe
    svcinfo_df = arcgis_table_to_df(moe_serviceinfo)
    # Create spatial and nospatial csvs
    isspatial(svcinfo_df, 'LOC', moe_gdf, 'SERVICEMXL', "MOEast", shpPath)
    
    
    #Clean up the workspace
    arcpy.env.workspace = ""
    # Clean up sde connections in loop
    for item in [sdeAL, sdeMOE, sdeMOW, sdeMOEPoly, sdeMOWPoly]:
        os.remove(item.getOutput(0))
    #close out the log file
    print("Closing the log file.")
    log.write("Log: Script Ran successfully at  " + str(date) + "\n")
    log.close()
    # Send Email
    # List of people to email in string format
    recepientAddress = "TargetEmails"
    # String as command line using the blat.exe SMTP program from www.blat.net to send email
    command = 'blat.exe -f email -to {} -s "Log File" -body "New log from Script. Please see attached report for more details.<br><br>This is an automated email. Please do not reply." -server emailserver -attach "{}" -html'.format(recepientAddress,logPath)
    # Enter system command
    os.system(command)
except:
    # Grab the traceback information
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    # Creae a message for it and send it to the log
    pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" \
            + str(sys.exc_info()[1])
    # Send arcpy errors to log
    msgs = "ArcPy ERRORS:\n" + arcpy.GetMessages(2) + "\n"
    log.write("" + pymsg + "\n")
    log.write("" + msgs + "")
    # Print any messages to console
    print(msgs)
    # Close log
    log.close()
    # Clean up sde connections
    for item in [sdeAL, sdeMOE, sdeMOW, sdeMOEPoly, sdeMOWPoly]:
        os.remove(item.getOutput(0))
    # list of emails to  be sent to in string format
    recepientAddress = "emails"
    # String as command line using the blat.exe SMTP program from www.blat.net to send email
    command = 'blat.exe -f email -to {} -s " Log File. Error Found." -body "New log from Script. Please see attached report for more details.<br><br>This is an automated email. Please do not reply." -server emailserver -attach "{}" -html'.format(recepientAddress,logPath)
    # Enter system command
    os.system(command)