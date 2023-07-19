# Project: MO Environmental Updates
# Create Date: 05/27/2022
# Last Updated: 01/17/2023
# Created by: Robert Domiano
# Purpose: Update MO E-Start Hazardous Waste Program Cleanup & Underground Storage Tank Facilities
# ArcGIS Version:  9.1
# Python Version:  3.2
# For a changelog of updates, visit the github at: 
# -----------------------------------------------------------------------
# Import modules
import arcpy
import os
import time
import pcbbuff as pcbGen
from pathlib import Path

# Set environment options
arcpy.env.overwriteOutput=True    
# Get workspace location and name of GDB from user
ws = r"workspace path"
# If workspace fldr doesn't exist, create a new one
Path(ws).mkdir(parents=True, exist_ok=True)
# Set ws geodatabase name
wsGDBName = "ContaminationLayers"
# get current script path
loc_fldr = os.getcwd()
# Set connection files based on current location
MoEastConnection = os.path.join(loc_fldr, 'sdefile.sde')
maximo_connection = os.path.join(loc_fldr, 'sdefile.sde')
# set workspace GDb location
wsGDB = os.path.join(ws, wsGDBName + ".gdb")
# Check if the gdb already exists
if not arcpy.Exists(wsGDB):
    # If not found, print to console
    arcpy.AddMessage("Workspace GDB not found, creating a GDB at {0} named {1}...".format(ws, wsGDBName))
    # Create geodatabase
    arcpy.management.CreateFileGDB(ws, wsGDBName)
    # Print message to pro console
    arcpy.AddMessage("Workspace GDB created...")
# If the workspace is found...
else:
    # Print this message to console
    arcpy.AddMessage("Workspace GDB found at {0}.".format(wsGDB))
    
# Assign gdb as workspace
arcpy.env.workspace = wsGDB
# MO Hazardous Waste from Department of Natural Resources
hwpServer = "https://gis.dnr.mo.gov/arcgis/rest/services/e_start/e_start/MapServer/0"
# set copy feature class location
hwpCopy = os.path.join(wsGDB,"DNRHWPSite")
# Set the path as a feature set to wrap the map service in a feature layer
# this allows the arcpy tool to manipulate it
hwp_fs = arcpy.FeatureSet()
hwp_fs.load(hwpServer)
# Query to copy only certain features
hwp_query = "SITESTAT IN ('Active', 'Brownfield Assessment', 'Long-Term Stewardship', 'Inactive VCP (Terminated/Withdrew)')"
# Generating a feature set takes some time with slower services. Add
# a delay to give it time to finish
arcpy.AddMessage("Creating feature set for Hazardous Waste Programs...")
time.sleep(240)
# Copy the features
arcpy.conversion.FeatureClassToFeatureClass(hwp_fs, wsGDB, "DNRHWPSite", hwp_query)
print("Copy of HWP points has been made at {0}.".format(wsGDB))
arcpy.AddMessage("Copy of HWP points has been made at {0}.".format(wsGDB))

# Modify HWP points to match schema
# DNR added an OBJECTID field and named their normal one OBJECTID_1 which needs to be dealt with
# First delete the OBJECTID
dnrSite = os.path.join(wsGDB, "DNRHWPSite")
arcpy.DeleteField_management(dnrSite, "OBJECTID")
# Rename object ID field to OBJECTID_1
arcpy.management.AlterField(dnrSite, "OBJECTID_1", "OBJECTID", "OBJECTID")
# Alter shape field to match SDE
arcpy.management.AlterField(dnrSite, "Shape", "SHAPE")

#MO Underground Storage Tank Facilities from MO Department of Natural Resources
ustServer = "https://gis.dnr.mo.gov/arcgis/rest/services/e_start/e_start/MapServer/3"
#Set copy feature class location
ustCopy = os.path.join(wsGDB, "DNRTankSite")
#Set feature set
ust_fs = arcpy.FeatureSet()
#Load usts into feature set
ust_fs.load(ustServer)
# Query for only copying certain features
ust_query = "FACSTAT IN ('Investigation/Corrective Action is Ongoing or Incomplete', 'No Further Action Letter Issued with Restriction', 'Administrative Closure')"
arcpy.AddMessage("Creating feature set for Underground Storage Sites...")
#Delay for load time
time.sleep(240)
#Copy the features
arcpy.conversion.FeatureClassToFeatureClass(ust_fs, wsGDB, "DNRTankSite", ust_query)
print("Copy of UST points has been made at {0}.".format(wsGDB))
arcpy.AddMessage("Copy of UST points has been made at {0}.".format(wsGDB))
# Modify DNR points to match schema
# DNR added an OBJECTID field and named their normal one OBJECTID_1 which needs to be dealt with
# First delete the OBJECTID
ustSite = os.path.join(wsGDB, "DNRTankSite")
arcpy.DeleteField_management(ustSite, "OBJECTID")
# Rename object ID field to OBJECTID_1
arcpy.management.AlterField(ustSite, "OBJECTID_1", "OBJECTID", "OBJECTID")
# Alter shape field to match SDE
arcpy.management.AlterField(ustSite, "Shape", "SHAPE")

# Assign variables for parcel layers
parcel_fc = maximo_connection + 'parcels'

# Conduct a spatial join for UST and HWP with the merged parcels
# Change these to in_memory after test
hwp_output = "in_memory/HWP_Join"
#Set up field mappings for spatial join
hwp_fmappings = arcpy.FieldMappings()
hwp_fmappings.addTable(hwpCopy)
#Spatial join parcels and hwp sites
arcpy.SpatialJoin_analysis(parcel_fc, hwpCopy, hwp_output, 
                            "JOIN_ONE_TO_ONE", "KEEP_COMMON",hwp_fmappings,
                            "WITHIN_A_DISTANCE", "50 Feet")
print("HWP Sites joined to parcels...")
arcpy.AddMessage("HWP Sites joined to parcels...")
# Set variables for ust join
ust_output = "in_memory/UST_Join"
# Set up field mappings for spatial join
ust_fmappings = arcpy.FieldMappings()
ust_fmappings.addTable(ustCopy)
#Spatial join parcels and ust sites
arcpy.SpatialJoin_analysis(parcel_fc, ustCopy, ust_output, 
                            "JOIN_ONE_TO_ONE", "KEEP_COMMON",ust_fmappings,
                            "WITHIN_A_DISTANCE", "50 Feet")
print("UST Sites joined to parcels...")
arcpy.AddMessage("UST Sites joined to parcels...")

# Merge the two features together to prepare for combining all the 
# polygon feature classes into one contamination FC
# Set merge variables
union_output = "in_memory/contamination_union"
arcpy.management.Merge([ust_output, hwp_output], union_output)
print("UST and HWP parcel joins unioned together...")
arcpy.AddMessage("UST and HWP parcel joins unioned together...")
#The merge will need to be dissolved based on SITENAME but usts use FACNAME instead
# A cursor is needed to move FACNAME to SITENAME
with arcpy.da.UpdateCursor(union_output, ["SITENAME","FACNAME"]) as cursor:
    # Iterate through all rows in the union
    for row in cursor:
        # If the sitename is not null...
        if row[0] is not None:
            #Test if the sitename is just a  blank string...
            if len(row[0]) == 0:
                #If it is, replace it with the facility name
                row[0] = row[1]
        # If the sitename is filled in...
        if row[0] is None:
            # Replace it with the facility name
            row[0] = row[1]
        # Update the row
        cursor.updateRow(row)
# Print messages
print("Empty SITENAME fields filled with FACNAME fields...")
arcpy.AddMessage("Empty SITENAME fields filled with FACNAME fields...")

# Dissolve based on newly updated field which should have no null or blank values
# Set dissolve variables
dis_output = "contamination_dissolve"
dis_output_loc = os.path.join(wsGDB, dis_output)   
#Perform dissolve based on site name with multi-part features
fields_dissolve = "AULID FIRST; OUID FIRST; SMARSID FIRST; FEDERALID FIRST; COUNTY FIRST; DNRPROGRAM FIRST; SITEOWN FIRST"
arcpy.management.Dissolve(union_output, dis_output_loc, "SITENAME",
                          fields_dissolve, 
                          "MULTI_PART")
# Print messages
print("UST/HWP dissolved based on SITNAME...")
arcpy.AddMessage("UST/HWP dissolved based on SITNAME...")
# Change field names to remove FIRST_ from them.
# List variable of field names to be changed
first_list = ["FIRST_AULID","FIRST_OUID","FIRST_SMARSID",
              "FIRST_FEDERALID","FIRST_COUNTY","FIRST_DNRPROGRAM","FIRST_SITEOWN"]
# Loop through field names to remove FIRST_ from them
for name in first_list:
    arcpy.management.AlterField(dis_output_loc, name, (name.replace('FIRST_', '')))  
# Print cleanup message
print("Removed FIRST_ lines from kept fields...")
arcpy.AddMessage("Removed FIRST_ lines from kept fields...")
#Set FUSRAP location
fusrap_input = os.path.join(wsGDB, "FUSRAP")
# Check for FUSRAP data existing
if not arcpy.Exists(fusrap_input):
    arcpy.AddMessage("FUSRAP Feature Class not found...")
    print("FUSRAP Feature Class not found...")
    # Create query for just FUSRAP type
    fusrap_where = "SUBTYPECD = 1"
    # Get the Production Contamination layer
    fusrap_sde = MoEastConnection + 'tamination feature class'
    # Create the production contamination in the wsGDB
    fusrap_name = "FUSRAP"
    #Copy the SDE file into the geodatabase
    arcpy.conversion.FeatureClassToFeatureClass(fusrap_sde, wsGDB, fusrap_name, fusrap_where)
    print("FUSRAP Feature Class created in {0}.".format(wsGDB))
    arcpy.AddMessage("FUSRAP Feature Class created in {0}.".format(wsGDB))
else:
    arcpy.AddMessage("FUSRAP Feature Class found...")
    print("FUSRAP Feature Class found...")
    
# Set the PCB input path
pcb_input = os.path.join(wsGDB, "PCB_Samples")
#Check if PCBs exist
if not arcpy.Exists(pcb_input):
    arcpy.AddMessage("PCB points not found...")
    #Create the points
    pcbGen.create_pcbPoints(ws, wsGDBName)
    # Print message
    arcpy.AddMessage("PCB Points and buffers created.")
# Else print message
else:
    arcpy.AddMessage("PCB Points found...")

# Combine the FUSRAP polygons to the combined UST/HWP polygons
comb_output = os.path.join(wsGDB, "Contamination")
arcpy.management.Merge([fusrap_input, pcb_input, dis_output_loc], comb_output)
print("FUSRAP, DNR, and HWP polygons combined.")
arcpy.AddMessage("FUSRAP, DNR, and HWP polygons combined.")

# Delete Unwanted fields
unloved_fields = ["AUL_ID", "OUID_1","SMARS_ID","SITE_FACILITY_NAME", 
                  "FEDERAL_ID", "COUNTY_1"]
# Delete the unloved fields :(
for field in unloved_fields:
    arcpy.management.DeleteField(comb_output, field)
    
# Rename fields to proper fields in schema that have been deleted
arcpy.management.AlterField(comb_output, "AULID", "AUL_ID")
arcpy.management.AlterField(comb_output, "SMARSID", "SMARS_ID")
arcpy.management.AlterField(comb_output, "SITENAME", "SITE_FACILITY_NAME")
arcpy.management.AlterField(comb_output, "FEDERALID", "FEDERAL_ID")

# Update site ownership with with valid fields
# Open an update cursor
with arcpy.da.UpdateCursor(comb_output, ["DNRPROGRAM", "SITEOWN", "SITE_OWNERSHIP", "SUBTYPECD", "NOTES"]) as cursor:
    for row in cursor:
        # If DNRPROGRAM has characters and SITEOWN is empty
        # replace SITE_OWNERSHIP with DNRPROGRAM field
        if row[0] is not None and row[1] is None:
            row[2] = row[0]
            row[3] = 3
        # If DNRPROGRAM has no characters and SITEOWN has characters
        # replace SITE_OWNERSHIP with SITEOWN field
        if row[0] is None and row[1] is not None:
            row[2] = row[1]
            row[3] = 3
        if row[0] is not None and row[1] is not None:
            row[2] = row[1]
            row[3] = 3
        # Update Notes for PCB samples
        if row[3] == 2:
            row[4] = "Dispose of all pipe, fittings and associated debris in 'cast iron projects' dumpster at . Wear nitrile gloves and face shield when inside of pipe is exposed."
        cursor.updateRow(row)
arcpy.AddMessage("Fields updated with new values...")
print("Fields updated with new values...")

# Clean Up Phase Mark 2
# Repair up geometry in case of issues
arcpy.management.RepairGeometry(comb_output)
# Delete unneeded fields
del_me = ["DNRPROGRAM", "SITEOWN"]
for item in del_me:
    arcpy.DeleteField_management(comb_output, item)
# Get list of fields in final output
field_list = arcpy.ListFields(comb_output)
# Go through list
for field in field_list:
    # Find alias names with FIRST_ in them
    if "FIRST_" in field.aliasName:
        # Set new alias name
        new_name = field.aliasName.replace('FIRST_', '')
        # Alter field with new alias name
        arcpy.management.AlterField(comb_output, field.name, new_field_alias=new_name)
# Add spatial index
arcpy.management.AddSpatialIndex(comb_output)
# Add global ids
for item in [comb_output, dnrSite, ustSite]:
    arcpy.AddGlobalIDs_management(item)
    
# Delete the intermediate files created
list_del = [dis_output_loc, fusrap_input, pcb_input]
for item in list_del:
    arcpy.management.Delete(item)
print("Spatial Index added, global IDs added, intermediate files deleted...")
arcpy.AddMessage("Spatial Index added, global IDs added, intermediate files deleted...")
# Set subtypes
# Create dictionary of subtype code and values
stypeDict = {"1": "Special PPE and Disposal - FUSRAP", "2": "Special PPE and Disposal - Legacy",
              "3": "Special PPE and Disposal - DNR Remediation"}
# Set the subtype field
arcpy.SetSubtypeField_management(comb_output, "SUBTYPECD")
#Code the subtype field
for code in stypeDict:
    arcpy.AddSubtype_management(comb_output, code, stypeDict[code])
# Set default subtype
arcpy.SetDefaultSubtype_management(comb_output, "1")
print("Subtypes set...")
arcpy.AddMessage("Subtypes set...")

# #######Copying new file to Prod SDE#####
# Assign variable to existing contamination layer
current_contamination = MoEastConnection + os.sep +'Contamination'
# Delete all features
arcpy.management.DeleteFeatures(current_contamination)
arcpy.AddMessage("All features deleted from layer...")
# Append the combined output into the sde feature class
arcpy.management.Append(comb_output, current_contamination, 'TEST')
