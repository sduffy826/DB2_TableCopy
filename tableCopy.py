#!/usr/bin/python

# This program handles unloading / loading db2 tables.  
#
# For tables that have 'identity' columns the tables need to exist in the target system (because only 'load' works
# and that's when tables exist).
#
# To run: Have file 'db2Databases.input', it should be a csv file, the file must have a 'Layout' record in it
# that defines the layout of the file, the keywords after '##Layout,' must be the values below but the order
# can change.  The record: '##Layout, alias, database, userid, password, isRemote'
# The 'isRemote' should be 'True' if the database is remote; we can't load remote db's, if it's remote then
# this program writes files that can be used to transmit the unloads to the target, and also the sqlcommands
# to load them (see more below).

# The other file needed is 'db2Tables.input', that specifies the tables to process, it also needs a layout
# record (and data that conforms to it).  The layout record is in the 
# following format: '##Layout, schema, tableName, identityColumn, typeOfLoad'
# The identityColumn is the column name, can skip, the typeOfLoad should be 'import' or 'load' (note, doesn't
# make sense if you have an identity column and use 'import'
#
# To do the unload/load (assuming db is remote):
#   Be on machine and id (yea redundant... i.e. ssh id@xxx.yyy.zzz.com; su - someId; cd theDirectory)
#   Add path for db2 (check db owner profile if don't know how, should be similar to '. /home/db2inst1/sqllib/db2profile')
#   Run job: python manualCopy.py <SourceDb> <TargetDb> (SourceDB/TargetDb values: prod, dev, test), I typically reply Y to all prompts
#   The commands below will copy the data and sqlcommand file to target (the part between () is not part of command (just info), change target dir if applicable)
#     ./db2LoadList.txt scp id@aaa.bbb.ccc.com:/home/db2inst1/db2data (Copies unloads to db2 machine, you'll be prompted for pw a bunch of times.. maybe change file so it's an arg
#     scp db2LoadCommands.sql id@aaa.bbb.ccc.com:/home/db2inst1/db2data (puts the load command file out on the target server)
#   Go to the target server (ssh id@aaa.bbb.ccc.com)
#   su - db2inst1  (or switch to the id where you targetted files)
#   cd db2data (make change if necessary)
#   db2 connect to <DatabaseAliasMatchingTargetDb> user <createorId> using <pw> (Connect to db, if you don't use user/using they'll be created by db2inst1)
#   db2 -tsvf db2LoadCommands.sql (Performs the loads and resets identity columns)
# Done, check it out
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

import os
import sys
import subprocess
import readline
import pdb
import pythonUtils

# Identify the input files that have the databases defined and the tables to be processed
databaseConfig = 'db2Databases.input'
tables2Export = 'db2Tables.input'

# Output files (last two used for remote processing, first is temp work area type of file)
db2OutFile = 'db2out.txt'
db2FileList = 'db2LoadList.txt'
db2Commands = 'db2LoadCommands.sql'

# If we want to limit the records looked at/unloaded we can do it with
# this where suffix... we'll only look at notes survey data with this
whereSuffix = ""

# Define variables to hold the database info
# files to be scp'd and the db2 commands to load

dbDict = pythonUtils.getFileMap(databaseConfig,'1')
dbDictPos = pythonUtils.getFileLayout(databaseConfig)

# Define variables to hold the position of the attributes in the dbDict map
c_Alias = dbDictPos['alias']         # not db alias, it's alias to ref by (i.e. prod, dev...)
c_Database = dbDictPos['database']
c_Userid = dbDictPos['userid']
c_Password = dbDictPos['password']
c_IsRemote = dbDictPos['isRemote']

# Define variables to hold the position of the attributes in the tables map
tables2Copy = pythonUtils.getFileMap(tables2Export,'1,2')
tables2CopyPos = pythonUtils.getFileLayout(tables2Export)

c_Schema = tables2CopyPos['schema']  
c_TableName = tables2CopyPos['tableName']
c_IdentityColumn = tables2CopyPos['identityColumn']
c_TypeOfLoad = tables2CopyPos['typeOfLoad']

tableCounts = {} # Map of counts (same key as tables2Copy)
tableIdentityValues = {} # Map with identity columns (same key as tables2Copy)

isRemote = False # Global flag to say how we're connected to db

# Function definitions

# ======================================================================
# Connect to the database passed in (uid/pw can be '')
# ======================================================================
def connectIt(dbName, uid, pw):
  if len(uid.strip()) > 0:
    theString = 'connect to '+dbName+' user '+uid+' using '+pw
  else:
    theString = 'connect to '+dbName
  isGood = runDB2(theString)
  return isGood

# ======================================================================
# Return True if connected to the database alias passed in (it's the
# name ised in the dbDict
# ======================================================================
def didConnect(dbName):
  global isRemote
  isGood = False
  if dbName in dbDict:
    theDb  = dbDict[dbName][c_Database]
    theId  = dbDict[dbName][c_Userid]
    thePw  = dbDict[dbName][c_Password]
    dumVar = dbDict[dbName][c_IsRemote]
    if (dumVar in ['True','true','yes']):
      isRemote = True
    else:
      isRemote = False
    isGood = connectIt(theDb,theId,thePw)
  return isGood

# ======================================================================
# Return number of records in the schema.tablename passed in, if < 0 then
# error was encountered
# ======================================================================
def getRecordCount(schema, tablename):
  theString = 'select count(*) from '+schema+'.'+tablename + whereSuffix
  isGood    = runDB2WithOutput(theString)
  if isGood:
    theNumber = pythonUtils.lastNumberInFile(db2OutFile)
  else:
    theNumber = -4
  return theNumber

# ======================================================================
# Gets the table counts for all the tables in the database alias passed
# in (and list of tables in tables2Copy)
# ======================================================================
def getTableCounts(db2Check):
  global tableCounts
  if didConnect(db2Check):
    totalCnt = 0
    for tableKey in tables2Copy:
      aTuple = tables2Copy[tableKey]

      theSchema = aTuple[c_Schema]
      theTable  = aTuple[c_TableName]
      
      recCount              = getRecordCount(theSchema,theTable)      
      tableCounts[tableKey] = recCount
      totalCnt              = totalCnt + recCount

    resetIt()
    return totalCnt

# ======================================================================
# Loads the table passed in, if the identityColName is passed in then
# the load is special (has identityoverride) and we'll also do an
# alter table command to reset the generated identity count to the
# max keyvalue + 1
# Note: the identityColName is really only applicable for a 'load' if 
#       it's defined it'll be a load
# ======================================================================
def loadIt(schema, tablename, identityColName, typeOfLoad, maxIdentityValue, fileWithNames, fileWithCommands):
  hasIdentity = (len(identityColName.strip()) > 0)  
  outputTable = schema+'.'+tablename   # Use var in case you want to prefix tablename with something for testing

  if (hasIdentity or typeOfLoad == 'load'):
    theString = 'load client from '+tablename+'.ixf of ixf '
    if hasIdentity:
      theString = theString + 'modified by identityoverride '
  else:
    theString = 'import from '+tablename+'.ixf of ixf ' 
  
  theString = theString + 'messages ' + tablename + '_load.msg ' +\
                          '"create into '+outputTable+'"'

  if isRemote: # We don't load remote tables write name of files and command to output files
    print 'Writing list/command to files'
    fileWithNames.write('$1 ' + tablename+'.ixf $2\n') # file to send put args for cmd and dest
    fileWithCommands.write(theString+';\n')            # load command execute on server with db2 -tsvf filename
    isGood = True
  else:
    print 'Loading ' + outputTable  
    isGood = runDB2(theString)

  if isGood and hasIdentity:
    theString = 'alter table '+outputTable+' alter column '+identityColName+' restart with '+str(maxIdentityValue+1)
    print 'FIXING Identity with: ' + theString
    if isRemote:
      fileWithCommands.write(theString+';\n')            
    else:
      isGood = runDB2(theString)

  return isGood

# ======================================================================
# Handles loading all the tables... connects to target db and then calls
# load for each table
# ======================================================================
def loadTables(db2Check,fileWithNames,fileWithCommands):
  if didConnect(db2Check):
    for tableKey in tables2Copy:
      aTuple = tables2Copy[tableKey]

      theSchema        = aTuple[c_Schema]
      theTable         = aTuple[c_TableName]
      identityColumn   = aTuple[c_IdentityColumn]
      typeOfLoad       = aTuple[c_TypeOfLoad]

      maxIdentityValue = tableIdentityValues[tableKey]

      loadIt(theSchema,theTable,identityColumn,typeOfLoad,maxIdentityValue,fileWithNames,fileWithCommands)

  resetIt()
  return

# ======================================================================
# Return the maxInValue for a given column passed in, useful for finding
# out max generated_key values
# ======================================================================
def maxIntValue(schema, tablename, columname):
  theString = 'select max('+columname+') from '+schema+'.'+tablename + whereSuffix
  isGood    = runDB2WithOutput(theString)
  if isGood:
    theNumber = pythonUtils.lastNumberInFile(db2OutFile)
  else:
    theNumber = -3
  return theNumber

# ======================================================================
# Disconnect from db
# ======================================================================
def resetIt():
  isGood = runDB2('connect reset')
  return isGood

# ======================================================================
# Run the db2 command passed in in the cli
# ======================================================================
def runDB2(theCmd):
  runIt = 'db2 "'+theCmd+'"'
  therc = subprocess.call(runIt,shell=True)
  return (therc == 0)

# ======================================================================
# Run the db2 command but instead of writing to stdout write output
# to a file
# ======================================================================
def runDB2WithOutput(theCmd):
  theFile = open(db2OutFile,'w')
  runIt   = 'db2 "'+theCmd+'"'
  therc   = subprocess.call(runIt,stdout=theFile,shell=True)
  theFile.close()
  return (therc == 0)

# ======================================================================
# Creates command to export the table passed in
# ======================================================================
def unloadIt(schema, tablename):
  theString = 'export to '+tablename+'.ixf of ixf messages ' + tablename+'.msg select * from '+schema+'.'+tablename + whereSuffix
  print 'Unloading ' + schema + '.' + tablename
  isGood = runDB2(theString)
  return isGood

# ======================================================================
# Unload all the tables for the db passed in, if the table has an
# identity column we'll update the table
# ======================================================================
def unloadTables(db2Check):
  global tableIdentityValues
  if didConnect(db2Check):
    for tableKey in tables2Copy:
      aTuple = tables2Copy[tableKey]

      theSchema = aTuple[c_Schema]
      theTable  = aTuple[c_TableName]

      unloadIt(theSchema,theTable)

      identityCol = aTuple[c_IdentityColumn]     
      if len(identityCol.strip()) > 0:
        identityNum = maxIntValue(theSchema,theTable,identityCol)
      else:
        identityNum = 0 
      tableIdentityValues[tableKey] = identityNum
  resetIt()
  return

# ---------------------------------------------------------------------------------
# Start of main program

# default source/target (can be overriden if user supplied)
sourceDb = 'test'
targetDb = ['dev']

# pdb.set_trace()  

if len(sys.argv) >= 3:
  sourceDb = sys.argv[1]
  targetDb = sys.argv[2:]

# Check target db's, if they have data make sure user wants to wipe em out
doUnload = True
for destDb in targetDb:
  theCnt = getTableCounts(destDb)
  if theCnt > 0:
    for tableKey in tables2Copy:
      print tables2Copy[tableKey][c_Schema] + ' ' + tables2Copy[tableKey][c_TableName] + ' recs: ' + str(tableCounts[tableKey])

    response = raw_input('\nThere are ' + str(theCnt) + ' records on file, do you want to continue (y/n)')
    if response is 'y':
      response = raw_input('You positive (y/n)?')
    doUnload = (response is 'y')

# If doUnload is on then continue, we'll unload then call the loads
if doUnload:
  unloadTables(sourceDb)
  fList     = open(db2FileList,'w')
  fCommands = open(db2Commands,'w')
  for destDb in targetDb:
    loadTables(destDb,fList,fCommands)
  fList.close()
  fCommands.close()
  print 'Done, if remote target databases check ' + db2FileList+'/'+db2Commands
else:
  print 'Done, no unloads/loads performed'

