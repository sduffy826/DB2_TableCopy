import os
import sys

# Get the layout, upon return the map[columnName] will hold the index position of the value
# -----------------------------------------------------------------------------------------
def getFileLayout(fileName):
  rtnMap = {}
  recordsRead = 0
  try:
    if (os.path.exists(fileName)): 
      theFile  = open(fileName,'r')
      for aLine in theFile.readlines():
        recordsRead += 1
        if (aLine.startswith('##Layout')):
          myList = map(str.strip,aLine.split(","))  # Split record to list and strip each element
          print 'LayoutRecord: ',myList
          indexPos = 0
          for theColName in myList:
            if (indexPos > 0):
              rtnMap[theColName] = indexPos - 1
            indexPos += 1
  except:
    if (recordsRead == 0):
      print 'Error processing: ' + fileName
      exit()
  else:
    theFile.close()
  if (len(rtnMap) == 0):
    print 'No layout found in ' + fileName
    exit()
  print 'Layout:',rtnMap
  return rtnMap

# Return the file passed in as a dictionary, the second argument to this routine identifies the key for the
# map (index positions); the positions are not zero offset, so 1,3 would mean that the key is in positions
# [0] and [2] in the array (after the split)
# The 'value' part of the dictionary is a tuple with the values from the record
# Note the file should be delimitted by comma (or change the split to be the delim)
# ---------------------------------------------------------------------------------------------------------
def getFileMap(fileName, keyFields):
  rtnMap = {}
  recordsRead = 0
  if (len(keyFields.strip()) > 0):
    keyPosition = keyFields.split(',')
  else:
    keyPosition = '1'
  try:
    if (os.path.exists(fileName)): 
      theFile  = open(fileName,'r')
      for aLine in theFile.readlines():
        recordsRead += 1
        if (len(aLine) > 0 and aLine[0] != "#"):    # Got data and not a comment    
          myList = map(str.strip,aLine.split(","))  # Split record to list and strip each element
          theKey = ''                               # Get the key for the record
          for keyIndexPos in keyPosition:
            theKey += '.' + myList[int(keyIndexPos)-1] 

          theKey = theKey.strip('.')                # Strip off the . in front
          if (theKey in rtnMap):                   # If already saw key put out warning
            print 'Warning: ' + theKey + ' is duplicated in file, second value used'
          print 'theKey:',theKey,' tuple:',tuple(myList)
          rtnMap[theKey] = tuple(myList)           # Create dictionary item
  except:
    if (recordsRead == 0):
      print 'Error processing: ' + fileName
      exit()
  else:
    theFile.close()
  return rtnMap

# this returns the last non-blank line in a file
# ----------------------------------------------
def lastNonBlankInFile(fileName):
  lastLine = ''
  try:
    theFile  = open(fileName,'r')
    for aLine in theFile.readlines():
      if len(aLine.strip()) > 0:
        lastLine = aLine
  except:
    lastLine = 'Error processing: ' + fileName
  else:
    theFile.close()
  return lastLine

# this returns the last integer found (on a line by itself) in
# a file (usefuly when piping output of db2 count(..) to file :)
# --------------------------------------------------------------
def lastNumberInFile(fileName):
  try:
    theFile   = open(fileName,'r')
    lastValue = -1
    for aLine in theFile.readlines():
      try:
        lineValue = int(aLine)
        lastValue = lineValue
      except:
        pass
  except:
    lastValue = -2
  else:
    theFile.close()
  return lastValue
