'''
Created on Aug 14, 2015

@author: edwin permana
'''
# This program will look at the files in the folder and insert into SQL

import os, os.path, re, codecs,  warnings
import hashlib
filepath = "/Users/edwin/workspace/mi_ingest/src/delta"
workFileList = []

def check_directory(match):
    list_dir = os.listdir(filepath)
#    match = match.lower()
    count = 0
    fileMatch = "\d+-" + match + "-\d+.TXT"
#    fileMatch = "\d+-" + match + "_STARTING_FILE.TXT"
#    fileMatch = "\d+-" + match + "_starting_file.TXT"
    re.compile(fileMatch)
    for file in list_dir:
#        file = file.upper();
#        print file
        if re.match(fileMatch,file):
            workFileList.append(file)
            count += 1
            print workFileList
    return count

def getPrimaryKey(theLine):
    binValue= ' '.join(format(ord(x), 'b') for x in theLine)
    hash_object = hashlib.sha256(binValue)    
    hex_dig = hash_object.hexdigest()
    
    return hex_dig

#if check_table_constraint(tableName,queryValues[0]) :
def check_table_constraint(table,column):
    strQ = ''
    matrix = [[],[],[],[],[],[],[],[],[],[]]
    #matrix = [["str1", "str2"],  ["str4", "str5"]]
    if (table=='ADJUST') or (table=='LOADIN')  or (table=='LOADOUT'):
            matrix = [["FACILITYID", "0"],  ["OBATCH", "2"] ,  ["FBATCH", "3"] ,  ["FCUSTCODE", "4"]]
            return matrix
    if (table=='PHY_TRN') :
            matrix = [["FACILITYID", "0"], ["FCUSTCODE", "2"], ["OBATCH", "3"] , ["OSEQUENCE", "4"] ,["OTRACK", "5"] ,["OSERIAL", "6"] ,["FBATCH", "7"] ,["FSEQUENCE", "8"] , ["FTRACK", "9"] , ["FSERIAL", "10"] ]
            return matrix
    if (table=='INV_MST'):   
            matrix = [["FACILITYID", "0"],  ["OBATCH", "2"] , ["OSEQUENCE", "3"] , ["FCUSTCODE", "4"],["FCLASS", "5"] ,["FRECTYPE", "6"] ,["FBATCH", "9"] ,["FSEQUENCE", "10"] , ["FTRACK2", "9"] , ["FSERIAL", "11"] ]
            return matrix      
    if (table=='INV_TRN') :
            matrix = [["FACILITYID", "0"],  ["OBATCH", "2"] , ["OSEQUENCE", "3"] , ["OTRACK", "4"] ,["FCUSTCODE", "5"],["FBDATE", "6"] ,["FBATCH", "7"] ,["FSEQUENCE", "8"] , ["FTRACK", "9"] , ["FTRACK2", "10"] ]
            return matrix           
    if (table=='PHY_MST') :
            matrix = [["FACILITYID", "0"], ["FCUSTCODE", "2"], ["OTRACK", "3"] ,["OSERIAL", "4"] , ["FTRACK", "5"] , ["FSERIAL", "6"] ]
            return matrix  
    if (table=='LOCATION') : 
            matrix = [["FACILITYID", "0"],  ["FLOCATION", "2"] ]
            return matrix
    if (table=='CUSTOMER')   :
            matrix = [["FACILITYID", "0"],  ["FCUSTCODE", "2"] ]
            return matrix
    if  (table=='CODE2') :
            matrix = [["FACILITYID", "0"],  ["FPRODUCT", "2"] ]
            return matrix
    
    return matrix

def getUpdateWhereStatement(table,column):
    sep =" AND "
    strQ = ''
    
    #matrix = [["str1", "str2"],  ["str4", "str5"]]
    if (table=='ADJUST') or (table=='LOADIN')  or (table=='LOADOUT'):
            strQ += " FACILITYID="+column[0] +""+sep+ " FBATCH="+column[3] + ""+ sep + " FCUSTCODE="+column[4]
            return strQ 
    if (table=='PHY_TRN') :
            strQ += " FACILITYID="+column[0] +sep+ " FCUSTCODE="+column[2] +  sep + " FBATCH="+column[7]+ sep + " FSEQUENCE="+column[8]+ sep + " FTRACK="+column[9]+ sep + " FSERIAL="+column[10]
            return strQ             
    if (table=='INV_MST'):   
            strQ +=  " FACILITYID="+column[0] +sep+" FCUSTCODE="+column[4] +sep+ " FCLASS="+column[5] +sep+" FRECTYPE="+column[6]+sep +" FBDATE="+column[7] +sep+ " FBATCH="+column[10]+sep+ " FSEQUENCE="+column[11] +sep + "FTRACK2=" +column[12] 
            return strQ    
    if (table=='INV_TRN') :
            strQ +=  " FACILITYID="+column[0] +sep+ " FCUSTCODE="+column[5] +  sep + " FBDATE="+column[6]+ sep + " FBATCH='"+column[7]+ sep + " FSEQUENCE="+column[8]+ sep + " FTRACK="+column[9] + sep + " FTRACK2="+column[10]
            return strQ       
    if (table=='PHY_MST') :
            strQ += " FACILITYID="+column[0] +sep+ " FCUSTCODE="+column[2] +  sep + " FTRACK="+column[5]+ sep + " FSERIAL="+column[6]
            return strQ 
    if (table=='LOCATION') : 
            strQ += " FACILITYID="+column[0] +sep+ " FLOCATION="+column[2] 
            return strQ 
    if (table=='CUSTOMER')   :
            strQ += " FACILITYID="+column[0] +sep+ " FCUSTCODE="+column[2] 
            return strQ 
    if  (table=='CODE2') :
            strQ += " FACILITYID="+column[0] +sep+ " FPRODUCT="+column[2] 
            return strQ 
    
    return strQ

def getDeleteWhereStatement(table,column):
    sep = " AND "   
    strQ = ''
    if (table=='ADJUST') or (table=='LOADIN')  or (table=='LOADOUT'):
            strQ += " FACILITYID="+column[0] +""+sep+ " FBATCH="+column[3] + ""+ sep + " FCUSTCODE="+column[4]
            return strQ 
    if (table=='PHY_TRN') :
            strQ += " FACILITYID="+column[0] +sep+ " FCUSTCODE="+column[2] +  sep + " FBATCH="+column[7]+ sep + " FSEQUENCE="+column[8]+ sep + " FTRACK="+column[9]+ sep + " FSERIAL="+column[10]
            return strQ             
    if (table=='INV_MST'):   
            strQ +=  " FACILITYID="+column[0] +sep+" FCUSTCODE="+column[4] +sep+ " FCLASS="+column[5] +sep+" FRECTYPE="+column[6]+sep +" FBDATE="+column[7] +sep+ " FBATCH="+column[10]+sep+ " FSEQUENCE="+column[11] +sep + "FTRACK2=" +column[12] 
            return strQ    
    if (table=='INV_TRN') :
            strQ +=  " FACILITYID="+column[0] +sep+ " FCUSTCODE="+column[5] +  sep + " FBDATE="+column[6]+ sep + " FBATCH='"+column[7]+ sep + " FSEQUENCE="+column[8]+ sep + " FTRACK="+column[9] + sep + " FTRACK2="+column[10]
            return strQ       
    if (table=='PHY_MST') :
            strQ += " FACILITYID="+column[0] +sep+ " FCUSTCODE="+column[2] +  sep + " FTRACK="+column[5]+ sep + " FSERIAL="+column[6]
            return strQ 
    if (table=='LOCATION') : 
            strQ += " FACILITYID="+column[0] +sep+ " FLOCATION="+column[2] 
            return strQ 
    if (table=='CUSTOMER')   :
            strQ += " FACILITYID="+column[0] +sep+ " FCUSTCODE="+column[2] 
            return strQ 
    if  (table=='CODE2') :
            strQ += " FACILITYID="+column[0] +sep+ " FPRODUCT="+column[2] 
            return strQ 
    
    return strQ

def getFixedWidthForFile(name):
# Function will generate a list
    filename = name + ".fixedwidthdef.txt"
    if (check_directory(name)):
        with open(filename) as myfile:            
            numlines = sum(1 for line in myfile if line.rstrip('\n'))
        list = [[[] for x in xrange(5)] for y in xrange(numlines)]
    
        workFileList.sort()
        # There are files that match, so read the config file
        fh = open(filename, "r")
        y = 0
        for line in fh:
            line = line.rstrip()
            if line != '':
    #            print line
                values = line.split()
                x = 0
                for value in values:
    #                print value
                    list[y][x] = value
                    x += 1
    #            print list
            y += 1
        print list
        return list
    else:
        return 0

try:
    from itertools import izip_longest  # added in Py 2.6
except ImportError:
    from itertools import zip_longest as izip_longest  # name change in Py 3.x

try:
    from itertools import accumulate  # added in Py 3.2
except ImportError:
    def accumulate(iterable):
        'Return running totals (simplified version).'
        total = next(iterable)
        yield total
        for value in iterable:
            total += value
            yield total

def make_parser(fieldwidths):
    cuts = tuple(cut for cut in accumulate(abs(fw) for fw in fieldwidths))
    pads = tuple(fw < 0 for fw in fieldwidths) # bool values for padding fields
    flds = tuple(izip_longest(pads, (0,)+cuts, cuts))[:-1]  # ignore final one
    parse = lambda line: tuple(line[i:j] for pad, i, j in flds if not pad)
    # optional informational function attributes
    parse.size = sum(abs(fw) for fw in fieldwidths)
    parse.fmtstring = ' '.join('{}{}'.format(abs(fw), 'x' if fw < 0 else 's')
                                                for fw in fieldwidths)
    return parse

def processFiles(workList, fixedWidth, tableName):
# Function to process the sorted list
    fieldWidths = []
    fieldNames = []
    fieldType = []
    count = 0
    fhandle = open (tableName+".sql","w")
    for fw in fixedWidth:
        fieldWidths.append(int(fw[3]))
        fieldNames.append(fw[0])
        fieldType.append(fw[4])
#    print fieldNames
    for file in workList:
        fh = codecs.open(file, "r",encoding='utf-8', errors='replace')
        for line in fh:
            parse = make_parser(fieldWidths)
            sqlCmd = line[4:10]  
            print sqlCmd
            fields = parse(line)
#            print ('{}'.format(fields))    
            i = 0
            pair = [[[] for x in xrange(2)] for y in xrange(len(fieldNames))]
            for value in fields:
#                print fieldNames[i].replace(" ",""),
#                print ('{}'.format(value.strip()))
#                print value
                if re.match("\ \ \/\ \ \/",value):
                    value = ''
                pair[i] = [ fieldNames[i], value.strip().replace("'", "\'") ,fieldType[i]]
                pair[i] = [ fieldNames[i], value.strip().replace("\\", '\\\\'),fieldType[i] ]
                print pair[i]
                i += 1
            
            
            queryString =""
            if sqlCmd == "INSERT" :
                queryString = insertStatement(tableName,pair,line,fieldNames)
            if sqlCmd == "UPDATE":
                queryString = updateStatement(tableName,pair,line,fieldNames)
            if sqlCmd == "DELETE":    
                queryString = deleteStatement(tableName,pair,line,fieldNames)
           

            print queryString
            fhandle.write(queryString)
            

    return

def insertStatement(tableName,pair,line,fieldNames):
    queryString = "UPSERT INTO " + tableName + " VALUES ('" +getPrimaryKey(line) +"', "
           
#   print queryString
    
    i = 0
    for queryValues in pair:
                #\ufffd
                #re_pattern = re.compile(u'[^\u0000-\uD7FF\uE000-\uFFFF]', re.UNICODE)
                #queryValues[1] = re_pattern.sub(u'\ufffd', queryValues[1])  
                queryValues[1] = queryValues[1].replace(u"\uFFFD", "?")
                queryValues[1] = queryValues[1].replace("'",r"\'")
#                print queryValues[1]
                
                
                if queryValues[1] == '' :
                    queryString += "NULL"
                else:
                    if queryValues[1] == '':
                        queryValues[1] = "NULL"
                        queryString += queryValues[0]+" is NULL"   
                    #match = re.match("\d{2}\/\d{2}\/\d{4}", queryValues[1])
                    if (queryValues[2] == 'D' or queryValues[2] == 'T') and queryValues[1] != 'NULL':
    #                       print ("Date found: " + queryValues[1])
                        if len(queryValues[1]) > 8 :             
                            queryString += "TO_DATE('" + queryValues[1] + "', 'MM/dd/yyyy HH:mm:ss')"
                        else:
                            queryString += "TO_DATE('" + queryValues[1] + "', 'yyyyMMdd')"  
                               
                    else:
                            if queryValues[2] == 'N'  and queryValues[1] != 'NULL':
                                    queryString += "" + queryValues[1] + ""
                                
                            else :
                                if  queryValues[1] != 'NULL':
                                    queryString += "'" + queryValues[1] + "'"  
                                    
                if i < len(fieldNames) - 1:
                        queryString += ","
                    
                i += 1

    queryString  += ");\n"
    return queryString

def updateStatement(tableName,pair,line,fieldNames):
    queryString = "UPSERT INTO " + tableName 
    
    queryString += " (SURKEY ,"
    i = 0
    for queryFieldName in pair:
                queryString += str(queryFieldName[0])
                if i < len(fieldNames) - 1:
                    queryString += ","
                i += 1
    queryString += ") SELECT SURKEY,"       
#   print queryString
    #must skip the obatch oserial value as ''
    i = 0
    column = []
    for queryValues in pair:
                #\ufffd
                #re_pattern = re.compile(u'[^\u0000-\uD7FF\uE000-\uFFFF]', re.UNICODE)
                #queryValues[1] = re_pattern.sub(u'\ufffd', queryValues[1])  
                queryValues[1] = queryValues[1].replace(u"\uFFFD", "?")
                queryValues[1] = queryValues[1].replace("'",r"\'")
#                print queryValues[1]
                
                
                if queryValues[1] == '' :
                    queryString += "NULL"
                    column.append("NULL")
                else:
                    if queryValues[1] == '':
                        queryValues[1] = "NULL"
                        queryString += queryValues[0]+" is NULL"   
                    #match = re.match("\d{2}\/\d{2}\/\d{4}", queryValues[1])
                    if (queryValues[2] == 'D' or queryValues[2] == 'T') and queryValues[1] != 'NULL':
    #                       print ("Date found: " + queryValues[1])
                        if len(queryValues[1]) > 8 :             
                            queryString += "TO_DATE('" + queryValues[1] + "', 'MM/dd/yyyy HH:mm:ss')"
                            column.append("TO_DATE('" + queryValues[1] + "', 'MM/dd/yyyy HH:mm:ss')")
                        else:
                            queryString += "TO_DATE('" + queryValues[1] + "', 'yyyyMMdd')"  
                            column.append("TO_DATE('" + queryValues[1] + "', 'yyyyMMdd')")
                               
                    else:
                            if queryValues[2] == 'N'  and queryValues[1] != 'NULL':
                                    queryString += "" + queryValues[1] + ""
                                    column.append("" + queryValues[1] + "")
                                
                            else :
                                if  queryValues[1] != 'NULL':
                                    queryString += "'" + queryValues[1] + "'"  
                                    column.append("'" + queryValues[1] + "'"  )
                                    
                if i < len(fieldNames) - 1:
                        queryString += ","
                    
                i += 1

    queryString  += " FROM "+tableName +" WHERE "
    #built the where based on list matching keys fbatch= per table    
    queryString  += getUpdateWhereStatement(tableName,column)

    queryString  += ";\n"
    return queryString

def deleteStatement(tableName,pair,line,fieldNames):
    queryString = "DELETE FROM " + tableName + " WHERE "
    
    i = 0
    column = []
    for queryValues in pair:
                #\ufffd
                #re_pattern = re.compile(u'[^\u0000-\uD7FF\uE000-\uFFFF]', re.UNICODE)
                #queryValues[1] = re_pattern.sub(u'\ufffd', queryValues[1])  
                queryValues[1] = queryValues[1].replace(u"\uFFFD", "?")
                queryValues[1] = queryValues[1].replace("'",r"\'")
#                print queryValues[1]
                
                
                if queryValues[1] == '' :
                   
                    column.append("NULL")
                else:
                    if queryValues[1] == '':
                        queryValues[1] = "NULL"
                        
                    #match = re.match("\d{2}\/\d{2}\/\d{4}", queryValues[1])
                    if (queryValues[2] == 'D' or queryValues[2] == 'T') and queryValues[1] != 'NULL':
    #                       print ("Date found: " + queryValues[1])
                        if len(queryValues[1]) > 8 :             
                            column.append("TO_DATE('" + queryValues[1] + "', 'MM/dd/yyyy HH:mm:ss')")
                        else:
                            column.append("TO_DATE('" + queryValues[1] + "', 'yyyyMMdd')")
                    else:
                            if queryValues[2] == 'N'  and queryValues[1] != 'NULL':
                                    column.append("" + queryValues[1] + "")
                            else :
                                if  queryValues[1] != 'NULL':
                                    column.append("'" + queryValues[1] + "'"  )
                                    
                    
                i += 1
    
    queryString += getDeleteWhereStatement(tableName,column)   
    queryString  += " ;\n"
    return queryString


dirList = os.listdir(filepath)
fileMatch = "\w+.fixedwidthdef.txt"
re.compile(fileMatch)
prefix = []



for files in dirList:
    if re.match(fileMatch, files):
        workFileList.append(files)
        prefix.append(re.split("\W+", files)[0])

for dataType in prefix:
    print "Processing: ",
    print dataType
    workFileList = []
    var = None
    var = getFixedWidthForFile(dataType)
    print workFileList
    print var
    if var:
        processFiles(workFileList, var, dataType)
    var = None

