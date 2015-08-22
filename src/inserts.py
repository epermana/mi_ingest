'''
Created on Aug 14, 2015

@author: edwin permana
'''
from gettext import NullTranslations

# This program will look at the files in the folder and insert into SQL

import os, os.path, re, codecs,  warnings
import hashlib
filepath = "/Users/edwin/workspace/mi_ingest/src/"
workFileList = []

def check_directory(match):
    list_dir = os.listdir(filepath)
    match = match.lower()
    count = 0
#    fileMatch = "\d+-" + match + "-\d+.TXT"
#    fileMatch = "\d+-" + match + "_STARTING_FILE.TXT"
    fileMatch = "\d+-" + match + "_starting_file.TXT"
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
           
            queryString = "UPSERT INTO " + tableName + " VALUES ('" +getPrimaryKey(line) +"', "
           
#            print queryString
    
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
           
            print queryString
            fhandle.write(queryString)
            

    return


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

