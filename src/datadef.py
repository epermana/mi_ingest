from collections import defaultdict

afile = open('DATA_FIXED_WIDTH.TXT','r')
numLines = sum(1 for line in open('DATA_FIXED_WIDTH.TXT','r'))
start_header = 0
compareMain = 0

d = defaultdict(list)
strQ =""

main = ''
for line in afile:
    line = line.rstrip()
        
    if line != '':
        if "*" in line:
            start_header += 1
            print line
        else:
            if main == compareMain:
                print main + " " + line
                splitData = line.split()
                splitData.insert(0,main)
                print main + " ",
                print splitData
                d[main].append(splitData)
            compareMain = main
    if start_header:
        if start_header > 1:
            compareMain = main
            start_header = 0
        else:
            main = afile.next().rstrip()
            print str(start_header) + " MAIN: " + main

       
# This function will generate valid MSSQL Schema
for table in d:
        strQ += "CREATE TABLE IF NOT EXISTS "
        strQ += table
        strQ += "( SURKEY VARCHAR NOT NULL PRIMARY KEY ,"
        fhandle = open ("MILLARD-DDL.SQL","w")
        for element in d[table]:
            fieldName = element[1]
            dataType  = element[2]
            dataLength= element[3]
            strQ += fieldName
            strQ += " "
            if dataType == "C":
                dt = "varchar"
                dl = int(dataLength) + 1
                strQ += "\tvarchar("
                strQ += str(dl)
                strQ += "),"
            if dataType == "N":
                dt = "double"
                strQ += "\tdouble,"
            if dataType == "T":
                dt = "date"
                strQ += "\tdate,"
            if dataType == "D":
                dt = "date"
                strQ += "\tdate,"
            if dataType == "L":
                dt = "char"
                strQ += "char(1),"
        strQ = strQ[:-1]
        
        strQ += ");\n"
        
        fhandle.write(strQ)
        print strQ


# This function will ganerate the files that show which fields are where
for table in d:
        print table
        filename = table + ".fixedwidthdef.txt"
        filehandle = open(filename, "w")
        i = 0
        for elements in d[table]:
            fieldName   = elements[1]
            fieldType   = elements[2]
            fieldLength = elements[3]
            filehandle.write(fieldName)
            filehandle.write("\t")
            filehandle.write(str(i))
            i += int(fieldLength) - 1
            filehandle.write("\t")
            filehandle.write(str(i))
            filehandle.write("\t")
            filehandle.write(fieldLength)
            filehandle.write("\t")
            filehandle.write(fieldType)
            filehandle.write("\n")
            i += 1

