
from com.spss.clementine.session.util import ReadTaskSettings, Files
from com.spss.clementine.component.persist import FileType
from com.spss.psapi.core import FileFormat

from java.io import File
from java.net import URI
from java.util import Date

import re

def sorgudaki_tablolar(sql_str):
    sql_str = sql_str.upper()
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)
    lines = [line for line in q.splitlines() if not re.match(r"^\s*(--)", line)]
    q = " ".join([re.split(r"--", line)[0] for line in lines])
    q = re.sub(r"\s*,\s*", ", ", q)
    q = re.sub(r"\s*\)\s*", " ) ", q)
    q = re.sub(r"\s*\(\s*", " ( ", q)
    q = re.sub(r"\s*;\s*", " ; ", q)
    q = re.sub(r"[\t]+", " ", q)
    q = re.sub(r"[ ]{2,}", " ", q)
    sql_str = q
    q = re.findall(r"(?<= FROM | JOIN ).+?(?= WHERE | SELECT | GROUP | ORDER | UNION | ON | \)|;|$)", sql_str)
    for i in q:
        print(i)
        if type(i) == str:
            q_temp = re.findall(r"(?<= FROM | JOIN ).+?(?= | WHERE | SELECT | GROUP | ORDER | UNION | ON | \)|;|$)", i)
            if len(q_temp) > 0:
                q.append(q_temp[0])
    q_create = re.findall(r"(?<=CREATE TABLE ).+?(?= )", sql_str)
    q_drop = re.findall(r"(?<=DROP TABLE ).+?(?= |$)", sql_str)
    q_trunc = re.findall(r"(?<=TRUNCATE TABLE ).+?(?= |$)", sql_str)
    temp = []
    for i in q:
        if not re.search(r"\(", i):
            temp.append(i)
    q = temp
    temp = []
    for i in q:
        for j in re.split(r",", i):
            temp.append(j)
    q = temp
    temp = []
    for i in q:
        if re.search(r" ", i.strip()):
            temp.append(re.split(" ", i.strip())[0])
        else:
            temp.append(i.strip())
    q = temp
    if len(q_create)>0:
        for i in q_create:
            q.append(i.strip())
    if len(q_drop)>0:
        for i in q_drop:
            q.append(i.strip())
    if len(q_trunc)>0:
        for i in q_trunc:
            q.append(i.strip())
    return q

def dizindeki_akislar(directory):
    streams = []
    folders = []
    yesterday = str(Date().toInstant().minusSeconds(1*24*60*60))[0:10]

    folders.append(h_repo.getFolder(directory))

    while (len(folders) > 0):
        folder = folders.pop()
        for stream in folder.getFileObjects():
            if stream.getMimeType().getSubType() == "x-vnd.spss-clementine-stream":
                streams.append(stream)
#                 if str(stream.getLastModifiedDate().toInstant())[0:10] >= yesterday: # or True: 
#                     streams.append(stream)

        for f in folder.getFolders():
            folders.append(f)  

    return streams

session = modeler.script.session()
stream = modeler.script.stream()
repo = session.getRepository()
h_repo = session.getRepository().getRepositoryHandle()
fileType = FileType.getFileTypeForFileFormat(FileFormat.PROCESSOR_STREAM)

sonuc = open("z:/saveFile.csv","w")
skipped = open("z:/skipped.csv","w")

sonuc.write("PATH;STREAM NAME;LAST MODIFIED BY;LAST MODIFIED DATE;NODE TYPE;NODE ID;DATASOURCE;OWNER;TABLE\n")

# dizin = '/Repository/TBUGDB/Prod/Projeler/BIREYSEL_ACRM'
akislar = dizindeki_akislar('/Prod/YGDB102_JOBS')
print "Toplam akis sayisi: "  + str(len(akislar))

i = len(akislar)
for s in akislar:
    uri = s.toURI()
    rts = ReadTaskSettings(uri,"")

    streamName = s.getName()
    print str(i) + " : " + streamName
    i=i-1
#     if streamName in ['12820A_KOBI_TIC_DEGER_SEGMENT.str']:
#         continue

    try:
        file = session.getAsLocalFile(rts, fileType) 
        fileXml = Files.readStreamDocument(session, None, file, "")
        file.delete()
    except:
        print streamName + " skipped."
        skipped.write(str(uri))
        continue

    

    lastSavedBy = str(s.getLastModifiedBy())
    lastSavedDate = str(s.getLastModifiedDate().toInstant())[0:10]
    
    fullPathName = str(s.getParentFolderPathFromFullPath(s.getFullPathName()))
#     print fullPathName

    finds = re.findall("((ODBCImportNode)(.*?)(\</properties\>))", str(fileXml), flags=re.DOTALL)
    
    for f in finds:
        nodeID = re.findall("(uid=\")(.*?)(\")", f[0], flags=re.DOTALL)[0][1]

        importModeRes = re.findall("(\<importMode\>)(.*)(\<\/importMode\>)", f[0], flags=re.DOTALL)
        importMode = importModeRes[0][1] if len(importModeRes) > 0 else ""

        dataSourceRes = re.findall("(\<datasource\>)(.*)(\<\/datasource\>)", f[0], flags=re.DOTALL)
        dataSource = dataSourceRes[0][1]  if len(dataSourceRes) > 0 else ""

        if importMode == "table":
            tableNameRes = re.findall("(\<tableName\>)(.*)(\<\/tableName\>)", f[0], flags=re.DOTALL)
            tableName = tableNameRes[0][1] if len(tableNameRes) > 0 else ""
            owner = ""
            if len(tableName.split(".")) == 3:
                owner = tableName.split(".")[0]
                tableName = tableName.split(".")[2]
            elif len(tableName.split(".")) == 2:
                owner = tableName.split(".")[0]
                tableName = tableName.split(".")[1]
            elif len(tableName.split(".")) == 1:
                owner = ""
                tableName = tableName.split(".")[0]

            sonuc.write(fullPathName +";"+ streamName +";"+ lastSavedBy +";"+ lastSavedDate +";"+  "KAYNAK" +";"+ nodeID +";"+ dataSource +";"+ owner +";"+ tableName +"\n")
        
        elif importMode == "query":
            queryTextRes = re.findall("(\<queryText\>)(.*)(\<\/queryText\>)",f[0],flags=re.DOTALL)
            queryText = queryTextRes[0][1] if len(queryTextRes) > 0 else ""
            tables = sorgudaki_tablolar(queryText)

            for tableName in tables:
                owner = ""
                if len(tableName.split(".")) == 3:
                    owner = tableName.split(".")[0]
                    tableName = tableName.split(".")[2]
                elif len(tableName.split(".")) == 2:
                    owner = tableName.split(".")[0]
                    tableName = tableName.split(".")[1]
                elif len(tableName.split(".")) == 1:
                    owner = ""
                    tableName = tableName.split(".")[0]
                else:
                    pass
                sonuc.write(fullPathName +";"+ streamName +";"+ lastSavedBy +";"+ lastSavedDate +";"+  "KAYNAK" +";"+ nodeID  +";"+ dataSource +";"+ owner +";"+ tableName +"\n")

    finds = re.findall("((ODBCExportNode)(.*?)(\</properties\>))", str(fileXml), flags=re.DOTALL)
    for f in finds:
        nodeID = re.findall("(uid=\")(.*?)(\")", f[0], flags=re.DOTALL)[0][1]
        tableNameRes = re.findall("(\<tableName\>)(.*)(\<\/tableName\>)", f[0], flags=re.DOTALL)
        tableName = tableNameRes[0][1] if len(tableNameRes) > 0 else ""
        dataSourceRes = re.findall("(\<datasource\>)(.*)(\<\/datasource\>)", f[0], flags=re.DOTALL)
        dataSource = dataSourceRes[0][1] if len(dataSourceRes) > 0 else ""

        owner = ""
        if len(tableName.split(".")) == 3:
            owner = tableName.split(".")[0]
            tableName = tableName.split(".")[2]
        elif len(tableName.split(".")) == 2:
            owner = tableName.split(".")[0]
            tableName = tableName.split(".")[1]
        elif len(tableName.split(".")) == 1:
            owner = ""
            tableName = tableName.split(".")[0]

        sonuc.write(fullPathName +";"+ streamName +";"+ lastSavedBy +";"+ lastSavedDate +";"+  "HEDEF" +";"+ nodeID  +";"+ dataSource +";"+ owner +";"+ tableName +"\n")

    sonuc.flush()

sonuc.close()
skipped.close()

# stream.findByType("excelexport","EXPORT_EXCEL").run(None)
# stream.findByType("databaseexport","EXPORT_DW").run(None)
