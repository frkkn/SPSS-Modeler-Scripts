
import modeler.api
import re, csv

session = modeler.script.session()
repo = session.getRepository()
h_repo = session.getRepository().getRepositoryHandle()

class Stream():
    def __init__(self, stream, nodes):
        self.stream = stream
        self.nodes = nodes

class Node():
    def __init__(self, node, tables):
        self.node = node
        self.tables = tables
        
class Table():
    def __init__(self, table):
        self.table = table
        
class KaynakFilter(modeler.api.NodeFilter):
    def accept(this, node):
        return node.getTypeName() in ["database", "databaseexport"]


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

    folders.append(h_repo.getFolder(directory))

    while (len(folders) > 0):
        folder = folders.pop()
        for stream in folder.getFileObjects():
            if stream.getMimeType().getSubType() == "x-vnd.spss-clementine-stream":
                streams.append(Stream(stream,nodelari_bul(stream)))

        for f in folder.getFolders():
            folders.append(f)  

    return streams

def nodelari_bul(stream_):
    stream = repo.retrieveStream(stream_.getFullPathName(), None, "LATEST", False)
    all_nodes = list(stream.findAll(KaynakFilter(), True))
    stream.close()
    del stream
    
    nodes_temp = []
    for node in all_nodes:
        nodes_temp.append(Node(node,tablolari_bul(node)))
    
    return nodes_temp

def tablolari_bul(node):
    tables = []

    if node.getTypeName() == "database":
        mode = node.getPropertyValue("mode")
        tablename = node.getPropertyValue("tablename")
        query = node.getPropertyValue("query")
        if mode == "Table":
            tables.append(Table(tablename))
        elif mode == "Query":
            temp_tables = sorgudaki_tablolar(query)
            for table in temp_tables:
                tables.append(Table(table))
            
    elif node.getTypeName() == "databaseexport":
        tables.append(Table(node.getPropertyValue("tablename")))
    
    return tables



# Insert paths in this list
dizin = '/Repository/TBUGDB/Prod/Projeler'
dizinler = []

f = h_repo.getFolder(dizin)

for i in f.getFolders():
    dizinler.append(dizin + "/" + str(i))

# -OR- Insert paths in this list
dizinler = [
u'/here/goes/the/path',
u'/here/goes/the/path2',
]
    
file_ = open(r"z:\saveFile.csv", "w")

str_ = "PATH;STREAM NAME;LAST MODIFIED BY;LAST MODIFIED DATE;NODE ID;NODE LABEL;NODE TYPE;DATASOURCE;TABLE\n"
file_.write(str_)
# file_.close()

for dizin in dizinler:
    s = dizindeki_akislar(dizin)
    
    # file_ = open(r"z:\saveFile.csv", "a")
    
    for stream in s:
        for node in stream.nodes:
                for table in node.tables:
                    str_ = str(stream.stream.getParentFolderPathFromFullPath(stream.stream.getFullPathName())) + ";"+ \
                        stream.stream.getName() +";"+ \
                        str(stream.stream.getLastModifiedBy()) +";"+ \
                        str(stream.stream.getLastModifiedDate()) +";"+ \
                        str(node.node.getID()) + ";" +     \
                        str(node.node.getLabel()) +";"+    \
                        str(node.node.getTypeName()) +";"+    \
                        str(node.node.getPropertyValue("datasource")) + ";"+ \
                        str(table.table) + \
                        "\n"
                    file_.write(str_)
    
    file_.close()              
