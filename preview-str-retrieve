s = modeler.script.stream()
session = modeler.script.session()
repo = session.getRepository()
h_repo = session.getRepository().getRepositoryHandle()

stream = "/Repository/TBUGDB/Development/Projeler/Urun_Egilim_Modeli/Akislar/001_CST_GPP_PD_RCRCL4.str"

print "retrieve.."
stream = repo.retrieveStream(stream, None, "LATEST", False)

print "nodelar.."
nodes = stream.findAll("databaseexport", None)

print str(len(nodes)) + " node bulundu"

basarili = 0
for node in nodes:
    try:    
        stream.preview(node)
        basarili += 1
        print str(basarili) + " basarili"
    except:
        print node.getLabel() + " hata"

print str(basarili) + " out of " + str(len(nodes))
        
stream.close()
