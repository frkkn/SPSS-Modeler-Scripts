import modeler.api
import time,datetime,sys


session = modeler.script.session()
repo = session.getRepository()
h_repo = session.getRepository().getRepositoryHandle()
stream_manager = session.getStreamManager()

isci_sayisi = 10
dizin = r"/path/"
ana_ad = "ana.str"
isci_ad = "isci.str"


ana = repo.retrieveStream(dizin+ana_ad, None, "LATEST", True) #main
isci = repo.retrieveStream(dizin+isci_ad, None, "LATEST", True) #worker

isci_ad = []
isci_no = {}
for i in range(isci_sayisi):    
    ad = "tmp"+str(i)+"_isci.str"
    isci_ad.append(ad)
    isci_no[ad] = i



for ad in isci_ad:
    print dizin+ad
    repo.storeStream(isci, dizin+ad, "LATEST")

isci.close()

isci_akis = {}
for ad in isci_ad:
    akis = repo.retrieveStream(dizin+ad, None, "LATEST", True)
    akis.setKeyedPropertyValue("parameters", "KERNEL", isci_no[ad])
    isci_akis[ad] = akis


#is burada yapilacak


h_ana = session.spawn(ana, None)
h_isci = {}
for ad in isci_ad:
    h_isci[ad] = session.spawn(isci_akis[ad], None)

print "bekle"
time.sleep(60)

h_ana.terminate()
for ad in isci_ad:
    h_isci[ad].terminate()

print "ana " + str(h_ana.getExecutionState())
for ad in isci_ad:
    print ad + " " + str(h_isci[ad].getExecutionState())

ana.close()
for ad in isci_ad:
    isci_akis[ad].close()

for d in stream_manager.getDiagrams():
    d.close()

for ad in isci_ad:
    h_repo.deleteFileObject(h_repo.getFileObject(dizin+ad))
    print ad + " silindi"

print "ok"
