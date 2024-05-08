import modeler.api

s = modeler.script.session()

repo = s.getRepository()

stream = repo.retrieveStream("/repository/address/Stream1.str",None,None,False)

script = stream.getScript()

script_edited = script.replace('A', 'a')

stream.setScript(script_edited)

repo.storeStream(stream, "/repository/address/Stream1.str", None)
