stream = modeler.script.stream()

n_old = stream.findByID("id6W4VDPU8FLI")

n_new = stream.createAt(n_old.getTypeName(), n_old.getName(), n_old.getXPosition(), n_old.getYPosition())

# n_new.setPropertyValuesFrom(n_old)
n_new.setPropertyValue("use_credential", True)
n_new.setPropertyValue("credential", "CRED_NAME_HERE")
n_new.setPropertyValue("datasource", "DS_NAME_HERE")
n_new.setPropertyValue("mode", n_eski.getPropertyValue("mode"))

mode = n_old.getPropertyValue("mode")
if mode == "Table":
    n_new.setPropertyValue("tablename", n_old.getPropertyValue("tablename"))
elif mode == "Query":
    n_new.setPropertyValue("query", n_old.getPropertyValue("query"))

