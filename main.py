from mongodb import MongoDB
from elastic import Elastic


def fprint(*args, **kwargs):
    print(*args, **kwargs, flush=True)


if __name__ == "__main__":   
    
    # Connexion a mongoDB
    db = MongoDB()

    # Connexion a Elastic
    es = Elastic()

    # Selection de la Collection mongoDB tutoriel.cities
    db.use_database("tutoriel")
    db.use_collection("cities")

    # reinitialisation de l'index Elastic cities
    if es.has_index("cities"):
        es.use_index("cities")
        es.delete_index()
    else:
        es.index = "cities"
    print()

    # boucle sur la liste des documents de la Collection pour Ajout dans l'index
    for doc in db.find({}, {"_id": 0}):
        fprint(f"Add doc: {doc} .. ", end="")
        es.add_doc(doc)
    
    # Force la mise a jour et l'enregistrement dans Elastic
    es.update_index()
    es.flush_index()
    print("Nombre de documents ajoutes :", es.count())
    
    # Fermeture des connexions
    es.close()
    db.close()
