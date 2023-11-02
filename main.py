from mongodb import MongoDB
from elastic import Elastic


def fprint(*args, **kwargs):
    print(*args, **kwargs, flush=True)


if __name__ == "__main__":   
    
    # Get the database
    db = MongoDB()
    es = Elastic()

    db.use_database("tutoriel")
    db.use_collection("cities")

    if es.has_index("cities"):
        es.use_index("cities")
        es.delete_index()
    else:
        es.index = "cities"
    print()

    for doc in db.find({}, {"_id": 0}):
        fprint(f"Add doc: {doc} .. ", end="")
        es.add_doc(doc)

    es.update_index()
    es.flush_index()
    print("Nombre de documents ajoutes :", es.count())
    es.close()
