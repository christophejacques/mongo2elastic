from typing import Union, Any
from elasticsearch import Elasticsearch
import locale


def eprint(*args, **kwargs):
    print(*args, **kwargs, end="")


locale.setlocale(locale.LC_ALL, '')
variable = getattr(locale, "_override_localeconv") 
variable = {'mon_thousands_sep': ' '}


def fprint(*args, **kwargs):
    print(*args, **kwargs, flush=True)


class Elastic:

    def __init__(self, host: str = "", port: int = 0):
        if host == "":
            host = "localhost"
        if port == 0:
            port = 9200
            
        self.es = Elasticsearch([{"scheme": "http", "host": host, "port": port}], request_timeout=2.0)
        fprint(f"Connecting to ElasticSearch ({host}:{port}) ... ", end="")
        if not self.es.ping():
            print("failed.")
            exit()

        fprint(f"done in v{self.es.info().get('version').get('number')} ")
        self.index: str = ""

    def close(self) -> None:
        self.es.close()
        print("ElasticSearch connection closed.")

    def has_index(self, index_name: str) -> bool:
        return bool(self.es.indices.exists(index=index_name))

    def use_index(self, index_name: str) -> None:
        if not self.es.indices.exists(index=index_name):
            raise Exception(f"L'index '{index_name}' n'existe pas.")

        self.index = index_name
        fprint(f"Current index set to : {self.index}")

    def get_mapping(self) -> list:
        liste: list = list()
        for mapping in self.es.indices.get_mapping(index=self.index)[self.index]["mappings"]["properties"]:
            liste.append(mapping)

        return liste

    def liste_index(self) -> None:
        print("Liste des index :")
        # for idx in filter(lambda x: not x.startswith("."), es.indices.get(index="*").keys()):
        for idx in self.es.indices.get(index="*").keys():
            print("  -", idx, end=" ")
            print(f"({self.es.cat.count(index=idx).body.split()[2]} enregs)")
            for mapping in self.es.indices.get_mapping(index=idx)[idx]["mappings"]["properties"]:
                print("     >", mapping, end=" : ")
                print(self.es.indices.get_mapping(index=idx)[idx]["mappings"]["properties"].get(mapping).get("type"))
            print()

        print()
        exit()

    def search(self, index_name: str, query: Union[str, dict], size: int = -1) -> dict[str, Any]:
        if size == -1:
            size = self.count(index_name)
        fprint(f"Searching in {index_name}, {query} ... ", end="")
        if isinstance(query, str):
            properties = self.es.search(index=index_name, q=query, size=size)
        elif isinstance(query, dict):
            properties = self.es.search(index=index_name, query=query, size=size)
        else:
            raise Exception("Le parametre Query doit etre de type str ou dict.")

        taille: int = properties["hits"]["total"].get("value", 0)
        print("found", taille, "documents", end="")
        if size < taille:
            print(f" (but get only {size})", end="")
        fprint()
        return properties.get("hits")

    def count(self, index_name=None) -> int:
        index_name = self.index if index_name is None else index_name

        return self.es.indices.stats(index=index_name)["indices"][index_name]["total"]["docs"].get("count", 0)
        # return self.es.count(index=index_name).get("count", 0)

    def flush_index(self, index_name=None) -> None:
        fprint(f"Flushing index '{self.index}' ... ", end="")
        index_name = self.index if index_name is None else index_name
        self.es.indices.flush(index=index_name)
        fprint("done.")

    def delete_index(self, index_name=None) -> None:
        index_name = self.index if index_name is None else index_name
        fprint(f"Suppression de tous les documents de l'index: '{index_name}'")
        nombre: int = self.count(index_name)
        fprint(f"- Suppression de {nombre} documents", end=" ... ")
        self.es.delete_by_query(index=index_name, q="_id: *")
        print("done.")

        print("- ", end="")
        self.update_index(index_name)

        print("- ", end="")
        self.flush_index(index_name)

    def drop_index(self, index_name: str) -> None:
        fprint(f"Suppression de l'index: '{index_name}'", end=" ... ")
        self.es.indices.delete(index=index_name)
        fprint("done")

    def aggregations(self):
        # query = {"match": {"age": 36}}
        # query = {"bool": {"must": [{"match": {"age": 36}}]}}
        # query = {"bool": {"should": [{"match": {"age": 36}}]}}
        # query = {"range": {"age": {"lte": 36}}}
        aggs = {
            "max_number": {
              "terms": {
                "field": "date",
                "format": "yyyy-MM-dd",
                "size": 1,
                "order": {
                  "_key": "desc"
                }
              },
              "aggs": {
                "top_hits": {
                  "top_hits": {
                    "size": 30
                  }
                }
              }
            }
          }

        # result = es.search(index="bank", query={"match": {"age": 36}})
        # result = es.search(index="bank", query=query)
        result = self.es.search(index="suivi-activite", aggs=aggs)

        # aggregations.max_number.buckets.top_hits.hits.hits.fields.total_individus
        total: int = 0
        liste: str = ""
        for doc in result["aggregations"]["max_number"]["buckets"]:  # .get("hits").get("hits"):
            print("date =", doc["top_hits"]["hits"]["hits"][0]["_source"]["date"])
            for elt in doc["top_hits"]["hits"]["hits"]:
                total += elt["_source"]["total_individus"]
                liste += elt["_source"]["code_partenaire"] + ", "

        print("codes partenaires =", liste[:-2])
        print("Total individus = ", end="")
        print(locale.format_string('%.d', total, grouping=True, monetary=True))
        exit()

    def add_doc(self, document) -> None:
        resp = self.es.index(index=self.index, document=document)
        fprint(resp['result'])

    def update_index(self, index_name=None) -> None:
        # Forcez Elasticsearch à mettre à jour les index
        fprint(f"Updating index '{self.index}' ... ", end="")
        index_name = self.index if index_name is None else index_name
        self.es.indices.refresh(index=index_name)
        fprint("done.")


if __name__ == "__main__":
    es = Elastic()

    es.use_index("suivi-activite")
    taille: int = es.count()
    mapping: list = es.get_mapping()
    fprint("Nb docs dans l'index :", taille)

    index_name = "creation"    
    es.use_index(index_name)
    es.delete_index()

    # query = {"match": {"date": "2023-10-05"}}
    query = "code_partenaire = 'ARA' or code_partenaire = 'DRP'"
    query = "code_partenaire in ('ARA', 'DRP', 'NAQ')"
    # properties = es.search("suivi-activite", query="_id:*", size=5)
    properties = es.search("suivi-activite", query=query, size=15)

    num: int = 0
    for docs in properties["hits"]:
        document: dict = {}
        num += 1
        print(f"{num:3}", docs.get("_id"), ": ", end="")
        for cle in mapping:
            document[cle] = docs["_source"].get(cle)
            print(str(document[cle])[:40], end=" / ")

        es.add_doc(document)

    es.update_index()
    es.flush_index()
    print("Nombre de documents ajoutes :", es.count())
    es.close()
