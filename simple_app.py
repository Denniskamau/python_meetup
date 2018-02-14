from elasticsearch import Elasticsearch

# create an instance of elasticsearch and assign it to port 9200
ES_HOST = {"host": "localhost", "port": 9200}
es = Elasticsearch(hosts=[ES_HOST])


def create_index(index_name):
    """Function to create index using the index name provided."""
    resp = es.indices.create(index=index_name)
    print(resp)


def document_add(index_name, doc_type, doc, doc_id=None):
    """Funtion to add a document by providing index_name,
    document type, document contents as doc and document id."""
    resp = es.index(index=index_name, doc_type=doc_type, body=doc, id=doc_id)
    print(resp)


def document_view(index_name, doc_type, doc_id):
    """Function to view document by providing the index name,
    type as doc_type, and id by doc_id"""
    resp = es.get(index=index_name, doc_type=doc_type, id=doc_id)
    document = resp["_source"]
    print(document)


def document_update(index_name, doc_type, doc_id, doc=None, new=None):
    """Edit a document by specifying the index name, type as doc_type,
    document id as doc_id, details to be edited as doc and new field as new."""
    if doc:
        resp = es.index(index=index_name, doc_type=doc_type,
                        id=doc_id, body=doc)
        print(resp)
    else:
        resp = es.update(index=index_name, doc_type=doc_type,
                         id=doc_id, body={"doc": new})


def document_delete(index_name, doc_type, doc_id):
    """Delete a document by specifying the index name,
    the document type and the document id"""
    resp = es.delete(index=index_name, doc_type=doc_type, id=doc_id)
    print(resp)


def delete_index(index_name):
    """Delete an index by specifying the index name"""
    resp = es.indices.delete(index=index_name)
    print(resp)
