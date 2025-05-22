PUT books
{
  "mappings": {
    "properties": {
      "title": {
        "type": "text"
      },
      "author": {
        "type": "text"
      },
      "publication_year": {
        "type": "integer"
      },
      "genre": {
        "type": "keyword"
      }
    }
  }
}

# Add some sample books
POST books/_bulk
{ "index": { "_id": "1" } }
{ "title": "Neuromancer", "author": "William Gibson", "publication_year": 1984, "genre": "science_fiction" }
{ "index": { "_id": "2" } }
{ "title": "The Alienist", "author": "Caleb Carr", "publication_year": 1994, "genre": "mystery" }
{ "index": { "_id": "3" } }
{ "title": "Alien: River of Pain", "author": "Tim Lebbon", "publication_year": 2014, "genre": "science_fiction" }


# Test query with pinned retriever
GET books/_search
{
  "search.retriever": {
    "pinned": {
      "retriever": {
        "standard": {
          "query": {
            "multi_match": {
              "query": "alien"
            }
          },
          "sort": [
            {
              "publication_year": "desc"
            }
          ]
        }
      },
      "ids": [
        {
          "_id": "1",
          "_index": "books"
        }
      ]
    }
  }
}