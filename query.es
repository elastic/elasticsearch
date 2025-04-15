% Title: Elasticsearch Query with Pinned Documents


POST my-index/_doc/1
{ "title": "Beginner's Guide to ML" }

POST my-index/_doc/2
{ "title": "Sponsored ML Course" }

POST my-index/_doc/3
{ "title": "Advanced Topics in Machine Learning" }


GET my-index/_search
{
  "query": {
    "match": {
      "title": "machine learning"
    }
  }
}

GET my-index/_search
{
  "query": {
    "pinned": {
      "ids": ["1", "2"],
      "organic": {
        "match": {
          "title": "machine learning"
        }
      }
    }
  }
}

GET my-index/_search
{
  "query": {
    "pinned": {
      "ids": ["doc-id-1", "doc-id-2"],  // pinned to top
      "organic": {
        "match": {
          "title": "machine learning"
        }
      }
    }
  }
}

GET my-index/_search
{
  "explain": true,
  "query": {
    "pinned": {
      "ids": ["1", "2"],
      "organic": {
        "match": {
          "title": "machine learning"
        }
      }
    }
  }
}

