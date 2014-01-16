#!/bin/sh

# Init an index that includes wordcount for the field that we will be searching on:

curl -s -XPUT "http://localhost:9200/termscore" -d'
{
   "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0
   },
   "mappings": {
      "doc": {
         "properties": {
            "text": {
               "type": "multi_field",
               "fields": {
                  "text": {
                     "type": "string"
                  },
                  "word_count": {
                     "type": "token_count",
                     "store": "yes",
                     "analyzer": "standard"
                  }
               }
            }
         }
      }
   }
}'

curl -s -XPOST "http://localhost:9200/termscore/doc" -d'
{
   "text": "John Smith is the main villain in the Matrix."
}'

curl -s -XPOST "http://localhost:9200/termscore/doc" -d'
{
   "text": "John \"Hannibal\" Smith is the leader of the A-team."
}'

curl -s -XPOST "http://localhost:9200/termscore/_refresh"

# search for people that are named John Smith:

curl -s -XPOST "http://localhost:9200/termscore/doc/_search?pretty" -d'
{
   "query": {
      "function_score": {
         "query": {
            "bool": {
               "must": [
                  {
                     "match": {
                        "text": {
                           "query": "john smith",
                           "operator": "and"
                        }
                     }
                  }
               ]
            }
         },
         "functions": [
            {
               "script_score": {
                  "params": {
                     "field": "text",
                     "terms": [
                        "john",
                        "smith"
                     ]
                  },
                  "script": "phrase_script_score",
                  "lang": "native"
               }
            }
         ],
         "boost_mode": "replace"
      }
   }
}'

curl -s -XPOST "http://localhost:9200/termscore/doc" -d'
{
   "text": "I am Sam. I am Sam. Sam I am. That Sam-I-am.    That Sam-I-am! I do not like that Sam-I-am. Do you like green eggs and ham? I do not like them, Sam-I-am.I do not like green eggs and ham."
}'

curl -s -XPOST "http://localhost:9200/termscore/doc" -d'
{
   "text": "Would you like them Here or there? I would not like them here or there. I would not like them anywhere. I do not like green eggs and ham. I do not like them, Sam-I-am."
}'

curl -s -XPOST "http://localhost:9200/termscore/doc" -d'
{
   "text": "Would you like them in a house? Would you like them with a mouse? I do not like them in a house. I do not like them with a mouse. I do not like them here or there. I do not like them anywhere. I do not like green eggs and ham. I do not like them, Sam-I-am."
}'

curl -s -XPOST "http://localhost:9200/termscore/_refresh"

# Search for "I am Sam" with cosine similarity:

curl -s -XPOST "http://localhost:9200/termscore/doc/_search?pretty" -d'
{
   "script_fields": {
      "i-tf": {
         "script": "_index[\"text\"][\"i\"].tf()"
      },
      "am-tf": {
         "script": "_index[\"text\"][\"am\"].tf()"
      },
      "sam-tf": {
         "script": "_index[\"text\"][\"sam\"].tf()"
      }
   },
   "query": {
      "function_score": {
         "query": {
            "bool": {
               "must": [
                  {
                     "match": {
                        "text": {
                           "query": "sam i am",
                           "operator": "and"
                        }
                     }
                  }
               ]
            }
         },
         "functions": [
            {
               "script_score": {
                  "params": {
                     "field": "text",
                     "terms": [
                        "sam",
                        "i",
                        "am"
                     ],
                     "weights": [
                        1.0,
                        1.0,
                        1.0
                     ]
                  },
                  "script": "cosine_sim_script_score",
                  "lang": "native"
               }
            }
         ],
         "boost_mode": "replace"
      }
   }
}'

# Search for "I am Sam" with naive tf-ifd score:

curl -s -XPOST "http://localhost:9200/termscore/doc/_search?pretty" -d'
{
   "script_fields": {
      "i-tf": {
         "script": "_index[\"text\"][\"i\"].tf()"
      },
      "am-tf": {
         "script": "_index[\"text\"][\"am\"].tf()"
      },
      "sam-tf": {
         "script": "_index[\"text\"][\"sam\"].tf()"
      }
   },
   "query": {
      "function_score": {
         "query": {
            "bool": {
               "must": [
                  {
                     "match": {
                        "text": {
                           "query": "sam i am",
                           "operator": "and"
                        }
                     }
                  }
               ]
            }
         },
         "functions": [
            {
               "script_score": {
                  "params": {
                     "field": "text",
                     "terms": [
                        "sam",
                        "i",
                        "am"
                     ]
                  },
                  "script": "tfidf_script_score",
                  "lang": "native"
               }
            }
         ],
         "boost_mode": "replace"
      }
   }
}'

# Search for "I am Sam" with language model scoring:

curl -s -XPOST "http://localhost:9200/termscore/doc/_search?pretty" -d'
{
   "script_fields": {
      "i-tf": {
         "script": "_index[\"text\"][\"i\"].tf()"
      },
      "am-tf": {
         "script": "_index[\"text\"][\"am\"].tf()"
      },
      "sam-tf": {
         "script": "_index[\"text\"][\"sam\"].tf()"
      }
   },
   "query": {
      "function_score": {
         "query": {
            "bool": {
               "must": [
                  {
                     "match": {
                        "text": {
                           "query": "sam i am",
                           "operator": "and"
                        }
                     }
                  }
               ]
            }
         },
         "functions": [
            {
               "script_score": {
                  "params": {
                     "field": "text",
                     "terms": [
                        "sam",
                        "i",
                        "am"
                     ],
                     "lambda": 0.5,
                     "word_count_field": "text.word_count"
                  },
                  "script": "language_model_script_score",
                  "lang": "native"
               }
            }
         ],
         "boost_mode": "replace"
      }
   }
}'