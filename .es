PUT my-index
{
  "mappings": {
    "properties": {
      "title": { "type": "keyword" },
      "text": {
        "type": "text"
      }
    }
  }
}

### Doc 1
POST my-index/_doc/1
{
  "title": "Solar power overview",
  "text": "Solar power is a renewable energy source that converts sunlight into electricity. Modern solar farms use large fields of photovoltaic panels. Many reports compare solar power with wind and hydro energy. This document explains how solar energy can power homes and businesses in urban areas."
}

### Doc 2
POST my-index/_doc/2
{
  "title": "Wind turbines guide",
  "text": "Wind turbines capture kinetic energy from moving air. Offshore wind farms are often built in shallow coastal waters. Engineers compare wind and solar energy when designing hybrid renewable systems. This guide focuses on turbine types, maintenance schedules, and grid integration."
}

### Doc 3
POST my-index/_doc/3
{
  "title": "Databases and indexing",
  "text": "Search engines rely on inverted indexes to retrieve documents efficiently. In many architectures, a ranking model reranks candidate documents. This document describes how ranking models can focus on specific passages or chunks of text. It also compares traditional BM25 with neural rerankers."
}

### Doc 4
POST my-index/_doc/4
{
  "title": "Machine learning for energy",
  "text": "Machine learning models can forecast electricity demand and optimize renewable energy usage. Some systems use rerankers to prioritize alerts and recommendations. This document covers supervised learning, feature engineering, and evaluation metrics for energy prediction."
}


POST my-index/_refresh

PUT _inference/rerank/elastic-rerank
{
  "service": "elasticsearch",
  "service_settings": {
    "model_id": ".rerank-v1",      // internal rerank model id
    "num_threads": 1,
    "adaptive_allocations": {
      "enabled": true,
      "min_number_of_allocations": 1,
      "max_number_of_allocations": 10
    }
  }
}

PUT _inference/rerank/cohere-rerank
{
  "service": "cohere",
  "service_settings": {
    "model_id": "rerank-english-v3.0",
    "api_key": "B3vCYF4rt6TyBtZiA8BrZRl7V3TeEbRt9MsOvcDo"
  },
  "task_settings": { }
}

PUT _inference/rerank/voyage-rerank
{
  "service": "voyageai",
  "service_settings": {
    "model_id": "rerank-lite-1",
    "api_key": "pa-gRwgKhVP-2LjV04VHx9kT1Cu_b2JdNN3ELXWs2x5KNU"
  },
  "task_settings": { }
}

############################################################
# ELASTIC RERANK (inference_id = .rerank-v1-elasticsearch)
############################################################

### 1. No chunking
GET my-index/_search
{
  "profile": true,
  "retriever": {
    "text_similarity_reranker": {
      "retriever": {
        "standard": {
          "query": { "match_all": {} }
        }
      },
      "rank_window_size": 10,
      "inference_id": ".rerank-v1-elasticsearch",
      "inference_text": "solar and wind renewable energy comparison",
      "field": "text"
    }
  },
  "size": 10
}

### 2. Full chunking (explicit chunking_settings)
GET my-index/_search
{
  "profile": true,
  "retriever": {
    "text_similarity_reranker": {
      "retriever": {
        "standard": {
          "query": { "match_all": {} }
        }
      },
      "rank_window_size": 10,
      "inference_id": ".rerank-v1-elasticsearch",
      "inference_text": "solar and wind renewable energy comparison",
      "field": "text",
      "chunk_rescorer": {
        "size": 3,
        "chunking_settings": {
          "strategy": "sentence",
          "max_chunk_size": 200,
          "sentence_overlap": 0
        }
      }
    }
  },
  "size": 10
}

### 3. Partial chunking (max_chunk_size only)
GET my-index/_search
{
  "profile": true,
  "retriever": {
    "text_similarity_reranker": {
      "retriever": {
        "standard": {
          "query": { "match_all": {} }
        }
      },
      "rank_window_size": 10,
      "inference_id": ".rerank-v1-elasticsearch",
      "inference_text": "solar and wind renewable energy comparison",
      "field": "text",
      "chunk_rescorer": {
        "size": 3,
        "chunking_settings": {
          "max_chunk_size": 200
        }
      }
    }
  },
  "size": 10
}

### 4. Auto-resolve (size only, no chunking_settings)
GET my-index/_search
{
  "profile": true,
  "retriever": {
    "text_similarity_reranker": {
      "retriever": {
        "standard": {
          "query": { "match_all": {} }
        }
      },
      "rank_window_size": 10,
      "inference_id": ".rerank-v1-elasticsearch",
      "inference_text": "solar and wind renewable energy comparison",
      "field": "text",
      "chunk_rescorer": {
        "size": 3
      }
    }
  },
  "size": 10
}

### 5. Auto-resolve (empty chunk_rescorer -> size defaults to 1)
GET my-index/_search
{
  "profile": true,
  "retriever": {
    "text_similarity_reranker": {
      "retriever": {
        "standard": {
          "query": { "match_all": {} }
        }
      },
      "rank_window_size": 10,
      "inference_id": ".rerank-v1-elasticsearch",
      "inference_text": "solar and wind renewable energy comparison",
      "field": "text",
      "chunk_rescorer": { }
    }
  },
  "size": 10
}



############################################################
# COHERE RERANK (inference_id = cohere-rerank)
############################################################

### 1. No chunking
GET my-index/_search
{
  "profile": true,
  "retriever": {
    "text_similarity_reranker": {
      "retriever": {
        "standard": {
          "query": { "match_all": {} }
        }
      },
      "rank_window_size": 10,
      "inference_id": "cohere-rerank",
      "inference_text": "solar and wind renewable energy comparison",
      "field": "text"
    }
  },
  "size": 10
}

### 2. Full chunking (explicit chunking_settings)
GET my-index/_search
{
  "profile": true,
  "retriever": {
    "text_similarity_reranker": {
      "retriever": {
        "standard": {
          "query": { "match_all": {} }
        }
      },
      "rank_window_size": 10,
      "inference_id": "cohere-rerank",
      "inference_text": "solar and wind renewable energy comparison",
      "field": "text",
      "chunk_rescorer": {
        "size": 3,
        "chunking_settings": {
          "strategy": "sentence",
          "max_chunk_size": 200,
          "sentence_overlap": 0
        }
      }
    }
  },
  "size": 10
}

### 3. Partial chunking (max_chunk_size only)
GET my-index/_search
{
  "profile": true,
  "retriever": {
    "text_similarity_reranker": {
      "retriever": {
        "standard": {
          "query": { "match_all": {} }
        }
      },
      "rank_window_size": 10,
      "inference_id": "cohere-rerank",
      "inference_text": "solar and wind renewable energy comparison",
      "field": "text",
      "chunk_rescorer": {
        "size": 3,
        "chunking_settings": {
          "max_chunk_size": 200
        }
      }
    }
  },
  "size": 10
}

### 4. Auto-resolve (size only, no chunking_settings)
GET my-index/_search
{
  "profile": true,
  "retriever": {
    "text_similarity_reranker": {
      "retriever": {
        "standard": {
          "query": { "match_all": {} }
        }
      },
      "rank_window_size": 10,
      "inference_id": "cohere-rerank",
      "inference_text": "solar and wind renewable energy comparison",
      "field": "text",
      "chunk_rescorer": {
        "size": 3
      }
    }
  },
  "size": 10
}

### 5. Auto-resolve (empty chunk_rescorer -> size defaults to 1)
GET my-index/_search
{
  "profile": true,
  "retriever": {
    "text_similarity_reranker": {
      "retriever": {
        "standard": {
          "query": { "match_all": {} }
        }
      },
      "rank_window_size": 10,
      "inference_id": "cohere-rerank",
      "inference_text": "solar and wind renewable energy comparison",
      "field": "text",
      "chunk_rescorer": { }
    }
  },
  "size": 10
}



############################################################
# VOYAGEAI RERANK (inference_id = voyage-rerank)
############################################################

### 1. No chunking
GET my-index/_search
{
  "profile": true,
  "retriever": {
    "text_similarity_reranker": {
      "retriever": {
        "standard": {
          "query": { "match_all": {} }
        }
      },
      "rank_window_size": 10,
      "inference_id": "voyage-rerank",
      "inference_text": "solar and wind renewable energy comparison",
      "field": "text"
    }
  },
  "size": 10
}

### 2. Full chunking (explicit chunking_settings)
GET my-index/_search
{
  "profile": true,
  "retriever": {
    "text_similarity_reranker": {
      "retriever": {
        "standard": {
          "query": { "match_all": {} }
        }
      },
      "rank_window_size": 10,
      "inference_id": "voyage-rerank",
      "inference_text": "solar and wind renewable energy comparison",
      "field": "text",
      "chunk_rescorer": {
        "size": 3,
        "chunking_settings": {
          "strategy": "sentence",
          "max_chunk_size": 200,
          "sentence_overlap": 0
        }
      }
    }
  },
  "size": 10
}

### 3. Partial chunking (max_chunk_size only)
GET my-index/_search
{
  "profile": true,
  "retriever": {
    "text_similarity_reranker": {
      "retriever": {
        "standard": {
          "query": { "match_all": {} }
        }
      },
      "rank_window_size": 10,
      "inference_id": "voyage-rerank",
      "inference_text": "solar and wind renewable energy comparison",
      "field": "text",
      "chunk_rescorer": {
        "size": 3,
        "chunking_settings": {
          "max_chunk_size": 200
        }
      }
    }
  },
  "size": 10
}

### 4. Auto-resolve (size only, no chunking_settings)
GET my-index/_search
{
  "profile": true,
  "retriever": {
    "text_similarity_reranker": {
      "retriever": {
        "standard": {
          "query": { "match_all": {} }
        }
      },
      "rank_window_size": 10,
      "inference_id": "voyage-rerank",
      "inference_text": "solar and wind renewable energy comparison",
      "field": "text",
      "chunk_rescorer": {
        "size": 3
      }
    }
  },
  "size": 10
}

### 5. Auto-resolve (empty chunk_rescorer -> size defaults to 1)
GET my-index/_search
{
  "profile": true,
  "retriever": {
    "text_similarity_reranker": {
      "retriever": {
        "standard": {
          "query": { "match_all": {} }
        }
      },
      "rank_window_size": 10,
      "inference_id": "voyage-rerank",
      "inference_text": "solar and wind renewable energy comparison",
      "field": "text",
      "chunk_rescorer": { }
    }
  },
  "size": 10
}