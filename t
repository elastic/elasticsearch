{
  "all-xpack-phone-home-2020.08.13-000005" : {
    "mappings" : {
      "dynamic" : "false",
      "properties" : {
        "cluster_name" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "cluster_stats" : {
          "properties" : {
            "indices" : {
              "properties" : {
                "analysis" : {
                  "properties" : {
                    "analyzer_types" : {
                      "properties" : {
                        "name" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "built_in_analyzers" : {
                      "properties" : {
                        "name" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "built_in_char_filters" : {
                      "properties" : {
                        "name" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "built_in_filters" : {
                      "properties" : {
                        "name" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "built_in_tokenizers" : {
                      "properties" : {
                        "name" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "char_filter_types" : {
                      "properties" : {
                        "name" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "filter_types" : {
                      "properties" : {
                        "name" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "tokenizer_types" : {
                      "properties" : {
                        "name" : {
                          "type" : "keyword"
                        }
                      }
                    }
                  }
                },
                "completion" : {
                  "properties" : {
                    "size_in_bytes" : {
                      "type" : "long",
                      "ignore_malformed" : true
                    }
                  }
                },
                "count" : {
                  "type" : "integer"
                },
                "docs" : {
                  "properties" : {
                    "count" : {
                      "type" : "long"
                    },
                    "deleted" : {
                      "type" : "long"
                    }
                  }
                },
                "fielddata" : {
                  "properties" : {
                    "evictions" : {
                      "type" : "long"
                    },
                    "memory_size_in_bytes" : {
                      "type" : "long",
                      "ignore_malformed" : true
                    }
                  }
                },
                "mappings" : {
                  "properties" : {
                    "field_types" : {
                      "properties" : {
                        "name" : {
                          "type" : "keyword"
                        }
                      }
                    }
                  }
                },
                "query_cache" : {
                  "properties" : {
                    "evictions" : {
                      "type" : "long"
                    },
                    "memory_size_in_bytes" : {
                      "type" : "long",
                      "ignore_malformed" : true
                    }
                  }
                },
                "segments" : {
                  "properties" : {
                    "count" : {
                      "type" : "long"
                    },
                    "doc_values_memory_in_bytes" : {
                      "type" : "long",
                      "ignore_malformed" : true
                    },
                    "fixed_bit_set_memory_in_bytes" : {
                      "type" : "long",
                      "ignore_malformed" : true
                    },
                    "index_writer_memory_in_bytes" : {
                      "type" : "long",
                      "ignore_malformed" : true
                    },
                    "memory_in_bytes" : {
                      "type" : "long",
                      "ignore_malformed" : true
                    },
                    "norms_memory_in_bytes" : {
                      "type" : "long",
                      "ignore_malformed" : true
                    },
                    "points_memory_in_bytes" : {
                      "type" : "long",
                      "ignore_malformed" : true
                    },
                    "stored_fields_memory_in_bytes" : {
                      "type" : "long",
                      "ignore_malformed" : true
                    },
                    "term_vectors_memory_in_bytes" : {
                      "type" : "long",
                      "ignore_malformed" : true
                    },
                    "terms_memory_in_bytes" : {
                      "type" : "long",
                      "ignore_malformed" : true
                    },
                    "version_map_memory_in_bytes" : {
                      "type" : "long"
                    }
                  }
                },
                "shards" : {
                  "properties" : {
                    "index" : {
                      "properties" : {
                        "primaries" : {
                          "properties" : {
                            "avg" : {
                              "type" : "half_float"
                            },
                            "max" : {
                              "type" : "short"
                            },
                            "min" : {
                              "type" : "short"
                            }
                          }
                        },
                        "replication" : {
                          "properties" : {
                            "avg" : {
                              "type" : "half_float"
                            },
                            "max" : {
                              "type" : "short"
                            },
                            "min" : {
                              "type" : "short"
                            }
                          }
                        },
                        "shards" : {
                          "properties" : {
                            "avg" : {
                              "type" : "half_float"
                            },
                            "max" : {
                              "type" : "short"
                            },
                            "min" : {
                              "type" : "short"
                            }
                          }
                        }
                      }
                    },
                    "primaries" : {
                      "type" : "integer"
                    },
                    "total" : {
                      "type" : "integer"
                    }
                  }
                },
                "store" : {
                  "properties" : {
                    "size_in_bytes" : {
                      "type" : "long",
                      "ignore_malformed" : true
                    },
                    "throttle_time_in_millis" : {
                      "type" : "long"
                    }
                  }
                }
              }
            },
            "nodes" : {
              "properties" : {
                "count" : {
                  "properties" : {
                    "coordinating_only" : {
                      "type" : "short"
                    },
                    "data" : {
                      "type" : "short"
                    },
                    "data_only" : {
                      "type" : "short"
                    },
                    "ingest" : {
                      "type" : "short"
                    },
                    "master" : {
                      "type" : "short"
                    },
                    "ml" : {
                      "type" : "short"
                    },
                    "total" : {
                      "type" : "short"
                    },
                    "voting_only" : {
                      "type" : "short"
                    }
                  }
                },
                "discovery_types" : {
                  "properties" : {
                    "count" : {
                      "type" : "long"
                    },
                    "single-node" : {
                      "type" : "long"
                    },
                    "type" : {
                      "type" : "keyword"
                    },
                    "zen" : {
                      "type" : "long"
                    }
                  }
                },
                "fs" : {
                  "properties" : {
                    "available_in_bytes" : {
                      "type" : "long",
                      "ignore_malformed" : true
                    },
                    "free_in_bytes" : {
                      "type" : "long",
                      "ignore_malformed" : true
                    },
                    "total_in_bytes" : {
                      "type" : "long",
                      "ignore_malformed" : true
                    }
                  }
                },
                "ingest" : {
                  "properties" : {
                    "number_of_pipelines" : {
                      "type" : "short"
                    },
                    "processor_stats" : {
                      "properties" : {
                        "circle" : {
                          "properties" : {
                            "count" : {
                              "type" : "short"
                            },
                            "failed" : {
                              "type" : "short"
                            }
                          }
                        }
                      }
                    }
                  }
                },
                "jvm" : {
                  "properties" : {
                    "max_uptime_in_millis" : {
                      "type" : "long"
                    },
                    "mem" : {
                      "properties" : {
                        "heap_max_in_bytes" : {
                          "type" : "long",
                          "ignore_malformed" : true
                        },
                        "heap_used_in_bytes" : {
                          "type" : "long",
                          "ignore_malformed" : true
                        }
                      }
                    },
                    "threads" : {
                      "type" : "long"
                    },
                    "versions" : {
                      "properties" : {
                        "bundled_jdk" : {
                          "type" : "boolean"
                        },
                        "count" : {
                          "type" : "short"
                        },
                        "using_bundled_jdk" : {
                          "type" : "boolean"
                        },
                        "version" : {
                          "type" : "keyword"
                        },
                        "vm_name" : {
                          "type" : "text",
                          "fields" : {
                            "keyword" : {
                              "type" : "keyword"
                            }
                          }
                        },
                        "vm_vendor" : {
                          "type" : "text",
                          "fields" : {
                            "keyword" : {
                              "type" : "keyword"
                            }
                          }
                        },
                        "vm_version" : {
                          "type" : "keyword"
                        }
                      }
                    }
                  }
                },
                "network_types" : {
                  "type" : "object"
                },
                "os" : {
                  "properties" : {
                    "allocated_processors" : {
                      "type" : "integer"
                    },
                    "available_processors" : {
                      "type" : "integer"
                    },
                    "mem" : {
                      "properties" : {
                        "free_in_bytes" : {
                          "type" : "long",
                          "ignore_malformed" : true
                        },
                        "free_percent" : {
                          "type" : "half_float"
                        },
                        "total_in_bytes" : {
                          "type" : "long",
                          "ignore_malformed" : true
                        },
                        "used_in_bytes" : {
                          "type" : "long",
                          "ignore_malformed" : true
                        },
                        "used_percent" : {
                          "type" : "half_float"
                        }
                      }
                    },
                    "names" : {
                      "properties" : {
                        "count" : {
                          "type" : "short"
                        },
                        "name" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "pretty_names" : {
                      "properties" : {
                        "count" : {
                          "type" : "long"
                        },
                        "pretty_name" : {
                          "type" : "text",
                          "fields" : {
                            "text" : {
                              "type" : "keyword",
                              "ignore_above" : 64
                            }
                          }
                        }
                      }
                    }
                  }
                },
                "packaging_types" : {
                  "properties" : {
                    "count" : {
                      "type" : "long"
                    },
                    "flavor" : {
                      "type" : "keyword"
                    },
                    "type" : {
                      "type" : "keyword"
                    }
                  }
                },
                "plugins" : {
                  "properties" : {
                    "classname" : {
                      "type" : "keyword"
                    },
                    "description" : {
                      "type" : "text",
                      "fields" : {
                        "keyword" : {
                          "type" : "keyword",
                          "ignore_above" : 64
                        }
                      }
                    },
                    "has_native_controller" : {
                      "type" : "boolean"
                    },
                    "name" : {
                      "type" : "keyword"
                    },
                    "version" : {
                      "type" : "keyword"
                    }
                  }
                },
                "process" : {
                  "properties" : {
                    "cpu" : {
                      "properties" : {
                        "percent" : {
                          "type" : "float"
                        }
                      }
                    },
                    "names" : {
                      "properties" : {
                        "avg" : {
                          "type" : "float"
                        },
                        "max" : {
                          "type" : "long"
                        },
                        "min" : {
                          "type" : "long"
                        }
                      }
                    }
                  }
                },
                "versions" : {
                  "type" : "keyword"
                }
              }
            },
            "status" : {
              "type" : "keyword"
            }
          }
        },
        "cluster_uuid" : {
          "type" : "keyword"
        },
        "collection" : {
          "type" : "keyword"
        },
        "collectionSource" : {
          "type" : "keyword"
        },
        "geoip" : {
          "properties" : {
            "city_name" : {
              "type" : "text",
              "fields" : {
                "keyword" : {
                  "type" : "keyword"
                }
              }
            },
            "continent_name" : {
              "type" : "text",
              "fields" : {
                "keyword" : {
                  "type" : "keyword"
                }
              }
            },
            "country_iso_code" : {
              "type" : "keyword"
            },
            "location" : {
              "type" : "geo_point"
            }
          }
        },
        "license" : {
          "properties" : {
            "expiry_date" : {
              "type" : "date",
              "ignore_malformed" : true
            },
            "issue_date" : {
              "type" : "date",
              "ignore_malformed" : true
            },
            "issued_to" : {
              "type" : "keyword"
            },
            "issuer" : {
              "type" : "keyword"
            },
            "max_nodes" : {
              "type" : "long",
              "ignore_malformed" : true
            },
            "status" : {
              "type" : "keyword"
            },
            "type" : {
              "type" : "keyword"
            },
            "uid" : {
              "type" : "keyword"
            }
          }
        },
        "stack_stats" : {
          "properties" : {
            "apm" : {
              "properties" : {
                "found" : {
                  "type" : "boolean"
                }
              }
            },
            "app_search" : {
              "properties" : {
                "accounts" : {
                  "properties" : {
                    "analytics_enabled_all_engines" : {
                      "type" : "boolean"
                    },
                    "created_at" : {
                      "type" : "date"
                    },
                    "engines" : {
                      "properties" : {
                        "created_at" : {
                          "type" : "date"
                        },
                        "document_count" : {
                          "type" : "long"
                        },
                        "language" : {
                          "type" : "keyword"
                        },
                        "slug" : {
                          "type" : "keyword"
                        },
                        "total_queries_last_30_days" : {
                          "type" : "long"
                        },
                        "tunings" : {
                          "properties" : {
                            "curation_count" : {
                              "type" : "long"
                            },
                            "synonym_count" : {
                              "type" : "long"
                            }
                          }
                        },
                        "updated_at" : {
                          "type" : "date"
                        }
                      }
                    },
                    "id" : {
                      "type" : "keyword"
                    },
                    "logging_enabled_all_engines" : {
                      "type" : "boolean"
                    },
                    "updated_at" : {
                      "type" : "date"
                    },
                    "users" : {
                      "properties" : {
                        "current_sign_in_at" : {
                          "type" : "date"
                        },
                        "current_sign_in_ip" : {
                          "type" : "text"
                        },
                        "email" : {
                          "type" : "text"
                        },
                        "role_type" : {
                          "type" : "keyword"
                        },
                        "sign_in_count" : {
                          "type" : "long"
                        }
                      }
                    }
                  }
                },
                "config" : {
                  "properties" : {
                    "auth_source" : {
                      "type" : "keyword"
                    },
                    "email_account_enabled" : {
                      "type" : "boolean"
                    },
                    "limits" : {
                      "properties" : {
                        "engine" : {
                          "properties" : {
                            "analytics" : {
                              "properties" : {
                                "total_tags" : {
                                  "type" : "short"
                                }
                              }
                            },
                            "document_size_in_bytes" : {
                              "type" : "keyword"
                            },
                            "query" : {
                              "type" : "short"
                            },
                            "source_engines_per_meta_engine" : {
                              "type" : "short"
                            },
                            "synonyms" : {
                              "properties" : {
                                "sets" : {
                                  "type" : "integer"
                                },
                                "terms_per_set" : {
                                  "type" : "short"
                                }
                              }
                            },
                            "total_facet_values_returned" : {
                              "type" : "integer"
                            },
                            "total_fields" : {
                              "type" : "short"
                            }
                          }
                        }
                      }
                    }
                  }
                },
                "stats" : {
                  "properties" : {
                    "total_accounts" : {
                      "type" : "long"
                    },
                    "total_engines" : {
                      "type" : "long"
                    },
                    "total_meta_engine_schema_conflicts" : {
                      "type" : "long"
                    },
                    "total_meta_engines" : {
                      "type" : "long"
                    },
                    "total_pending_invites" : {
                      "type" : "long"
                    },
                    "total_queries_last_30_days" : {
                      "type" : "long"
                    },
                    "total_role_mappings" : {
                      "type" : "long"
                    },
                    "total_sign_ins" : {
                      "type" : "long"
                    },
                    "total_source_engines" : {
                      "type" : "long"
                    },
                    "total_users" : {
                      "type" : "long"
                    }
                  }
                }
              }
            },
            "beats" : {
              "properties" : {
                "architecture" : {
                  "properties" : {
                    "architectures" : {
                      "properties" : {
                        "architecture" : {
                          "type" : "keyword"
                        },
                        "count" : {
                          "type" : "long"
                        },
                        "name" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "count" : {
                      "type" : "long"
                    }
                  }
                },
                "count" : {
                  "type" : "short"
                },
                "eventsPublished" : {
                  "type" : "long"
                },
                "functionbeat" : {
                  "properties" : {
                    "functions" : {
                      "properties" : {
                        "count" : {
                          "type" : "long"
                        }
                      }
                    }
                  }
                },
                "heartbeat" : {
                  "properties" : {
                    "endpoints" : {
                      "type" : "long"
                    },
                    "http" : {
                      "properties" : {
                        "endpoints" : {
                          "type" : "long"
                        },
                        "monitors" : {
                          "type" : "long"
                        }
                      }
                    },
                    "icmp" : {
                      "properties" : {
                        "endpoints" : {
                          "type" : "long"
                        },
                        "monitors" : {
                          "type" : "long"
                        }
                      }
                    },
                    "monitors" : {
                      "type" : "long"
                    },
                    "tcp" : {
                      "properties" : {
                        "endpoints" : {
                          "type" : "long"
                        },
                        "monitors" : {
                          "type" : "long"
                        }
                      }
                    }
                  }
                },
                "hosts" : {
                  "type" : "long"
                },
                "input" : {
                  "properties" : {
                    "count" : {
                      "type" : "long"
                    },
                    "names" : {
                      "type" : "keyword"
                    }
                  }
                },
                "module" : {
                  "properties" : {
                    "count" : {
                      "type" : "long"
                    },
                    "names" : {
                      "type" : "keyword"
                    }
                  }
                },
                "outputs" : {
                  "properties" : {
                    "elasticsearch" : {
                      "type" : "long"
                    },
                    "file" : {
                      "type" : "long"
                    },
                    "kafka" : {
                      "type" : "long"
                    },
                    "logstash" : {
                      "type" : "long"
                    },
                    "redis" : {
                      "type" : "long"
                    }
                  }
                },
                "queue" : {
                  "properties" : {
                    "mem" : {
                      "type" : "long"
                    },
                    "spool" : {
                      "type" : "long"
                    }
                  }
                },
                "types" : {
                  "properties" : {
                    "auditbeat" : {
                      "type" : "long"
                    },
                    "filebeat" : {
                      "type" : "long"
                    },
                    "functionbeat" : {
                      "type" : "long"
                    },
                    "heartbeat" : {
                      "type" : "long"
                    },
                    "metricbeat" : {
                      "type" : "long"
                    },
                    "packetbeat" : {
                      "type" : "long"
                    },
                    "winlogbeat" : {
                      "type" : "long"
                    }
                  }
                },
                "versions" : {
                  "properties" : {
                    "count" : {
                      "type" : "short"
                    },
                    "version" : {
                      "type" : "keyword"
                    }
                  }
                }
              }
            },
            "data" : {
              "properties" : {
                "names" : {
                  "type" : "keyword"
                },
                "package" : {
                  "properties" : {
                    "endpoint" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "names" : {
                      "type" : "keyword"
                    },
                    "system" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    }
                  }
                },
                "pattern" : {
                  "properties" : {
                    "acquia" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "apache" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "apm" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "app-search" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "arcsight" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "artifactory" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "aruba" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "auditbeat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "barracuda" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "bluecoat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "checkpoint" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "cisco" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "citrix" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "cyberark" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "cylance" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "drupal" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "ecs-corelight" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "endgame" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "enterprise-search" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "filebeat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "fireeye" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "fluentbit" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "fluentd" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "fortinet" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "functionbeat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "heartbeat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "infoblox" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "joomla" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "kaspersky" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "logs-endpoint" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "logstash" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "magento" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "magento2" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "mcafee" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "meow" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "metricbeat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "metrics-endpoint" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "names" : {
                      "type" : "keyword"
                    },
                    "nginx" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "packetbeat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "paloaltonetworks" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "prometheusbeat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "rsa" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "search" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "sharepoint" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "shopify" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "siem-signals" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "sigma_doc" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "sitecore" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "snort" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "sonicwall" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "sophos" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "squarespace" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "squid" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "suricata" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "symantec" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "telegraf" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "tippingpoint" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "tomcat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "trendmicro" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "tripwire" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "wazuh" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "weebly" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "winlogbeat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "wordpress" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "zeek" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "zscaler" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    }
                  }
                },
                "shipper" : {
                  "properties" : {
                    "apm" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "archsight" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "auditbeat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "endgame" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "endpoint" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "filebeat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "functionbeat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "heartbeat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "logstash" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "metricbeat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "names" : {
                      "type" : "keyword"
                    },
                    "packetbeat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "winlogbeat" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    }
                  }
                },
                "type" : {
                  "properties" : {
                    "logs" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "metrics" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    },
                    "names" : {
                      "type" : "keyword"
                    },
                    "traces" : {
                      "properties" : {
                        "doc_count" : {
                          "type" : "long"
                        },
                        "ecs_index_count" : {
                          "type" : "long"
                        },
                        "index_count" : {
                          "type" : "long"
                        },
                        "size_in_bytes" : {
                          "type" : "long"
                        }
                      }
                    }
                  }
                }
              }
            },
            "enterprise_search" : {
              "properties" : {
                "ilm" : {
                  "properties" : {
                    "ilm_enabled" : {
                      "type" : "boolean"
                    },
                    "ilm_settings_default" : {
                      "type" : "boolean"
                    }
                  }
                },
                "successful_boot" : {
                  "type" : "boolean"
                },
                "versions" : {
                  "properties" : {
                    "count" : {
                      "type" : "short"
                    },
                    "version" : {
                      "type" : "keyword"
                    }
                  }
                }
              }
            },
            "kibana" : {
              "properties" : {
                "cloud" : {
                  "properties" : {
                    "count" : {
                      "type" : "long"
                    },
                    "name" : {
                      "type" : "keyword"
                    },
                    "regions" : {
                      "properties" : {
                        "count" : {
                          "type" : "long"
                        },
                        "region" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "vm_types" : {
                      "properties" : {
                        "count" : {
                          "type" : "long"
                        },
                        "vm_type" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "vms" : {
                      "type" : "long"
                    },
                    "zones" : {
                      "properties" : {
                        "count" : {
                          "type" : "long"
                        },
                        "zone" : {
                          "type" : "keyword"
                        }
                      }
                    }
                  }
                },
                "count" : {
                  "type" : "short"
                },
                "dashboard" : {
                  "properties" : {
                    "total" : {
                      "type" : "long"
                    }
                  }
                },
                "graph_workspace" : {
                  "properties" : {
                    "total" : {
                      "type" : "long"
                    }
                  }
                },
                "index_pattern" : {
                  "properties" : {
                    "total" : {
                      "type" : "long"
                    }
                  }
                },
                "indices" : {
                  "type" : "long"
                },
                "os" : {
                  "properties" : {
                    "distroReleases" : {
                      "properties" : {
                        "count" : {
                          "type" : "long"
                        },
                        "distroRelease" : {
                          "type" : "keyword",
                          "fields" : {
                            "text" : {
                              "type" : "text"
                            }
                          },
                          "ignore_above" : 64
                        }
                      }
                    },
                    "distros" : {
                      "properties" : {
                        "count" : {
                          "type" : "long"
                        },
                        "distro" : {
                          "type" : "keyword",
                          "fields" : {
                            "text" : {
                              "type" : "text"
                            }
                          },
                          "ignore_above" : 64
                        }
                      }
                    },
                    "platformReleases" : {
                      "properties" : {
                        "count" : {
                          "type" : "long"
                        },
                        "platformRelease" : {
                          "type" : "keyword",
                          "fields" : {
                            "text" : {
                              "type" : "text"
                            }
                          },
                          "ignore_above" : 64
                        }
                      }
                    },
                    "platforms" : {
                      "properties" : {
                        "count" : {
                          "type" : "long"
                        },
                        "platform" : {
                          "type" : "keyword",
                          "fields" : {
                            "text" : {
                              "type" : "text"
                            }
                          },
                          "ignore_above" : 64
                        }
                      }
                    }
                  }
                },
                "plugins" : {
                  "properties" : {
                    "actions" : {
                      "properties" : {
                        "count_active_by_type" : {
                          "type" : "object"
                        },
                        "count_active_total" : {
                          "type" : "long"
                        },
                        "count_by_type" : {
                          "type" : "object"
                        },
                        "count_total" : {
                          "type" : "long"
                        }
                      }
                    },
                    "alerts" : {
                      "properties" : {
                        "connectors_per_alert" : {
                          "properties" : {
                            "avg" : {
                              "type" : "float"
                            },
                            "max" : {
                              "type" : "float"
                            },
                            "min" : {
                              "type" : "float"
                            }
                          }
                        },
                        "count_active_by_type" : {
                          "type" : "object"
                        },
                        "count_active_total" : {
                          "type" : "long"
                        },
                        "count_by_type" : {
                          "type" : "object"
                        },
                        "count_disabled_total" : {
                          "type" : "long"
                        },
                        "count_total" : {
                          "type" : "long"
                        },
                        "schedule_time" : {
                          "properties" : {
                            "avg" : {
                              "type" : "text"
                            },
                            "max" : {
                              "type" : "text"
                            },
                            "min" : {
                              "type" : "text"
                            }
                          }
                        },
                        "throttle_time" : {
                          "properties" : {
                            "avg" : {
                              "type" : "text"
                            },
                            "max" : {
                              "type" : "text"
                            },
                            "min" : {
                              "type" : "text"
                            }
                          }
                        }
                      }
                    },
                    "apm" : {
                      "properties" : {
                        "agents" : {
                          "properties" : {
                            "dotnet" : {
                              "properties" : {
                                "agent" : {
                                  "properties" : {
                                    "version" : {
                                      "type" : "keyword",
                                      "ignore_above" : 1024
                                    }
                                  }
                                },
                                "service" : {
                                  "properties" : {
                                    "framework" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    },
                                    "language" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    },
                                    "runtime" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            },
                            "go" : {
                              "properties" : {
                                "agent" : {
                                  "properties" : {
                                    "version" : {
                                      "type" : "keyword",
                                      "ignore_above" : 1024
                                    }
                                  }
                                },
                                "service" : {
                                  "properties" : {
                                    "framework" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    },
                                    "language" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    },
                                    "runtime" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            },
                            "java" : {
                              "properties" : {
                                "agent" : {
                                  "properties" : {
                                    "version" : {
                                      "type" : "keyword",
                                      "ignore_above" : 1024
                                    }
                                  }
                                },
                                "service" : {
                                  "properties" : {
                                    "framework" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    },
                                    "language" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    },
                                    "runtime" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            },
                            "js-base" : {
                              "properties" : {
                                "agent" : {
                                  "properties" : {
                                    "version" : {
                                      "type" : "keyword",
                                      "ignore_above" : 1024
                                    }
                                  }
                                },
                                "service" : {
                                  "properties" : {
                                    "framework" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    },
                                    "language" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    },
                                    "runtime" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            },
                            "nodejs" : {
                              "properties" : {
                                "agent" : {
                                  "properties" : {
                                    "version" : {
                                      "type" : "keyword",
                                      "ignore_above" : 1024
                                    }
                                  }
                                },
                                "service" : {
                                  "properties" : {
                                    "framework" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    },
                                    "language" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    },
                                    "runtime" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            },
                            "python" : {
                              "properties" : {
                                "agent" : {
                                  "properties" : {
                                    "version" : {
                                      "type" : "keyword",
                                      "ignore_above" : 1024
                                    }
                                  }
                                },
                                "service" : {
                                  "properties" : {
                                    "framework" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    },
                                    "language" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    },
                                    "runtime" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            },
                            "ruby" : {
                              "properties" : {
                                "agent" : {
                                  "properties" : {
                                    "version" : {
                                      "type" : "keyword",
                                      "ignore_above" : 1024
                                    }
                                  }
                                },
                                "service" : {
                                  "properties" : {
                                    "framework" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    },
                                    "language" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    },
                                    "runtime" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            },
                            "rum-js" : {
                              "properties" : {
                                "agent" : {
                                  "properties" : {
                                    "version" : {
                                      "type" : "keyword",
                                      "ignore_above" : 1024
                                    }
                                  }
                                },
                                "service" : {
                                  "properties" : {
                                    "framework" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    },
                                    "language" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    },
                                    "runtime" : {
                                      "properties" : {
                                        "composite" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "name" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        },
                                        "version" : {
                                          "type" : "keyword",
                                          "ignore_above" : 1024
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        },
                        "cardinality" : {
                          "properties" : {
                            "transaction" : {
                              "properties" : {
                                "name" : {
                                  "properties" : {
                                    "all_agents" : {
                                      "properties" : {
                                        "1d" : {
                                          "type" : "long"
                                        }
                                      }
                                    },
                                    "rum" : {
                                      "properties" : {
                                        "1d" : {
                                          "type" : "long"
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            },
                            "user_agent" : {
                              "properties" : {
                                "original" : {
                                  "properties" : {
                                    "all_agents" : {
                                      "properties" : {
                                        "1d" : {
                                          "type" : "long"
                                        }
                                      }
                                    },
                                    "rum" : {
                                      "properties" : {
                                        "1d" : {
                                          "type" : "long"
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        },
                        "cloud" : {
                          "properties" : {
                            "availability_zone" : {
                              "type" : "keyword",
                              "ignore_above" : 1024
                            },
                            "provider" : {
                              "type" : "keyword",
                              "ignore_above" : 1024
                            },
                            "region" : {
                              "type" : "keyword",
                              "ignore_above" : 1024
                            }
                          }
                        },
                        "counts" : {
                          "properties" : {
                            "agent_configuration" : {
                              "properties" : {
                                "all" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "error" : {
                              "properties" : {
                                "1d" : {
                                  "type" : "long"
                                },
                                "all" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "max_error_groups_per_service" : {
                              "properties" : {
                                "1d" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "max_transaction_groups_per_service" : {
                              "properties" : {
                                "1d" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "metric" : {
                              "properties" : {
                                "1d" : {
                                  "type" : "long"
                                },
                                "all" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "onboarding" : {
                              "properties" : {
                                "1d" : {
                                  "type" : "long"
                                },
                                "all" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "services" : {
                              "properties" : {
                                "1d" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "sourcemap" : {
                              "properties" : {
                                "1d" : {
                                  "type" : "long"
                                },
                                "all" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "span" : {
                              "properties" : {
                                "1d" : {
                                  "type" : "long"
                                },
                                "all" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "traces" : {
                              "properties" : {
                                "1d" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "transaction" : {
                              "properties" : {
                                "1d" : {
                                  "type" : "long"
                                },
                                "all" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        },
                        "has_any_services" : {
                          "type" : "boolean"
                        },
                        "indices" : {
                          "properties" : {
                            "all" : {
                              "properties" : {
                                "total" : {
                                  "properties" : {
                                    "docs" : {
                                      "properties" : {
                                        "count" : {
                                          "type" : "long"
                                        }
                                      }
                                    },
                                    "store" : {
                                      "properties" : {
                                        "size_in_bytes" : {
                                          "type" : "long"
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            },
                            "shards" : {
                              "properties" : {
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        },
                        "integrations" : {
                          "properties" : {
                            "ml" : {
                              "properties" : {
                                "all_jobs_count" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        },
                        "retainment" : {
                          "properties" : {
                            "error" : {
                              "properties" : {
                                "ms" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "metric" : {
                              "properties" : {
                                "ms" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "onboarding" : {
                              "properties" : {
                                "ms" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "span" : {
                              "properties" : {
                                "ms" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "transaction" : {
                              "properties" : {
                                "ms" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        },
                        "services_per_agent" : {
                          "properties" : {
                            "dotnet" : {
                              "type" : "long",
                              "null_value" : 0
                            },
                            "go" : {
                              "type" : "long",
                              "null_value" : 0
                            },
                            "java" : {
                              "type" : "long",
                              "null_value" : 0
                            },
                            "js-base" : {
                              "type" : "long",
                              "null_value" : 0
                            },
                            "nodejs" : {
                              "type" : "long",
                              "null_value" : 0
                            },
                            "python" : {
                              "type" : "long",
                              "null_value" : 0
                            },
                            "ruby" : {
                              "type" : "long",
                              "null_value" : 0
                            },
                            "rum-js" : {
                              "type" : "long",
                              "null_value" : 0
                            }
                          }
                        },
                        "tasks" : {
                          "properties" : {
                            "agent_configuration" : {
                              "properties" : {
                                "took" : {
                                  "properties" : {
                                    "ms" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            },
                            "agents" : {
                              "properties" : {
                                "took" : {
                                  "properties" : {
                                    "ms" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            },
                            "cardinality" : {
                              "properties" : {
                                "took" : {
                                  "properties" : {
                                    "ms" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            },
                            "cloud" : {
                              "properties" : {
                                "took" : {
                                  "properties" : {
                                    "ms" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            },
                            "groupings" : {
                              "properties" : {
                                "took" : {
                                  "properties" : {
                                    "ms" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            },
                            "indices_stats" : {
                              "properties" : {
                                "took" : {
                                  "properties" : {
                                    "ms" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            },
                            "integrations" : {
                              "properties" : {
                                "took" : {
                                  "properties" : {
                                    "ms" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            },
                            "processor_events" : {
                              "properties" : {
                                "took" : {
                                  "properties" : {
                                    "ms" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            },
                            "services" : {
                              "properties" : {
                                "took" : {
                                  "properties" : {
                                    "ms" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            },
                            "versions" : {
                              "properties" : {
                                "took" : {
                                  "properties" : {
                                    "ms" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            }
                          }
                        },
                        "version" : {
                          "properties" : {
                            "apm_server" : {
                              "properties" : {
                                "major" : {
                                  "type" : "long"
                                },
                                "minor" : {
                                  "type" : "long"
                                },
                                "patch" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        }
                      }
                    },
                    "app_search" : {
                      "properties" : {
                        "ui_clicked" : {
                          "properties" : {
                            "create_first_engine_button" : {
                              "type" : "long"
                            },
                            "engine_table_link" : {
                              "type" : "long"
                            },
                            "header_launch_button" : {
                              "type" : "long"
                            }
                          }
                        },
                        "ui_error" : {
                          "properties" : {
                            "cannot_connect" : {
                              "type" : "long"
                            },
                            "no_as_account" : {
                              "type" : "long"
                            }
                          }
                        },
                        "ui_viewed" : {
                          "properties" : {
                            "engines_overview" : {
                              "type" : "long"
                            },
                            "setup_guide" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    },
                    "application_usage" : {
                      "properties" : {
                        "apm" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "appSearch" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "canvas" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "dashboard" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "dashboards" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "dev_tools" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "discover" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "enterprise_search" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "home" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "ingestManager" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "logs" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "management" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "maps" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "metrics" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "ml" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "monitoring" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "securitySolution:alerts" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "securitySolution:case" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "securitySolution:endpointAlerts" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "securitySolution:hosts" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "securitySolution:management" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "securitySolution:network" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "securitySolution:overview" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "securitySolution:timelines" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "siem" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "uptime" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "visualize" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        },
                        "workplaceSearch" : {
                          "properties" : {
                            "clicks_30_days" : {
                              "type" : "long"
                            },
                            "clicks_7_days" : {
                              "type" : "long"
                            },
                            "clicks_90_days" : {
                              "type" : "long"
                            },
                            "clicks_total" : {
                              "type" : "long"
                            },
                            "minutes_on_screen_30_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_7_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_90_days" : {
                              "type" : "float"
                            },
                            "minutes_on_screen_total" : {
                              "type" : "float"
                            }
                          }
                        }
                      }
                    },
                    "canvas" : {
                      "properties" : {
                        "elements" : {
                          "properties" : {
                            "per_page" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "double"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "functions" : {
                          "properties" : {
                            "in_use" : {
                              "type" : "keyword"
                            },
                            "per_element" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "double"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "pages" : {
                          "properties" : {
                            "per_workpad" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "double"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "workpads" : {
                          "properties" : {
                            "total" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    },
                    "cloud" : {
                      "properties" : {
                        "isCloudEnabled" : {
                          "type" : "boolean"
                        }
                      }
                    },
                    "csp" : {
                      "properties" : {
                        "rulesChangedFromDefault" : {
                          "type" : "boolean"
                        },
                        "strict" : {
                          "type" : "boolean"
                        },
                        "warnLegacyBrowsers" : {
                          "type" : "boolean"
                        }
                      }
                    },
                    "infraops" : {
                      "properties" : {
                        "last_24_hours" : {
                          "properties" : {
                            "hits" : {
                              "properties" : {
                                "infraops_docker" : {
                                  "type" : "long"
                                },
                                "infraops_hosts" : {
                                  "type" : "long"
                                },
                                "infraops_kubernetes" : {
                                  "type" : "long"
                                },
                                "logs" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        }
                      }
                    },
                    "ingest_manager" : {
                      "properties" : {
                        "agents" : {
                          "properties" : {
                            "error" : {
                              "type" : "long"
                            },
                            "offline" : {
                              "type" : "long"
                            },
                            "online" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "fleet_enabled" : {
                          "type" : "boolean"
                        },
                        "packages" : {
                          "properties" : {
                            "disabled" : {
                              "type" : "keyword"
                            },
                            "enabled" : {
                              "type" : "keyword"
                            }
                          }
                        }
                      }
                    },
                    "kql" : {
                      "properties" : {
                        "defaultQueryLanguage" : {
                          "type" : "keyword"
                        },
                        "optInCount" : {
                          "type" : "long"
                        },
                        "optOutCount" : {
                          "type" : "long"
                        }
                      }
                    },
                    "lens" : {
                      "properties" : {
                        "events_30_days" : {
                          "properties" : {
                            "app_date_change" : {
                              "type" : "long"
                            },
                            "app_filters_updated" : {
                              "type" : "long"
                            },
                            "app_query_change" : {
                              "type" : "long"
                            },
                            "chart_switch" : {
                              "type" : "long"
                            },
                            "drop_empty" : {
                              "type" : "long"
                            },
                            "drop_non_empty" : {
                              "type" : "long"
                            },
                            "drop_onto_dimension" : {
                              "type" : "long"
                            },
                            "drop_onto_workspace" : {
                              "type" : "long"
                            },
                            "drop_total" : {
                              "type" : "long"
                            },
                            "indexpattern_changed" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_field_changed" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_operation_avg" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_operation_cardinality" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_operation_count" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_operation_date_histogram" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_operation_max" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_operation_min" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_operation_sum" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_operation_terms" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_removed" : {
                              "type" : "long"
                            },
                            "indexpattern_existence_toggled" : {
                              "type" : "long"
                            },
                            "indexpattern_field_info_click" : {
                              "type" : "long"
                            },
                            "indexpattern_filters_cleared" : {
                              "type" : "long"
                            },
                            "indexpattern_show_all_fields_clicked" : {
                              "type" : "long"
                            },
                            "indexpattern_type_filter_toggled" : {
                              "type" : "long"
                            },
                            "loaded" : {
                              "type" : "long"
                            },
                            "loaded_404" : {
                              "type" : "long"
                            },
                            "save_failed" : {
                              "type" : "long"
                            },
                            "suggestion_clicked" : {
                              "type" : "long"
                            },
                            "suggestion_confirmed" : {
                              "type" : "long"
                            },
                            "xy_change_layer_display" : {
                              "type" : "long"
                            },
                            "xy_layer_added" : {
                              "type" : "long"
                            },
                            "xy_layer_removed" : {
                              "type" : "long"
                            }
                          }
                        },
                        "events_90_days" : {
                          "properties" : {
                            "app_date_change" : {
                              "type" : "long"
                            },
                            "app_filters_updated" : {
                              "type" : "long"
                            },
                            "app_query_change" : {
                              "type" : "long"
                            },
                            "chart_switch" : {
                              "type" : "long"
                            },
                            "drop_empty" : {
                              "type" : "long"
                            },
                            "drop_non_empty" : {
                              "type" : "long"
                            },
                            "drop_onto_dimension" : {
                              "type" : "long"
                            },
                            "drop_onto_workspace" : {
                              "type" : "long"
                            },
                            "drop_total" : {
                              "type" : "long"
                            },
                            "indexpattern_changed" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_field_changed" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_operation_avg" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_operation_cardinality" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_operation_count" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_operation_date_histogram" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_operation_max" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_operation_min" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_operation_sum" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_operation_terms" : {
                              "type" : "long"
                            },
                            "indexpattern_dimension_removed" : {
                              "type" : "long"
                            },
                            "indexpattern_existence_toggled" : {
                              "type" : "long"
                            },
                            "indexpattern_field_info_click" : {
                              "type" : "long"
                            },
                            "indexpattern_filters_cleared" : {
                              "type" : "long"
                            },
                            "indexpattern_show_all_fields_clicked" : {
                              "type" : "long"
                            },
                            "indexpattern_type_filter_toggled" : {
                              "type" : "long"
                            },
                            "loaded" : {
                              "type" : "long"
                            },
                            "loaded_404" : {
                              "type" : "long"
                            },
                            "save_failed" : {
                              "type" : "long"
                            },
                            "suggestion_clicked" : {
                              "type" : "long"
                            },
                            "suggestion_confirmed" : {
                              "type" : "long"
                            },
                            "xy_change_layer_display" : {
                              "type" : "long"
                            },
                            "xy_layer_added" : {
                              "type" : "long"
                            },
                            "xy_layer_removed" : {
                              "type" : "long"
                            }
                          }
                        },
                        "saved_30_days" : {
                          "properties" : {
                            "bar_stacked" : {
                              "type" : "long"
                            }
                          }
                        },
                        "saved_30_days_total" : {
                          "type" : "long"
                        },
                        "saved_90_days" : {
                          "properties" : {
                            "bar_stacked" : {
                              "type" : "long"
                            }
                          }
                        },
                        "saved_90_days_total" : {
                          "type" : "long"
                        },
                        "saved_overall" : {
                          "properties" : {
                            "bar_stacked" : {
                              "type" : "long"
                            }
                          }
                        },
                        "saved_overall_total" : {
                          "type" : "long"
                        },
                        "suggestion_events_30_days" : {
                          "properties" : {
                            "back_to_current" : {
                              "type" : "long"
                            },
                            "position_0_of_5" : {
                              "type" : "long"
                            },
                            "reload" : {
                              "type" : "long"
                            }
                          }
                        },
                        "suggestion_events_90_days" : {
                          "properties" : {
                            "back_to_current" : {
                              "type" : "long"
                            },
                            "position_0_of_5" : {
                              "type" : "long"
                            },
                            "reload" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    },
                    "localization" : {
                      "properties" : {
                        "labelsCount" : {
                          "type" : "long"
                        },
                        "locale" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "maps" : {
                      "properties" : {
                        "attributesPerMap" : {
                          "properties" : {
                            "dataSourcesCount" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "long"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "layersCount" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "long"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        },
                        "mapsTotalCount" : {
                          "type" : "long"
                        },
                        "timeCaptured" : {
                          "type" : "date"
                        }
                      }
                    },
                    "ml" : {
                      "properties" : {
                        "file_data_visualizer" : {
                          "properties" : {
                            "index_creation_count" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    },
                    "reporting" : {
                      "properties" : {
                        "PNG" : {
                          "properties" : {
                            "available" : {
                              "type" : "boolean"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "_all" : {
                          "type" : "long"
                        },
                        "available" : {
                          "type" : "boolean"
                        },
                        "browser_type" : {
                          "type" : "keyword",
                          "ignore_above" : 256
                        },
                        "csv" : {
                          "properties" : {
                            "available" : {
                              "type" : "boolean"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "enabled" : {
                          "type" : "boolean"
                        },
                        "last7Days" : {
                          "properties" : {
                            "PNG" : {
                              "properties" : {
                                "available" : {
                                  "type" : "boolean"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "_all" : {
                              "type" : "long"
                            },
                            "csv" : {
                              "properties" : {
                                "available" : {
                                  "type" : "boolean"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "printable_pdf" : {
                              "properties" : {
                                "app" : {
                                  "properties" : {
                                    "canvas workpad" : {
                                      "type" : "long"
                                    },
                                    "dashboard" : {
                                      "type" : "long"
                                    },
                                    "visualization" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "available" : {
                                  "type" : "boolean"
                                },
                                "layout" : {
                                  "properties" : {
                                    "preserve_layout" : {
                                      "type" : "long"
                                    },
                                    "print" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "status" : {
                              "properties" : {
                                "cancelled" : {
                                  "type" : "long"
                                },
                                "completed" : {
                                  "type" : "long"
                                },
                                "completed_with_warnings" : {
                                  "type" : "long"
                                },
                                "failed" : {
                                  "type" : "long"
                                },
                                "pending" : {
                                  "type" : "long"
                                },
                                "processing" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "statuses" : {
                              "properties" : {
                                "cancelled" : {
                                  "properties" : {
                                    "PNG" : {
                                      "properties" : {
                                        "canvas workpad" : {
                                          "type" : "long"
                                        },
                                        "dashboard" : {
                                          "type" : "long"
                                        },
                                        "visualization" : {
                                          "type" : "long"
                                        }
                                      }
                                    },
                                    "printable_pdf" : {
                                      "properties" : {
                                        "canvas workpad" : {
                                          "type" : "long"
                                        },
                                        "dashboard" : {
                                          "type" : "long"
                                        },
                                        "visualization" : {
                                          "type" : "long"
                                        }
                                      }
                                    }
                                  }
                                },
                                "completed" : {
                                  "properties" : {
                                    "PNG" : {
                                      "properties" : {
                                        "canvas workpad" : {
                                          "type" : "long"
                                        },
                                        "dashboard" : {
                                          "type" : "long"
                                        },
                                        "visualization" : {
                                          "type" : "long"
                                        }
                                      }
                                    },
                                    "printable_pdf" : {
                                      "properties" : {
                                        "canvas workpad" : {
                                          "type" : "long"
                                        },
                                        "dashboard" : {
                                          "type" : "long"
                                        },
                                        "visualization" : {
                                          "type" : "long"
                                        }
                                      }
                                    }
                                  }
                                },
                                "failed" : {
                                  "properties" : {
                                    "PNG" : {
                                      "properties" : {
                                        "canvas workpad" : {
                                          "type" : "long"
                                        },
                                        "dashboard" : {
                                          "type" : "long"
                                        },
                                        "visualization" : {
                                          "type" : "long"
                                        }
                                      }
                                    },
                                    "printable_pdf" : {
                                      "properties" : {
                                        "canvas workpad" : {
                                          "type" : "long"
                                        },
                                        "dashboard" : {
                                          "type" : "long"
                                        },
                                        "visualization" : {
                                          "type" : "long"
                                        }
                                      }
                                    }
                                  }
                                },
                                "pending" : {
                                  "properties" : {
                                    "PNG" : {
                                      "properties" : {
                                        "canvas workpad" : {
                                          "type" : "long"
                                        },
                                        "dashboard" : {
                                          "type" : "long"
                                        },
                                        "visualization" : {
                                          "type" : "long"
                                        }
                                      }
                                    },
                                    "printable_pdf" : {
                                      "properties" : {
                                        "canvas workpad" : {
                                          "type" : "long"
                                        },
                                        "dashboard" : {
                                          "type" : "long"
                                        },
                                        "visualization" : {
                                          "type" : "long"
                                        }
                                      }
                                    }
                                  }
                                },
                                "processing" : {
                                  "properties" : {
                                    "PNG" : {
                                      "properties" : {
                                        "canvas workpad" : {
                                          "type" : "long"
                                        },
                                        "dashboard" : {
                                          "type" : "long"
                                        },
                                        "visualization" : {
                                          "type" : "long"
                                        }
                                      }
                                    },
                                    "printable_pdf" : {
                                      "properties" : {
                                        "canvas workpad" : {
                                          "type" : "long"
                                        },
                                        "dashboard" : {
                                          "type" : "long"
                                        },
                                        "visualization" : {
                                          "type" : "long"
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        },
                        "printable_pdf" : {
                          "properties" : {
                            "app" : {
                              "properties" : {
                                "canvas workpad" : {
                                  "type" : "long"
                                },
                                "dashboard" : {
                                  "type" : "long"
                                },
                                "visualization" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "available" : {
                              "type" : "boolean"
                            },
                            "layout" : {
                              "properties" : {
                                "preserve_layout" : {
                                  "type" : "long"
                                },
                                "print" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "status" : {
                          "properties" : {
                            "completed" : {
                              "type" : "long"
                            },
                            "failed" : {
                              "type" : "long"
                            },
                            "pending" : {
                              "type" : "long"
                            },
                            "processing" : {
                              "type" : "long"
                            }
                          }
                        },
                        "statuses" : {
                          "properties" : {
                            "cancelled" : {
                              "properties" : {
                                "PNG" : {
                                  "properties" : {
                                    "canvas workpad" : {
                                      "type" : "long"
                                    },
                                    "dashboard" : {
                                      "type" : "long"
                                    },
                                    "visualization" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "printable_pdf" : {
                                  "properties" : {
                                    "canvas workpad" : {
                                      "type" : "long"
                                    },
                                    "dashboard" : {
                                      "type" : "long"
                                    },
                                    "visualization" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            },
                            "completed" : {
                              "properties" : {
                                "PNG" : {
                                  "properties" : {
                                    "canvas workpad" : {
                                      "type" : "long"
                                    },
                                    "dashboard" : {
                                      "type" : "long"
                                    },
                                    "visualization" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "printable_pdf" : {
                                  "properties" : {
                                    "canvas workpad" : {
                                      "type" : "long"
                                    },
                                    "dashboard" : {
                                      "type" : "long"
                                    },
                                    "visualization" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            },
                            "failed" : {
                              "properties" : {
                                "PNG" : {
                                  "properties" : {
                                    "canvas workpad" : {
                                      "type" : "long"
                                    },
                                    "dashboard" : {
                                      "type" : "long"
                                    },
                                    "visualization" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "printable_pdf" : {
                                  "properties" : {
                                    "canvas workpad" : {
                                      "type" : "long"
                                    },
                                    "dashboard" : {
                                      "type" : "long"
                                    },
                                    "visualization" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            },
                            "pending" : {
                              "properties" : {
                                "PNG" : {
                                  "properties" : {
                                    "canvas workpad" : {
                                      "type" : "long"
                                    },
                                    "dashboard" : {
                                      "type" : "long"
                                    },
                                    "visualization" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "printable_pdf" : {
                                  "properties" : {
                                    "canvas workpad" : {
                                      "type" : "long"
                                    },
                                    "dashboard" : {
                                      "type" : "long"
                                    },
                                    "visualization" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            },
                            "processing" : {
                              "properties" : {
                                "PNG" : {
                                  "properties" : {
                                    "canvas workpad" : {
                                      "type" : "long"
                                    },
                                    "dashboard" : {
                                      "type" : "long"
                                    },
                                    "visualization" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "printable_pdf" : {
                                  "properties" : {
                                    "canvas workpad" : {
                                      "type" : "long"
                                    },
                                    "dashboard" : {
                                      "type" : "long"
                                    },
                                    "visualization" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    },
                    "rollups" : {
                      "properties" : {
                        "index_patterns" : {
                          "properties" : {
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "saved_searches" : {
                          "properties" : {
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "visualizations" : {
                          "properties" : {
                            "saved_searches" : {
                              "properties" : {
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    },
                    "sample-data" : {
                      "properties" : {
                        "installed" : {
                          "type" : "keyword"
                        },
                        "last_install_date" : {
                          "type" : "date",
                          "ignore_malformed" : true
                        },
                        "last_install_set" : {
                          "type" : "keyword"
                        },
                        "last_uninstall_date" : {
                          "type" : "date",
                          "ignore_malformed" : true
                        },
                        "last_uninstall_set" : {
                          "type" : "date",
                          "ignore_malformed" : true
                        },
                        "uninstalled" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "search" : {
                      "properties" : {
                        "averageDuration" : {
                          "type" : "float"
                        },
                        "errorCount" : {
                          "type" : "long"
                        },
                        "search_telemetry" : {
                          "properties" : {
                            "average_duration" : {
                              "type" : "float"
                            },
                            "error_count" : {
                              "type" : "long"
                            },
                            "success_count" : {
                              "type" : "long"
                            }
                          }
                        },
                        "successCount" : {
                          "type" : "long"
                        }
                      }
                    },
                    "security_solution" : {
                      "properties" : {
                        "detections" : {
                          "properties" : {
                            "detection_rules" : {
                              "properties" : {
                                "custom" : {
                                  "properties" : {
                                    "disabled" : {
                                      "type" : "long"
                                    },
                                    "enabled" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "elastic" : {
                                  "properties" : {
                                    "disabled" : {
                                      "type" : "long"
                                    },
                                    "enabled" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            },
                            "ml_jobs" : {
                              "properties" : {
                                "custom" : {
                                  "properties" : {
                                    "disabled" : {
                                      "type" : "long"
                                    },
                                    "enabled" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "elastic" : {
                                  "properties" : {
                                    "disabled" : {
                                      "type" : "long"
                                    },
                                    "enabled" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            }
                          }
                        },
                        "endpoints" : {
                          "properties" : {
                            "active_within_last_24_hours" : {
                              "type" : "long"
                            },
                            "os" : {
                              "properties" : {
                                "count" : {
                                  "type" : "long"
                                },
                                "full_name" : {
                                  "type" : "keyword"
                                },
                                "platform" : {
                                  "type" : "keyword"
                                },
                                "version" : {
                                  "type" : "keyword"
                                }
                              }
                            },
                            "policies" : {
                              "properties" : {
                                "malware" : {
                                  "properties" : {
                                    "active" : {
                                      "type" : "long"
                                    },
                                    "failure" : {
                                      "type" : "long"
                                    },
                                    "inactive" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            },
                            "total_installed" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    },
                    "spaces" : {
                      "properties" : {
                        "available" : {
                          "type" : "boolean"
                        },
                        "count" : {
                          "type" : "long"
                        },
                        "disabledFeatures" : {
                          "properties" : {
                            "advancedSettings" : {
                              "type" : "long"
                            },
                            "apm" : {
                              "type" : "long"
                            },
                            "canvas" : {
                              "type" : "long"
                            },
                            "dashboard" : {
                              "type" : "long"
                            },
                            "dev_tools" : {
                              "type" : "long"
                            },
                            "discover" : {
                              "type" : "long"
                            },
                            "graph" : {
                              "type" : "long"
                            },
                            "indexPatterns" : {
                              "type" : "long"
                            },
                            "infrastructure" : {
                              "type" : "long"
                            },
                            "logs" : {
                              "type" : "long"
                            },
                            "maps" : {
                              "type" : "long"
                            },
                            "ml" : {
                              "type" : "long"
                            },
                            "monitoring" : {
                              "type" : "long"
                            },
                            "savedObjectsManagement" : {
                              "type" : "long"
                            },
                            "siem" : {
                              "type" : "long"
                            },
                            "timelion" : {
                              "type" : "long"
                            },
                            "uptime" : {
                              "type" : "long"
                            },
                            "visualize" : {
                              "type" : "long"
                            }
                          }
                        },
                        "enabled" : {
                          "type" : "boolean"
                        },
                        "usesFeatureControls" : {
                          "type" : "boolean"
                        }
                      }
                    },
                    "static_telemetry" : {
                      "properties" : {
                        "ece" : {
                          "properties" : {
                            "account_id" : {
                              "type" : "keyword"
                            },
                            "es_uuid" : {
                              "type" : "keyword"
                            },
                            "kb_uuid" : {
                              "type" : "keyword"
                            },
                            "license" : {
                              "properties" : {
                                "expiry_date_in_millis" : {
                                  "type" : "long"
                                },
                                "issue_date_in_millis" : {
                                  "type" : "long"
                                },
                                "issued_to" : {
                                  "type" : "text",
                                  "fields" : {
                                    "keyword" : {
                                      "type" : "keyword"
                                    }
                                  }
                                },
                                "issuer" : {
                                  "type" : "text",
                                  "fields" : {
                                    "keyword" : {
                                      "type" : "keyword"
                                    }
                                  }
                                },
                                "max_resource_units" : {
                                  "type" : "long"
                                },
                                "start_date_in_millis" : {
                                  "type" : "long"
                                },
                                "type" : {
                                  "type" : "keyword"
                                },
                                "uuid" : {
                                  "type" : "keyword"
                                }
                              }
                            }
                          }
                        },
                        "eck" : {
                          "properties" : {
                            "build" : {
                              "properties" : {
                                "date" : {
                                  "type" : "date"
                                },
                                "hash" : {
                                  "type" : "text"
                                },
                                "version" : {
                                  "type" : "keyword",
                                  "fields" : {
                                    "major" : {
                                      "type" : "keyword",
                                      "normalizer" : "major"
                                    },
                                    "minor" : {
                                      "type" : "keyword",
                                      "normalizer" : "minor"
                                    },
                                    "patch" : {
                                      "type" : "keyword",
                                      "normalizer" : "patch"
                                    }
                                  }
                                }
                              }
                            },
                            "custom_operator_namespace" : {
                              "type" : "boolean"
                            },
                            "distribution" : {
                              "type" : "text",
                              "fields" : {
                                "keyword" : {
                                  "type" : "keyword",
                                  "ignore_above" : 256
                                }
                              }
                            },
                            "operator_roles" : {
                              "type" : "keyword"
                            },
                            "operator_uuid" : {
                              "type" : "keyword"
                            }
                          }
                        },
                        "ess" : {
                          "properties" : {
                            "account_id" : {
                              "type" : "keyword"
                            },
                            "es_uuid" : {
                              "type" : "keyword"
                            },
                            "kb_uuid" : {
                              "type" : "keyword"
                            },
                            "license" : {
                              "properties" : {
                                "expiry_date_in_millis" : {
                                  "type" : "long"
                                },
                                "issue_date_in_millis" : {
                                  "type" : "long"
                                },
                                "issued_to" : {
                                  "type" : "text",
                                  "fields" : {
                                    "keyword" : {
                                      "type" : "keyword"
                                    }
                                  }
                                },
                                "issuer" : {
                                  "type" : "text",
                                  "fields" : {
                                    "keyword" : {
                                      "type" : "keyword"
                                    }
                                  }
                                },
                                "max_resource_units" : {
                                  "type" : "long"
                                },
                                "start_date_in_millis" : {
                                  "type" : "long"
                                },
                                "type" : {
                                  "type" : "keyword"
                                },
                                "uuid" : {
                                  "type" : "keyword"
                                }
                              }
                            }
                          }
                        }
                      }
                    },
                    "telemetry" : {
                      "properties" : {
                        "opt_in_status" : {
                          "type" : "boolean"
                        },
                        "usage_fetcher" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "tsvb-validation" : {
                      "properties" : {
                        "failed_validations" : {
                          "type" : "long"
                        }
                      }
                    },
                    "ui_metric" : {
                      "properties" : {
                        "DashboardPanelVersionInUrl" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "Kibana_home" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "apm" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "canvas" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "console" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "cross_cluster_replication" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "index_lifecycle_management" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "index_management" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "infra_logs" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "infra_metrics" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "ingest_pipelines" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "kibana-user_agent" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "remote_clusters" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "rollup_jobs" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "siem" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "snapshot_restore" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "uptime" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        },
                        "visualize" : {
                          "properties" : {
                            "key" : {
                              "type" : "keyword"
                            },
                            "value" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    },
                    "upgrade-assistant-telemetry" : {
                      "properties" : {
                        "features" : {
                          "properties" : {
                            "deprecation_logging" : {
                              "properties" : {
                                "enabled" : {
                                  "type" : "boolean"
                                }
                              }
                            }
                          }
                        },
                        "ui_open" : {
                          "properties" : {
                            "cluster" : {
                              "type" : "long"
                            },
                            "indices" : {
                              "type" : "long"
                            },
                            "overview" : {
                              "type" : "long"
                            }
                          }
                        },
                        "ui_reindex" : {
                          "properties" : {
                            "close" : {
                              "type" : "long"
                            },
                            "open" : {
                              "type" : "long"
                            },
                            "start" : {
                              "type" : "long"
                            },
                            "stop" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    },
                    "uptime" : {
                      "properties" : {
                        "last_24_hours" : {
                          "properties" : {
                            "hits" : {
                              "properties" : {
                                "autoRefreshEnabled" : {
                                  "type" : "boolean"
                                },
                                "autorefreshInterval" : {
                                  "type" : "long"
                                },
                                "dateRangeEnd" : {
                                  "type" : "text",
                                  "fields" : {
                                    "keyword" : {
                                      "type" : "keyword",
                                      "ignore_above" : 256
                                    }
                                  }
                                },
                                "dateRangeStart" : {
                                  "type" : "text",
                                  "fields" : {
                                    "keyword" : {
                                      "type" : "keyword",
                                      "ignore_above" : 256
                                    }
                                  }
                                },
                                "monitor_frequency" : {
                                  "type" : "long"
                                },
                                "monitor_name_stats" : {
                                  "properties" : {
                                    "avg_length" : {
                                      "type" : "float"
                                    },
                                    "max_length" : {
                                      "type" : "long"
                                    },
                                    "min_length" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "monitor_page" : {
                                  "type" : "long"
                                },
                                "no_of_unique_monitors" : {
                                  "type" : "long"
                                },
                                "no_of_unique_observer_locations" : {
                                  "type" : "long"
                                },
                                "observer_location_name_stats" : {
                                  "properties" : {
                                    "avg_length" : {
                                      "type" : "float"
                                    },
                                    "max_length" : {
                                      "type" : "long"
                                    },
                                    "min_length" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "overview_page" : {
                                  "type" : "long"
                                },
                                "settings_page" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        }
                      }
                    },
                    "visualization_types" : {
                      "properties" : {
                        "area" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "gauge" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "goal" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "heatmap" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "histogram" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "horizontal_bar" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "input_control_vis" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "line" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "markdown" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "metric" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "metrics" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "pie" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "region_map" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "table" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "tagcloud" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "tile_map" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "timelion" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "vega" : {
                          "properties" : {
                            "saved_30_days_total" : {
                              "type" : "long"
                            },
                            "saved_7_days_total" : {
                              "type" : "long"
                            },
                            "saved_90_days_total" : {
                              "type" : "long"
                            },
                            "spaces_avg" : {
                              "type" : "long"
                            },
                            "spaces_max" : {
                              "type" : "long"
                            },
                            "spaces_min" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    },
                    "workplace_search" : {
                      "properties" : {
                        "ui_clicked" : {
                          "properties" : {
                            "header_launch_button" : {
                              "type" : "long"
                            },
                            "onboarding_card_button" : {
                              "type" : "long"
                            },
                            "org_name_change_button" : {
                              "type" : "long"
                            },
                            "recent_activity_source_details_link" : {
                              "type" : "long"
                            }
                          }
                        },
                        "ui_error" : {
                          "properties" : {
                            "cannot_connect" : {
                              "type" : "long"
                            }
                          }
                        },
                        "ui_viewed" : {
                          "properties" : {
                            "overview" : {
                              "type" : "long"
                            },
                            "setup_guide" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    }
                  }
                },
                "search" : {
                  "properties" : {
                    "total" : {
                      "type" : "long"
                    }
                  }
                },
                "timelion_sheet" : {
                  "properties" : {
                    "total" : {
                      "type" : "long"
                    }
                  }
                },
                "versions" : {
                  "properties" : {
                    "count" : {
                      "type" : "short"
                    },
                    "version" : {
                      "type" : "keyword"
                    }
                  }
                },
                "visualization" : {
                  "properties" : {
                    "total" : {
                      "type" : "long"
                    }
                  }
                }
              }
            },
            "logstash" : {
              "properties" : {
                "count" : {
                  "type" : "short"
                },
                "versions" : {
                  "properties" : {
                    "count" : {
                      "type" : "short"
                    },
                    "version" : {
                      "type" : "keyword"
                    }
                  }
                }
              }
            },
            "workplace_search" : {
              "properties" : {
                "config" : {
                  "properties" : {
                    "auth_source" : {
                      "type" : "keyword"
                    },
                    "limits" : {
                      "properties" : {
                        "custom_api_source" : {
                          "properties" : {
                            "document_size_in_bytes" : {
                              "type" : "keyword"
                            },
                            "total_fields" : {
                              "type" : "short"
                            }
                          }
                        }
                      }
                    }
                  }
                },
                "organizations" : {
                  "properties" : {
                    "created_at" : {
                      "type" : "date"
                    },
                    "groups" : {
                      "properties" : {
                        "user_count" : {
                          "type" : "long"
                        }
                      }
                    },
                    "id" : {
                      "type" : "keyword"
                    },
                    "org_sources" : {
                      "properties" : {
                        "created_at" : {
                          "type" : "date"
                        },
                        "document_count" : {
                          "type" : "long"
                        },
                        "service_type" : {
                          "type" : "keyword"
                        },
                        "updated_at" : {
                          "type" : "date"
                        }
                      }
                    },
                    "private_sources" : {
                      "properties" : {
                        "created_at" : {
                          "type" : "date"
                        },
                        "document_count" : {
                          "type" : "long"
                        },
                        "service_type" : {
                          "type" : "keyword"
                        },
                        "updated_at" : {
                          "type" : "date"
                        }
                      }
                    },
                    "updated_at" : {
                      "type" : "date"
                    },
                    "users" : {
                      "properties" : {
                        "current_sign_in_at" : {
                          "type" : "date"
                        },
                        "role_type" : {
                          "type" : "keyword"
                        },
                        "sign_in_count" : {
                          "type" : "long"
                        }
                      }
                    }
                  }
                },
                "stats" : {
                  "properties" : {
                    "total_active_non_admin_users" : {
                      "type" : "long"
                    },
                    "total_admin_users" : {
                      "type" : "long"
                    },
                    "total_non_admin_users" : {
                      "type" : "long"
                    },
                    "total_org_sources" : {
                      "type" : "long"
                    },
                    "total_organizations" : {
                      "type" : "long"
                    },
                    "total_pending_invites" : {
                      "type" : "long"
                    },
                    "total_private_sources" : {
                      "type" : "long"
                    },
                    "total_queries_last_30_days" : {
                      "type" : "long"
                    },
                    "total_role_mappings" : {
                      "type" : "long"
                    },
                    "total_sign_ins" : {
                      "type" : "long"
                    }
                  }
                }
              }
            },
            "xpack" : {
              "properties" : {
                "ccr" : {
                  "properties" : {
                    "auto_follow_patterns_count" : {
                      "type" : "integer"
                    },
                    "available" : {
                      "type" : "boolean"
                    },
                    "enabled" : {
                      "type" : "boolean"
                    },
                    "follower_indices_count" : {
                      "type" : "integer"
                    },
                    "last_follow_time_in_millis" : {
                      "type" : "long"
                    }
                  }
                },
                "data_frame" : {
                  "properties" : {
                    "available" : {
                      "type" : "boolean"
                    },
                    "enabled" : {
                      "type" : "boolean"
                    },
                    "stats" : {
                      "properties" : {
                        "documents_indexed" : {
                          "type" : "long"
                        },
                        "documents_processed" : {
                          "type" : "long"
                        },
                        "index_failures" : {
                          "type" : "long"
                        },
                        "index_time_in_ms" : {
                          "type" : "long"
                        },
                        "index_total" : {
                          "type" : "long"
                        },
                        "pages_processed" : {
                          "type" : "long"
                        },
                        "search_failures" : {
                          "type" : "long"
                        },
                        "search_time_in_ms" : {
                          "type" : "long"
                        },
                        "search_total" : {
                          "type" : "long"
                        },
                        "trigger_count" : {
                          "type" : "long"
                        }
                      }
                    },
                    "transforms" : {
                      "properties" : {
                        "_all" : {
                          "type" : "long"
                        },
                        "started" : {
                          "type" : "long"
                        },
                        "stopped" : {
                          "type" : "long"
                        }
                      }
                    }
                  }
                },
                "data_streams" : {
                  "properties" : {
                    "available" : {
                      "type" : "boolean"
                    },
                    "data_streams" : {
                      "type" : "long"
                    },
                    "enabled" : {
                      "type" : "boolean"
                    },
                    "indices_count" : {
                      "type" : "long"
                    }
                  }
                },
                "eql" : {
                  "properties" : {
                    "available" : {
                      "type" : "boolean"
                    },
                    "enabled" : {
                      "type" : "boolean"
                    },
                    "features" : {
                      "properties" : {
                        "event" : {
                          "type" : "long"
                        },
                        "join" : {
                          "type" : "long"
                        },
                        "joins" : {
                          "properties" : {
                            "join_queries_five_or_more" : {
                              "type" : "long"
                            },
                            "join_queries_four" : {
                              "type" : "long"
                            },
                            "join_queries_three" : {
                              "type" : "long"
                            },
                            "join_queries_two" : {
                              "type" : "long"
                            },
                            "join_until" : {
                              "type" : "long"
                            }
                          }
                        },
                        "keys" : {
                          "properties" : {
                            "join_keys_five_or_more" : {
                              "type" : "long"
                            },
                            "join_keys_four" : {
                              "type" : "long"
                            },
                            "join_keys_one" : {
                              "type" : "long"
                            },
                            "join_keys_three" : {
                              "type" : "long"
                            },
                            "join_keys_two" : {
                              "type" : "long"
                            }
                          }
                        },
                        "pipes" : {
                          "properties" : {
                            "pipe_head" : {
                              "type" : "long"
                            },
                            "pipe_tail" : {
                              "type" : "long"
                            }
                          }
                        },
                        "sequence" : {
                          "type" : "long"
                        },
                        "sequences" : {
                          "properties" : {
                            "sequence_maxspan" : {
                              "type" : "long"
                            },
                            "sequence_queries_five_or_more" : {
                              "type" : "long"
                            },
                            "sequence_queries_four" : {
                              "type" : "long"
                            },
                            "sequence_queries_three" : {
                              "type" : "long"
                            },
                            "sequence_queries_two" : {
                              "type" : "long"
                            },
                            "sequence_until" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    },
                    "queries" : {
                      "properties" : {
                        "_all" : {
                          "properties" : {
                            "failed" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "all" : {
                          "properties" : {
                            "failed" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    }
                  }
                },
                "frozen_indices" : {
                  "properties" : {
                    "available" : {
                      "type" : "boolean"
                    },
                    "enabled" : {
                      "type" : "boolean"
                    },
                    "indices_count" : {
                      "type" : "integer"
                    }
                  }
                },
                "graph" : {
                  "properties" : {
                    "available" : {
                      "type" : "boolean"
                    },
                    "enabled" : {
                      "type" : "boolean"
                    }
                  }
                },
                "ilm" : {
                  "properties" : {
                    "policy_count" : {
                      "type" : "long"
                    },
                    "policy_stats" : {
                      "properties" : {
                        "indices_managed" : {
                          "type" : "long"
                        },
                        "phases" : {
                          "properties" : {
                            "cold" : {
                              "properties" : {
                                "actions" : {
                                  "type" : "keyword"
                                },
                                "min_age" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "delete" : {
                              "properties" : {
                                "actions" : {
                                  "type" : "keyword"
                                },
                                "min_age" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "hot" : {
                              "properties" : {
                                "actions" : {
                                  "type" : "keyword"
                                },
                                "min_age" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "warm" : {
                              "properties" : {
                                "actions" : {
                                  "type" : "keyword"
                                },
                                "min_age" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                },
                "ingest_manager" : {
                  "properties" : {
                    "agents" : {
                      "properties" : {
                        "error" : {
                          "type" : "long"
                        },
                        "offline" : {
                          "type" : "long"
                        },
                        "online" : {
                          "type" : "long"
                        },
                        "total" : {
                          "type" : "long"
                        }
                      }
                    },
                    "fleet_enabled" : {
                      "type" : "boolean"
                    },
                    "packages" : {
                      "properties" : {
                        "enabled" : {
                          "type" : "boolean"
                        },
                        "name" : {
                          "type" : "keyword"
                        },
                        "version" : {
                          "type" : "keyword"
                        }
                      }
                    }
                  }
                },
                "logstash" : {
                  "properties" : {
                    "available" : {
                      "type" : "boolean"
                    },
                    "enabled" : {
                      "type" : "boolean"
                    }
                  }
                },
                "ml" : {
                  "properties" : {
                    "available" : {
                      "type" : "boolean"
                    },
                    "data_frame_analytics_jobs" : {
                      "properties" : {
                        "_all" : {
                          "properties" : {
                            "count" : {
                              "type" : "integer"
                            }
                          }
                        },
                        "stopped" : {
                          "properties" : {
                            "count" : {
                              "type" : "integer"
                            }
                          }
                        }
                      }
                    },
                    "datafeeds" : {
                      "properties" : {
                        "_all" : {
                          "properties" : {
                            "count" : {
                              "type" : "long"
                            }
                          }
                        },
                        "started" : {
                          "properties" : {
                            "count" : {
                              "type" : "long"
                            }
                          }
                        },
                        "stopped" : {
                          "properties" : {
                            "count" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    },
                    "enabled" : {
                      "type" : "boolean"
                    },
                    "inference" : {
                      "properties" : {
                        "ingest_processors" : {
                          "properties" : {
                            "_all" : {
                              "properties" : {
                                "count" : {
                                  "type" : "long"
                                },
                                "num_docs_processed" : {
                                  "properties" : {
                                    "max" : {
                                      "type" : "long"
                                    },
                                    "min" : {
                                      "type" : "long"
                                    },
                                    "sum" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "num_failures" : {
                                  "properties" : {
                                    "max" : {
                                      "type" : "long"
                                    },
                                    "min" : {
                                      "type" : "long"
                                    },
                                    "sum" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "pipelines" : {
                                  "properties" : {
                                    "count" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "time_ms" : {
                                  "properties" : {
                                    "max" : {
                                      "type" : "long"
                                    },
                                    "min" : {
                                      "type" : "long"
                                    },
                                    "sum" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            }
                          }
                        },
                        "trained_models" : {
                          "properties" : {
                            "_all" : {
                              "properties" : {
                                "count" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        }
                      }
                    },
                    "jobs" : {
                      "properties" : {
                        "_all" : {
                          "properties" : {
                            "count" : {
                              "type" : "long"
                            },
                            "created_by" : {
                              "properties" : {
                                "categorization_wizard" : {
                                  "type" : "long"
                                },
                                "ml_module_apache_access" : {
                                  "type" : "long"
                                },
                                "ml_module_apm_jsbase" : {
                                  "type" : "long"
                                },
                                "ml_module_apm_nodejs" : {
                                  "type" : "long"
                                },
                                "ml_module_apm_transaction" : {
                                  "type" : "long"
                                },
                                "ml_module_auditbeat_process_docker" : {
                                  "type" : "long"
                                },
                                "ml_module_auditbeat_process_hosts" : {
                                  "type" : "long"
                                },
                                "ml_module_logs_ui_analysis" : {
                                  "type" : "long"
                                },
                                "ml_module_logs_ui_categories" : {
                                  "type" : "long"
                                },
                                "ml_module_metricbeat_system" : {
                                  "type" : "long"
                                },
                                "ml_module_metricbeat_system_ecs" : {
                                  "type" : "long"
                                },
                                "ml_module_nginx_access" : {
                                  "type" : "long"
                                },
                                "ml_module_sample" : {
                                  "type" : "long"
                                },
                                "ml_module_siem_auditbeat" : {
                                  "type" : "long"
                                },
                                "ml_module_siem_packetbeat" : {
                                  "type" : "long"
                                },
                                "ml_module_siem_winlogbeat" : {
                                  "type" : "long"
                                },
                                "ml_module_uptime_heartbeat" : {
                                  "type" : "long"
                                },
                                "multi_metric_wizard" : {
                                  "type" : "long"
                                },
                                "population_wizard" : {
                                  "type" : "long"
                                },
                                "single_metric_wizard" : {
                                  "type" : "long"
                                },
                                "unknown" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "detectors" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "long"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "forecasts" : {
                              "properties" : {
                                "duration_ms" : {
                                  "properties" : {
                                    "avg" : {
                                      "type" : "double"
                                    },
                                    "max" : {
                                      "type" : "long"
                                    },
                                    "min" : {
                                      "type" : "long"
                                    },
                                    "total" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "forecasted_jobs" : {
                                  "type" : "long"
                                },
                                "memory_bytes" : {
                                  "properties" : {
                                    "avg" : {
                                      "type" : "double"
                                    },
                                    "max" : {
                                      "type" : "long"
                                    },
                                    "min" : {
                                      "type" : "long"
                                    },
                                    "total" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "processing_time_ms" : {
                                  "properties" : {
                                    "avg" : {
                                      "type" : "double"
                                    },
                                    "max" : {
                                      "type" : "long"
                                    },
                                    "min" : {
                                      "type" : "long"
                                    },
                                    "total" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "records" : {
                                  "properties" : {
                                    "avg" : {
                                      "type" : "double"
                                    },
                                    "max" : {
                                      "type" : "long"
                                    },
                                    "min" : {
                                      "type" : "long"
                                    },
                                    "total" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "status" : {
                                  "properties" : {
                                    "failed" : {
                                      "type" : "long"
                                    },
                                    "finished" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "model_size" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "long"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        },
                        "closed" : {
                          "properties" : {
                            "count" : {
                              "type" : "long"
                            },
                            "created_by" : {
                              "properties" : {
                                "ml_module_siem_auditbeat" : {
                                  "type" : "long"
                                },
                                "ml_module_siem_winlogbeat" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "detectors" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "float"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "model_size" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "float"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        },
                        "closing" : {
                          "properties" : {
                            "count" : {
                              "type" : "long"
                            },
                            "detectors" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "long"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "model_size" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "long"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        },
                        "failed" : {
                          "properties" : {
                            "count" : {
                              "type" : "long"
                            },
                            "detectors" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "long"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "model_size" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "long"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        },
                        "opened" : {
                          "properties" : {
                            "count" : {
                              "type" : "long"
                            },
                            "detectors" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "long"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "model_size" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "long"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        },
                        "opening" : {
                          "properties" : {
                            "count" : {
                              "type" : "long"
                            },
                            "detectors" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "long"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "model_size" : {
                              "properties" : {
                                "avg" : {
                                  "type" : "long"
                                },
                                "max" : {
                                  "type" : "long"
                                },
                                "min" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        }
                      }
                    },
                    "node_count" : {
                      "type" : "integer"
                    }
                  }
                },
                "monitoring" : {
                  "properties" : {
                    "available" : {
                      "type" : "boolean"
                    },
                    "collection_enabled" : {
                      "type" : "boolean"
                    },
                    "enabled" : {
                      "type" : "boolean"
                    },
                    "enabled_exporters" : {
                      "properties" : {
                        "http" : {
                          "type" : "short"
                        },
                        "local" : {
                          "type" : "short"
                        }
                      }
                    }
                  }
                },
                "reporting" : {
                  "properties" : {
                    "_all" : {
                      "type" : "long"
                    },
                    "available" : {
                      "type" : "boolean"
                    },
                    "browser_type" : {
                      "type" : "keyword"
                    },
                    "csv" : {
                      "properties" : {
                        "available" : {
                          "type" : "boolean"
                        },
                        "total" : {
                          "type" : "long"
                        }
                      }
                    },
                    "enabled" : {
                      "type" : "boolean"
                    },
                    "last7Days" : {
                      "properties" : {
                        "_all" : {
                          "type" : "long"
                        },
                        "csv" : {
                          "properties" : {
                            "available" : {
                              "type" : "boolean"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "printable_pdf" : {
                          "properties" : {
                            "app" : {
                              "properties" : {
                                "dashboard" : {
                                  "type" : "long"
                                },
                                "visualization" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "available" : {
                              "type" : "boolean"
                            },
                            "layout" : {
                              "properties" : {
                                "preserve_layout" : {
                                  "type" : "long"
                                },
                                "print" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "status" : {
                          "properties" : {
                            "completed" : {
                              "type" : "long"
                            },
                            "failed" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    },
                    "lastDay" : {
                      "properties" : {
                        "_all" : {
                          "type" : "long"
                        },
                        "csv" : {
                          "properties" : {
                            "available" : {
                              "type" : "boolean"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "printable_pdf" : {
                          "properties" : {
                            "app" : {
                              "properties" : {
                                "dashboard" : {
                                  "type" : "long"
                                },
                                "visualization" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "available" : {
                              "type" : "boolean"
                            },
                            "layout" : {
                              "properties" : {
                                "preserve_layout" : {
                                  "type" : "long"
                                },
                                "print" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "status" : {
                          "properties" : {
                            "completed" : {
                              "type" : "long"
                            },
                            "failed" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    },
                    "printable_pdf" : {
                      "properties" : {
                        "app" : {
                          "properties" : {
                            "dashboard" : {
                              "type" : "long"
                            },
                            "visualization" : {
                              "type" : "long"
                            }
                          }
                        },
                        "available" : {
                          "type" : "boolean"
                        },
                        "layout" : {
                          "properties" : {
                            "preserve_layout" : {
                              "type" : "long"
                            },
                            "print" : {
                              "type" : "long"
                            }
                          }
                        },
                        "total" : {
                          "type" : "long"
                        }
                      }
                    },
                    "status" : {
                      "properties" : {
                        "completed" : {
                          "type" : "long"
                        },
                        "failed" : {
                          "type" : "long"
                        }
                      }
                    }
                  }
                },
                "rollup" : {
                  "properties" : {
                    "available" : {
                      "type" : "boolean"
                    },
                    "enabled" : {
                      "type" : "boolean"
                    }
                  }
                },
                "security" : {
                  "properties" : {
                    "anonymous" : {
                      "properties" : {
                        "enabled" : {
                          "type" : "boolean"
                        }
                      }
                    },
                    "audit" : {
                      "properties" : {
                        "enabled" : {
                          "type" : "boolean"
                        },
                        "outputs" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "available" : {
                      "type" : "boolean"
                    },
                    "enabled" : {
                      "type" : "boolean"
                    },
                    "enabled_realms" : {
                      "properties" : {
                        "name" : {
                          "type" : "keyword"
                        },
                        "order" : {
                          "type" : "integer"
                        },
                        "size" : {
                          "type" : "keyword"
                        },
                        "type" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "ipfilter" : {
                      "properties" : {
                        "http" : {
                          "type" : "boolean"
                        },
                        "transport" : {
                          "type" : "boolean"
                        }
                      }
                    },
                    "realm_info" : {
                      "properties" : {
                        "count" : {
                          "type" : "long"
                        },
                        "custom_count" : {
                          "type" : "long"
                        },
                        "names" : {
                          "type" : "keyword"
                        }
                      }
                    },
                    "realms" : {
                      "properties" : {
                        "active_directory" : {
                          "properties" : {
                            "available" : {
                              "type" : "boolean"
                            },
                            "enabled" : {
                              "type" : "boolean"
                            },
                            "load_balance_type" : {
                              "type" : "keyword"
                            },
                            "name" : {
                              "type" : "keyword"
                            },
                            "order" : {
                              "type" : "integer"
                            },
                            "size" : {
                              "type" : "long"
                            },
                            "ssl" : {
                              "type" : "boolean"
                            },
                            "user_search" : {
                              "type" : "boolean"
                            }
                          }
                        },
                        "file" : {
                          "properties" : {
                            "available" : {
                              "type" : "boolean"
                            },
                            "enabled" : {
                              "type" : "boolean"
                            },
                            "name" : {
                              "type" : "keyword"
                            },
                            "order" : {
                              "type" : "integer"
                            },
                            "size" : {
                              "type" : "long"
                            }
                          }
                        },
                        "kerberos" : {
                          "properties" : {
                            "available" : {
                              "type" : "boolean"
                            },
                            "enabled" : {
                              "type" : "boolean"
                            },
                            "name" : {
                              "type" : "keyword"
                            },
                            "order" : {
                              "type" : "integer"
                            }
                          }
                        },
                        "ldap" : {
                          "properties" : {
                            "available" : {
                              "type" : "boolean"
                            },
                            "enabled" : {
                              "type" : "boolean"
                            },
                            "load_balance_type" : {
                              "type" : "keyword"
                            },
                            "name" : {
                              "type" : "keyword"
                            },
                            "order" : {
                              "type" : "integer"
                            },
                            "size" : {
                              "type" : "long"
                            },
                            "ssl" : {
                              "type" : "boolean"
                            },
                            "user_search" : {
                              "type" : "boolean"
                            }
                          }
                        },
                        "native" : {
                          "properties" : {
                            "available" : {
                              "type" : "boolean"
                            },
                            "enabled" : {
                              "type" : "boolean"
                            },
                            "name" : {
                              "type" : "keyword"
                            },
                            "order" : {
                              "type" : "integer"
                            },
                            "size" : {
                              "type" : "long"
                            }
                          }
                        },
                        "oidc" : {
                          "properties" : {
                            "available" : {
                              "type" : "boolean"
                            },
                            "enabled" : {
                              "type" : "boolean"
                            },
                            "name" : {
                              "type" : "keyword"
                            },
                            "order" : {
                              "type" : "integer"
                            }
                          }
                        },
                        "pki" : {
                          "properties" : {
                            "available" : {
                              "type" : "boolean"
                            },
                            "enabled" : {
                              "type" : "boolean"
                            },
                            "name" : {
                              "type" : "keyword"
                            },
                            "order" : {
                              "type" : "integer"
                            }
                          }
                        },
                        "saml" : {
                          "properties" : {
                            "available" : {
                              "type" : "boolean"
                            },
                            "enabled" : {
                              "type" : "boolean"
                            },
                            "name" : {
                              "type" : "keyword"
                            },
                            "order" : {
                              "type" : "integer"
                            }
                          }
                        }
                      }
                    },
                    "role_mapping" : {
                      "properties" : {
                        "native" : {
                          "properties" : {
                            "enabled" : {
                              "type" : "long"
                            },
                            "size" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    },
                    "roles" : {
                      "properties" : {
                        "file" : {
                          "properties" : {
                            "dls" : {
                              "type" : "boolean"
                            },
                            "fls" : {
                              "type" : "boolean"
                            },
                            "size" : {
                              "type" : "long"
                            }
                          }
                        },
                        "native" : {
                          "properties" : {
                            "dls" : {
                              "type" : "boolean"
                            },
                            "fls" : {
                              "type" : "boolean"
                            },
                            "size" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    },
                    "ssl" : {
                      "properties" : {
                        "http" : {
                          "properties" : {
                            "enabled" : {
                              "type" : "boolean"
                            }
                          }
                        },
                        "transport" : {
                          "properties" : {
                            "enabled" : {
                              "type" : "boolean"
                            }
                          }
                        }
                      }
                    },
                    "system_key" : {
                      "properties" : {
                        "enabled" : {
                          "type" : "boolean"
                        }
                      }
                    }
                  }
                },
                "slm" : {
                  "properties" : {
                    "available" : {
                      "type" : "boolean"
                    },
                    "enabled" : {
                      "type" : "boolean"
                    },
                    "policy_count" : {
                      "type" : "long"
                    },
                    "policy_stats" : {
                      "properties" : {
                        "policy_stats" : {
                          "properties" : {
                            "policy" : {
                              "type" : "keyword"
                            },
                            "snapshot_deletion_failures" : {
                              "type" : "long"
                            },
                            "snapshots_deleted" : {
                              "type" : "long"
                            },
                            "snapshots_failed" : {
                              "type" : "long"
                            },
                            "snapshots_taken" : {
                              "type" : "long"
                            }
                          }
                        },
                        "retention_deletion_time_millis" : {
                          "type" : "long"
                        },
                        "retention_failed" : {
                          "type" : "long"
                        },
                        "retention_runs" : {
                          "type" : "long"
                        },
                        "retention_timed_out" : {
                          "type" : "long"
                        },
                        "total_snapshot_deletion_failures" : {
                          "type" : "long"
                        },
                        "total_snapshots_deleted" : {
                          "type" : "long"
                        },
                        "total_snapshots_failed" : {
                          "type" : "long"
                        },
                        "total_snapshots_taken" : {
                          "type" : "long"
                        }
                      }
                    }
                  }
                },
                "sql" : {
                  "properties" : {
                    "available" : {
                      "type" : "boolean"
                    },
                    "enabled" : {
                      "type" : "boolean"
                    },
                    "features" : {
                      "properties" : {
                        "command" : {
                          "type" : "long"
                        },
                        "groupby" : {
                          "type" : "long"
                        },
                        "having" : {
                          "type" : "long"
                        },
                        "join" : {
                          "type" : "long"
                        },
                        "limit" : {
                          "type" : "long"
                        },
                        "local" : {
                          "type" : "long"
                        },
                        "orderby" : {
                          "type" : "long"
                        },
                        "subselect" : {
                          "type" : "long"
                        },
                        "where" : {
                          "type" : "long"
                        }
                      }
                    },
                    "queries" : {
                      "properties" : {
                        "_all" : {
                          "properties" : {
                            "failed" : {
                              "type" : "long"
                            },
                            "paging" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "canvas" : {
                          "properties" : {
                            "failed" : {
                              "type" : "long"
                            },
                            "paging" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "cli" : {
                          "properties" : {
                            "failed" : {
                              "type" : "long"
                            },
                            "paging" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "jdbc" : {
                          "properties" : {
                            "failed" : {
                              "type" : "long"
                            },
                            "paging" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "odbc" : {
                          "properties" : {
                            "failed" : {
                              "type" : "long"
                            },
                            "paging" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "rest" : {
                          "properties" : {
                            "failed" : {
                              "type" : "long"
                            },
                            "paging" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "translate" : {
                          "properties" : {
                            "count" : {
                              "type" : "long"
                            }
                          }
                        }
                      }
                    }
                  }
                },
                "transform" : {
                  "properties" : {
                    "available" : {
                      "type" : "boolean",
                      "copy_to" : [
                        "stack_stats.xpack.data_frame.available"
                      ]
                    },
                    "enabled" : {
                      "type" : "boolean",
                      "copy_to" : [
                        "stack_stats.xpack.data_frame.enabled"
                      ]
                    },
                    "stats" : {
                      "properties" : {
                        "documents_indexed" : {
                          "type" : "long",
                          "copy_to" : [
                            "stack_stats.xpack.data_frame.stats.documents_indexed"
                          ]
                        },
                        "documents_processed" : {
                          "type" : "long",
                          "copy_to" : [
                            "stack_stats.xpack.data_frame.stats.documents_processed"
                          ]
                        },
                        "exponential_avg_checkpoint_duration_ms" : {
                          "type" : "long"
                        },
                        "exponential_avg_documents_indexed" : {
                          "type" : "long"
                        },
                        "exponential_avg_documents_processed" : {
                          "type" : "long"
                        },
                        "index_failures" : {
                          "type" : "long",
                          "copy_to" : [
                            "stack_stats.xpack.data_frame.stats.index_failures"
                          ]
                        },
                        "index_time_in_ms" : {
                          "type" : "long",
                          "copy_to" : [
                            "stack_stats.xpack.data_frame.stats.index_time_in_ms"
                          ]
                        },
                        "index_total" : {
                          "type" : "long",
                          "copy_to" : [
                            "stack_stats.xpack.data_frame.stats.index_total"
                          ]
                        },
                        "pages_processed" : {
                          "type" : "long",
                          "copy_to" : [
                            "stack_stats.xpack.data_frame.stats.pages_processed"
                          ]
                        },
                        "search_failures" : {
                          "type" : "long",
                          "copy_to" : [
                            "stack_stats.xpack.data_frame.stats.search_failures"
                          ]
                        },
                        "search_time_in_ms" : {
                          "type" : "long",
                          "copy_to" : [
                            "stack_stats.xpack.data_frame.stats.search_time_in_ms"
                          ]
                        },
                        "search_total" : {
                          "type" : "long",
                          "copy_to" : [
                            "stack_stats.xpack.data_frame.stats.search_total"
                          ]
                        },
                        "trigger_count" : {
                          "type" : "long",
                          "copy_to" : [
                            "stack_stats.xpack.data_frame.stats.trigger_count"
                          ]
                        }
                      }
                    },
                    "transforms" : {
                      "properties" : {
                        "_all" : {
                          "type" : "long",
                          "copy_to" : [
                            "stack_stats.xpack.data_frame.transforms._all"
                          ]
                        },
                        "started" : {
                          "type" : "long",
                          "copy_to" : [
                            "stack_stats.xpack.data_frame.transforms.started"
                          ]
                        },
                        "stopped" : {
                          "type" : "long",
                          "copy_to" : [
                            "stack_stats.xpack.data_frame.transforms.stopped"
                          ]
                        }
                      }
                    }
                  }
                },
                "vectors" : {
                  "properties" : {
                    "available" : {
                      "type" : "boolean"
                    },
                    "dense_vector_dims_avg_count" : {
                      "type" : "short"
                    },
                    "dense_vector_fields_count" : {
                      "type" : "long"
                    },
                    "enabled" : {
                      "type" : "boolean"
                    }
                  }
                },
                "watcher" : {
                  "properties" : {
                    "available" : {
                      "type" : "boolean"
                    },
                    "count" : {
                      "properties" : {
                        "active" : {
                          "type" : "long"
                        },
                        "total" : {
                          "type" : "long"
                        }
                      }
                    },
                    "enabled" : {
                      "type" : "boolean"
                    },
                    "execution" : {
                      "properties" : {
                        "actions" : {
                          "properties" : {
                            "_all" : {
                              "properties" : {
                                "total" : {
                                  "type" : "long"
                                },
                                "total_time_in_ms" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "email" : {
                              "properties" : {
                                "total" : {
                                  "type" : "long"
                                },
                                "total_time_in_ms" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "hipchat" : {
                              "properties" : {
                                "total" : {
                                  "type" : "long"
                                },
                                "total_time_in_ms" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "index" : {
                              "properties" : {
                                "total" : {
                                  "type" : "long"
                                },
                                "total_time_in_ms" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "jira" : {
                              "properties" : {
                                "total" : {
                                  "type" : "long"
                                },
                                "total_time_in_ms" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "logging" : {
                              "properties" : {
                                "total" : {
                                  "type" : "long"
                                },
                                "total_time_in_ms" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "pagerduty" : {
                              "properties" : {
                                "total" : {
                                  "type" : "long"
                                },
                                "total_time_in_ms" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "slack" : {
                              "properties" : {
                                "total" : {
                                  "type" : "long"
                                },
                                "total_time_in_ms" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "webhook" : {
                              "properties" : {
                                "total" : {
                                  "type" : "long"
                                },
                                "total_time_in_ms" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        }
                      }
                    },
                    "watch" : {
                      "properties" : {
                        "action" : {
                          "properties" : {
                            "email" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "hipchat" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "index" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "jira" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "logging" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "pagerduty" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "slack" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "webhook" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        },
                        "condition" : {
                          "properties" : {
                            "_all" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "always" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "array_compare" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "compare" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "never" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "script" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        },
                        "input" : {
                          "properties" : {
                            "_all" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "chain" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "http" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "none" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "search" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "simple" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        },
                        "metadata" : {
                          "properties" : {
                            "active" : {
                              "type" : "long"
                            },
                            "total" : {
                              "type" : "long"
                            }
                          }
                        },
                        "transform" : {
                          "properties" : {
                            "chain" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "script" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "search" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            }
                          }
                        },
                        "trigger" : {
                          "properties" : {
                            "_all" : {
                              "properties" : {
                                "active" : {
                                  "type" : "long"
                                },
                                "total" : {
                                  "type" : "long"
                                }
                              }
                            },
                            "schedule" : {
                              "properties" : {
                                "_all" : {
                                  "properties" : {
                                    "active" : {
                                      "type" : "long"
                                    },
                                    "total" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "cron" : {
                                  "properties" : {
                                    "active" : {
                                      "type" : "long"
                                    },
                                    "total" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "daily" : {
                                  "properties" : {
                                    "active" : {
                                      "type" : "long"
                                    },
                                    "total" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "hourly" : {
                                  "properties" : {
                                    "active" : {
                                      "type" : "long"
                                    },
                                    "total" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "interval" : {
                                  "properties" : {
                                    "active" : {
                                      "type" : "long"
                                    },
                                    "total" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "monthly" : {
                                  "properties" : {
                                    "active" : {
                                      "type" : "long"
                                    },
                                    "total" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "weekly" : {
                                  "properties" : {
                                    "active" : {
                                      "type" : "long"
                                    },
                                    "total" : {
                                      "type" : "long"
                                    }
                                  }
                                },
                                "yearly" : {
                                  "properties" : {
                                    "active" : {
                                      "type" : "long"
                                    },
                                    "total" : {
                                      "type" : "long"
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        },
        "telemetry" : {
          "properties" : {
            "api_version" : {
              "type" : "long"
            },
            "headers" : {
              "properties" : {
                "agent" : {
                  "type" : "text",
                  "fields" : {
                    "keyword" : {
                      "type" : "keyword",
                      "ignore_above" : 256
                    }
                  }
                },
                "referrer" : {
                  "type" : "text",
                  "fields" : {
                    "keyword" : {
                      "type" : "keyword",
                      "ignore_above" : 128
                    }
                  }
                }
              }
            },
            "processed_timestamp" : {
              "type" : "date"
            },
            "usage" : {
              "properties" : {
                "kibana" : {
                  "type" : "boolean"
                },
                "logstash" : {
                  "type" : "boolean"
                },
                "xpack" : {
                  "properties" : {
                    "graph" : {
                      "type" : "boolean"
                    },
                    "ml" : {
                      "type" : "boolean"
                    },
                    "monitoring" : {
                      "type" : "boolean"
                    },
                    "paid_subscription" : {
                      "properties" : {
                        "basic_security" : {
                          "type" : "boolean"
                        },
                        "basic_usage" : {
                          "type" : "boolean"
                        },
                        "basic_usage_validations" : {
                          "properties" : {
                            "booleans" : {
                              "type" : "boolean"
                            },
                            "is_paid_subscription" : {
                              "type" : "boolean"
                            },
                            "no_counts_totals" : {
                              "type" : "boolean"
                            }
                          }
                        }
                      }
                    },
                    "reporting" : {
                      "type" : "boolean"
                    },
                    "security" : {
                      "type" : "boolean"
                    },
                    "solutions" : {
                      "properties" : {
                        "enterprise_search" : {
                          "type" : "boolean"
                        },
                        "enterprise_search_data" : {
                          "type" : "boolean"
                        },
                        "observability" : {
                          "type" : "boolean"
                        },
                        "observability_data" : {
                          "type" : "boolean"
                        },
                        "observability_data_with_ECS" : {
                          "type" : "boolean"
                        },
                        "observability_data_with_logstash" : {
                          "type" : "boolean"
                        },
                        "security" : {
                          "type" : "boolean"
                        },
                        "security_data" : {
                          "type" : "boolean"
                        },
                        "security_data_with_ECS" : {
                          "type" : "boolean"
                        },
                        "security_data_with_logstash" : {
                          "type" : "boolean"
                        }
                      }
                    },
                    "watcher" : {
                      "type" : "boolean"
                    }
                  }
                }
              }
            }
          }
        },
        "timestamp" : {
          "type" : "date"
        },
        "user_agent" : {
          "properties" : {
            "device" : {
              "type" : "text",
              "fields" : {
                "keyword" : {
                  "type" : "keyword",
                  "ignore_above" : 256
                }
              }
            },
            "major" : {
              "type" : "keyword"
            },
            "minor" : {
              "type" : "keyword"
            },
            "name" : {
              "type" : "text",
              "fields" : {
                "keyword" : {
                  "type" : "keyword",
                  "ignore_above" : 256
                }
              }
            },
            "os" : {
              "type" : "text",
              "fields" : {
                "keyword" : {
                  "type" : "keyword",
                  "ignore_above" : 256
                }
              }
            },
            "os_name" : {
              "type" : "text",
              "fields" : {
                "keyword" : {
                  "type" : "keyword",
                  "ignore_above" : 256
                }
              }
            }
          }
        },
        "version" : {
          "type" : "keyword",
          "fields" : {
            "major" : {
              "type" : "keyword",
              "normalizer" : "major"
            },
            "minor" : {
              "type" : "keyword",
              "normalizer" : "minor"
            },
            "patch" : {
              "type" : "keyword",
              "normalizer" : "patch"
            }
          }
        }
      }
    }
  }
}

