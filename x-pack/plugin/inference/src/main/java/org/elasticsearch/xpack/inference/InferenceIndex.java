/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;

/**
 * Defines the settings and the versioned mappings of the {@code .inference} system index.
 *
 * <p><b>Compatibility constraint on every mappings bump:</b> {@link InferenceIndexMappingManager}
 * force-installs this node's latest mappings on the write path via an origin-carrying put-mapping
 * request, bypassing the minimum-mappings-version downgrade the server applies elsewhere. In a
 * mixed-version cluster the result is published in cluster state to older nodes, which must be able
 * to parse it. Therefore every {@code mappingsVN()} must be <em>additive</em> and stick to field
 * types and mapping parameters that all node versions a rolling upgrade can pair this node with
 * already understand. A mapping that needs a newer construct must not rely on the force-install and
 * needs an explicit compatibility strategy instead. This invariant is enforced by
 * {@code InferenceIndexMappingsCompatibilityTests}, which parses each mappings version with the
 * oldest supported index version.
 */
public class InferenceIndex {

    private InferenceIndex() {}

    public static final String INDEX_NAME = ".inference";
    public static final String INDEX_PATTERN = INDEX_NAME + "*";
    public static final String INDEX_ALIAS = ".inference-alias";

    public static Settings settings() {
        return builder().build();
    }

    // Public to allow tests to create the index with custom settings
    public static Settings.Builder builder() {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1");
    }

    /**
     * Reject any unknown fields being added by setting dynamic mappings to
     * {@code strict} for the top level object. A document that contains unknown
     * fields in the document root will be rejected at index time.
     *
     * The {@code service_settings} and {@code task_settings} objects
     * have dynamic mappings set to {@code false} which means all fields will
     * be accepted without throwing an error but those fields are not indexed.
     *
     * The reason for mixing {@code strict} and {@code false} dynamic settings
     * is that {@code service_settings} and {@code task_settings} are defined by
     * the inference services and therefore are not known when creating the
     * index. However, the top level settings are known in advance and can
     * be strictly mapped.
     *
     * If the top level strict mapping changes then the no new documents should
     * be indexed until the index mappings have been updated, this happens
     * automatically once all nodes in the cluster are of a compatible version.
     *
     * @return The index mappings
     */
    public static String mappingsV4() {
        return """
            {
              "_doc" : {
                "_meta" : {
                  "managed_index_mappings_version": 4
                },
                "dynamic": "strict",
                "properties" : {
                  "doc_type": {
                    "type": "keyword"
                  },
                  "model_id": {
                    "type": "keyword"
                  },
                  "task_type": {
                    "type": "keyword"
                  },
                  "service": {
                    "type": "keyword"
                  },
                  "service_settings": {
                    "dynamic": false,
                    "properties": {
                    }
                  },
                  "task_settings": {
                    "dynamic": false,
                    "properties": {
                    }
                  },
                  "chunking_settings": {
                    "dynamic": false,
                    "properties": {
                      "strategy": {
                        "type": "keyword"
                      }
                    }
                  },
                  "metadata": {
                    "dynamic": false,
                    "properties": {
                      "heuristics": {
                        "dynamic": false,
                        "properties": {
                          "properties": {
                            "type": "keyword"
                          },
                          "status": {
                            "type": "keyword"
                          },
                          "release_date": {
                            "type": "date"
                          },
                          "end_of_life_date": {
                            "type": "date"
                          }
                        }
                      },
                      "display": {
                        "dynamic": false,
                        "properties": {
                          "name": {
                            "type": "keyword"
                          }
                        }
                      },
                      "internal": {
                        "dynamic": false,
                        "properties": {
                          "fingerprint": {
                            "type": "keyword"
                          },
                          "version": {
                            "type": "long"
                          }
                        }
                      },
                      "regions": {
                        "dynamic": false,
                        "properties": {
                          "csp": {
                            "type": "keyword"
                          },
                          "region": {
                            "type": "keyword"
                          },
                          "geo": {
                            "type": "keyword"
                          }
                        }
                      },
                      "denied_by_region_policy": {
                        "type": "boolean"
                      }
                    }
                  },
                  "region_policy": {
                    "dynamic": false,
                    "properties": {
                      "allowed_geos": {
                        "type": "keyword"
                      },
                      "allowed_regions": {
                        "properties": {
                          "csp": {
                            "type": "keyword"
                          },
                          "region": {
                            "type": "keyword"
                          }
                        }
                      }
                    }
                  },
                  "created_at": {
                    "type": "date"
                  },
                  "created_by": {
                    "type": "keyword"
                  },
                  "updated_at": {
                    "type": "date"
                  },
                  "updated_by": {
                    "type": "keyword"
                  }
                }
              }
            }
            """;
    }

    public static String mappingsV3() {
        return """
            {
              "_doc" : {
                "_meta" : {
                  "managed_index_mappings_version": 3
                },
                "dynamic": "strict",
                "properties" : {
                  "model_id": {
                    "type": "keyword"
                  },
                  "task_type": {
                    "type": "keyword"
                  },
                  "service": {
                    "type": "keyword"
                  },
                  "service_settings": {
                    "dynamic": false,
                    "properties": {
                    }
                  },
                  "task_settings": {
                    "dynamic": false,
                    "properties": {
                    }
                  },
                  "chunking_settings": {
                    "dynamic": false,
                    "properties": {
                      "strategy": {
                        "type": "keyword"
                      }
                    }
                  },
                  "metadata": {
                    "dynamic": false,
                    "properties": {
                      "heuristics": {
                        "dynamic": false,
                        "properties": {
                          "properties": {
                            "type": "keyword"
                          },
                          "status": {
                            "type": "keyword"
                          },
                          "release_date": {
                            "type": "date"
                          },
                          "end_of_life_date": {
                            "type": "date"
                          }
                        }
                      },
                      "display": {
                        "dynamic": false,
                        "properties": {
                          "name": {
                            "type": "keyword"
                          }
                        }
                      },
                      "internal": {
                        "dynamic": false,
                        "properties": {
                          "fingerprint": {
                            "type": "keyword"
                          },
                          "version": {
                            "type": "long"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """;
    }

    public static String mappingsV2() {
        return """
            {
              "_doc" : {
                "_meta" : {
                  "managed_index_mappings_version": 2
                },
                "dynamic": "strict",
                "properties" : {
                  "model_id": {
                    "type": "keyword"
                  },
                  "task_type": {
                    "type": "keyword"
                  },
                  "service": {
                    "type": "keyword"
                  },
                  "service_settings": {
                    "dynamic": false,
                    "properties": {
                    }
                  },
                  "task_settings": {
                    "dynamic": false,
                    "properties": {
                    }
                  },
                  "chunking_settings": {
                    "dynamic": false,
                    "properties": {
                      "strategy": {
                        "type": "keyword"
                      }
                    }
                  }
                }
              }
            }
            """;
    }

    public static String mappingsV1() {
        return """
            {
              "_doc" : {
                "_meta" : {
                  "managed_index_mappings_version": 1
                },
                "dynamic": "strict",
                "properties" : {
                  "model_id": {
                    "type": "keyword"
                  },
                  "task_type": {
                    "type": "keyword"
                  },
                  "service": {
                    "type": "keyword"
                  },
                  "service_settings": {
                    "dynamic": false,
                    "properties": {
                    }
                  },
                  "task_settings": {
                    "dynamic": false,
                    "properties": {
                    }
                  }
                }
              }
            }
            """;
    }
}
