/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.indices.SystemIndexDescriptor;

import java.util.Map;

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
     * Returns true when the .inference index already has v4 mappings that include the {@code doc_type} field,
     * or when the index does not yet exist and it is safe to assume it will be created with v4 mappings.
     * <p>
     * When the index does not yet exist, returns true only if all nodes in the cluster carry the
     * {@link InferenceFeatures#INFERENCE_INFERENCE_INDEX_DOC_TYPE} feature, which guarantees that whichever
     * node creates the index will apply the v4 mappings. Returns false if any node is missing the feature.
     * <p>
     * When the index exists, returns false if it still carries v3 or earlier mappings (before the mapping
     * migration completes during a rolling upgrade).
     * <p>
     * Callers are responsible for also checking the region-policy feature flag before acting on this result.
     */
    public static boolean inferenceIndexHasV4Mappings(ClusterState clusterState, FeatureService featureService) {
        var projectMetadata = clusterState.metadata().getProject();
        IndexMetadata indexMetadata = projectMetadata.index(InferenceIndex.INDEX_NAME);
        if (indexMetadata == null) {
            // The primary index name may have become an alias after a system index migration
            // (e.g. ".inference" → ".inference-reindexed-for-10"). ProjectMetadata.index() only
            // resolves concrete names, so we must fall back to the indices lookup, mirroring the
            // pattern used by SystemIndexMappingUpdateService.getSystemIndexMetadata().
            IndexAbstraction indexAbstraction = projectMetadata.getIndicesLookup().get(InferenceIndex.INDEX_NAME);
            if (indexAbstraction != null && indexAbstraction.getWriteIndex() != null) {
                indexMetadata = projectMetadata.getIndexSafe(indexAbstraction.getWriteIndex());
            }
        }
        if (indexMetadata == null) {
            // The index doesn't exist yet. Return true only when all nodes carry the doc_type feature,
            // which guarantees that whoever creates the index will apply v4 mappings. An old node missing
            // the feature would create the index with v3 mappings, causing a strict_dynamic_mapping_exception
            // if doc_type were written.
            return featureService.clusterHasFeature(clusterState, InferenceFeatures.INFERENCE_INFERENCE_INDEX_DOC_TYPE);
        }
        MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata == null) {
            return false;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> meta = (Map<String, Object>) mappingMetadata.sourceAsMap().get("_meta");
        if (meta == null) {
            return false;
        }
        if (meta.containsKey(SystemIndexDescriptor.VERSION_META_KEY) == false) {
            return false;
        }
        if (meta.get(SystemIndexDescriptor.VERSION_META_KEY) instanceof Integer version) {
            return version >= 4;
        }
        return false;
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
