/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataStats;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.IndexWriteLoad;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class ClusterStateTests extends ESTestCase {

    public void testSupersedes() {
        final DiscoveryNode node1 = DiscoveryNodeUtils.builder("node1").roles(emptySet()).build();
        final DiscoveryNode node2 = DiscoveryNodeUtils.builder("node2").roles(emptySet()).build();
        final DiscoveryNodes nodes = DiscoveryNodes.builder().add(node1).add(node2).build();
        ClusterName name = ClusterName.DEFAULT;
        ClusterState noMaster1 = ClusterState.builder(name).version(randomInt(5)).nodes(nodes).build();
        ClusterState noMaster2 = ClusterState.builder(name).version(randomInt(5)).nodes(nodes).build();
        ClusterState withMaster1a = ClusterState.builder(name).version(randomInt(5)).nodes(nodes.withMasterNodeId(node1.getId())).build();
        ClusterState withMaster1b = ClusterState.builder(name).version(randomInt(5)).nodes(nodes.withMasterNodeId(node1.getId())).build();
        ClusterState withMaster2 = ClusterState.builder(name).version(randomInt(5)).nodes(nodes.withMasterNodeId(node2.getId())).build();

        // states with no master should never supersede anything
        assertFalse(noMaster1.supersedes(noMaster2));
        assertFalse(noMaster1.supersedes(withMaster1a));

        // states should never supersede states from another master
        assertFalse(withMaster1a.supersedes(withMaster2));
        assertFalse(withMaster1a.supersedes(noMaster1));

        // state from the same master compare by version
        assertThat(withMaster1a.supersedes(withMaster1b), equalTo(withMaster1a.version() > withMaster1b.version()));
    }

    public void testBuilderRejectsNullCustom() {
        final ClusterState.Builder builder = ClusterState.builder(ClusterName.DEFAULT);
        final String key = randomAlphaOfLength(10);
        assertThat(expectThrows(NullPointerException.class, () -> builder.putCustom(key, null)).getMessage(), containsString(key));
    }

    public void testBuilderRejectsNullInCustoms() {
        final ClusterState.Builder builder = ClusterState.builder(ClusterName.DEFAULT);
        final String key = randomAlphaOfLength(10);
        final Map<String, ClusterState.Custom> customs = new HashMap<>();
        customs.put(key, null);
        assertThat(expectThrows(NullPointerException.class, () -> builder.customs(customs)).getMessage(), containsString(key));
    }

    public void testCopyAndUpdate() throws IOException {
        var state = buildClusterState();
        var newStateUuid = UUIDs.base64UUID();

        var copy = state.copyAndUpdate(builder -> builder.stateUUID(newStateUuid));

        assertThat(copy, not(sameInstance(state)));
        assertThat(copy.stateUUID(), equalTo(newStateUuid));
    }

    public void testCopyAndUpdateMetadata() throws IOException {
        var state = buildClusterState();
        var newClusterUuid = UUIDs.base64UUID();

        var copy = state.copyAndUpdateMetadata(metadata -> metadata.clusterUUID(newClusterUuid));

        assertThat(copy, not(sameInstance(state)));
        assertThat(copy.metadata().clusterUUID(), equalTo(newClusterUuid));
    }

    public void testToXContent() throws IOException {
        final ClusterState clusterState = buildClusterState();

        IndexRoutingTable index = clusterState.getRoutingTable().getIndicesRouting().get("index");

        String ephemeralId = clusterState.getNodes().get("nodeId1").getEphemeralId();
        String allocationId = index.shard(0).getPromotableAllocationIds().iterator().next();

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        writeChunks(
            clusterState,
            builder,
            new ToXContent.MapParams(singletonMap(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API)),
            37
        );
        builder.endObject();

        assertEquals(
            XContentHelper.stripWhitespace(
                Strings.format(
                    """
                        {
                          "cluster_uuid": "clusterUUID",
                          "version": 0,
                          "state_uuid": "stateUUID",
                          "master_node": "nodeId1",
                          "blocks": {
                            "global": {
                              "1": {
                                "description": "description",
                                "retryable": true,
                                "disable_state_persistence": true,
                                "levels": [
                                  "read",
                                  "write",
                                  "metadata_read",
                                  "metadata_write"
                                ]
                              }
                            },
                            "indices": {
                              "index": {
                                "2": {
                                  "description": "description2",
                                  "retryable": false,
                                  "levels": [
                                    "read",
                                    "write",
                                    "metadata_read",
                                    "metadata_write"
                                  ]
                                }
                              }
                            }
                          },
                          "nodes": {
                            "nodeId1": {
                              "name": "",
                              "ephemeral_id": "%s",
                              "transport_address": "127.0.0.1:111",
                              "external_id": "",
                              "attributes": {},
                              "roles": [
                                "data",
                                "data_cold",
                                "data_content",
                                "data_frozen",
                                "data_hot",
                                "data_warm",
                                "index",
                                "ingest",
                                "master",
                                "ml",
                                "remote_cluster_client",
                                "search",
                                "transform",
                                "voting_only"
                              ],
                              "version": "%s",
                              "min_index_version":%s,
                              "max_index_version":%s
                            }
                          },
                          "transport_versions" : [
                            {
                              "node_id" : "nodeId1",
                              "transport_version" : "%s"
                            }
                          ],
                          "metadata": {
                            "cluster_uuid": "clusterUUID",
                            "cluster_uuid_committed": false,
                            "cluster_coordination": {
                              "term": 1,
                              "last_committed_config": [
                                "commitedConfigurationNodeId"
                              ],
                              "last_accepted_config": [
                                "acceptedConfigurationNodeId"
                              ],
                              "voting_config_exclusions": [
                                {
                                  "node_id": "exlucdedNodeId",
                                  "node_name": "excludedNodeName"
                                }
                              ]
                            },
                            "templates": {
                              "template": {
                                "order": 0,
                                "index_patterns": [
                                  "pattern1",
                                  "pattern2"
                                ],
                                "settings": {
                                  "index": {
                                    "version": {
                                      "created": "%s"
                                    }
                                  }
                                },
                                "mappings": {
                                  "key1": {}
                                },
                                "aliases": {}
                              }
                            },
                            "indices": {
                              "index": {
                                "version": 1,
                                "mapping_version": 1,
                                "settings_version": 1,
                                "aliases_version": 1,
                                "routing_num_shards": 1,
                                "state": "open",
                                "settings": {
                                  "index": {
                                    "number_of_shards": "1",
                                    "number_of_replicas": "2",
                                    "version": {
                                      "created": "%s"
                                    }
                                  }
                                },
                                "mappings": {
                                  "type": {
                                    "type1": {
                                      "key": "value"
                                    }
                                  }
                                },
                                "aliases": [
                                  "alias"
                                ],
                                "primary_terms": {
                                  "0": 1
                                },
                                "in_sync_allocations": {
                                  "0": [
                                    "allocationId"
                                  ]
                                },
                                "rollover_info": {
                                  "rolloveAlias": {
                                    "met_conditions": {},
                                    "time": 1
                                  }
                                },
                                "system": false,
                                "timestamp_range": {
                                  "shards": []
                                },
                                "stats": {
                                    "write_load": {
                                      "loads": [-1.0],
                                      "uptimes": [-1]
                                    },
                                    "avg_size": {
                                        "total_size_in_bytes": 120,
                                        "shard_count": 1
                                    }
                                },
                                "write_load_forecast" : 8.0
                              }
                            },
                            "index-graveyard": {
                              "tombstones": []
                            },
                            "reserved_state" : { }
                          },
                          "routing_table": {
                            "indices": {
                              "index": {
                                "shards": {
                                  "0": [
                                    {
                                      "state": "STARTED",
                                      "primary": true,
                                      "node": "nodeId2",
                                      "relocating_node": null,
                                      "shard": 0,
                                      "index": "index",
                                      "allocation_id": {
                                        "id": "%s"
                                      },
                                      "relocation_failure_info" : {
                                        "failed_attempts" : 0
                                      }
                                    }
                                  ]
                                }
                              }
                            }
                          },
                          "routing_nodes": {
                            "unassigned": [],
                            "nodes": {
                              "nodeId2": [
                                {
                                  "state": "STARTED",
                                  "primary": true,
                                  "node": "nodeId2",
                                  "relocating_node": null,
                                  "shard": 0,
                                  "index": "index",
                                  "allocation_id": {
                                    "id": "%s"
                                  },
                                  "relocation_failure_info" : {
                                    "failed_attempts" : 0
                                  }
                                }
                              ],
                              "nodeId1": []
                            }
                          }
                        }""",
                    ephemeralId,
                    Version.CURRENT,
                    IndexVersion.MINIMUM_COMPATIBLE,
                    IndexVersion.current(),
                    TransportVersion.current(),
                    IndexVersion.current(),
                    IndexVersion.current(),
                    allocationId,
                    allocationId
                )
            ),
            Strings.toString(builder)
        );

    }

    public void testToXContent_FlatSettingTrue_ReduceMappingFalse() throws IOException {
        Map<String, String> mapParams = new HashMap<>() {
            {
                put("flat_settings", "true");
                put("reduce_mappings", "false");
                put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API);
            }
        };

        final ClusterState clusterState = buildClusterState();
        IndexRoutingTable index = clusterState.getRoutingTable().getIndicesRouting().get("index");

        String ephemeralId = clusterState.getNodes().get("nodeId1").getEphemeralId();
        String allocationId = index.shard(0).getPromotableAllocationIds().iterator().next();

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        writeChunks(clusterState, builder, new ToXContent.MapParams(mapParams), 37);
        builder.endObject();

        assertEquals(
            Strings.format(
                """
                    {
                      "cluster_uuid" : "clusterUUID",
                      "version" : 0,
                      "state_uuid" : "stateUUID",
                      "master_node" : "nodeId1",
                      "blocks" : {
                        "global" : {
                          "1" : {
                            "description" : "description",
                            "retryable" : true,
                            "disable_state_persistence" : true,
                            "levels" : [
                              "read",
                              "write",
                              "metadata_read",
                              "metadata_write"
                            ]
                          }
                        },
                        "indices" : {
                          "index" : {
                            "2" : {
                              "description" : "description2",
                              "retryable" : false,
                              "levels" : [
                                "read",
                                "write",
                                "metadata_read",
                                "metadata_write"
                              ]
                            }
                          }
                        }
                      },
                      "nodes" : {
                        "nodeId1" : {
                          "name" : "",
                          "ephemeral_id" : "%s",
                          "transport_address" : "127.0.0.1:111",
                          "external_id" : "",
                          "attributes" : { },
                          "roles" : [
                            "data",
                            "data_cold",
                            "data_content",
                            "data_frozen",
                            "data_hot",
                            "data_warm",
                            "index",
                            "ingest",
                            "master",
                            "ml",
                            "remote_cluster_client",
                            "search",
                            "transform",
                            "voting_only"
                          ],
                          "version" : "%s",
                          "min_index_version" : %s,
                          "max_index_version" : %s
                        }
                      },
                      "transport_versions" : [
                        {
                          "node_id" : "nodeId1",
                          "transport_version" : "%s"
                        }
                      ],
                      "metadata" : {
                        "cluster_uuid" : "clusterUUID",
                        "cluster_uuid_committed" : false,
                        "cluster_coordination" : {
                          "term" : 1,
                          "last_committed_config" : [
                            "commitedConfigurationNodeId"
                          ],
                          "last_accepted_config" : [
                            "acceptedConfigurationNodeId"
                          ],
                          "voting_config_exclusions" : [
                            {
                              "node_id" : "exlucdedNodeId",
                              "node_name" : "excludedNodeName"
                            }
                          ]
                        },
                        "templates" : {
                          "template" : {
                            "order" : 0,
                            "index_patterns" : [
                              "pattern1",
                              "pattern2"
                            ],
                            "settings" : {
                              "index.version.created" : "%s"
                            },
                            "mappings" : {
                              "key1" : { }
                            },
                            "aliases" : { }
                          }
                        },
                        "indices" : {
                          "index" : {
                            "version" : 1,
                            "mapping_version" : 1,
                            "settings_version" : 1,
                            "aliases_version" : 1,
                            "routing_num_shards" : 1,
                            "state" : "open",
                            "settings" : {
                              "index.number_of_replicas" : "2",
                              "index.number_of_shards" : "1",
                              "index.version.created" : "%s"
                            },
                            "mappings" : {
                              "type" : {
                                "type1" : {
                                  "key" : "value"
                                }
                              }
                            },
                            "aliases" : [
                              "alias"
                            ],
                            "primary_terms" : {
                              "0" : 1
                            },
                            "in_sync_allocations" : {
                              "0" : [
                                "allocationId"
                              ]
                            },
                            "rollover_info" : {
                              "rolloveAlias" : {
                                "met_conditions" : { },
                                "time" : 1
                              }
                            },
                            "system" : false,
                            "timestamp_range" : {
                              "shards" : [ ]
                            },
                            "stats" : {
                              "write_load" : {
                                "loads" : [
                                  -1.0
                                ],
                                "uptimes" : [
                                  -1
                                ]
                              },
                              "avg_size" : {
                                "total_size_in_bytes" : 120,
                                "shard_count" : 1
                              }
                            },
                            "write_load_forecast" : 8.0
                          }
                        },
                        "index-graveyard" : {
                          "tombstones" : [ ]
                        },
                        "reserved_state" : { }
                      },
                      "routing_table" : {
                        "indices" : {
                          "index" : {
                            "shards" : {
                              "0" : [
                                {
                                  "state" : "STARTED",
                                  "primary" : true,
                                  "node" : "nodeId2",
                                  "relocating_node" : null,
                                  "shard" : 0,
                                  "index" : "index",
                                  "allocation_id" : {
                                    "id" : "%s"
                                  },
                                  "relocation_failure_info" : {
                                    "failed_attempts" : 0
                                  }
                                }
                              ]
                            }
                          }
                        }
                      },
                      "routing_nodes" : {
                        "unassigned" : [ ],
                        "nodes" : {
                          "nodeId2" : [
                            {
                              "state" : "STARTED",
                              "primary" : true,
                              "node" : "nodeId2",
                              "relocating_node" : null,
                              "shard" : 0,
                              "index" : "index",
                              "allocation_id" : {
                                "id" : "%s"
                              },
                              "relocation_failure_info" : {
                                "failed_attempts" : 0
                              }
                            }
                          ],
                          "nodeId1" : [ ]
                        }
                      }
                    }""",
                ephemeralId,
                Version.CURRENT,
                IndexVersion.MINIMUM_COMPATIBLE,
                IndexVersion.current(),
                TransportVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                allocationId,
                allocationId
            ),
            Strings.toString(builder)
        );

    }

    public void testToXContent_FlatSettingFalse_ReduceMappingTrue() throws IOException {
        Map<String, String> mapParams = new HashMap<>() {
            {
                put("flat_settings", "false");
                put("reduce_mappings", "true");
                put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API);
            }
        };

        final ClusterState clusterState = buildClusterState();

        IndexRoutingTable index = clusterState.getRoutingTable().getIndicesRouting().get("index");

        String ephemeralId = clusterState.getNodes().get("nodeId1").getEphemeralId();
        String allocationId = index.shard(0).getPromotableAllocationIds().iterator().next();

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        writeChunks(clusterState, builder, new ToXContent.MapParams(mapParams), 37);
        builder.endObject();

        assertEquals(
            Strings.format(
                """
                    {
                      "cluster_uuid" : "clusterUUID",
                      "version" : 0,
                      "state_uuid" : "stateUUID",
                      "master_node" : "nodeId1",
                      "blocks" : {
                        "global" : {
                          "1" : {
                            "description" : "description",
                            "retryable" : true,
                            "disable_state_persistence" : true,
                            "levels" : [
                              "read",
                              "write",
                              "metadata_read",
                              "metadata_write"
                            ]
                          }
                        },
                        "indices" : {
                          "index" : {
                            "2" : {
                              "description" : "description2",
                              "retryable" : false,
                              "levels" : [
                                "read",
                                "write",
                                "metadata_read",
                                "metadata_write"
                              ]
                            }
                          }
                        }
                      },
                      "nodes" : {
                        "nodeId1" : {
                          "name" : "",
                          "ephemeral_id" : "%s",
                          "transport_address" : "127.0.0.1:111",
                          "external_id" : "",
                          "attributes" : { },
                          "roles" : [
                            "data",
                            "data_cold",
                            "data_content",
                            "data_frozen",
                            "data_hot",
                            "data_warm",
                            "index",
                            "ingest",
                            "master",
                            "ml",
                            "remote_cluster_client",
                            "search",
                            "transform",
                            "voting_only"
                          ],
                          "version" : "%s",
                          "min_index_version" : %s,
                          "max_index_version" : %s
                        }
                      },
                      "transport_versions" : [
                        {
                          "node_id" : "nodeId1",
                          "transport_version" : "%s"
                        }
                      ],
                      "metadata" : {
                        "cluster_uuid" : "clusterUUID",
                        "cluster_uuid_committed" : false,
                        "cluster_coordination" : {
                          "term" : 1,
                          "last_committed_config" : [
                            "commitedConfigurationNodeId"
                          ],
                          "last_accepted_config" : [
                            "acceptedConfigurationNodeId"
                          ],
                          "voting_config_exclusions" : [
                            {
                              "node_id" : "exlucdedNodeId",
                              "node_name" : "excludedNodeName"
                            }
                          ]
                        },
                        "templates" : {
                          "template" : {
                            "order" : 0,
                            "index_patterns" : [
                              "pattern1",
                              "pattern2"
                            ],
                            "settings" : {
                              "index" : {
                                "version" : {
                                  "created" : "%s"
                                }
                              }
                            },
                            "mappings" : { },
                            "aliases" : { }
                          }
                        },
                        "indices" : {
                          "index" : {
                            "version" : 1,
                            "mapping_version" : 1,
                            "settings_version" : 1,
                            "aliases_version" : 1,
                            "routing_num_shards" : 1,
                            "state" : "open",
                            "settings" : {
                              "index" : {
                                "number_of_shards" : "1",
                                "number_of_replicas" : "2",
                                "version" : {
                                  "created" : "%s"
                                }
                              }
                            },
                            "mappings" : {
                              "type" : {
                                "type1" : {
                                  "key" : "value"
                                }
                              }
                            },
                            "aliases" : [
                              "alias"
                            ],
                            "primary_terms" : {
                              "0" : 1
                            },
                            "in_sync_allocations" : {
                              "0" : [
                                "allocationId"
                              ]
                            },
                            "rollover_info" : {
                              "rolloveAlias" : {
                                "met_conditions" : { },
                                "time" : 1
                              }
                            },
                            "system" : false,
                            "timestamp_range" : {
                              "shards" : [ ]
                            },
                            "stats" : {
                              "write_load" : {
                                "loads" : [
                                  -1.0
                                ],
                                "uptimes" : [
                                  -1
                                ]
                              },
                              "avg_size" : {
                                "total_size_in_bytes" : 120,
                                "shard_count" : 1
                              }
                            },
                            "write_load_forecast" : 8.0
                          }
                        },
                        "index-graveyard" : {
                          "tombstones" : [ ]
                        },
                        "reserved_state" : { }
                      },
                      "routing_table" : {
                        "indices" : {
                          "index" : {
                            "shards" : {
                              "0" : [
                                {
                                  "state" : "STARTED",
                                  "primary" : true,
                                  "node" : "nodeId2",
                                  "relocating_node" : null,
                                  "shard" : 0,
                                  "index" : "index",
                                  "allocation_id" : {
                                    "id" : "%s"
                                  },
                                  "relocation_failure_info" : {
                                    "failed_attempts" : 0
                                  }
                                }
                              ]
                            }
                          }
                        }
                      },
                      "routing_nodes" : {
                        "unassigned" : [ ],
                        "nodes" : {
                          "nodeId2" : [
                            {
                              "state" : "STARTED",
                              "primary" : true,
                              "node" : "nodeId2",
                              "relocating_node" : null,
                              "shard" : 0,
                              "index" : "index",
                              "allocation_id" : {
                                "id" : "%s"
                              },
                              "relocation_failure_info" : {
                                "failed_attempts" : 0
                              }
                            }
                          ],
                          "nodeId1" : [ ]
                        }
                      }
                    }""",
                ephemeralId,
                Version.CURRENT,
                IndexVersion.MINIMUM_COMPATIBLE,
                IndexVersion.current(),
                TransportVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                allocationId,
                allocationId
            ),
            Strings.toString(builder)
        );

    }

    public void testToXContentSameTypeName() throws IOException {
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .stateUUID("stateUUID")
            .metadata(
                Metadata.builder()
                    .clusterUUID("clusterUUID")
                    .coordinationMetadata(CoordinationMetadata.builder().build())
                    .put(
                        IndexMetadata.builder("index")
                            .state(IndexMetadata.State.OPEN)
                            .settings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id))
                            .putMapping(
                                new MappingMetadata(
                                    "type",
                                    // the type name is the root value,
                                    // the original logic in ClusterState.toXContent will reduce
                                    new HashMap<>() {
                                        {
                                            put("type", new HashMap<String, Object>() {
                                                {
                                                    put("key", "value");
                                                }
                                            });
                                        }
                                    }
                                )
                            )
                            .numberOfShards(1)
                            .primaryTerm(0, 1L)
                            .numberOfReplicas(2)
                    )
            )
            .build();

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        writeChunks(clusterState, builder, ToXContent.EMPTY_PARAMS, 27);
        builder.endObject();

        assertEquals(Strings.format("""
            {
              "cluster_uuid" : "clusterUUID",
              "version" : 0,
              "state_uuid" : "stateUUID",
              "master_node" : null,
              "blocks" : { },
              "nodes" : { },
              "transport_versions" : [ ],
              "metadata" : {
                "cluster_uuid" : "clusterUUID",
                "cluster_uuid_committed" : false,
                "cluster_coordination" : {
                  "term" : 0,
                  "last_committed_config" : [ ],
                  "last_accepted_config" : [ ],
                  "voting_config_exclusions" : [ ]
                },
                "templates" : { },
                "indices" : {
                  "index" : {
                    "version" : 2,
                    "mapping_version" : 1,
                    "settings_version" : 1,
                    "aliases_version" : 1,
                    "routing_num_shards" : 1,
                    "state" : "open",
                    "settings" : {
                      "index" : {
                        "number_of_shards" : "1",
                        "number_of_replicas" : "2",
                        "version" : {
                          "created" : "%s"
                        }
                      }
                    },
                    "mappings" : {
                      "type" : {
                        "key" : "value"
                      }
                    },
                    "aliases" : [ ],
                    "primary_terms" : {
                      "0" : 1
                    },
                    "in_sync_allocations" : {
                      "0" : [ ]
                    },
                    "rollover_info" : { },
                    "system" : false,
                    "timestamp_range" : {
                      "shards" : [ ]
                    }
                  }
                },
                "index-graveyard" : {
                  "tombstones" : [ ]
                },
                "reserved_state" : { }
              },
              "routing_table" : {
                "indices" : { }
              },
              "routing_nodes" : {
                "unassigned" : [ ],
                "nodes" : { }
              }
            }""", Version.CURRENT.id), Strings.toString(builder));
    }

    private ClusterState buildClusterState() throws IOException {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .state(IndexMetadata.State.OPEN)
            .settings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id))
            .putMapping(new MappingMetadata("type", new HashMap<>() {
                {
                    put("type1", new HashMap<String, Object>() {
                        {
                            put("key", "value");
                        }
                    });
                }
            }))
            .putAlias(AliasMetadata.builder("alias").indexRouting("indexRouting").build())
            .numberOfShards(1)
            .primaryTerm(0, 1L)
            .putInSyncAllocationIds(0, new HashSet<>() {
                {
                    add("allocationId");
                }
            })
            .numberOfReplicas(2)
            .putRolloverInfo(new RolloverInfo("rolloveAlias", new ArrayList<>(), 1L))
            .stats(new IndexMetadataStats(IndexWriteLoad.builder(1).build(), 120, 1))
            .indexWriteLoadForecast(8.0)
            .build();

        return ClusterState.builder(ClusterName.DEFAULT)
            .stateUUID("stateUUID")
            .nodes(
                DiscoveryNodes.builder()
                    .masterNodeId("nodeId1")
                    .add(DiscoveryNodeUtils.create("nodeId1", new TransportAddress(InetAddress.getByName("127.0.0.1"), 111)))
                    .build()
            )
            .putTransportVersion("nodeId1", TransportVersion.current())
            .blocks(
                ClusterBlocks.builder()
                    .addGlobalBlock(
                        new ClusterBlock(1, "description", true, true, true, RestStatus.ACCEPTED, EnumSet.allOf((ClusterBlockLevel.class)))
                    )
                    .addBlocks(indexMetadata)
                    .addIndexBlock(
                        "index",
                        new ClusterBlock(
                            2,
                            "description2",
                            false,
                            false,
                            false,
                            RestStatus.ACCEPTED,
                            EnumSet.allOf((ClusterBlockLevel.class))
                        )
                    )
                    .build()
            )
            .metadata(
                Metadata.builder()
                    .clusterUUID("clusterUUID")
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .term(1)
                            .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(new HashSet<>() {
                                {
                                    add("commitedConfigurationNodeId");
                                }
                            }))
                            .lastAcceptedConfiguration(new CoordinationMetadata.VotingConfiguration(new HashSet<>() {
                                {
                                    add("acceptedConfigurationNodeId");
                                }
                            }))
                            .addVotingConfigExclusion(new CoordinationMetadata.VotingConfigExclusion("exlucdedNodeId", "excludedNodeName"))
                            .build()
                    )
                    .persistentSettings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id).build())
                    .transientSettings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id).build())
                    .put(indexMetadata, false)
                    .put(
                        IndexTemplateMetadata.builder("template")
                            .patterns(List.of("pattern1", "pattern2"))
                            .order(0)
                            .settings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id))
                            .putMapping("type", "{ \"key1\": {} }")
                            .build()
                    )
            )
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(new Index("index", "indexUUID"))
                            .addIndexShard(
                                new IndexShardRoutingTable.Builder(new ShardId("index", "indexUUID", 0)).addShard(
                                    TestShardRouting.newShardRouting(
                                        new ShardId("index", "indexUUID", 0),
                                        "nodeId2",
                                        true,
                                        ShardRoutingState.STARTED
                                    )
                                )
                            )
                            .build()
                    )
                    .build()
            )
            .build();
    }

    public void testNodesIfRecovered() throws IOException {
        final var initialState = buildClusterState();

        final var recoveredState = ClusterState.builder(initialState).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();
        assertEquals(1, recoveredState.nodes().size());
        assertEquals(recoveredState.nodes(), recoveredState.nodesIfRecovered());

        final var notRecoveredState = ClusterState.builder(initialState)
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
            .build();
        assertEquals(DiscoveryNodes.EMPTY_NODES, notRecoveredState.nodesIfRecovered());
    }

    private static void writeChunks(ClusterState clusterState, XContentBuilder builder, ToXContent.Params params, int expectedChunks)
        throws IOException {
        final var iterator = clusterState.toXContentChunked(params);
        int chunks = 0;
        while (iterator.hasNext()) {
            iterator.next().toXContent(builder, params);
            chunks += 1;
        }
        assertEquals(expectedChunks, chunks);
    }

    public void testGetMinTransportVersion() throws IOException {
        var builder = ClusterState.builder(buildClusterState());
        int numNodes = randomIntBetween(2, 20);
        TransportVersion minVersion = TransportVersion.current();

        for (int i = 0; i < numNodes; i++) {
            TransportVersion tv = TransportVersionUtils.randomVersion();
            builder.putTransportVersion("nodeTv" + i, tv);
            minVersion = Collections.min(List.of(minVersion, tv));
        }

        var newState = builder.build();
        assertThat(newState.getMinTransportVersion(), equalTo(minVersion));
    }
}
