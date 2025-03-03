/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
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
import org.elasticsearch.cluster.metadata.MetadataTests;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

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
        writeChunks(clusterState, builder, new ToXContent.MapParams(singletonMap(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API)));
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
                              "3": {
                                "description": "description",
                                "retryable": true,
                                "disable_state_persistence": true,
                                "levels": [
                                  "read",
                                  "write",
                                  "metadata_read",
                                  "metadata_write",
                                  "refresh"
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
                                    "metadata_write",
                                    "refresh"
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
                          "nodes_versions" : [
                            {
                              "node_id" : "nodeId1",
                              "transport_version" : "%s",
                              "mappings_versions" : {
                                ".tasks" : {
                                  "version" : 1,
                                  "hash" : 1
                                }
                              }
                            }
                          ],
                          "nodes_features" : [
                            {
                              "node_id" : "nodeId1",
                              "features" : [ "f1", "f2" ]
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
                                "mappings_updated_version" : %s,
                                "system": false,
                                "timestamp_range": {
                                  "shards": []
                                },
                                "event_ingested_range": {
                                  "unknown": true
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
                    IndexVersions.MINIMUM_COMPATIBLE,
                    IndexVersion.current(),
                    TransportVersion.current(),
                    IndexVersion.current(),
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
        Map<String, String> mapParams = Map.of(
            "flat_settings",
            "true",
            "reduce_mappings",
            "false",
            Metadata.CONTEXT_MODE_PARAM,
            Metadata.CONTEXT_MODE_API
        );

        final ClusterState clusterState = buildClusterState();
        IndexRoutingTable index = clusterState.getRoutingTable().getIndicesRouting().get("index");

        String ephemeralId = clusterState.getNodes().get("nodeId1").getEphemeralId();
        String allocationId = index.shard(0).getPromotableAllocationIds().iterator().next();

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        writeChunks(clusterState, builder, new ToXContent.MapParams(mapParams));
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
                          "3" : {
                            "description" : "description",
                            "retryable" : true,
                            "disable_state_persistence" : true,
                            "levels" : [
                              "read",
                              "write",
                              "metadata_read",
                              "metadata_write",
                              "refresh"
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
                                "metadata_write",
                                "refresh"
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
                      "nodes_versions" : [
                        {
                          "node_id" : "nodeId1",
                          "transport_version" : "%s",
                          "mappings_versions" : {
                            ".tasks" : {
                              "version" : 1,
                              "hash" : 1
                            }
                          }
                        }
                      ],
                      "nodes_features" : [
                        {
                          "node_id" : "nodeId1",
                          "features" : [
                            "f1",
                            "f2"
                          ]
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
                            "mappings_updated_version" : %s,
                            "system" : false,
                            "timestamp_range" : {
                              "shards" : [ ]
                            },
                            "event_ingested_range" : {
                              "unknown" : true
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
                IndexVersions.MINIMUM_COMPATIBLE,
                IndexVersion.current(),
                TransportVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current(),
                allocationId,
                allocationId
            ),
            Strings.toString(builder)
        );

    }

    public void testToXContent_FlatSettingFalse_ReduceMappingTrue() throws IOException {
        Map<String, String> mapParams = Map.of(
            "flat_settings",
            "false",
            "reduce_mappings",
            "true",
            Metadata.CONTEXT_MODE_PARAM,
            Metadata.CONTEXT_MODE_API
        );

        final ClusterState clusterState = buildClusterState();

        IndexRoutingTable index = clusterState.getRoutingTable().getIndicesRouting().get("index");

        String ephemeralId = clusterState.getNodes().get("nodeId1").getEphemeralId();
        String allocationId = index.shard(0).getPromotableAllocationIds().iterator().next();

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        writeChunks(clusterState, builder, new ToXContent.MapParams(mapParams));
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
                          "3" : {
                            "description" : "description",
                            "retryable" : true,
                            "disable_state_persistence" : true,
                            "levels" : [
                              "read",
                              "write",
                              "metadata_read",
                              "metadata_write",
                              "refresh"
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
                                "metadata_write",
                                "refresh"
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
                      "nodes_versions" : [
                        {
                          "node_id" : "nodeId1",
                          "transport_version" : "%s",
                          "mappings_versions" : {
                            ".tasks" : {
                              "version" : 1,
                              "hash" : 1
                            }
                          }
                        }
                      ],
                      "nodes_features" : [
                        {
                          "node_id" : "nodeId1",
                          "features" : [
                            "f1",
                            "f2"
                          ]
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
                            "mappings_updated_version" : %s,
                            "system" : false,
                            "timestamp_range" : {
                              "shards" : [ ]
                            },
                            "event_ingested_range" : {
                              "unknown" : true
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
                IndexVersions.MINIMUM_COMPATIBLE,
                IndexVersion.current(),
                TransportVersion.current(),
                IndexVersion.current(),
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
                            .settings(Settings.builder().put(SETTING_VERSION_CREATED, IndexVersion.current()))
                            .putMapping(
                                new MappingMetadata(
                                    "type",
                                    // the type name is the root value,
                                    // the original logic in ClusterState.toXContent will reduce
                                    Map.of("type", Map.of("key", "value"))
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
        writeChunks(clusterState, builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        assertEquals(Strings.format("""
            {
              "cluster_uuid" : "clusterUUID",
              "version" : 0,
              "state_uuid" : "stateUUID",
              "master_node" : null,
              "blocks" : { },
              "nodes" : { },
              "nodes_versions" : [ ],
              "nodes_features" : [ ],
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
                    "mappings_updated_version" : %s,
                    "system" : false,
                    "timestamp_range" : {
                      "shards" : [ ]
                    },
                    "event_ingested_range" : {
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
            }""", IndexVersion.current(), IndexVersion.current()), Strings.toString(builder));
    }

    public void testNodeFeaturesSorted() throws IOException {
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodeFeatures(Map.of("node2", Set.of("nf1", "f2", "nf2"), "node1", Set.of("f3", "f2", "f1"), "node3", Set.of()))
            .build();

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        writeChunks(clusterState, builder, new ToXContent.MapParams(Map.of("metric", ClusterState.Metric.NODES.toString())));
        builder.endObject();

        assertThat(Strings.toString(builder), equalTo("""
            {
              "cluster_uuid" : "_na_",
              "nodes" : { },
              "nodes_versions" : [ ],
              "nodes_features" : [
                {
                  "node_id" : "node1",
                  "features" : [
                    "f1",
                    "f2",
                    "f3"
                  ]
                },
                {
                  "node_id" : "node2",
                  "features" : [
                    "f2",
                    "nf1",
                    "nf2"
                  ]
                },
                {
                  "node_id" : "node3",
                  "features" : [ ]
                }
              ]
            }"""));
    }

    private ClusterState buildClusterState() throws IOException {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .state(IndexMetadata.State.OPEN)
            .settings(Settings.builder().put(SETTING_VERSION_CREATED, IndexVersion.current()))
            .putMapping(new MappingMetadata("type", Map.of("type1", Map.of("key", "value"))))
            .putAlias(AliasMetadata.builder("alias").indexRouting("indexRouting").build())
            .numberOfShards(1)
            .primaryTerm(0, 1L)
            .putInSyncAllocationIds(0, Set.of("allocationId"))
            .numberOfReplicas(2)
            .putRolloverInfo(new RolloverInfo("rolloveAlias", new ArrayList<>(), 1L))
            .stats(new IndexMetadataStats(IndexWriteLoad.builder(1).build(), 120, 1))
            .indexWriteLoadForecast(8.0)
            .eventIngestedRange(IndexLongFieldRange.UNKNOWN, TransportVersions.V_8_0_0)
            .build();

        return ClusterState.builder(ClusterName.DEFAULT)
            .stateUUID("stateUUID")
            .nodes(
                DiscoveryNodes.builder()
                    .masterNodeId("nodeId1")
                    .add(DiscoveryNodeUtils.create("nodeId1", new TransportAddress(InetAddress.getByName("127.0.0.1"), 111)))
                    .build()
            )
            .nodeIdsToCompatibilityVersions(
                Map.of(
                    "nodeId1",
                    new CompatibilityVersions(TransportVersion.current(), Map.of(".tasks", new SystemIndexDescriptor.MappingsVersion(1, 1)))
                )
            )
            .nodeFeatures(Map.of("nodeId1", Set.of("f1", "f2")))
            .blocks(
                ClusterBlocks.builder()
                    .addGlobalBlock(
                        new ClusterBlock(3, "description", true, true, true, RestStatus.ACCEPTED, EnumSet.allOf((ClusterBlockLevel.class)))
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
                            .lastCommittedConfiguration(new CoordinationMetadata.VotingConfiguration(Set.of("commitedConfigurationNodeId")))
                            .lastAcceptedConfiguration(new CoordinationMetadata.VotingConfiguration(Set.of("acceptedConfigurationNodeId")))
                            .addVotingConfigExclusion(new CoordinationMetadata.VotingConfigExclusion("exlucdedNodeId", "excludedNodeName"))
                            .build()
                    )
                    .persistentSettings(Settings.builder().put(SETTING_VERSION_CREATED, IndexVersion.current()).build())
                    .transientSettings(Settings.builder().put(SETTING_VERSION_CREATED, IndexVersion.current()).build())
                    .put(indexMetadata, false)
                    .put(
                        IndexTemplateMetadata.builder("template")
                            .patterns(List.of("pattern1", "pattern2"))
                            .order(0)
                            .settings(Settings.builder().put(SETTING_VERSION_CREATED, IndexVersion.current()))
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

    private static void writeChunks(ClusterState clusterState, XContentBuilder builder, ToXContent.Params params) throws IOException {
        final var iterator = clusterState.toXContentChunked(params);
        int chunks = 0;
        while (iterator.hasNext()) {
            iterator.next().toXContent(builder, params);
            chunks += 1;
        }
        assertEquals(expectedChunkCount(params, clusterState), chunks);
    }

    public void testGetMinTransportVersion() throws IOException {
        assertEquals(TransportVersions.MINIMUM_COMPATIBLE, ClusterState.EMPTY_STATE.getMinTransportVersion());

        var builder = ClusterState.builder(buildClusterState());
        int numNodes = randomIntBetween(2, 20);
        TransportVersion minVersion = TransportVersion.current();

        for (int i = 0; i < numNodes; i++) {
            TransportVersion tv = TransportVersionUtils.randomVersion();
            builder.putCompatibilityVersions("nodeTv" + i, tv, SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS);
            minVersion = Collections.min(List.of(minVersion, tv));
        }

        var newState = builder.build();
        assertThat(newState.getMinTransportVersion(), equalTo(minVersion));

        assertEquals(
            TransportVersions.MINIMUM_COMPATIBLE,
            ClusterState.builder(newState)
                .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
                .build()
                .getMinTransportVersion()
        );
    }

    public void testHasMixedSystemIndexVersions() throws IOException {
        // equal mappings versions
        {
            var builder = ClusterState.builder(buildClusterState());
            builder.nodeIdsToCompatibilityVersions(
                Map.of(
                    "node1",
                    new CompatibilityVersions(
                        TransportVersion.current(),
                        Map.of(".system-index", new SystemIndexDescriptor.MappingsVersion(1, 0))
                    ),
                    "node2",
                    new CompatibilityVersions(
                        TransportVersion.current(),
                        Map.of(".system-index", new SystemIndexDescriptor.MappingsVersion(1, 0))
                    )
                )
            );
            assertFalse(builder.build().hasMixedSystemIndexVersions());
        }

        // unequal mappings versions
        {
            var builder = ClusterState.builder(buildClusterState());
            builder.nodeIdsToCompatibilityVersions(
                Map.of(
                    "node1",
                    new CompatibilityVersions(
                        TransportVersion.current(),
                        Map.of(".system-index", new SystemIndexDescriptor.MappingsVersion(1, 0))
                    ),
                    "node2",
                    new CompatibilityVersions(
                        TransportVersion.current(),
                        Map.of(".system-index", new SystemIndexDescriptor.MappingsVersion(2, 0))
                    )
                )
            );
            assertTrue(builder.build().hasMixedSystemIndexVersions());
        }

        // one node has a mappings version that the other is missing
        {
            var builder = ClusterState.builder(buildClusterState());
            builder.nodeIdsToCompatibilityVersions(
                Map.of(
                    "node1",
                    new CompatibilityVersions(
                        TransportVersion.current(),
                        Map.of(
                            ".system-index",
                            new SystemIndexDescriptor.MappingsVersion(1, 0),
                            ".another-system-index",
                            new SystemIndexDescriptor.MappingsVersion(1, 0)
                        )
                    ),
                    "node2",
                    new CompatibilityVersions(
                        TransportVersion.current(),
                        Map.of(".system-index", new SystemIndexDescriptor.MappingsVersion(1, 0))
                    )
                )
            );
            assertTrue(builder.build().hasMixedSystemIndexVersions());
        }
    }

    public static int expectedChunkCount(ToXContent.Params params, ClusterState clusterState) {
        final var metrics = ClusterState.Metric.parseString(params.param("metric", "_all"), true);

        long chunkCount = 0;

        // header chunk
        chunkCount += 1;

        // blocks
        if (metrics.contains(ClusterState.Metric.BLOCKS)) {
            chunkCount += 2 + clusterState.blocks().indices().size();
        }

        // nodes, nodes_versions, nodes_features
        if (metrics.contains(ClusterState.Metric.NODES)) {
            chunkCount += 7 + clusterState.nodes().size() + clusterState.compatibilityVersions().size() + clusterState.clusterFeatures()
                .nodeFeatures()
                .size();
        }

        // metadata
        if (metrics.contains(ClusterState.Metric.METADATA)) {
            chunkCount += MetadataTests.expectedChunkCount(params, clusterState.metadata());
        }

        // routing table
        if (metrics.contains(ClusterState.Metric.ROUTING_TABLE)) {
            chunkCount += 2;
            for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
                chunkCount += 2;
                for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                    chunkCount += 2 + indexRoutingTable.shard(shardId).size();
                }
            }
        }

        // routing nodes
        if (metrics.contains(ClusterState.Metric.ROUTING_NODES)) {
            final var routingNodes = clusterState.getRoutingNodes();
            chunkCount += 4 + routingNodes.unassigned().size();
            for (RoutingNode routingNode : routingNodes) {
                chunkCount += 2 + routingNode.size();
            }
        }

        // customs
        if (metrics.contains(ClusterState.Metric.CUSTOMS)) {
            for (ClusterState.Custom custom : clusterState.customs().values()) {
                chunkCount += 2;

                if (custom instanceof HealthMetadata) {
                    chunkCount += 1;
                } else if (custom instanceof RepositoryCleanupInProgress repositoryCleanupInProgress) {
                    chunkCount += 2 + repositoryCleanupInProgress.entries().size();
                } else if (custom instanceof RestoreInProgress restoreInProgress) {
                    chunkCount += 2 + Iterables.size(restoreInProgress);
                } else if (custom instanceof SnapshotDeletionsInProgress snapshotDeletionsInProgress) {
                    chunkCount += 2 + snapshotDeletionsInProgress.getEntries().size();
                } else if (custom instanceof SnapshotsInProgress snapshotsInProgress) {
                    chunkCount += 2 + snapshotsInProgress.asStream().count();
                } else {
                    // could be anything, we have to just try it
                    chunkCount += Iterables.size(
                        (Iterable<ToXContent>) (() -> Iterators.map(custom.toXContentChunked(params), Function.identity()))
                    );
                }
            }
        }

        return Math.toIntExact(chunkCount);
    }
}
