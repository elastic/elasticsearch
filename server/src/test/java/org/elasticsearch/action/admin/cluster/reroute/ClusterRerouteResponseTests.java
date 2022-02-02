/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.RerouteExplanation;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ClusterRerouteResponseTests extends ESTestCase {

    public void testToXContent() throws IOException {
        DiscoveryNode node0 = new DiscoveryNode("node0", new TransportAddress(TransportAddress.META_ADDRESS, 9000), Version.CURRENT);
        DiscoveryNodes nodes = new DiscoveryNodes.Builder().add(node0).masterNodeId(node0.getId()).build();
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(
                Settings.builder()
                    .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), true)
                    .put(IndexSettings.MAX_SCRIPT_FIELDS_SETTING.getKey(), 10)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .build()
            )
            .build();
        ImmutableOpenMap.Builder<String, IndexMetadata> openMapBuilder = ImmutableOpenMap.builder();
        openMapBuilder.put("index", indexMetadata);
        Metadata metadata = Metadata.builder().indices(openMapBuilder.build()).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(nodes).metadata(metadata).build();

        RoutingExplanations routingExplanations = new RoutingExplanations();
        routingExplanations.add(new RerouteExplanation(new AllocateReplicaAllocationCommand("index", 0, "node0"), Decision.YES));
        ClusterRerouteResponse clusterRerouteResponse = new ClusterRerouteResponse(true, clusterState, routingExplanations);
        {
            XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
            clusterRerouteResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals(
                XContentHelper.stripWhitespace("""
                    {
                      "acknowledged": true,
                      "state": {
                        "cluster_uuid": "_na_",
                        "version": 0,
                        "state_uuid": "%s",
                        "master_node": "node0",
                        "blocks": {},
                        "nodes": {
                          "node0": {
                            "name": "",
                            "ephemeral_id": "%s",
                            "transport_address": "0.0.0.0:9000",
                            "attributes": {},
                            "roles": [
                              "data",
                              "data_cold",
                              "data_content",
                              "data_frozen",
                              "data_hot",
                              "data_warm",
                              "ingest",
                              "master",
                              "ml",
                              "remote_cluster_client",
                              "transform",
                              "voting_only"
                            ]
                          }
                        },
                        "metadata": {
                          "cluster_uuid": "_na_",
                          "cluster_uuid_committed": false,
                          "cluster_coordination": {
                            "term": 0,
                            "last_committed_config": [],
                            "last_accepted_config": [],
                            "voting_config_exclusions": []
                          },
                          "templates": {},
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
                                  "shard": {
                                    "check_on_startup": "true"
                                  },
                                  "number_of_shards": "1",
                                  "number_of_replicas": "0",
                                  "version": {
                                    "created": "%s"
                                  },
                                  "max_script_fields": "10"
                                }
                              },
                              "mappings": {},
                              "aliases": [],
                              "primary_terms": {
                                "0": 0
                              },
                              "in_sync_allocations": {
                                "0": []
                              },
                              "rollover_info": {},
                              "system": false,
                              "timestamp_range": {
                                "shards": []
                              }
                            }
                          },
                          "index-graveyard": {
                            "tombstones": []
                          }
                        },
                        "routing_table": {
                          "indices": {}
                        },
                        "routing_nodes": {
                          "unassigned": [],
                          "nodes": {
                            "node0": []
                          }
                        }
                      }
                    }""".formatted(clusterState.stateUUID(), node0.getEphemeralId(), Version.CURRENT.id)),
                XContentHelper.stripWhitespace(Strings.toString(builder))
            );
        }
        {
            XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
            Map<String, String> params = new HashMap<>();
            params.put("explain", "true");
            params.put("metric", "version,master_node");
            clusterRerouteResponse.toXContent(builder, new ToXContent.MapParams(params));
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "acknowledged": true,
                  "state": {
                    "cluster_uuid": "_na_",
                    "version": 0,
                    "state_uuid": "%s",
                    "master_node": "node0"
                  },
                  "explanations": [
                    {
                      "command": "allocate_replica",
                      "parameters": {
                        "index": "index",
                        "shard": 0,
                        "node": "node0"
                      },
                      "decisions": [
                        {
                          "decider": null,
                          "decision": "YES",
                          "explanation": "none"
                        }
                      ]
                    }
                  ]
                }""".formatted(clusterState.stateUUID())), XContentHelper.stripWhitespace(Strings.toString(builder)));
        }
        {
            XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
            Map<String, String> params = new HashMap<>();
            params.put("metric", "metadata");
            params.put("settings_filter", "index.number*,index.version.created");
            clusterRerouteResponse.toXContent(builder, new ToXContent.MapParams(params));
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "acknowledged" : true,
                  "state" : {
                    "cluster_uuid" : "_na_",
                    "metadata" : {
                      "cluster_uuid" : "_na_",
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
                          "version" : 1,
                          "mapping_version" : 1,
                          "settings_version" : 1,
                          "aliases_version" : 1,
                          "routing_num_shards" : 1,
                          "state" : "open",
                          "settings" : {
                            "index" : {
                              "max_script_fields" : "10",
                              "shard" : {
                                "check_on_startup" : "true"
                              }
                            }
                          },
                          "mappings" : { },
                          "aliases" : [ ],
                          "primary_terms" : {
                            "0" : 0
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
                      }
                    }
                  }
                }"""), XContentHelper.stripWhitespace(Strings.toString(builder)));
        }
    }
}
