/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTests;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.RerouteExplanation;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class ClusterRerouteResponseTests extends ESTestCase {

    @Override
    protected List<String> filteredWarnings() {
        return appendToCopy(super.filteredWarnings(), ClusterRerouteResponse.STATE_FIELD_DEPRECATION_MESSAGE);
    }

    public void testToXContent() throws IOException {
        assertXContent(createClusterRerouteResponse(createClusterState()), new ToXContent.MapParams(Map.of("metric", "none")), """
            {
              "acknowledged": true
            }""");
    }

    public void testToXContentWithExplain() {
        var clusterState = createClusterState();
        assertXContent(
            createClusterRerouteResponse(clusterState),
            new ToXContent.MapParams(Map.of("explain", "true", "metric", "none")),
            Strings.format("""
                {
                  "acknowledged": true,
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
                }""", clusterState.stateUUID())
        );
    }

    public void testToXContentWithDeprecatedClusterState() {
        var clusterState = createClusterState();
        assertXContent(
            createClusterRerouteResponse(clusterState),
            ToXContent.EMPTY_PARAMS,
            Strings.format(
                """
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
                            "min_index_version": %s,
                            "max_index_version": %s
                          }
                        },
                        "nodes_versions": [
                          {
                            "node_id": "node0",
                            "transport_version": "8000099",
                            "mappings_versions": {
                              ".system-index": {
                                "version": 1,
                                "hash": 0
                              }
                            }
                          }
                        ],
                        "nodes_features": [
                          {
                            "node_id": "node0",
                            "features": []
                          }
                        ],
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
                              "mappings_updated_version" : %s,
                              "system": false,
                              "timestamp_range": {
                                "shards": []
                              },
                              "event_ingested_range": {
                                "unknown":true
                              }
                            }
                          },
                          "index-graveyard": {
                            "tombstones": []
                          },
                          "reserved_state":{}
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
                    }""",
                clusterState.stateUUID(),
                clusterState.getNodes().get("node0").getEphemeralId(),
                Version.CURRENT,
                IndexVersions.MINIMUM_COMPATIBLE,
                IndexVersion.current(),
                IndexVersion.current(),
                IndexVersion.current()
            ),
            """
                The [state] field in the response to the reroute API is deprecated and will be removed in a future version. \
                Specify ?metric=none to adopt the future behaviour."""
        );
    }

    public void testToXContentWithDeprecatedClusterStateAndMetadata() {
        assertXContent(
            createClusterRerouteResponse(createClusterState()),
            new ToXContent.MapParams(Map.of("metric", "metadata", "settings_filter", "index.number*,index.version.created")),
            Strings.format("""
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
                          "mappings_updated_version" : %s,
                          "system" : false,
                          "timestamp_range" : {
                            "shards" : [ ]
                          },
                          "event_ingested_range" : {
                            "unknown" : true
                          }
                        }
                      },
                      "index-graveyard" : {
                        "tombstones" : [ ]
                      },
                      "reserved_state":{}
                    }
                  }
                }""", IndexVersion.current()),
            """
                The [state] field in the response to the reroute API is deprecated and will be removed in a future version. \
                Specify ?metric=none to adopt the future behaviour."""
        );
    }

    private void assertXContent(
        ClusterRerouteResponse response,
        ToXContent.Params params,
        String expectedBody,
        String... criticalDeprecationWarnings
    ) {
        try {
            var builder = jsonBuilder();
            if (randomBoolean()) {
                builder.prettyPrint();
            }
            ChunkedToXContent.wrapAsToXContent(response).toXContent(builder, params);
            assertEquals(XContentHelper.stripWhitespace(expectedBody), XContentHelper.stripWhitespace(Strings.toString(builder)));
        } catch (IOException e) {
            fail(e);
        }

        int[] expectedChunks = new int[] { 3 };
        if (Objects.equals(params.param("metric"), "none") == false) {
            expectedChunks[0] += 2 + ClusterStateTests.expectedChunkCount(params, response.getState());
        }
        if (params.paramAsBoolean("explain", false)) {
            expectedChunks[0]++;
        }

        AbstractChunkedSerializingTestCase.assertChunkCount(response, params, o -> expectedChunks[0]);
        assertCriticalWarnings(criticalDeprecationWarnings);
    }

    private static ClusterRerouteResponse createClusterRerouteResponse(ClusterState clusterState) {
        return new ClusterRerouteResponse(
            true,
            clusterState,
            new RoutingExplanations().add(new RerouteExplanation(new AllocateReplicaAllocationCommand("index", 0, "node0"), Decision.YES))
        );
    }

    private static ClusterState createClusterState() {
        var node0 = DiscoveryNodeUtils.create("node0", new TransportAddress(TransportAddress.META_ADDRESS, 9000));
        return ClusterState.builder(new ClusterName("test"))
            .nodes(new DiscoveryNodes.Builder().add(node0).masterNodeId(node0.getId()).build())
            .putCompatibilityVersions(
                node0.getId(),
                TransportVersions.V_8_0_0,
                Map.of(".system-index", new SystemIndexDescriptor.MappingsVersion(1, 0))
            )
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder("index")
                            .settings(
                                indexSettings(1, 0).put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), true)
                                    .put(IndexSettings.MAX_SCRIPT_FIELDS_SETTING.getKey(), 10)
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                                    .build()
                            )
                            .eventIngestedRange(IndexLongFieldRange.UNKNOWN, TransportVersion.current())
                            .build(),
                        false
                    )
                    .build()
            )
            .build();
    }
}
