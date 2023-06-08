/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
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
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class ClusterRerouteResponseTests extends ESTestCase {

    @Override
    protected List<String> filteredWarnings() {
        return appendToCopy(super.filteredWarnings(), ClusterRerouteResponse.STATE_FIELD_DEPRECATION_MESSAGE);
    }

    public void testToXContent() throws IOException {
        assertXContent(createClusterRerouteResponse(createClusterState()), new ToXContent.MapParams(Map.of("metric", "none")), 2, """
            {
              "acknowledged": true
            }""");
    }

    public void testToXContentWithExplain() {
        var clusterState = createClusterState();
        assertXContent(
            createClusterRerouteResponse(clusterState),
            new ToXContent.MapParams(Map.of("explain", "true", "metric", "none")),
            2,
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
        assertXContent(createClusterRerouteResponse(clusterState), ToXContent.EMPTY_PARAMS, 35, Strings.format("""
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
                    "version": "%s"
                  }
                },
                "transport_versions": [
                  {
                    "node_id": "node0",
                    "transport_version": "8000099"
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
                      "system": false,
                      "timestamp_range": {
                        "shards": []
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
            }""", clusterState.stateUUID(), clusterState.getNodes().get("node0").getEphemeralId(), Version.CURRENT, Version.CURRENT.id), """
            The [state] field in the response to the reroute API is deprecated and will be removed in a future version. \
            Specify ?metric=none to adopt the future behaviour.""");
    }

    public void testToXContentWithDeprecatedClusterStateAndMetadata() {
        assertXContent(
            createClusterRerouteResponse(createClusterState()),
            new ToXContent.MapParams(Map.of("metric", "metadata", "settings_filter", "index.number*,index.version.created")),
            19,
            """
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
                      },
                      "reserved_state":{}
                    }
                  }
                }""",
            """
                The [state] field in the response to the reroute API is deprecated and will be removed in a future version. \
                Specify ?metric=none to adopt the future behaviour."""
        );
    }

    private void assertXContent(
        ClusterRerouteResponse response,
        ToXContent.Params params,
        int expectedChunks,
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
            throw new AssertionError("unexpected", e);
        }

        AbstractChunkedSerializingTestCase.assertChunkCount(response, params, ignored -> expectedChunks);
        assertCriticalWarnings(criticalDeprecationWarnings);

        // check the v7 API too
        AbstractChunkedSerializingTestCase.assertChunkCount(new ChunkedToXContent() {
            @Override
            public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
                return response.toXContentChunkedV7(outerParams);
            }

            @Override
            public boolean isFragment() {
                return response.isFragment();
            }
        }, params, ignored -> expectedChunks);
        // the v7 API should not emit any deprecation warnings
        assertCriticalWarnings();
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
            .putTransportVersion(node0.getId(), TransportVersion.V_8_0_0)
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder("index")
                            .settings(
                                indexSettings(1, 0).put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), true)
                                    .put(IndexSettings.MAX_SCRIPT_FIELDS_SETTING.getKey(), 10)
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .build()
                            )
                            .build(),
                        false
                    )
                    .build()
            )
            .build();
    }
}
