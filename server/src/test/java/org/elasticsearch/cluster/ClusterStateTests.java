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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Setting;
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
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

public class ClusterStateTests extends ESTestCase {
    private static final Setting<Integer> PROJECT_SETTING = Setting.intSetting("project.setting", 0, Setting.Property.ProjectScope);
    private static final Setting<Integer> PROJECT_SETTING2 = Setting.intSetting("project.setting2", 0, Setting.Property.ProjectScope);

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

    public void testCopyAndUpdateProject() throws IOException {
        var projectId = randomProjectIdOrDefault();
        var state = buildClusterState(projectId);
        var indexName = getTestName();

        assertThat(state.metadata().getProject(projectId).hasIndex(indexName), equalTo(false));

        var copy = state.copyAndUpdateProject(
            projectId,
            project -> project.put(IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), randomUUID(), 1, 1)))
        );

        assertThat(copy, not(sameInstance(state)));
        assertThat(copy.metadata(), not(sameInstance(state.metadata())));
        assertThat(copy.metadata().getProject(projectId), not(sameInstance(state.metadata().getProject(projectId))));
        assertThat(copy.metadata().getProject(projectId).hasIndex(indexName), equalTo(true));
    }

    public void testGetNonExistingProjectStateThrows() {
        final List<ProjectMetadata> projects = IntStream.range(0, between(1, 3))
            .mapToObj(i -> MetadataTests.randomProject(ProjectId.fromId("p_" + i), between(0, 5)))
            .toList();
        final Metadata metadata = MetadataTests.randomMetadata(projects);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        expectThrows(IllegalArgumentException.class, () -> clusterState.projectState(randomProjectIdOrDefault()));
    }

    public void testToStringWithMultipleProjects() throws IOException {
        final DiscoveryNode node1 = DiscoveryNodeUtils.create("node01");
        final DiscoveryNode node2 = DiscoveryNodeUtils.create("node02");
        final DiscoveryNode node3 = DiscoveryNodeUtils.create("node03");
        final ClusterState clusterState = buildMultiProjectClusterState(node1, node2, node3);

        final String toString = clusterState.toString();
        assertThat(toString, startsWith("""
            cluster uuid: N8nJxElHSP23swO0bPLOcQ [committed: true]
            version: 404
            state uuid: dIP3KIhRQPux2CgaDWgTMA
            from_diff: false
            meta data version: 86
               coordination_metadata:
                  term: 22
                  last_committed_config: VotingConfiguration{node01}
                  last_accepted_config: VotingConfiguration{}
                  voting tombstones: []
            """));

        // blocks
        assertThat(toString, containsString("""
            blocks:\s
               _global_:
                  6,cluster read-only (api), blocks WRITE,METADATA_WRITE
               3LftaL7hgfXAsF60Gm6jcD:
                  another-index:
                     5,index read-only (api), blocks WRITE,METADATA_WRITE
               tb5W0bx765nDVIwqJPw92G:
                  common-index:
                     9,index metadata (api), blocks METADATA_READ,METADATA_WRITE
            """));

        // project indices
        assertThat(toString, containsString("""
               project[tb5W0bx765nDVIwqJPw92G]:
                  [common-index/tE62Ga40yvlmOSujUvruVw]: v[2], mv[1], sv[1], av[1]
                  0: p_term [0], isa_ids []
                  1: p_term [0], isa_ids []
                  2: p_term [0], isa_ids []
            """));
        assertThat(toString, containsString("\n   project[3LftaL7hgfXAsF60Gm6jcD]:\n"));
        assertThat(toString, containsString("\n      [common-index/dyQMAHOKifstVZeq1fbe2g]: "));
        assertThat(toString, containsString("\n      [another-index/3BgcDKea85VWlp4Tr514s6]: "));
        assertThat(toString, containsString("\n   project[WHyuJ0uqBYOPgHX9kYUXlZ]: -\n"));

        // project customs
        assertThat(toString, containsString("\n   project[tb5W0bx765nDVIwqJPw92G]:\n      index-graveyard: IndexGraveyard[[]]\n"));
        assertThat(toString, containsString("\n   project[3LftaL7hgfXAsF60Gm6jcD]:\n      index-graveyard: IndexGraveyard[[]]\n"));
        assertThat(toString, containsString("\n   project[WHyuJ0uqBYOPgHX9kYUXlZ]:\n      index-graveyard: IndexGraveyard[[]]\n"));

        // nodes
        assertThat(toString, containsString("\ncluster features:\n   node0"));
        assertThat(toString, containsString("\n   node01: []\n"));
        assertThat(toString, containsString("\n   node02: []\n"));
        assertThat(toString, containsString("\n   node03: []\n"));

        assertThat(toString, containsString("\n   {node01}{" + node1.getEphemeralId() + "}{0.0.0.0}{"));
        assertThat(toString, containsString("\n   {node02}{" + node2.getEphemeralId() + "}{0.0.0.0}{"));
        assertThat(toString, containsString("\n   {node03}{" + node3.getEphemeralId() + "}{0.0.0.0}{"));

        // routing table
        assertThat(toString, containsString("global_routing_table{"));
        assertThat(toString, containsString("tb5W0bx765nDVIwqJPw92G=>routing_table:\n"));
        assertThat(toString, containsString("3LftaL7hgfXAsF60Gm6jcD=>routing_table:\n"));
        assertThat(toString, containsString("WHyuJ0uqBYOPgHX9kYUXlZ=>routing_table:\n"));
        assertThat(toString, containsString("-- index [[another-index/3BgcDKea85VWlp4Tr514s6]]\n----shard_id [another-index][0]\n"));
        assertThat(toString, containsString("-- index [[common-index/tE62Ga40yvlmOSujUvruVw]]\n----shard_id [common-index][0]\n"));
        assertThat(toString, containsString("\n----shard_id [common-index][1]\n"));
        assertThat(toString, containsString("\n----shard_id [common-index][2]\n"));
    }

    public void testToXContentWithMultipleProjects() throws IOException {
        DiscoveryNode[] nodes = new DiscoveryNode[5];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = DiscoveryNodeUtils.create(
                Strings.format("node_%02d", i + 1),
                new TransportAddress(InetAddress.getByName("0.0.0.0"), (i + 1))
            );
        }
        final ClusterState clusterState = buildMultiProjectClusterState(nodes);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        writeChunks(
            clusterState,
            builder,
            new ToXContent.MapParams(
                Map.ofEntries(
                    Map.entry(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API),
                    Map.entry("multi-project", "true"),
                    Map.entry("metric", "version,master_node,blocks,nodes,metadata,routing_table,customs,projects_settings")
                    // not routing_nodes because the order is not predictable
                )
            )
        );
        builder.endObject();

        final String actual = Strings.toString(builder)
            .replaceAll("\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d*)?Z\"", "\"_DATE_\"");
        final String expected = Strings.format(
            """
                {
                  "cluster_uuid": "N8nJxElHSP23swO0bPLOcQ",
                  "version": 404,
                  "state_uuid": "dIP3KIhRQPux2CgaDWgTMA",
                  "master_node": null,
                  "blocks": {
                    "global": {
                      "6": {
                        "retryable": false,
                        "description": "cluster read-only (api)",
                        "levels": [ "write", "metadata_write"]
                      }
                    },
                    "projects": [
                      {
                        "id": "tb5W0bx765nDVIwqJPw92G",
                        "indices": {
                          "common-index": {
                            "9": {
                              "retryable": false,
                              "description": "index metadata (api)",
                              "levels": [ "metadata_read", "metadata_write"]
                            }
                          }
                        }
                      },
                      {
                        "id": "3LftaL7hgfXAsF60Gm6jcD",
                        "indices": {
                          "another-index": {
                            "5": {
                              "retryable": false,
                              "description": "index read-only (api)",
                              "levels": [ "write", "metadata_write"]
                            }
                          }
                        }
                      }
                    ]
                  },
                  "nodes": {
                    "node_01": {
                      "name": "",
                      "ephemeral_id": "%s",
                      "transport_address": "0.0.0.0:1",
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
                    },
                    "node_02": {
                      "name": "",
                      "ephemeral_id": "%s",
                      "transport_address": "0.0.0.0:2",
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
                    },
                    "node_03": {
                      "name": "",
                      "ephemeral_id": "%s",
                      "transport_address": "0.0.0.0:3",
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
                    },
                    "node_04": {
                      "name": "",
                      "ephemeral_id": "%s",
                      "transport_address": "0.0.0.0:4",
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
                    },
                    "node_05": {
                      "name": "",
                      "ephemeral_id": "%s",
                      "transport_address": "0.0.0.0:5",
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
                  "nodes_versions": [],
                  "nodes_features": [
                     {"node_id":"node_01","features":[]},
                     {"node_id":"node_02","features":[]},
                     {"node_id":"node_03","features":[]},
                     {"node_id":"node_04","features":[]},
                     {"node_id":"node_05","features":[]}
                  ],
                  "metadata": {
                    "cluster_uuid": "N8nJxElHSP23swO0bPLOcQ",
                    "cluster_uuid_committed": true,
                    "cluster_coordination": {
                      "term": 22,
                      "last_committed_config": [ "node_01" ],
                      "last_accepted_config": [],
                      "voting_config_exclusions": []
                    },
                    "projects": [
                      {
                        "id": "tb5W0bx765nDVIwqJPw92G",
                        "templates": {},
                        "indices": {
                          "common-index": {
                            "version": 2,
                            "mapping_version": 1,
                            "settings_version": 1,
                            "aliases_version": 1,
                            "routing_num_shards": 3,
                            "state": "open",
                            "settings": {
                              "index": {
                                "number_of_shards": "3",
                                "number_of_replicas": "1",
                                "uuid": "tE62Ga40yvlmOSujUvruVw",
                                "version": { "created": "%s" }
                              }
                            },
                            "mappings": {},
                            "aliases": [],
                            "primary_terms": { "0":0, "1":0, "2":0 },
                            "in_sync_allocations": { "0":[], "1":[], "2":[] },
                            "rollover_info": {},
                            "mappings_updated_version": %s,
                            "system": false,
                            "timestamp_range": { "shards":[] },
                            "event_ingested_range": { "shards": [] }
                          }
                        },
                        "index-graveyard": { "tombstones": [] },
                        "reserved_state": {}
                      },
                      {
                        "id": "3LftaL7hgfXAsF60Gm6jcD",
                        "templates": {},
                        "indices": {
                          "another-index": {
                            "version": 2,
                            "mapping_version": 1,
                            "settings_version": 1,
                            "aliases_version": 1,
                            "routing_num_shards": 1,
                            "state": "open",
                            "settings": {
                              "index": {
                                "number_of_shards": "1",
                                "number_of_replicas": "1",
                                "uuid": "3BgcDKea85VWlp4Tr514s6",
                                "version": { "created": "%s" }
                              }
                            },
                            "mappings": {},
                            "aliases": [],
                            "primary_terms": { "0": 0 },
                            "in_sync_allocations": { "0": [] },
                            "rollover_info": {},
                            "mappings_updated_version": %s,
                            "system": false,
                            "timestamp_range": { "shards": [] },
                            "event_ingested_range": { "shards": [] }
                          },
                          "common-index": {
                            "version": 2,
                            "mapping_version": 1,
                            "settings_version": 1,
                            "aliases_version": 1,
                            "routing_num_shards": 1,
                            "state": "open",
                            "settings": {
                              "index": {
                                "number_of_shards": "1",
                                "number_of_replicas": "2",
                                "uuid": "dyQMAHOKifstVZeq1fbe2g",
                                "version": { "created": "%s" }
                              }
                            },
                            "mappings": {},
                            "aliases": [],
                            "primary_terms": { "0": 0 },
                            "in_sync_allocations": { "0": [] },
                            "rollover_info": {},
                            "mappings_updated_version": %s,
                            "system": false,
                            "timestamp_range": { "shards": [] },
                            "event_ingested_range": { "shards": [] }
                          }
                        },
                        "index-graveyard": { "tombstones": [] },
                        "reserved_state": {}
                      },
                      {
                        "id": "WHyuJ0uqBYOPgHX9kYUXlZ",
                        "templates": {},
                        "indices": {},
                        "index-graveyard": { "tombstones": [] },
                        "reserved_state": {}
                      }
                    ],
                    "reserved_state": {}
                  },
                  "routing_table": {
                    "projects": [
                      {
                        "id": "3LftaL7hgfXAsF60Gm6jcD",
                        "indices": {
                          "another-index": {
                            "shards": {
                              "0": [
                                {
                                  "state": "UNASSIGNED",
                                  "primary": true,
                                  "node": null,
                                  "relocating_node": null,
                                  "shard": 0,
                                  "index": "another-index",
                                  "recovery_source": { "type": "EMPTY_STORE" },
                                  "unassigned_info": {
                                    "reason": "INDEX_CREATED",
                                    "at": "_DATE_",
                                    "delayed": false,
                                    "allocation_status": "no_attempt"
                                  },
                                  "relocation_failure_info": { "failed_attempts": 0 }
                                },
                                {
                                  "state": "UNASSIGNED",
                                  "primary": false,
                                  "node": null,
                                  "relocating_node": null,
                                  "shard": 0,
                                  "index": "another-index",
                                  "recovery_source": { "type": "PEER" },
                                  "unassigned_info": {
                                    "reason": "INDEX_CREATED",
                                    "at": "_DATE_",
                                    "delayed": false,
                                    "allocation_status": "no_attempt"
                                  },
                                  "relocation_failure_info": { "failed_attempts": 0 }
                                }
                              ]
                            }
                          },
                          "common-index": {
                            "shards": {
                              "0": [
                                {
                                  "state": "UNASSIGNED",
                                  "primary": true,
                                  "node": null,
                                  "relocating_node": null,
                                  "shard": 0,
                                  "index": "common-index",
                                  "recovery_source": { "type": "EMPTY_STORE" },
                                  "unassigned_info": {
                                    "reason": "INDEX_CREATED",
                                    "at": "_DATE_",
                                    "delayed": false,
                                    "allocation_status": "no_attempt"
                                  },
                                  "relocation_failure_info": { "failed_attempts": 0 }
                                },
                                {
                                  "state": "UNASSIGNED",
                                  "primary": false,
                                  "node": null,
                                  "relocating_node": null,
                                  "shard": 0,
                                  "index": "common-index",
                                  "recovery_source": { "type": "PEER" },
                                  "unassigned_info": {
                                    "reason": "INDEX_CREATED",
                                    "at": "_DATE_",
                                    "delayed": false,
                                    "allocation_status": "no_attempt"
                                  },
                                  "relocation_failure_info": { "failed_attempts": 0 }
                                },
                                {
                                  "state": "UNASSIGNED",
                                  "primary": false,
                                  "node": null,
                                  "relocating_node": null,
                                  "shard": 0,
                                  "index": "common-index",
                                  "recovery_source": { "type": "PEER" },
                                  "unassigned_info": {
                                    "reason": "INDEX_CREATED",
                                    "at": "_DATE_",
                                    "delayed": false,
                                    "allocation_status": "no_attempt"
                                  },
                                  "relocation_failure_info": { "failed_attempts": 0 }
                                }
                              ]
                            }
                          }
                        }
                      },
                      {
                        "id": "WHyuJ0uqBYOPgHX9kYUXlZ",
                        "indices": {}
                      },
                      {
                        "id": "tb5W0bx765nDVIwqJPw92G",
                        "indices": {
                          "common-index": {
                            "shards": {
                              "0": [
                                {
                                  "state": "UNASSIGNED",
                                  "primary": true,
                                  "node": null,
                                  "relocating_node": null,
                                  "shard": 0,
                                  "index": "common-index",
                                  "recovery_source": { "type": "EMPTY_STORE" },
                                  "unassigned_info": {
                                    "reason": "INDEX_CREATED",
                                    "at": "_DATE_",
                                    "delayed": false,
                                    "allocation_status": "no_attempt"
                                  },
                                  "relocation_failure_info": { "failed_attempts": 0 }
                                },
                                {
                                  "state": "UNASSIGNED",
                                  "primary": false,
                                  "node": null,
                                  "relocating_node": null,
                                  "shard": 0,
                                  "index": "common-index",
                                  "recovery_source": { "type": "PEER" },
                                  "unassigned_info": {
                                    "reason": "INDEX_CREATED",
                                    "at": "_DATE_",
                                    "delayed": false,
                                    "allocation_status": "no_attempt"
                                  },
                                  "relocation_failure_info": { "failed_attempts": 0 }
                                }
                              ],
                              "1": [
                                {
                                  "state": "UNASSIGNED",
                                  "primary": true,
                                  "node": null,
                                  "relocating_node": null,
                                  "shard": 1,
                                  "index": "common-index",
                                  "recovery_source": { "type": "EMPTY_STORE" },
                                  "unassigned_info": {
                                    "reason": "INDEX_CREATED",
                                    "at": "_DATE_",
                                    "delayed": false,
                                    "allocation_status": "no_attempt"
                                  },
                                  "relocation_failure_info": { "failed_attempts": 0 }
                                },
                                {
                                  "state": "UNASSIGNED",
                                  "primary": false,
                                  "node": null,
                                  "relocating_node": null,
                                  "shard": 1,
                                  "index": "common-index",
                                  "recovery_source": { "type": "PEER" },
                                  "unassigned_info": {
                                    "reason": "INDEX_CREATED",
                                    "at": "_DATE_",
                                    "delayed": false,
                                    "allocation_status": "no_attempt"
                                  },
                                  "relocation_failure_info": { "failed_attempts": 0 }
                                }
                              ],
                              "2": [
                                {
                                  "state": "UNASSIGNED",
                                  "primary": true,
                                  "node": null,
                                  "relocating_node": null,
                                  "shard": 2,
                                  "index": "common-index",
                                  "recovery_source": { "type": "EMPTY_STORE" },
                                  "unassigned_info": {
                                    "reason": "INDEX_CREATED",
                                    "at": "_DATE_",
                                    "delayed": false,
                                    "allocation_status": "no_attempt"
                                  },
                                  "relocation_failure_info": { "failed_attempts": 0 }
                                },
                                {
                                  "state": "UNASSIGNED",
                                  "primary": false,
                                  "node": null,
                                  "relocating_node": null,
                                  "shard": 2,
                                  "index": "common-index",
                                  "recovery_source": { "type": "PEER" },
                                  "unassigned_info": {
                                    "reason": "INDEX_CREATED",
                                    "at": "_DATE_",
                                    "delayed": false,
                                    "allocation_status": "no_attempt"
                                  },
                                  "relocation_failure_info": { "failed_attempts": 0 }
                                }
                              ]
                            }
                          }
                        }
                      }
                    ]
                  },
                  "projects_settings": [
                    {
                      "id": "3LftaL7hgfXAsF60Gm6jcD",
                      "settings": {
                        "project.setting": "42",
                        "project.setting2": "43"
                      }
                    },
                    {
                      "id": "tb5W0bx765nDVIwqJPw92G",
                      "settings": {
                        "project.setting": "1"
                      }
                    }
                  ]
                }
                """,
            // Node 1
            clusterState.getNodes().get("node_01").getEphemeralId(),
            Version.CURRENT,
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersion.current(),
            // Node 2
            clusterState.getNodes().get("node_02").getEphemeralId(),
            Version.CURRENT,
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersion.current(),
            // Node 3
            clusterState.getNodes().get("node_03").getEphemeralId(),
            Version.CURRENT,
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersion.current(),
            // Node 4
            clusterState.getNodes().get("node_04").getEphemeralId(),
            Version.CURRENT,
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersion.current(),
            // Node 5
            clusterState.getNodes().get("node_05").getEphemeralId(),
            Version.CURRENT,
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersion.current(),
            // project:tb5W0bx765nDVIwqJPw92G index:common-index
            IndexVersion.current(),
            IndexVersion.current(),
            // project:3LftaL7hgfXAsF60Gm6jcD index:another-index
            IndexVersion.current(),
            IndexVersion.current(),
            // project:3LftaL7hgfXAsF60Gm6jcD index:common-index
            IndexVersion.current(),
            IndexVersion.current()
        );

        // We cannot guarantee the order of the routing table, so we parse & sort the generated XContent
        final BytesReference sorted = sortRoutingTableXContent(actual);
        assertToXContentEquivalent(new BytesArray(expected), sorted, XContentType.JSON);
    }

    @SuppressWarnings("unchecked")
    private static BytesReference sortRoutingTableXContent(String jsonContent) throws IOException {
        final Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(jsonContent), true, XContentType.JSON).v2();
        final List<Map<String, Object>> routingTable = ObjectPath.eval("routing_table.projects", map);
        assertThat("Cannot find routing table in " + map.keySet(), routingTable, notNullValue());
        routingTable.sort(Comparator.comparing((Map<String, Object> m) -> {
            final Object projectId = m.get("id");
            assertThat(projectId, notNullValue());
            assertThat(projectId, instanceOf(String.class));
            return (String) projectId;
        }));
        return XContentTestUtils.convertToXContent(map, XContentType.JSON);
    }

    private static ClusterState buildMultiProjectClusterState(DiscoveryNode... nodes) {
        ProjectId projectId1 = ProjectId.fromId("3LftaL7hgfXAsF60Gm6jcD");
        ProjectId projectId2 = ProjectId.fromId("tb5W0bx765nDVIwqJPw92G");
        final Metadata metadata = Metadata.builder()
            .clusterUUID("N8nJxElHSP23swO0bPLOcQ")
            .clusterUUIDCommitted(true)
            .version(86L)
            .coordinationMetadata(
                CoordinationMetadata.builder()
                    .term(22)
                    .lastCommittedConfiguration(CoordinationMetadata.VotingConfiguration.of(nodes[0]))
                    .build()
            )
            .put(
                ProjectMetadata.builder(projectId1)
                    .put(
                        IndexMetadata.builder("common-index")
                            .settings(
                                indexSettings(IndexVersion.current(), 1, 2).put(IndexMetadata.SETTING_INDEX_UUID, "dyQMAHOKifstVZeq1fbe2g")
                            )
                    )
                    .put(
                        IndexMetadata.builder("another-index")
                            .settings(
                                indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, "3BgcDKea85VWlp4Tr514s6")
                            )
                    )
            )
            .put(
                ProjectMetadata.builder(projectId2)
                    .put(
                        IndexMetadata.builder("common-index")
                            .settings(
                                indexSettings(IndexVersion.current(), 3, 1).put(IndexMetadata.SETTING_INDEX_UUID, "tE62Ga40yvlmOSujUvruVw")
                            )
                    )
            )
            .put(ProjectMetadata.builder(ProjectId.fromId("WHyuJ0uqBYOPgHX9kYUXlZ")))
            .build();
        final DiscoveryNodes.Builder discoveryNodes = DiscoveryNodes.builder();
        for (var node : nodes) {
            discoveryNodes.add(node);
        }

        return ClusterState.builder(new ClusterName("my-cluster"))
            .stateUUID("dIP3KIhRQPux2CgaDWgTMA")
            .version(404)
            .metadata(metadata)
            .nodes(discoveryNodes.build())
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew))
            .putProjectSettings(projectId1, Settings.builder().put(PROJECT_SETTING.getKey(), 42).put(PROJECT_SETTING2.getKey(), 43).build())
            .putProjectSettings(projectId2, Settings.builder().put(PROJECT_SETTING.getKey(), 1).build())
            .blocks(
                ClusterBlocks.builder()
                    .addGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK)
                    .addIndexBlock(projectId2, "common-index", IndexMetadata.INDEX_METADATA_BLOCK)
                    .addIndexBlock(projectId1, "another-index", IndexMetadata.INDEX_READ_ONLY_BLOCK)
            )
            .build();
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
                                      "uptimes": [-1],
                                      "recent_loads": [-1.0],
                                      "peak_loads": [-1.0]
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
                                ],
                                "recent_loads" : [
                                  -1.0
                                ],
                                "peak_loads" : [
                                  -1.0
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
                                ],
                                "recent_loads" : [
                                  -1.0
                                ],
                                "peak_loads" : [
                                  -1.0
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
        return buildClusterState(ProjectId.DEFAULT);
    }

    private static ClusterState buildClusterState(ProjectId projectId) throws IOException {
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
            .eventIngestedRange(IndexLongFieldRange.UNKNOWN)
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
                    .put(
                        ProjectMetadata.builder(projectId)
                            .put(indexMetadata, false)
                            .put(
                                IndexTemplateMetadata.builder("template")
                                    .patterns(List.of("pattern1", "pattern2"))
                                    .order(0)
                                    .settings(Settings.builder().put(SETTING_VERSION_CREATED, IndexVersion.current()))
                                    .putMapping("type", "{ \"key1\": {} }")
                                    .build()
                            )
                            .build()
                    )
            )
            .routingTable(
                GlobalRoutingTable.builder()
                    .put(
                        projectId,
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
            final ToXContent chunk = iterator.next();
            chunk.toXContent(builder, params);
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
            chunkCount += 2; // chunks for before and after
            final Map<ProjectId, ProjectMetadata> projects = clusterState.metadata().projects();
            if (projects.size() == 1 && projects.containsKey(Metadata.DEFAULT_PROJECT_ID)) {
                chunkCount += clusterState.blocks().indices(Metadata.DEFAULT_PROJECT_ID).size();
            } else {
                for (var projectId : clusterState.metadata().projects().keySet()) {
                    final Map<String, Set<ClusterBlock>> indicesBlocks = clusterState.blocks().indices(projectId);
                    if (indicesBlocks.isEmpty() == false) {
                        chunkCount += 2 + indicesBlocks.size();
                    }
                }
            }
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
            final boolean multiProject = params.paramAsBoolean("multi-project", false);
            chunkCount += 2;
            for (var projectRouting : clusterState.globalRoutingTable()) {
                if (multiProject) {
                    chunkCount += 2;
                }
                for (IndexRoutingTable indexRoutingTable : projectRouting) {
                    chunkCount += 2;
                    for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                        chunkCount += 2 + indexRoutingTable.shard(shardId).size();
                    }
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

        // settings
        if (metrics.contains(ClusterState.Metric.PROJECTS_SETTINGS)) {
            Map<ProjectId, Settings> projectsSettings = clusterState.projectsSettings();
            if (projectsSettings.isEmpty() == false) {
                chunkCount += 2 + projectsSettings.size();
            }
        }

        return Math.toIntExact(chunkCount);
    }
}
