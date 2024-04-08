/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.cluster;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.stats.AnalysisStats;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsNodeResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.admin.cluster.stats.MappingStats;
import org.elasticsearch.action.admin.cluster.stats.SearchUsageStats;
import org.elasticsearch.action.admin.cluster.stats.VersionStats;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterSnapshotStats;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.License;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.plugins.PluginRuntimeInfo;
import org.elasticsearch.test.BuildUtils;
import org.elasticsearch.transport.TransportInfo;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.MonitoringFeatureSetUsage;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.BaseMonitoringDocTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentHelper.stripWhitespace;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterStatsMonitoringDocTests extends BaseMonitoringDocTestCase<ClusterStatsMonitoringDoc> {

    private String clusterName;
    private String version;
    private ClusterHealthStatus clusterStatus;
    private List<XPackFeatureSet.Usage> usages;
    private ClusterStatsResponse clusterStats;
    private ClusterState clusterState;
    private License license;
    private final boolean needToEnableTLS = randomBoolean();
    private final boolean apmIndicesExist = randomBoolean();

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterName = randomAlphaOfLength(5);
        version = randomAlphaOfLengthBetween(6, 32);
        clusterStatus = randomFrom(ClusterHealthStatus.values());
        usages = emptyList();
        clusterStats = mock(ClusterStatsResponse.class);
        clusterState = mock(ClusterState.class);
        final License.OperationMode operationMode = randomFrom(License.OperationMode.values());
        license = License.builder()
            .uid(randomAlphaOfLength(5))
            .type(operationMode.name().toLowerCase(Locale.ROOT))
            .issuer(randomAlphaOfLength(5))
            .issuedTo(randomAlphaOfLength(5))
            .issueDate(timestamp)
            .expiryDate(timestamp + randomIntBetween(1, 10) * 1_000L)
            .maxNodes(License.OperationMode.ENTERPRISE == operationMode ? -1 : randomIntBetween(1, 5))
            .maxResourceUnits(License.OperationMode.ENTERPRISE == operationMode ? randomIntBetween(1, 42) : -1)
            .build();

        final DiscoveryNode masterNode = masterNode();
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder()
            .masterNodeId(masterNode.getId())
            .localNodeId(masterNode.getId())
            .add(masterNode);

        when(clusterState.nodes()).thenReturn(builder.build());
        when(clusterState.toXContentChunked(any())).thenReturn(Collections.emptyIterator());
    }

    @Override
    protected ClusterStatsMonitoringDoc createMonitoringDoc(
        String cluster,
        long timestamp,
        long interval,
        MonitoringDoc.Node node,
        MonitoredSystem system,
        String type,
        String id
    ) {
        return new ClusterStatsMonitoringDoc(
            cluster,
            timestamp,
            interval,
            node,
            clusterName,
            version,
            clusterStatus,
            license,
            apmIndicesExist,
            usages,
            clusterStats,
            clusterState,
            needToEnableTLS
        );
    }

    @Override
    protected void assertMonitoringDoc(final ClusterStatsMonitoringDoc document) {
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), is(ClusterStatsMonitoringDoc.TYPE));
        assertThat(document.getId(), nullValue());

        assertThat(document.getClusterName(), equalTo(clusterName));
        assertThat(document.getVersion(), equalTo(version));
        assertThat(document.getStatus(), equalTo(clusterStatus));
        assertThat(document.getLicense(), equalTo(license));
        assertThat(document.getAPMIndicesExist(), is(apmIndicesExist));
        assertThat(document.getUsages(), is(usages));
        assertThat(document.getClusterStats(), is(clusterStats));
        assertThat(document.getClusterState(), is(clusterState));
    }

    public void testConstructorClusterNameMustNotBeNull() {
        expectThrows(
            NullPointerException.class,
            () -> new ClusterStatsMonitoringDoc(
                cluster,
                timestamp,
                interval,
                node,
                null,
                version,
                clusterStatus,
                license,
                apmIndicesExist,
                usages,
                clusterStats,
                clusterState,
                needToEnableTLS
            )
        );
    }

    public void testConstructorVersionMustNotBeNull() {
        expectThrows(
            NullPointerException.class,
            () -> new ClusterStatsMonitoringDoc(
                cluster,
                timestamp,
                interval,
                node,
                clusterName,
                null,
                clusterStatus,
                license,
                apmIndicesExist,
                usages,
                clusterStats,
                clusterState,
                needToEnableTLS
            )
        );
    }

    public void testConstructorClusterHealthStatusMustNotBeNull() {
        expectThrows(
            NullPointerException.class,
            () -> new ClusterStatsMonitoringDoc(
                cluster,
                timestamp,
                interval,
                node,
                clusterName,
                version,
                null,
                license,
                apmIndicesExist,
                usages,
                clusterStats,
                clusterState,
                needToEnableTLS
            )
        );
    }

    public void testNodesHash() {
        final int nodeCount = randomIntBetween(0, 5);
        final DiscoveryNode masterNode = masterNode();
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder()
            .add(masterNode)
            .masterNodeId(masterNode.getId())
            .localNodeId(masterNode.getId());

        for (int i = 0; i < nodeCount; ++i) {
            builder.add(
                DiscoveryNodeUtils.builder(randomAlphaOfLength(2 + i))
                    .name(randomAlphaOfLength(5))
                    .ephemeralId(randomAlphaOfLength(5))
                    .address(randomAlphaOfLength(5), randomAlphaOfLength(5), new TransportAddress(TransportAddress.META_ADDRESS, 9301 + i))
                    .attributes(randomBoolean() ? singletonMap("attr", randomAlphaOfLength(3)) : emptyMap())
                    .roles(
                        singleton(
                            randomValueOtherThan(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE, () -> randomFrom(DiscoveryNodeRole.roles()))
                        )
                    )
                    .build()
            );
        }

        final DiscoveryNodes nodes = builder.build();
        StringBuilder ephemeralIds = new StringBuilder();

        for (final DiscoveryNode node : nodes) {
            ephemeralIds.append(node.getEphemeralId());
        }

        assertThat(ClusterStatsMonitoringDoc.nodesHash(nodes), equalTo(ephemeralIds.toString().hashCode()));
    }

    @Override
    public void testToXContent() throws IOException {
        final String clusterUuid = "_cluster";
        final ClusterName testClusterName = new ClusterName("_cluster_name");
        final TransportAddress transportAddress = new TransportAddress(TransportAddress.META_ADDRESS, 9300);
        final DiscoveryNode discoveryNode = new DiscoveryNode(
            "_node_name",
            "_node_id",
            "_ephemeral_id",
            "_host_name",
            "_host_address",
            transportAddress,
            singletonMap("attr", "value"),
            singleton(DiscoveryNodeRole.MASTER_ROLE),
            null,
            "_external_id"
        );

        final ClusterState testClusterState = ClusterState.builder(testClusterName)
            .metadata(
                Metadata.builder()
                    .clusterUUID(clusterUuid)
                    .transientSettings(Settings.builder().put("cluster.metadata.display_name", "my_prod_cluster").build())
                    .build()
            )
            .stateUUID("_state_uuid")
            .version(12L)
            .nodes(
                DiscoveryNodes.builder().masterNodeId(discoveryNode.getId()).localNodeId(discoveryNode.getId()).add(discoveryNode).build()
            )
            .build();

        final License testLicense = License.builder()
            .uid("442ca961-9c00-4bb2-b5c9-dfaacd547403")
            .type("trial")
            .issuer("elasticsearch")
            .issuedTo("customer")
            .issueDate(1451606400000L)
            .expiryDate(1502107402133L)
            .maxNodes(2)
            .build();

        final List<XPackFeatureSet.Usage> usageList = singletonList(new MonitoringFeatureSetUsage(false, null));

        final NodeInfo mockNodeInfo = mock(NodeInfo.class);
        var mockNodeVersion = randomAlphaOfLengthBetween(6, 32);
        when(mockNodeInfo.getVersion()).thenReturn(mockNodeVersion);
        when(mockNodeInfo.getNode()).thenReturn(discoveryNode);

        final TransportInfo mockTransportInfo = mock(TransportInfo.class);
        when(mockNodeInfo.getInfo(TransportInfo.class)).thenReturn(mockTransportInfo);

        final BoundTransportAddress bound = new BoundTransportAddress(new TransportAddress[] { transportAddress }, transportAddress);
        when(mockTransportInfo.address()).thenReturn(bound);
        when(mockNodeInfo.getSettings()).thenReturn(
            Settings.builder()
                .put(NetworkModule.TRANSPORT_TYPE_KEY, "_transport")
                .put(NetworkModule.HTTP_TYPE_KEY, "_http")
                .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "_disco")
                .build()
        );

        final PluginsAndModules mockPluginsAndModules = mock(PluginsAndModules.class);
        when(mockNodeInfo.getInfo(PluginsAndModules.class)).thenReturn(mockPluginsAndModules);
        String pluginEsBuildVersion = randomAlphaOfLength(10);
        final PluginDescriptor pluginDescriptor = new PluginDescriptor(
            "_plugin",
            "_plugin_desc",
            "_plugin_version",
            pluginEsBuildVersion,
            "1.8",
            "_plugin_class",
            null,
            Collections.emptyList(),
            false,
            false,
            false,
            false
        );
        final PluginRuntimeInfo pluginRuntimeInfo = new PluginRuntimeInfo(pluginDescriptor);
        when(mockPluginsAndModules.getPluginInfos()).thenReturn(List.of(pluginRuntimeInfo));

        final OsInfo mockOsInfo = mock(OsInfo.class);
        when(mockNodeInfo.getInfo(OsInfo.class)).thenReturn(mockOsInfo);
        when(mockOsInfo.getAvailableProcessors()).thenReturn(32);
        when(mockOsInfo.getAllocatedProcessors()).thenReturn(16);
        when(mockOsInfo.getName()).thenReturn("_os_name");
        when(mockOsInfo.getPrettyName()).thenReturn("_pretty_os_name");
        when(mockOsInfo.getArch()).thenReturn("_architecture");

        final JvmInfo mockJvmInfo = mock(JvmInfo.class);
        when(mockNodeInfo.getInfo(JvmInfo.class)).thenReturn(mockJvmInfo);
        when(mockJvmInfo.version()).thenReturn("_jvm_version");
        when(mockJvmInfo.getVmName()).thenReturn("_jvm_vm_name");
        when(mockJvmInfo.getVmVersion()).thenReturn("_jvm_vm_version");
        when(mockJvmInfo.getVmVendor()).thenReturn("_jvm_vm_vendor");
        when(mockJvmInfo.getUsingBundledJdk()).thenReturn(true);

        when(mockNodeInfo.getBuild()).thenReturn(BuildUtils.newBuild(Build.current(), Map.of("type", Build.Type.DOCKER)));

        final NodeStats mockNodeStats = mock(NodeStats.class);
        when(mockNodeStats.getTimestamp()).thenReturn(0L);

        final FsInfo mockFsInfo = mock(FsInfo.class);
        when(mockNodeStats.getFs()).thenReturn(mockFsInfo);
        when(mockFsInfo.iterator()).thenReturn(Iterators.single(new FsInfo.Path("_fs_path", "_fs_mount", 100L, 49L, 51L)));

        final OsStats mockOsStats = mock(OsStats.class);
        when(mockNodeStats.getOs()).thenReturn(mockOsStats);
        when(mockOsStats.getMem()).thenReturn(new OsStats.Mem(100, 99, 79));

        final ProcessStats mockProcessStats = mock(ProcessStats.class);
        when(mockNodeStats.getProcess()).thenReturn(mockProcessStats);
        when(mockProcessStats.getOpenFileDescriptors()).thenReturn(42L);
        when(mockProcessStats.getCpu()).thenReturn(new ProcessStats.Cpu((short) 3, 32L));

        final JvmStats.Threads mockThreads = mock(JvmStats.Threads.class);
        when(mockThreads.getCount()).thenReturn(9);

        final JvmStats.Mem mockMem = mock(JvmStats.Mem.class);
        when(mockMem.getHeapUsed()).thenReturn(new ByteSizeValue(512, ByteSizeUnit.MB));
        when(mockMem.getHeapMax()).thenReturn(new ByteSizeValue(24, ByteSizeUnit.GB));

        final JvmStats mockJvmStats = mock(JvmStats.class);
        when(mockNodeStats.getJvm()).thenReturn(mockJvmStats);
        when(mockJvmStats.getThreads()).thenReturn(mockThreads);
        when(mockJvmStats.getMem()).thenReturn(mockMem);
        when(mockJvmStats.getUptime()).thenReturn(TimeValue.timeValueHours(3));

        final ShardId shardId = new ShardId("_index", "_index_id", 7);
        final UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "_message");
        final ShardRouting shardRouting = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            unassignedInfo,
            ShardRouting.Role.DEFAULT
        );

        final ShardStats mockShardStats = mock(ShardStats.class);
        when(mockShardStats.getShardRouting()).thenReturn(shardRouting);
        CommonStats commonStats = mock(CommonStats.class);
        when(commonStats.getFieldData()).thenReturn(new FieldDataStats(1, 0, null, new FieldDataStats.GlobalOrdinalsStats(1, null)));
        when(mockShardStats.getStats()).thenReturn(commonStats);

        final ClusterStatsNodeResponse mockNodeResponse = mock(ClusterStatsNodeResponse.class);
        when(mockNodeResponse.clusterStatus()).thenReturn(ClusterHealthStatus.RED);
        when(mockNodeResponse.nodeInfo()).thenReturn(mockNodeInfo);
        when(mockNodeResponse.nodeStats()).thenReturn(mockNodeStats);
        when(mockNodeResponse.shardsStats()).thenReturn(new ShardStats[] { mockShardStats });
        when(mockNodeResponse.searchUsageStats()).thenReturn(new SearchUsageStats());

        final Metadata metadata = testClusterState.metadata();
        final ClusterStatsResponse clusterStatsResponse = new ClusterStatsResponse(
            1451606400000L,
            "_cluster",
            testClusterName,
            singletonList(mockNodeResponse),
            emptyList(),
            MappingStats.of(metadata, () -> {}),
            AnalysisStats.of(metadata, () -> {}),
            VersionStats.of(metadata, singletonList(mockNodeResponse)),
            ClusterSnapshotStats.EMPTY
        );

        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", 1504169190855L);

        final ClusterStatsMonitoringDoc doc = new ClusterStatsMonitoringDoc(
            "_cluster",
            1502107402133L,
            1506593717631L,
            node,
            testClusterName.value(),
            "_version",
            ClusterHealthStatus.GREEN,
            testLicense,
            apmIndicesExist,
            usageList,
            clusterStatsResponse,
            testClusterState,
            needToEnableTLS
        );

        final BytesReference xContent = XContentHelper.toXContent(doc, XContentType.JSON, false);
        Object[] args = new Object[] {
            needToEnableTLS ? ",\"cluster_needs_tls\": true" : "",
            mockNodeVersion,
            pluginEsBuildVersion,
            Version.CURRENT,
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersion.current(),
            apmIndicesExist };
        final String expectedJson = Strings.format("""
            {
              "cluster_uuid": "_cluster",
              "timestamp": "2017-08-07T12:03:22.133Z",
              "interval_ms": 1506593717631,
              "type": "cluster_stats",
              "source_node": {
                "uuid": "_uuid",
                "host": "_host",
                "transport_address": "_addr",
                "ip": "_ip",
                "name": "_name",
                "timestamp": "2017-08-31T08:46:30.855Z"
              },
              "cluster_name": "_cluster_name",
              "version": "_version",
              "license": {
                "status": "expired",
                "uid": "442ca961-9c00-4bb2-b5c9-dfaacd547403",
                "type": "trial",
                "issue_date": "2016-01-01T00:00:00.000Z",
                "issue_date_in_millis": 1451606400000,
                "expiry_date": "2017-08-07T12:03:22.133Z",
                "expiry_date_in_millis": 1502107402133,
                "max_nodes": 2,
                "max_resource_units": null,
                "issued_to": "customer",
                "issuer": "elasticsearch",
                "start_date_in_millis": -1
                %s
              },
              "cluster_stats": {
                "cluster_uuid": "_cluster",
                "timestamp": 1451606400000,
                "status": "red",
                "indices": {
                  "count": 1,
                  "shards": {
                    "total": 1,
                    "primaries": 1,
                    "replication": 0.0,
                    "index": {
                      "shards": {
                        "min": 1,
                        "max": 1,
                        "avg": 1.0
                      },
                      "primaries": {
                        "min": 1,
                        "max": 1,
                        "avg": 1.0
                      },
                      "replication": {
                        "min": 0.0,
                        "max": 0.0,
                        "avg": 0.0
                      }
                    }
                  },
                  "docs": {
                    "count": 0,
                    "deleted": 0,
                    "total_size_in_bytes": 0
                  },
                  "store": {
                    "size_in_bytes": 0,
                    "total_data_set_size_in_bytes": 0,
                    "reserved_in_bytes": 0
                  },
                  "fielddata": {
                    "memory_size_in_bytes": 1,
                    "evictions": 0,
                    "global_ordinals":{"build_time_in_millis":1}
                  },
                  "query_cache": {
                    "memory_size_in_bytes": 0,
                    "total_count": 0,
                    "hit_count": 0,
                    "miss_count": 0,
                    "cache_size": 0,
                    "cache_count": 0,
                    "evictions": 0
                  },
                  "completion": {
                    "size_in_bytes": 0
                  },
                  "segments": {
                    "count": 0,
                    "memory_in_bytes": 0,
                    "terms_memory_in_bytes": 0,
                    "stored_fields_memory_in_bytes": 0,
                    "term_vectors_memory_in_bytes": 0,
                    "norms_memory_in_bytes": 0,
                    "points_memory_in_bytes": 0,
                    "doc_values_memory_in_bytes": 0,
                    "index_writer_memory_in_bytes": 0,
                    "version_map_memory_in_bytes": 0,
                    "fixed_bit_set_memory_in_bytes": 0,
                    "max_unsafe_auto_id_timestamp": -9223372036854775808,
                    "file_sizes": {}
                  },
                  "mappings": {
                    "total_field_count" : 0,
                    "total_deduplicated_field_count" : 0,
                    "total_deduplicated_mapping_size_in_bytes" : 0,
                    "field_types": [],
                    "runtime_field_types": []
                  },
                  "analysis": {
                    "char_filter_types": [],
                    "tokenizer_types": [],
                    "filter_types": [],
                    "analyzer_types": [],
                    "built_in_char_filters": [],
                    "built_in_tokenizers": [],
                    "built_in_filters": [],
                    "built_in_analyzers": [],
                    "synonyms": {}
                  },
                  "versions": [],
                  "search" : {
                    "total" : 0,
                    "queries" : {},
                    "rescorers" : {},
                    "sections" : {}
                  },
                  "dense_vector": {
                    "value_count": 0
                  }
                },
                "nodes": {
                  "count": {
                    "total": 1,
                    "coordinating_only": 0,
                    "data": 0,
                    "data_cold": 0,
                    "data_content": 0,
                    "data_frozen": 0,
                    "data_hot": 0,
                    "data_warm": 0,
                    "index": 0,
                    "ingest": 0,
                    "master": 1,
                    "ml": 0,
                    "remote_cluster_client": 0,
                    "search": 0,
                    "transform": 0,
                    "voting_only": 0
                  },
                  "versions": [
                    "%s"
                  ],
                  "os": {
                    "available_processors": 32,
                    "allocated_processors": 16,
                    "names": [
                      {
                        "name": "_os_name",
                        "count": 1
                      }
                    ],
                    "pretty_names": [
                      {
                        "pretty_name": "_pretty_os_name",
                        "count": 1
                      }
                    ],
                    "architectures": [
                      {
                        "arch": "_architecture",
                        "count": 1
                      }
                    ],
                    "mem": {
                      "total_in_bytes": 100,
                      "adjusted_total_in_bytes": 99,
                      "free_in_bytes": 79,
                      "used_in_bytes": 21,
                      "free_percent": 79,
                      "used_percent": 21
                    }
                  },
                  "process": {
                    "cpu": {
                      "percent": 3
                    },
                    "open_file_descriptors": {
                      "min": 42,
                      "max": 42,
                      "avg": 42
                    }
                  },
                  "jvm": {
                    "max_uptime_in_millis": 10800000,
                    "versions": [
                      {
                        "version": "_jvm_version",
                        "vm_name": "_jvm_vm_name",
                        "vm_version": "_jvm_vm_version",
                        "vm_vendor": "_jvm_vm_vendor",
                        "bundled_jdk": true,
                        "using_bundled_jdk": true,
                        "count": 1
                      }
                    ],
                    "mem": {
                      "heap_used_in_bytes": 536870912,
                      "heap_max_in_bytes": 25769803776
                    },
                    "threads": 9
                  },
                  "fs": {
                    "total_in_bytes": 100,
                    "free_in_bytes": 49,
                    "available_in_bytes": 51
                  },
                  "plugins": [
                    {
                      "name": "_plugin",
                      "version": "_plugin_version",
                      "elasticsearch_version": "%s",
                      "java_version": "1.8",
                      "description": "_plugin_desc",
                      "classname": "_plugin_class",
                      "extended_plugins": [],
                      "has_native_controller": false,
                      "licensed": false,
                      "is_official": false
                    }
                  ],
                  "network_types": {
                    "transport_types": {
                      "_transport": 1
                    },
                    "http_types": {
                      "_http": 1
                    }
                  },
                  "discovery_types": {
                    "_disco": 1
                  },
                  "packaging_types": [
                    {
                      "flavor": "default",
                      "type": "docker",
                      "count": 1
                    }
                  ],
                  "ingest": {
                    "number_of_pipelines": 0,
                    "processor_stats": {}
                  },
                  "indexing_pressure": {
                    "memory": {
                      "current": {
                        "combined_coordinating_and_primary_in_bytes": 0,
                        "coordinating_in_bytes": 0,
                        "primary_in_bytes": 0,
                        "replica_in_bytes": 0,
                        "all_in_bytes": 0
                      },
                      "total": {
                        "combined_coordinating_and_primary_in_bytes": 0,
                        "coordinating_in_bytes": 0,
                        "primary_in_bytes": 0,
                        "replica_in_bytes": 0,
                        "all_in_bytes": 0,
                        "coordinating_rejections": 0,
                        "primary_rejections": 0,
                        "replica_rejections": 0,
                        "primary_document_rejections": 0
                      },
                      "limit_in_bytes": 0
                    }
                  }
                },
                "snapshots": {
                  "current_counts": {
                    "snapshots": 0,
                    "shard_snapshots": 0,
                    "snapshot_deletions": 0,
                    "concurrent_operations": 0,
                    "cleanups": 0
                  },
                  "repositories": {}
                }
              },
              "cluster_state": {
                "nodes_hash": 1314980060,
                "status": "green",
                "cluster_uuid": "_cluster",
                "version": 12,
                "state_uuid": "_state_uuid",
                "master_node": "_node_id",
                "nodes": {
                  "_node_id": {
                    "name": "_node_name",
                    "ephemeral_id": "_ephemeral_id",
                    "transport_address": "0.0.0.0:9300",
                    "external_id": "_external_id",
                    "attributes": {
                      "attr": "value"
                    },
                    "roles": [
                      "master"
                    ],
                    "version": "%s",
                    "min_index_version":%s,
                    "max_index_version":%s
                  }
                },
                "nodes_versions": [],
                "nodes_features": [
                  {
                    "node_id": "_node_id",
                    "features": []
                  }
                ]
              },
              "cluster_settings": {
                "cluster": {
                  "metadata": {
                    "display_name": "my_prod_cluster"
                  }
                }
              },
              "stack_stats": {
                "apm": {
                  "found": %s
                },
                "xpack": {
                  "monitoring": {
                    "available": true,
                    "enabled": true,
                    "collection_enabled": false
                  }
                }
              }
            }""", args);
        assertEquals(stripWhitespace(expectedJson), xContent.utf8ToString());
    }

    private DiscoveryNode masterNode() {
        return DiscoveryNodeUtils.builder("_node_id")
            .name("_node_name")
            .ephemeralId("_ephemeral_id")
            .address("_host_name", "_host_address", new TransportAddress(TransportAddress.META_ADDRESS, 9300))
            .attributes(singletonMap("attr", "value"))
            .roles(singleton(DiscoveryNodeRole.MASTER_ROLE))
            .build();
    }

}
