/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsNodeResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.ingest.core.IngestInfo;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStatsMonitoringDoc;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolverTestCase;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.DummyOsInfo;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.transport.TransportInfo;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ClusterStatsResolverTests extends MonitoringIndexNameResolverTestCase<ClusterStatsMonitoringDoc, ClusterStatsResolver> {

    @Override
    protected ClusterStatsMonitoringDoc newMarvelDoc() {
        ClusterStatsMonitoringDoc doc = new ClusterStatsMonitoringDoc(randomMonitoringId(), randomAsciiOfLength(2));
        doc.setClusterUUID(randomAsciiOfLength(5));
        doc.setTimestamp(Math.abs(randomLong()));
        doc.setSourceNode(new DiscoveryNode("id", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT));
        doc.setClusterStats(randomClusterStats());
        return doc;
    }

    @Override
    protected boolean checkResolvedId() {
        return false;
    }

    public void testClusterStatsResolver() throws Exception {
        ClusterStatsMonitoringDoc doc = newMarvelDoc();
        doc.setTimestamp(1437580442979L);

        ClusterStatsResolver resolver = newResolver();
        assertThat(resolver.index(doc), equalTo(".monitoring-es-" + MarvelTemplateUtils.TEMPLATE_VERSION + "-2015.07.22"));
        assertThat(resolver.type(doc), equalTo(ClusterStatsResolver.TYPE));
        assertThat(resolver.id(doc), nullValue());

        assertSource(resolver.source(doc, XContentType.JSON),
                "cluster_uuid",
                "timestamp",
                "source_node",
                "cluster_stats");
    }

    /**
     * @return a testing {@link ClusterStatsResponse} used to resolve a marvel document.
     */
    private ClusterStatsResponse randomClusterStats() {
        ClusterStatsNodeResponse[] responses = {
                new ClusterStatsNodeResponse(new DiscoveryNode("node_0", DummyTransportAddress.INSTANCE,
                        emptyMap(), emptySet(), Version.CURRENT),
                        ClusterHealthStatus.GREEN, randomNodeInfo(), randomNodeStats(), randomShardStats())
        };
        return new ClusterStatsResponse(Math.abs(randomLong()), ClusterName.DEFAULT, UUID.randomUUID().toString(), responses);
    }

    /**
     * @return a random {@link NodeInfo} used to resolve a marvel document.
     */
    private NodeInfo randomNodeInfo() {
        BoundTransportAddress transportAddress = new BoundTransportAddress(new TransportAddress[]{DummyTransportAddress.INSTANCE},
                DummyTransportAddress.INSTANCE);
        return new NodeInfo(Version.CURRENT, org.elasticsearch.Build.CURRENT,
                new DiscoveryNode("node_0", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT), emptyMap(),
                Settings.EMPTY, DummyOsInfo.INSTANCE, new ProcessInfo(randomInt(), randomBoolean()), JvmInfo.jvmInfo(),
                new ThreadPoolInfo(Collections.singletonList(new ThreadPool.Info("test_threadpool", ThreadPool.ThreadPoolType.FIXED, 5))),
                new TransportInfo(transportAddress, Collections.emptyMap()), new HttpInfo(transportAddress, randomLong()),
                new PluginsAndModules(), new IngestInfo(Collections.emptyList()));

    }

    /**
     * @return a random {@link NodeStats} used to resolve a marvel document.
     */
    private NodeStats randomNodeStats() {
        Index index = new Index("test", UUID.randomUUID().toString());
        FsInfo.Path[] pathInfo = new FsInfo.Path[]{
                new FsInfo.Path("/test", "/dev/sda", 10, -8, 0),
        };
        Map<Index, List<IndexShardStats>> statsByShard = new HashMap<>();
        statsByShard.put(index, Collections.singletonList(new IndexShardStats(new ShardId(index, 0), randomShardStats())));
        return new NodeStats(new DiscoveryNode("node_0", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT), 0,
                new NodeIndicesStats(new CommonStats(), statsByShard), null, null, null, null,
                new FsInfo(0, pathInfo), null, null, null, null, null, null);
    }

    /**
     * @return a random ShardStats[] used to resolve a marvel document.
     */
    private ShardStats[] randomShardStats() {
        Index index = new Index("test", UUID.randomUUID().toString());
        Path shardPath = createTempDir().resolve("indices").resolve(index.getUUID()).resolve("0");
        ShardRouting shardRouting = ShardRouting.newUnassigned(index, 0, null, false,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
        CommonStats shardCommonStats = new CommonStats();
        shardCommonStats.fieldData = new FieldDataStats();
        shardCommonStats.queryCache = new QueryCacheStats();
        return new ShardStats[]{
                new ShardStats(shardRouting, new ShardPath(false, shardPath, shardPath, new ShardId(index, 0)), shardCommonStats, null)
        };
    }
}
