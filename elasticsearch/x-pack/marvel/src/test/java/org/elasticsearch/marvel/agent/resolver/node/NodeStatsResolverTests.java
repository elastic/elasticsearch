/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.node;

import org.apache.lucene.util.Constants;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingTestUtils;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.marvel.agent.collector.node.NodeStatsMonitoringDoc;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolverTestCase;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.watcher.execution.InternalWatchExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class NodeStatsResolverTests extends MonitoringIndexNameResolverTestCase<NodeStatsMonitoringDoc, NodeStatsResolver> {

    @Override
    protected NodeStatsMonitoringDoc newMarvelDoc() {
        NodeStatsMonitoringDoc doc = new NodeStatsMonitoringDoc(randomMonitoringId(), randomAsciiOfLength(2));
        doc.setClusterUUID(randomAsciiOfLength(5));
        doc.setTimestamp(Math.abs(randomLong()));
        doc.setSourceNode(new DiscoveryNode("id", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT));
        doc.setNodeMaster(randomBoolean());
        doc.setNodeId(UUID.randomUUID().toString());
        doc.setDiskThresholdDeciderEnabled(randomBoolean());
        doc.setDiskThresholdWaterMarkHigh(randomDouble());
        doc.setMlockall(randomBoolean());
        doc.setNodeStats(randomNodeStats());
        return doc;
    }

    @Override
    protected boolean checkResolvedId() {
        return false;
    }

    @Override
    protected void assertSourceField(String field, Map<String, Object> sourceFields) {
        // Assertions on node stats fields that are not reported on Windows platforms
        if (Constants.WINDOWS && field.startsWith("node_stats.os.cpu.load_average")) {
            return;
        }
        super.assertSourceField(field, sourceFields);
    }

    public void testNodeStatsResolver() throws IOException {
        NodeStatsMonitoringDoc doc = newMarvelDoc();
        doc.setTimestamp(1437580442979L);

        NodeStatsResolver resolver = newResolver();
        assertThat(resolver.index(doc), equalTo(".monitoring-es-" + MarvelTemplateUtils.TEMPLATE_VERSION + "-2015.07.22"));
        assertThat(resolver.type(doc), equalTo(NodeStatsResolver.TYPE));
        assertThat(resolver.id(doc), nullValue());

        assertSource(resolver.source(doc, XContentType.JSON),
                "cluster_uuid",
                "timestamp",
                "source_node",
                "node_stats");
    }

    /**
     * @return a random {@link NodeStats} object.
     */
    private NodeStats randomNodeStats() {
        Index index = new Index("test-" + randomIntBetween(0, 5), UUID.randomUUID().toString());
        ShardId shardId = new ShardId(index, 0);
        Path path = createTempDir().resolve("indices").resolve(index.getUUID()).resolve("0");
        ShardRouting shardRouting = ShardRouting.newUnassigned(index, 0, null, true,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        ShardRoutingTestUtils.initialize(shardRouting, "node-0");
        ShardRoutingTestUtils.moveToStarted(shardRouting);
        CommonStats stats = new CommonStats();
        stats.fieldData = new FieldDataStats();
        stats.queryCache = new QueryCacheStats();
        stats.docs = new DocsStats();
        stats.store = new StoreStats();
        stats.indexing = new IndexingStats();
        stats.search = new SearchStats();
        stats.segments = new SegmentsStats();
        ShardStats shardStats = new ShardStats(shardRouting, new ShardPath(false, path, path, shardId), stats, null);
        FsInfo.Path[] pathInfo = new FsInfo.Path[]{
                new FsInfo.Path("/test", "/dev/sda", 10, -8, 0),
        };
        Map<Index, List<IndexShardStats>> statsByShard = new HashMap<>();
        statsByShard.put(index, Collections.singletonList(new IndexShardStats(shardId, new ShardStats[]{shardStats})));
        List<ThreadPoolStats.Stats> threadPoolStats = Arrays.asList(
                new ThreadPoolStats.Stats(ThreadPool.Names.BULK, 0, 0, 0, 0, 0, 0),
                new ThreadPoolStats.Stats(ThreadPool.Names.GENERIC, 0, 0, 0, 0, 0, 0),
                new ThreadPoolStats.Stats(ThreadPool.Names.GET, 0, 0, 0, 0, 0, 0),
                new ThreadPoolStats.Stats(ThreadPool.Names.INDEX, 0, 0, 0, 0, 0, 0),
                new ThreadPoolStats.Stats(ThreadPool.Names.MANAGEMENT, 0, 0, 0, 0, 0, 0),
                new ThreadPoolStats.Stats(ThreadPool.Names.SEARCH, 0, 0, 0, 0, 0, 0),
                new ThreadPoolStats.Stats(InternalWatchExecutor.THREAD_POOL_NAME, 0, 0, 0, 0, 0, 0)
        );
        return new NodeStats(new DiscoveryNode("node_0", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT), 0,
                new NodeIndicesStats(new CommonStats(), statsByShard), OsProbe.getInstance().osStats(),
                ProcessProbe.getInstance().processStats(), JvmStats.jvmStats(),
                new ThreadPoolStats(threadPoolStats),
                new FsInfo(0, pathInfo), null, null, null, null, null, null);
    }
}
