/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.node;

import org.apache.lucene.util.Constants;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.xpack.monitoring.collector.node.NodeStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolverTestCase;
import org.elasticsearch.xpack.watcher.execution.InternalWatchExecutor;

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

public class NodeStatsResolverTests extends MonitoringIndexNameResolverTestCase<NodeStatsMonitoringDoc, NodeStatsResolver> {

    @Override
    protected NodeStatsMonitoringDoc newMonitoringDoc() {
        NodeStatsMonitoringDoc doc = new NodeStatsMonitoringDoc(randomMonitoringId(),
                randomAlphaOfLength(2), randomAlphaOfLength(5),
                new DiscoveryNode("id", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                randomBoolean(), randomNodeStats(), randomBoolean());
        return doc;
    }

    @Override
    protected void assertSourceField(String field, Map<String, Object> sourceFields) {
        // Assertions on node stats fields that are not reported on Windows platforms
        if (Constants.WINDOWS) {
            if (field.startsWith("node_stats.os.cpu.load_average")) {
                return;
            }
        }

        // we only report IoStats on Linux
        if (Constants.LINUX == false) {
            if (field.startsWith("node_stats.fs.io_stats")) {
                return;
            }
        }

        // node_stats.fs.data.spins can be null and it's only reported on Linux
        if (field.startsWith("node_stats.fs.data.spins")) {
            return;
        }

        // cgroups can be null, and it's only reported on Linux
        if (field.startsWith("node_stats.os.cgroup")) {
            return;
        }

        // load average is unavailable on macOS for 5m and 15m (but we get 1m), but it's also possible on Linux too
        if ("node_stats.os.cpu.load_average.5m".equals(field) || "node_stats.os.cpu.load_average.15m".equals(field)) {
            return;
        }

        super.assertSourceField(field, sourceFields);
    }

    public void testNodeStatsResolver() throws IOException {
        NodeStatsMonitoringDoc doc = newMonitoringDoc();

        NodeStatsResolver resolver = newResolver();
        assertThat(resolver.index(doc), equalTo(".monitoring-es-" + MonitoringTemplateUtils.TEMPLATE_VERSION + "-2015.07.22"));

        assertSource(resolver.source(doc, XContentType.JSON),
                Sets.newHashSet(
                        "cluster_uuid",
                        "timestamp",
                        "type",
                        "source_node",
                        "node_stats"), XContentType.JSON);
    }

    /**
     * @return a random {@link NodeStats} object.
     */
    private NodeStats randomNodeStats() {
        Index index = new Index("test-" + randomIntBetween(0, 5), UUID.randomUUID().toString());
        ShardId shardId = new ShardId(index, 0);
        Path path = createTempDir().resolve("indices").resolve(index.getUUID()).resolve("0");
        ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        shardRouting = shardRouting.initialize("node-0", null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        shardRouting = shardRouting.moveToStarted();
        CommonStats stats = new CommonStats();
        stats.fieldData = new FieldDataStats();
        stats.queryCache = new QueryCacheStats();
        stats.requestCache = new RequestCacheStats();
        stats.docs = new DocsStats();
        stats.store = new StoreStats();
        stats.indexing = new IndexingStats();
        stats.search = new SearchStats();
        stats.segments = new SegmentsStats();
        ShardStats shardStats = new ShardStats(shardRouting, new ShardPath(false, path, path, shardId), stats, null, null);
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
        return new NodeStats(new DiscoveryNode("node_0", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                1437580442979L,
                new NodeIndicesStats(new CommonStats(), statsByShard), OsProbe.getInstance().osStats(),
                ProcessProbe.getInstance().processStats(), JvmStats.jvmStats(),
                new ThreadPoolStats(threadPoolStats),
                new FsInfo(0, randomIoStats(), pathInfo), null, null, null, null, null, null);
    }

    @Nullable
    private FsInfo.IoStats randomIoStats() {
        if (Constants.LINUX) {
            final int stats = randomIntBetween(1, 3);
            final FsInfo.DeviceStats[] devices = new FsInfo.DeviceStats[stats];

            for (int i = 0; i < devices.length; ++i) {
                devices[i] = new FsInfo.DeviceStats(253, 0, "dm-" + i, 287734, 7185242, 8398869, 118857776, null);
            }

            return new FsInfo.IoStats(devices);
        }

        return null;
    }
}
