/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.indices;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponseTestUtils;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.xpack.monitoring.collector.indices.IndicesStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolverTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class IndicesStatsResolverTests extends MonitoringIndexNameResolverTestCase<IndicesStatsMonitoringDoc, IndicesStatsResolver> {

    @Override
    protected IndicesStatsMonitoringDoc newMonitoringDoc() {
        IndicesStatsMonitoringDoc doc = new IndicesStatsMonitoringDoc(randomMonitoringId(),
                randomAlphaOfLength(2), randomAlphaOfLength(5), 1437580442979L,
                new DiscoveryNode("id", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                randomIndicesStats());
        return doc;
    }

    public void testIndicesStatsResolver() throws Exception {
        IndicesStatsMonitoringDoc doc = newMonitoringDoc();

        IndicesStatsResolver resolver = newResolver();
        assertThat(resolver.index(doc), equalTo(".monitoring-es-" + MonitoringTemplateUtils.TEMPLATE_VERSION + "-2015.07.22"));

        assertSource(resolver.source(doc, XContentType.JSON),
                Sets.newHashSet("cluster_uuid", "timestamp", "type", "source_node", "indices_stats"), XContentType.JSON);
    }

    /**
     * @return a random {@link IndicesStatsResponse} object.
     */
    private IndicesStatsResponse randomIndicesStats() {
        Index index = new Index("test", UUID.randomUUID().toString());

        List<ShardStats> shardStats = new ArrayList<>();
        for (int i=0; i < randomIntBetween(2, 5); i++) {
            ShardId shardId = new ShardId(index, i);
            Path path = createTempDir().resolve("indices").resolve(index.getUUID()).resolve(String.valueOf(i));
            ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
            shardRouting = shardRouting.initialize("node-0", null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            shardRouting = shardRouting.moveToStarted();
            CommonStats stats = new CommonStats();
            stats.fieldData = new FieldDataStats();
            stats.queryCache = new QueryCacheStats();
            stats.docs = new DocsStats();
            stats.store = new StoreStats();
            stats.indexing = new IndexingStats();
            stats.search = new SearchStats();
            stats.segments = new SegmentsStats();
            stats.merge = new MergeStats();
            stats.refresh = new RefreshStats();
            shardStats.add(new ShardStats(shardRouting, new ShardPath(false, path, path, shardId), stats, null, null));
        }
        return IndicesStatsResponseTestUtils.newIndicesStatsResponse(shardStats.toArray(new ShardStats[shardStats.size()]),
                shardStats.size(), shardStats.size(), 0, emptyList());
    }
}
