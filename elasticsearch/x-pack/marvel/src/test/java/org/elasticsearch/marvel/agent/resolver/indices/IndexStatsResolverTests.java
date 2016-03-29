/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.indices;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
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
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.marvel.agent.collector.indices.IndexStatsMonitoringDoc;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolverTestCase;

import java.nio.file.Path;
import java.util.UUID;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class IndexStatsResolverTests extends MonitoringIndexNameResolverTestCase<IndexStatsMonitoringDoc, IndexStatsResolver> {

    @Override
    protected IndexStatsMonitoringDoc newMarvelDoc() {
        IndexStatsMonitoringDoc doc = new IndexStatsMonitoringDoc(randomMonitoringId(), randomAsciiOfLength(2));
        doc.setClusterUUID(randomAsciiOfLength(5));
        doc.setTimestamp(Math.abs(randomLong()));
        doc.setSourceNode(new DiscoveryNode("id", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT));
        doc.setIndexStats(randomIndexStats());
        return doc;
    }

    @Override
    protected boolean checkResolvedId() {
        return false;
    }

    public void testIndexStatsResolver() throws Exception {
        IndexStatsMonitoringDoc doc = newMarvelDoc();
        doc.setTimestamp(1437580442979L);
        doc.setSourceNode(new DiscoveryNode("id", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT));

        IndexStatsResolver resolver = newResolver();
        assertThat(resolver.index(doc), equalTo(".monitoring-es-" + MarvelTemplateUtils.TEMPLATE_VERSION + "-2015.07.22"));
        assertThat(resolver.type(doc), equalTo(IndexStatsResolver.TYPE));
        assertThat(resolver.id(doc), nullValue());

        assertSource(resolver.source(doc, XContentType.JSON),
                "cluster_uuid",
                "timestamp",
                "source_node",
                "index_stats");
    }

    /**
     * @return a random {@link IndexStats} object.
     */
    private IndexStats randomIndexStats() {
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
        stats.merge = new MergeStats();
        stats.refresh = new RefreshStats();
        ShardStats shardStats = new ShardStats(shardRouting, new ShardPath(false, path, path, shardId), stats, null);
        return new IndexStats(index.getName(), new ShardStats[]{shardStats});
    }
}
