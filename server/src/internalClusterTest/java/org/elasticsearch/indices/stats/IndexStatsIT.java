/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.stats;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = Scope.SUITE, numDataNodes = 2, numClientNodes = 0)
@SuppressCodecs("*") // requires custom completion format
public class IndexStatsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        // Filter/Query cache is cleaned periodically, default is 60s, so make sure it runs often. Thread.sleep for 60s is bad
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(IndicesService.INDICES_CACHE_CLEAN_INTERVAL_SETTING.getKey(), "1ms")
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true)
            .build();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 0)
            .build();
    }

    private Settings.Builder settingsBuilder() {
        return Settings.builder().put(indexSettings());
    }

    public void testFieldDataStats() {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 2))
                .setMapping("field", "type=text,fielddata=true", "field2", "type=text,fielddata=true")
        );
        ensureGreen();
        prepareIndex("test").setId("1").setSource("field", "value1", "field2", "value1").get();
        prepareIndex("test").setId("2").setSource("field", "value2", "field2", "value2").get();
        indicesAdmin().prepareRefresh().get();

        NodesStatsResponse nodesStats = clusterAdmin().prepareNodesStats("data:true").setIndices(true).get();
        assertThat(
            nodesStats.getNodes().get(0).getIndices().getFieldData().getMemorySizeInBytes() + nodesStats.getNodes()
                .get(1)
                .getIndices()
                .getFieldData()
                .getMemorySizeInBytes(),
            equalTo(0L)
        );
        IndicesStatsResponse indicesStats = indicesAdmin().prepareStats("test").clear().setFieldData(true).get();
        assertThat(indicesStats.getTotal().getFieldData().getMemorySizeInBytes(), equalTo(0L));

        // sort to load it to field data...
        prepareSearch().addSort("field", SortOrder.ASC).get().decRef();
        prepareSearch().addSort("field", SortOrder.ASC).get().decRef();

        nodesStats = clusterAdmin().prepareNodesStats("data:true").setIndices(true).get();
        assertThat(
            nodesStats.getNodes().get(0).getIndices().getFieldData().getMemorySizeInBytes() + nodesStats.getNodes()
                .get(1)
                .getIndices()
                .getFieldData()
                .getMemorySizeInBytes(),
            greaterThan(0L)
        );
        indicesStats = indicesAdmin().prepareStats("test").clear().setFieldData(true).get();
        assertThat(indicesStats.getTotal().getFieldData().getMemorySizeInBytes(), greaterThan(0L));

        // sort to load it to field data...
        prepareSearch().addSort("field2", SortOrder.ASC).get().decRef();
        prepareSearch().addSort("field2", SortOrder.ASC).get().decRef();

        // now check the per field stats
        nodesStats = clusterAdmin().prepareNodesStats("data:true")
            .setIndices(new CommonStatsFlags().set(CommonStatsFlags.Flag.FieldData, true).fieldDataFields("*"))
            .get();
        assertThat(
            nodesStats.getNodes().get(0).getIndices().getFieldData().getMemorySizeInBytes() + nodesStats.getNodes()
                .get(1)
                .getIndices()
                .getFieldData()
                .getMemorySizeInBytes(),
            greaterThan(0L)
        );
        assertThat(
            nodesStats.getNodes().get(0).getIndices().getFieldData().getFields().get("field") + nodesStats.getNodes()
                .get(1)
                .getIndices()
                .getFieldData()
                .getFields()
                .get("field"),
            greaterThan(0L)
        );
        assertThat(
            nodesStats.getNodes().get(0).getIndices().getFieldData().getFields().get("field") + nodesStats.getNodes()
                .get(1)
                .getIndices()
                .getFieldData()
                .getFields()
                .get("field"),
            lessThan(
                nodesStats.getNodes().get(0).getIndices().getFieldData().getMemorySizeInBytes() + nodesStats.getNodes()
                    .get(1)
                    .getIndices()
                    .getFieldData()
                    .getMemorySizeInBytes()
            )
        );

        indicesStats = indicesAdmin().prepareStats("test").clear().setFieldData(true).setFieldDataFields("*").get();
        assertThat(indicesStats.getTotal().getFieldData().getMemorySizeInBytes(), greaterThan(0L));
        assertThat(indicesStats.getTotal().getFieldData().getFields().get("field"), greaterThan(0L));
        assertThat(
            indicesStats.getTotal().getFieldData().getFields().get("field"),
            lessThan(indicesStats.getTotal().getFieldData().getMemorySizeInBytes())
        );

        indicesAdmin().prepareClearCache().setFieldDataCache(true).get();
        nodesStats = clusterAdmin().prepareNodesStats("data:true").setIndices(true).get();
        assertThat(
            nodesStats.getNodes().get(0).getIndices().getFieldData().getMemorySizeInBytes() + nodesStats.getNodes()
                .get(1)
                .getIndices()
                .getFieldData()
                .getMemorySizeInBytes(),
            equalTo(0L)
        );
        indicesStats = indicesAdmin().prepareStats("test").clear().setFieldData(true).get();
        assertThat(indicesStats.getTotal().getFieldData().getMemorySizeInBytes(), equalTo(0L));

    }

    public void testClearAllCaches() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_replicas", 0).put("index.number_of_shards", 2))
                .setMapping("field", "type=text,fielddata=true")
        );
        ensureGreen();
        clusterAdmin().prepareHealth().setWaitForGreenStatus().get();
        prepareIndex("test").setId("1").setSource("field", "value1").get();
        prepareIndex("test").setId("2").setSource("field", "value2").get();
        indicesAdmin().prepareRefresh().get();

        NodesStatsResponse nodesStats = clusterAdmin().prepareNodesStats("data:true").setIndices(true).get();
        assertThat(
            nodesStats.getNodes().get(0).getIndices().getFieldData().getMemorySizeInBytes() + nodesStats.getNodes()
                .get(1)
                .getIndices()
                .getFieldData()
                .getMemorySizeInBytes(),
            equalTo(0L)
        );
        assertThat(
            nodesStats.getNodes().get(0).getIndices().getQueryCache().getMemorySizeInBytes() + nodesStats.getNodes()
                .get(1)
                .getIndices()
                .getQueryCache()
                .getMemorySizeInBytes(),
            equalTo(0L)
        );

        IndicesStatsResponse indicesStats = indicesAdmin().prepareStats("test").clear().setFieldData(true).setQueryCache(true).get();
        assertThat(indicesStats.getTotal().getFieldData().getMemorySizeInBytes(), equalTo(0L));
        assertThat(indicesStats.getTotal().getQueryCache().getMemorySizeInBytes(), equalTo(0L));

        // sort to load it to field data and filter to load filter cache
        prepareSearch().setPostFilter(QueryBuilders.termQuery("field", "value1")).addSort("field", SortOrder.ASC).get().decRef();
        prepareSearch().setPostFilter(QueryBuilders.termQuery("field", "value2")).addSort("field", SortOrder.ASC).get().decRef();

        nodesStats = clusterAdmin().prepareNodesStats("data:true").setIndices(true).get();
        assertThat(
            nodesStats.getNodes().get(0).getIndices().getFieldData().getMemorySizeInBytes() + nodesStats.getNodes()
                .get(1)
                .getIndices()
                .getFieldData()
                .getMemorySizeInBytes(),
            greaterThan(0L)
        );
        assertThat(
            nodesStats.getNodes().get(0).getIndices().getQueryCache().getMemorySizeInBytes() + nodesStats.getNodes()
                .get(1)
                .getIndices()
                .getQueryCache()
                .getMemorySizeInBytes(),
            greaterThan(0L)
        );

        indicesStats = indicesAdmin().prepareStats("test").clear().setFieldData(true).setQueryCache(true).get();
        assertThat(indicesStats.getTotal().getFieldData().getMemorySizeInBytes(), greaterThan(0L));
        assertThat(indicesStats.getTotal().getQueryCache().getMemorySizeInBytes(), greaterThan(0L));

        indicesAdmin().prepareClearCache().get();
        Thread.sleep(100); // Make sure the filter cache entries have been removed...
        nodesStats = clusterAdmin().prepareNodesStats("data:true").setIndices(true).get();
        assertThat(
            nodesStats.getNodes().get(0).getIndices().getFieldData().getMemorySizeInBytes() + nodesStats.getNodes()
                .get(1)
                .getIndices()
                .getFieldData()
                .getMemorySizeInBytes(),
            equalTo(0L)
        );
        assertThat(
            nodesStats.getNodes().get(0).getIndices().getQueryCache().getMemorySizeInBytes() + nodesStats.getNodes()
                .get(1)
                .getIndices()
                .getQueryCache()
                .getMemorySizeInBytes(),
            equalTo(0L)
        );

        indicesStats = indicesAdmin().prepareStats("test").clear().setFieldData(true).setQueryCache(true).get();
        assertThat(indicesStats.getTotal().getFieldData().getMemorySizeInBytes(), equalTo(0L));
        assertThat(indicesStats.getTotal().getQueryCache().getMemorySizeInBytes(), equalTo(0L));
    }

    public void testQueryCache() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("idx")
                .setSettings(Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true))
        );
        ensureGreen();

        // index docs until we have at least one doc on each shard, otherwise, our tests will not work
        // since refresh will not refresh anything on a shard that has 0 docs and its search response get cached
        int pageDocs = randomIntBetween(2, 100);
        int numDocs = 0;
        int counter = 0;
        while (true) {
            IndexRequestBuilder[] builders = new IndexRequestBuilder[pageDocs];
            for (int i = 0; i < pageDocs; ++i) {
                builders[i] = prepareIndex("idx").setId(Integer.toString(counter++))
                    .setSource(jsonBuilder().startObject().field("common", "field").field("str_value", "s" + i).endObject());
            }
            indexRandom(true, builders);
            numDocs += pageDocs;

            boolean allHaveDocs = true;
            for (ShardStats stats : indicesAdmin().prepareStats("idx").setDocs(true).get().getShards()) {
                if (stats.getStats().getDocs().getCount() == 0) {
                    allHaveDocs = false;
                    break;
                }
            }

            if (allHaveDocs) {
                break;
            }
        }

        assertThat(
            indicesAdmin().prepareStats("idx").setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(),
            equalTo(0L)
        );
        assertThat(indicesAdmin().prepareStats("idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(), equalTo(0L));
        assertThat(indicesAdmin().prepareStats("idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(), equalTo(0L));
        for (int i = 0; i < 10; i++) {
            assertHitCount(prepareSearch("idx").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0), numDocs);
            assertThat(
                indicesAdmin().prepareStats("idx").setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(),
                greaterThan(0L)
            );
        }
        assertThat(
            indicesAdmin().prepareStats("idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            greaterThan(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            greaterThan(0L)
        );

        // index the data again...
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; ++i) {
            builders[i] = prepareIndex("idx").setId(Integer.toString(i))
                .setSource(jsonBuilder().startObject().field("common", "field").field("str_value", "s" + i).endObject());
        }
        indexRandom(true, builders);
        refresh();
        assertBusy(() -> {
            assertThat(
                indicesAdmin().prepareStats("idx").setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(),
                equalTo(0L)
            );
        });

        for (int i = 0; i < 10; i++) {
            assertHitCount(prepareSearch("idx").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0), numDocs);
            assertThat(
                indicesAdmin().prepareStats("idx").setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(),
                greaterThan(0L)
            );
        }

        indicesAdmin().prepareClearCache().setRequestCache(true).get(); // clean the cache
        assertThat(
            indicesAdmin().prepareStats("idx").setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(),
            equalTo(0L)
        );

        // test explicit request parameter

        assertHitCount(prepareSearch("idx").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0).setRequestCache(false), numDocs);
        assertThat(
            indicesAdmin().prepareStats("idx").setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(),
            equalTo(0L)
        );

        assertHitCount(prepareSearch("idx").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0).setRequestCache(true), numDocs);
        assertThat(
            indicesAdmin().prepareStats("idx").setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(),
            greaterThan(0L)
        );

        // set the index level setting to false, and see that the reverse works

        indicesAdmin().prepareClearCache().setRequestCache(true).get(); // clean the cache
        updateIndexSettings(Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), false), "idx");

        assertHitCount(prepareSearch("idx").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0), numDocs);
        assertThat(
            indicesAdmin().prepareStats("idx").setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(),
            equalTo(0L)
        );

        assertHitCount(prepareSearch("idx").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0).setRequestCache(true), numDocs);
        assertThat(
            indicesAdmin().prepareStats("idx").setRequestCache(true).get().getTotal().getRequestCache().getMemorySizeInBytes(),
            greaterThan(0L)
        );
    }

    public void testNonThrottleStats() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(
                settingsBuilder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "1")
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "0")
                    .put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), "2")
                    .put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), "2")
                    .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
                    .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "10000")
            )
        );
        ensureGreen();
        long termUpto = 0;
        IndicesStatsResponse stats;
        // Provoke slowish merging by making many unique terms:
        for (int i = 0; i < 100; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < 100; j++) {
                sb.append(' ');
                sb.append(termUpto++);
                sb.append(" some random text that keeps repeating over and over again hambone");
            }
            prepareIndex("test").setId("" + termUpto).setSource("field" + (i % 10), sb.toString()).get();
        }
        refresh();
        stats = indicesAdmin().prepareStats().get();
        // nodesStats = clusterAdmin().prepareNodesStats().setIndices(true).get();

        stats = indicesAdmin().prepareStats().get();
        assertThat(stats.getPrimaries().getIndexing().getTotal().getThrottleTime().millis(), equalTo(0L));
    }

    public void testThrottleStats() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(
                settingsBuilder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "1")
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "0")
                    .put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), "2")
                    .put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), "2")
                    .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
                    .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "1")
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC.name())
            )
        );
        ensureGreen();
        long termUpto = 0;
        IndicesStatsResponse stats;
        // make sure we see throttling kicking in:
        boolean done = false;
        long start = System.currentTimeMillis();
        while (done == false) {
            for (int i = 0; i < 100; i++) {
                // Provoke slowish merging by making many unique terms:
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < 100; j++) {
                    sb.append(' ');
                    sb.append(termUpto++);
                }
                prepareIndex("test").setId("" + termUpto).setSource("field" + (i % 10), sb.toString()).get();
                if (i % 2 == 0) {
                    refresh();
                }
            }
            refresh();
            stats = indicesAdmin().prepareStats().get();
            // nodesStats = clusterAdmin().prepareNodesStats().setIndices(true).get();
            done = stats.getPrimaries().getIndexing().getTotal().getThrottleTime().millis() > 0;
            if (System.currentTimeMillis() - start > 300 * 1000) { // Wait 5 minutes for throttling to kick in
                fail("index throttling didn't kick in after 5 minutes of intense merging");
            }
        }

        // Optimize & flush and wait; else we sometimes get a "Delete Index failed - not acked"
        // when ESIntegTestCase.after tries to remove indices created by the test:
        logger.info("test: now optimize");
        indicesAdmin().prepareForceMerge("test").get();
        flush();
        logger.info("test: test done");
    }

    public void testSimpleStats() throws Exception {
        createIndex("test1", "test2");
        ensureGreen();

        prepareIndex("test1").setId(Integer.toString(1)).setSource("field", "value").get();
        prepareIndex("test1").setId(Integer.toString(2)).setSource("field", "value").get();
        prepareIndex("test2").setId(Integer.toString(1)).setSource("field", "value").get();
        refresh();

        NumShards test1 = getNumShards("test1");
        long test1ExpectedWrites = 2 * test1.dataCopies;
        NumShards test2 = getNumShards("test2");
        long test2ExpectedWrites = test2.dataCopies;
        long totalExpectedWrites = test1ExpectedWrites + test2ExpectedWrites;

        IndicesStatsResponse stats = indicesAdmin().prepareStats().get();
        assertThat(stats.getPrimaries().getDocs().getCount(), equalTo(3L));
        assertThat(stats.getTotal().getDocs().getCount(), equalTo(totalExpectedWrites));
        assertThat(stats.getPrimaries().getIndexing().getTotal().getIndexCount(), equalTo(3L));
        assertThat(stats.getPrimaries().getIndexing().getTotal().getIndexFailedCount(), equalTo(0L));
        assertThat(stats.getPrimaries().getIndexing().getTotal().isThrottled(), equalTo(false));
        assertThat(stats.getPrimaries().getIndexing().getTotal().getThrottleTime().millis(), equalTo(0L));
        assertThat(stats.getTotal().getIndexing().getTotal().getIndexCount(), equalTo(totalExpectedWrites));
        assertThat(stats.getTotal().getStore(), notNullValue());
        assertThat(stats.getTotal().getMerge(), notNullValue());
        assertThat(stats.getTotal().getFlush(), notNullValue());
        assertThat(stats.getTotal().getRefresh(), notNullValue());

        assertThat(stats.getIndex("test1").getPrimaries().getDocs().getCount(), equalTo(2L));
        assertThat(stats.getIndex("test1").getTotal().getDocs().getCount(), equalTo(test1ExpectedWrites));
        assertThat(stats.getIndex("test1").getPrimaries().getStore(), notNullValue());
        assertThat(stats.getIndex("test1").getPrimaries().getMerge(), notNullValue());
        assertThat(stats.getIndex("test1").getPrimaries().getFlush(), notNullValue());
        assertThat(stats.getIndex("test1").getPrimaries().getRefresh(), notNullValue());

        assertThat(stats.getIndex("test2").getPrimaries().getDocs().getCount(), equalTo(1L));
        assertThat(stats.getIndex("test2").getTotal().getDocs().getCount(), equalTo(test2ExpectedWrites));

        // make sure that number of requests in progress is 0
        assertThat(stats.getIndex("test1").getTotal().getIndexing().getTotal().getIndexCurrent(), equalTo(0L));
        assertThat(stats.getIndex("test1").getTotal().getIndexing().getTotal().getDeleteCurrent(), equalTo(0L));
        assertThat(stats.getIndex("test1").getTotal().getSearch().getTotal().getFetchCurrent(), equalTo(0L));
        assertThat(stats.getIndex("test1").getTotal().getSearch().getTotal().getQueryCurrent(), equalTo(0L));
        assertThat(stats.getIndex("test2").getTotal().getIndexing().getTotal().getIndexCurrent(), equalTo(0L));
        assertThat(stats.getIndex("test2").getTotal().getIndexing().getTotal().getDeleteCurrent(), equalTo(0L));
        assertThat(stats.getIndex("test2").getTotal().getSearch().getTotal().getFetchCurrent(), equalTo(0L));
        assertThat(stats.getIndex("test2").getTotal().getSearch().getTotal().getQueryCurrent(), equalTo(0L));

        // check flags
        stats = indicesAdmin().prepareStats().clear().setFlush(true).setRefresh(true).setMerge(true).get();

        assertThat(stats.getTotal().getDocs(), nullValue());
        assertThat(stats.getTotal().getStore(), nullValue());
        assertThat(stats.getTotal().getIndexing(), nullValue());
        assertThat(stats.getTotal().getMerge(), notNullValue());
        assertThat(stats.getTotal().getFlush(), notNullValue());
        assertThat(stats.getTotal().getRefresh(), notNullValue());

        // check get
        GetResponse getResponse = client().prepareGet("test2", "1").get();
        assertThat(getResponse.isExists(), equalTo(true));

        stats = indicesAdmin().prepareStats().get();
        assertThat(stats.getTotal().getGet().getCount(), equalTo(1L));
        assertThat(stats.getTotal().getGet().getExistsCount(), equalTo(1L));
        assertThat(stats.getTotal().getGet().getMissingCount(), equalTo(0L));

        // missing get
        getResponse = client().prepareGet("test2", "2").get();
        assertThat(getResponse.isExists(), equalTo(false));

        stats = indicesAdmin().prepareStats().get();
        assertThat(stats.getTotal().getGet().getCount(), equalTo(2L));
        assertThat(stats.getTotal().getGet().getExistsCount(), equalTo(1L));
        assertThat(stats.getTotal().getGet().getMissingCount(), equalTo(1L));

        // clear all
        stats = indicesAdmin().prepareStats()
            .setDocs(false)
            .setStore(false)
            .setIndexing(false)
            .setFlush(true)
            .setRefresh(true)
            .setMerge(true)
            .clear() // reset defaults
            .get();

        assertThat(stats.getTotal().getDocs(), nullValue());
        assertThat(stats.getTotal().getStore(), nullValue());
        assertThat(stats.getTotal().getIndexing(), nullValue());
        assertThat(stats.getTotal().getGet(), nullValue());
        assertThat(stats.getTotal().getSearch(), nullValue());

        // index failed
        try {
            prepareIndex("test1").setId(Integer.toString(1))
                .setSource("field", "value")
                .setVersion(1)
                .setVersionType(VersionType.EXTERNAL)
                .get();
            fail("Expected a version conflict");
        } catch (VersionConflictEngineException e) {}
        try {
            prepareIndex("test2").setId(Integer.toString(1))
                .setSource("field", "value")
                .setVersion(1)
                .setVersionType(VersionType.EXTERNAL)
                .get();
            fail("Expected a version conflict");
        } catch (VersionConflictEngineException e) {}
        try {
            prepareIndex("test2").setId(Integer.toString(1))
                .setSource("field", "value")
                .setVersion(1)
                .setVersionType(VersionType.EXTERNAL)
                .get();
            fail("Expected a version conflict");
        } catch (VersionConflictEngineException e) {}

        stats = indicesAdmin().prepareStats().get();
        assertThat(stats.getIndex("test2").getPrimaries().getIndexing().getTotal().getIndexFailedCount(), equalTo(2L));
        assertThat(stats.getPrimaries().getIndexing().getTotal().getIndexFailedCount(), equalTo(3L));
    }

    public void testMergeStats() {
        assertAcked(prepareCreate("test_index"));

        ensureGreen();

        // clear all
        IndicesStatsResponse stats = indicesAdmin().prepareStats()
            .setDocs(false)
            .setStore(false)
            .setIndexing(false)
            .setFlush(true)
            .setRefresh(true)
            .setMerge(true)
            .clear() // reset defaults
            .get();

        assertThat(stats.getTotal().getDocs(), nullValue());
        assertThat(stats.getTotal().getStore(), nullValue());
        assertThat(stats.getTotal().getIndexing(), nullValue());
        assertThat(stats.getTotal().getGet(), nullValue());
        assertThat(stats.getTotal().getSearch(), nullValue());

        for (int i = 0; i < 20; i++) {
            prepareIndex("test_index").setId(Integer.toString(i)).setSource("field", "value").get();
            indicesAdmin().prepareFlush().get();
        }
        indicesAdmin().prepareForceMerge().setMaxNumSegments(1).get();
        stats = indicesAdmin().prepareStats().setMerge(true).get();

        assertThat(stats.getTotal().getMerge(), notNullValue());
        assertThat(stats.getTotal().getMerge().getTotal(), greaterThan(0L));
    }

    public void testSegmentsStats() {
        assertAcked(
            prepareCreate("test_index").setSettings(
                Settings.builder()
                    .put(SETTING_NUMBER_OF_REPLICAS, between(0, 1))
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
            )
        );
        ensureGreen();

        NumShards test1 = getNumShards("test_index");

        for (int i = 0; i < 100; i++) {
            indexDoc("test_index", Integer.toString(i), "field", "value");
        }

        IndicesStatsResponse stats = indicesAdmin().prepareStats().setSegments(true).get();
        assertThat(stats.getTotal().getSegments().getIndexWriterMemoryInBytes(), greaterThan(0L));
        assertThat(stats.getTotal().getSegments().getVersionMapMemoryInBytes(), greaterThan(0L));

        indicesAdmin().prepareFlush().get();
        indicesAdmin().prepareForceMerge().setMaxNumSegments(1).get();
        indicesAdmin().prepareRefresh().get();

        final boolean includeSegmentFileSizes = randomBoolean();
        stats = indicesAdmin().prepareStats().setSegments(true).setIncludeSegmentFileSizes(includeSegmentFileSizes).get();

        assertThat(stats.getTotal().getSegments(), notNullValue());
        assertThat(stats.getTotal().getSegments().getCount(), equalTo((long) test1.totalNumShards));
        if (includeSegmentFileSizes) {
            assertThat(stats.getTotal().getSegments().getFiles().size(), greaterThan(0));
            for (Map.Entry<String, SegmentsStats.FileStats> cursor : stats.getTotal().getSegments().getFiles().entrySet()) {
                assertThat(cursor.getValue().getExt(), notNullValue());
                assertThat(cursor.getValue().getTotal(), greaterThan(0L));
                assertThat(cursor.getValue().getCount(), greaterThan(0L));
                assertThat(cursor.getValue().getMin(), greaterThan(0L));
                assertThat(cursor.getValue().getMax(), greaterThan(0L));
            }
        }
    }

    public void testAllFlags() throws Exception {
        // rely on 1 replica for this tests
        assertAcked(prepareCreate("test_index"));
        createIndex("test_index_2");

        ensureGreen();

        prepareIndex("test_index").setId(Integer.toString(1)).setSource("field", "value").get();
        prepareIndex("test_index").setId(Integer.toString(2)).setSource("field", "value").get();
        prepareIndex("test_index_2").setId(Integer.toString(1)).setSource("field", "value").get();

        indicesAdmin().prepareRefresh().get();
        IndicesStatsRequestBuilder builder = indicesAdmin().prepareStats();
        Flag[] values = CommonStatsFlags.SHARD_LEVEL.getFlags();
        for (Flag flag : values) {
            set(flag, builder, false);
        }

        IndicesStatsResponse stats = builder.get();
        for (Flag flag : values) {
            assertThat(isSet(flag, stats.getPrimaries()), equalTo(false));
            assertThat(isSet(flag, stats.getTotal()), equalTo(false));
        }

        for (Flag flag : values) {
            set(flag, builder, true);
        }
        stats = builder.get();
        for (Flag flag : values) {
            assertThat(isSet(flag, stats.getPrimaries()), equalTo(true));
            assertThat(isSet(flag, stats.getTotal()), equalTo(true));
        }
        Random random = random();
        EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
        for (Flag flag : values) {
            if (random.nextBoolean()) {
                flags.add(flag);
            }
        }

        for (Flag flag : values) {
            set(flag, builder, false); // clear all
        }

        for (Flag flag : flags) { // set the flags
            set(flag, builder, true);
        }
        stats = builder.get();
        for (Flag flag : flags) { // check the flags
            assertThat(isSet(flag, stats.getPrimaries()), equalTo(true));
            assertThat(isSet(flag, stats.getTotal()), equalTo(true));
        }

        for (Flag flag : EnumSet.complementOf(flags)) { // check the complement
            assertThat(isSet(flag, stats.getPrimaries()), equalTo(false));
            assertThat(isSet(flag, stats.getTotal()), equalTo(false));
        }

    }

    public void testEncodeDecodeCommonStats() throws IOException {
        CommonStatsFlags flags = new CommonStatsFlags();
        Flag[] values = CommonStatsFlags.Flag.values();
        assertThat(flags.anySet(), equalTo(true));

        for (Flag flag : values) {
            flags.set(flag, false);
        }
        assertThat(flags.anySet(), equalTo(false));
        for (Flag flag : values) {
            flags.set(flag, true);
        }
        assertThat(flags.anySet(), equalTo(true));
        Random random = random();
        flags.set(values[random.nextInt(values.length)], false);
        assertThat(flags.anySet(), equalTo(true));

        {
            BytesStreamOutput out = new BytesStreamOutput();
            flags.writeTo(out);
            out.close();
            BytesReference bytes = out.bytes();
            CommonStatsFlags readStats = new CommonStatsFlags(bytes.streamInput());
            for (Flag flag : values) {
                assertThat(flags.isSet(flag), equalTo(readStats.isSet(flag)));
            }
        }

        {
            for (Flag flag : values) {
                flags.set(flag, random.nextBoolean());
            }
            BytesStreamOutput out = new BytesStreamOutput();
            flags.writeTo(out);
            out.close();
            BytesReference bytes = out.bytes();
            CommonStatsFlags readStats = new CommonStatsFlags(bytes.streamInput());
            for (Flag flag : values) {
                assertThat(flags.isSet(flag), equalTo(readStats.isSet(flag)));
            }
        }
    }

    public void testFlagOrdinalOrder() {
        Flag[] flags = new Flag[] {
            Flag.Store,
            Flag.Indexing,
            Flag.Get,
            Flag.Search,
            Flag.Merge,
            Flag.Flush,
            Flag.Refresh,
            Flag.QueryCache,
            Flag.FieldData,
            Flag.Docs,
            Flag.Warmer,
            Flag.Completion,
            Flag.Segments,
            Flag.Translog,
            Flag.RequestCache,
            Flag.Recovery,
            Flag.Bulk,
            Flag.Shards,
            Flag.Mappings,
            Flag.DenseVector };

        assertThat(flags.length, equalTo(Flag.values().length));
        for (int i = 0; i < flags.length; i++) {
            assertThat("ordinal has changed - this breaks the wire protocol. Only append to new values", i, equalTo(flags[i].ordinal()));
        }
    }

    public void testMultiIndex() throws Exception {
        assertAcked(prepareCreate("test1"));
        createIndex("test2");

        ensureGreen();

        prepareIndex("test1").setId(Integer.toString(1)).setSource("field", "value").get();
        prepareIndex("test1").setId(Integer.toString(2)).setSource("field", "value").get();
        prepareIndex("test2").setId(Integer.toString(1)).setSource("field", "value").get();
        refresh();

        int numShards1 = getNumShards("test1").totalNumShards;
        int numShards2 = getNumShards("test2").totalNumShards;

        IndicesStatsRequestBuilder builder = indicesAdmin().prepareStats();
        IndicesStatsResponse stats = builder.get();

        assertThat(stats.getTotalShards(), equalTo(numShards1 + numShards2));

        stats = builder.setIndices("_all").get();
        assertThat(stats.getTotalShards(), equalTo(numShards1 + numShards2));

        stats = builder.setIndices("_all").get();
        assertThat(stats.getTotalShards(), equalTo(numShards1 + numShards2));

        stats = builder.setIndices("*").get();
        assertThat(stats.getTotalShards(), equalTo(numShards1 + numShards2));

        stats = builder.setIndices("test1").get();
        assertThat(stats.getTotalShards(), equalTo(numShards1));

        stats = builder.setIndices("test1", "test2").get();
        assertThat(stats.getTotalShards(), equalTo(numShards1 + numShards2));

        stats = builder.setIndices("*2").get();
        assertThat(stats.getTotalShards(), equalTo(numShards2));

    }

    public void testCompletionFieldsParam() throws Exception {
        assertAcked(prepareCreate("test1").setMapping("""
            {
              "properties": {
                "bar": {
                  "type": "text",
                  "fields": {
                    "completion": {
                      "type": "completion"
                    }
                  }
                },
                "baz": {
                  "type": "text",
                  "fields": {
                    "completion": {
                      "type": "completion"
                    }
                  }
                }
              }
            }"""));
        ensureGreen();

        prepareIndex("test1").setId(Integer.toString(1)).setSource("""
            {"bar":"bar","baz":"baz"}""", XContentType.JSON).get();
        refresh();

        IndicesStatsRequestBuilder builder = indicesAdmin().prepareStats();
        IndicesStatsResponse stats = builder.get();

        assertThat(stats.getTotal().completion.getSizeInBytes(), greaterThan(0L));
        assertThat(stats.getTotal().completion.getFields(), is(nullValue()));

        stats = builder.setCompletionFields("bar.completion").get();
        assertThat(stats.getTotal().completion.getSizeInBytes(), greaterThan(0L));
        assertThat(stats.getTotal().completion.getFields().containsField("bar.completion"), is(true));
        assertThat(stats.getTotal().completion.getFields().get("bar.completion"), greaterThan(0L));
        assertThat(stats.getTotal().completion.getFields().containsField("baz.completion"), is(false));

        stats = builder.setCompletionFields("bar.completion", "baz.completion").get();
        assertThat(stats.getTotal().completion.getSizeInBytes(), greaterThan(0L));
        assertThat(stats.getTotal().completion.getFields().containsField("bar.completion"), is(true));
        assertThat(stats.getTotal().completion.getFields().get("bar.completion"), greaterThan(0L));
        assertThat(stats.getTotal().completion.getFields().containsField("baz.completion"), is(true));
        assertThat(stats.getTotal().completion.getFields().get("baz.completion"), greaterThan(0L));

        stats = builder.setCompletionFields("*").get();
        assertThat(stats.getTotal().completion.getSizeInBytes(), greaterThan(0L));
        assertThat(stats.getTotal().completion.getFields().containsField("bar.completion"), is(true));
        assertThat(stats.getTotal().completion.getFields().get("bar.completion"), greaterThan(0L));
        assertThat(stats.getTotal().completion.getFields().containsField("baz.completion"), is(true));
        assertThat(stats.getTotal().completion.getFields().get("baz.completion"), greaterThan(0L));

        stats = builder.setCompletionFields("*r*").get();
        assertThat(stats.getTotal().completion.getSizeInBytes(), greaterThan(0L));
        assertThat(stats.getTotal().completion.getFields().containsField("bar.completion"), is(true));
        assertThat(stats.getTotal().completion.getFields().get("bar.completion"), greaterThan(0L));
        assertThat(stats.getTotal().completion.getFields().containsField("baz.completion"), is(false));

    }

    public void testGroupsParam() throws Exception {
        createIndex("test1");

        ensureGreen();

        prepareIndex("test1").setId(Integer.toString(1)).setSource("foo", "bar").get();
        refresh();

        prepareSearch("_all").setStats("bar", "baz").get().decRef();

        IndicesStatsRequestBuilder builder = indicesAdmin().prepareStats();
        IndicesStatsResponse stats = builder.get();

        assertThat(stats.getTotal().search.getTotal().getQueryCount(), greaterThan(0L));
        assertThat(stats.getTotal().search.getGroupStats(), is(nullValue()));

        stats = builder.setGroups("bar").get();
        assertThat(stats.getTotal().search.getGroupStats().get("bar").getQueryCount(), greaterThan(0L));
        assertThat(stats.getTotal().search.getGroupStats().containsKey("baz"), is(false));

        stats = builder.setGroups("bar", "baz").get();
        assertThat(stats.getTotal().search.getGroupStats().get("bar").getQueryCount(), greaterThan(0L));
        assertThat(stats.getTotal().search.getGroupStats().get("baz").getQueryCount(), greaterThan(0L));

        stats = builder.setGroups("*").get();
        assertThat(stats.getTotal().search.getGroupStats().get("bar").getQueryCount(), greaterThan(0L));
        assertThat(stats.getTotal().search.getGroupStats().get("baz").getQueryCount(), greaterThan(0L));

        stats = builder.setGroups("*r").get();
        assertThat(stats.getTotal().search.getGroupStats().get("bar").getQueryCount(), greaterThan(0L));
        assertThat(stats.getTotal().search.getGroupStats().containsKey("baz"), is(false));

    }

    private static void set(Flag flag, IndicesStatsRequestBuilder builder, boolean set) {
        switch (flag) {
            case Docs -> builder.setDocs(set);
            case FieldData -> builder.setFieldData(set);
            case QueryCache -> builder.setQueryCache(set);
            case Flush -> builder.setFlush(set);
            case Get -> builder.setGet(set);
            case Indexing -> builder.setIndexing(set);
            case Merge -> builder.setMerge(set);
            case Refresh -> builder.setRefresh(set);
            case Search -> builder.setSearch(set);
            case Store -> builder.setStore(set);
            case Warmer -> builder.setWarmer(set);
            case Completion -> builder.setCompletion(set);
            case Segments -> builder.setSegments(set);
            case Translog -> builder.setTranslog(set);
            case RequestCache -> builder.setRequestCache(set);
            case Recovery -> builder.setRecovery(set);
            case Bulk -> builder.setBulk(set);
            case Shards ->
                // We don't actually expose shards in IndexStats, but this test fails if it isn't handled
                builder.request().flags().set(Flag.Shards, set);
            case DenseVector -> builder.setDenseVector(set);
            default -> fail("new flag? " + flag);
        }
    }

    private static boolean isSet(Flag flag, CommonStats response) {
        switch (flag) {
            case Docs:
                return response.getDocs() != null;
            case FieldData:
                return response.getFieldData() != null;
            case QueryCache:
                return response.getQueryCache() != null;
            case Flush:
                return response.getFlush() != null;
            case Get:
                return response.getGet() != null;
            case Indexing:
                return response.getIndexing() != null;
            case Merge:
                return response.getMerge() != null;
            case Refresh:
                return response.getRefresh() != null;
            case Search:
                return response.getSearch() != null;
            case Store:
                return response.getStore() != null;
            case Warmer:
                return response.getWarmer() != null;
            case Completion:
                return response.getCompletion() != null;
            case Segments:
                return response.getSegments() != null;
            case Translog:
                return response.getTranslog() != null;
            case RequestCache:
                return response.getRequestCache() != null;
            case Recovery:
                return response.getRecoveryStats() != null;
            case Bulk:
                return response.getBulk() != null;
            case Shards:
                return response.getShards() != null;
            case Mappings:
                return response.getNodeMappings() != null;
            case DenseVector:
                return response.getDenseVectorStats() != null;
            default:
                fail("new flag? " + flag);
                return false;
        }
    }

    private void assertEquals(QueryCacheStats stats1, QueryCacheStats stats2) {
        assertEquals(stats1.getCacheCount(), stats2.getCacheCount());
        assertEquals(stats1.getCacheSize(), stats2.getCacheSize());
        assertEquals(stats1.getEvictions(), stats2.getEvictions());
        assertEquals(stats1.getHitCount(), stats2.getHitCount());
        assertEquals(stats2.getMemorySizeInBytes(), stats2.getMemorySizeInBytes());
        assertEquals(stats1.getMissCount(), stats2.getMissCount());
        assertEquals(stats1.getTotalCount(), stats2.getTotalCount());
    }

    private void assertCumulativeQueryCacheStats(IndicesStatsResponse response) {
        assertAllSuccessful(response);
        QueryCacheStats total = response.getTotal().queryCache;
        QueryCacheStats indexTotal = new QueryCacheStats();
        QueryCacheStats shardTotal = new QueryCacheStats();
        for (IndexStats indexStats : response.getIndices().values()) {
            indexTotal.add(indexStats.getTotal().queryCache);
            for (ShardStats shardStats : response.getShards()) {
                shardTotal.add(shardStats.getStats().queryCache);
            }
        }
        assertEquals(total, indexTotal);
        assertEquals(total, shardTotal);
    }

    public void testFilterCacheStats() throws Exception {
        Settings settings = Settings.builder()
            .put(indexSettings())
            .put("number_of_replicas", 0)
            .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "200ms")
            .build();
        assertAcked(prepareCreate("index").setSettings(settings).get());
        indexRandom(
            false,
            true,
            prepareIndex("index").setId("1").setSource("foo", "bar"),
            prepareIndex("index").setId("2").setSource("foo", "baz")
        );
        persistGlobalCheckpoint("index"); // Need to persist the global checkpoint for the soft-deletes retention MP.
        refresh();
        ensureGreen();

        IndicesStatsResponse response = indicesAdmin().prepareStats("index").setQueryCache(true).get();
        assertCumulativeQueryCacheStats(response);
        assertEquals(0, response.getTotal().queryCache.getCacheSize());

        // the query cache has an optimization that disables it automatically if there is contention,
        // so we run it in an assertBusy block which should eventually succeed
        assertBusy(() -> {
            assertNoFailures(prepareSearch("index").setQuery(QueryBuilders.constantScoreQuery(QueryBuilders.matchQuery("foo", "baz"))));
            IndicesStatsResponse stats = indicesAdmin().prepareStats("index").setQueryCache(true).get();
            assertCumulativeQueryCacheStats(stats);
            assertThat(stats.getTotal().queryCache.getHitCount(), equalTo(0L));
            assertThat(stats.getTotal().queryCache.getMissCount(), greaterThan(0L));
            assertThat(stats.getTotal().queryCache.getCacheSize(), greaterThan(0L));
        });

        assertBusy(() -> {
            assertNoFailures(prepareSearch("index").setQuery(QueryBuilders.constantScoreQuery(QueryBuilders.matchQuery("foo", "baz"))));
            IndicesStatsResponse stats = indicesAdmin().prepareStats("index").setQueryCache(true).get();
            assertCumulativeQueryCacheStats(stats);
            assertThat(stats.getTotal().queryCache.getHitCount(), greaterThan(0L));
            assertThat(stats.getTotal().queryCache.getMissCount(), greaterThan(0L));
            assertThat(stats.getTotal().queryCache.getCacheSize(), greaterThan(0L));
        });

        assertEquals(DocWriteResponse.Result.DELETED, client().prepareDelete("index", "1").get().getResult());
        assertEquals(DocWriteResponse.Result.DELETED, client().prepareDelete("index", "2").get().getResult());
        // Here we are testing that a fully deleted segment should be dropped and its cached is evicted.
        // In order to instruct the merge policy not to keep a fully deleted segment,
        // we need to flush and make that commit safe so that the SoftDeletesPolicy can drop everything.
        persistGlobalCheckpoint("index");
        assertBusy(() -> {
            for (final ShardStats shardStats : indicesAdmin().prepareStats("index").get().getIndex("index").getShards()) {
                final long maxSeqNo = shardStats.getSeqNoStats().getMaxSeqNo();
                assertTrue(
                    shardStats.getRetentionLeaseStats()
                        .retentionLeases()
                        .leases()
                        .stream()
                        .allMatch(retentionLease -> retentionLease.retainingSequenceNumber() == maxSeqNo + 1)
                );
            }
        });
        flush("index");
        logger.info("--> force merging to a single segment");
        BroadcastResponse forceMergeResponse = indicesAdmin().prepareForceMerge("index").setFlush(true).setMaxNumSegments(1).get();
        assertAllSuccessful(forceMergeResponse);
        logger.info("--> refreshing");
        refresh();

        logger.info("--> verifying that cache size is 0");
        response = indicesAdmin().prepareStats("index").setQueryCache(true).get();
        assertCumulativeQueryCacheStats(response);
        assertThat(response.getTotal().queryCache.getHitCount(), greaterThan(0L));
        assertThat(response.getTotal().queryCache.getEvictions(), greaterThan(0L));
        assertThat(response.getTotal().queryCache.getCacheSize(), equalTo(0L));
        assertThat(response.getTotal().queryCache.getCacheCount(), greaterThan(0L));

        indexRandom(
            true,
            prepareIndex("index").setId("1").setSource("foo", "bar"),
            prepareIndex("index").setId("2").setSource("foo", "baz")
        );

        assertBusy(() -> {
            assertNoFailures(prepareSearch("index").setQuery(QueryBuilders.constantScoreQuery(QueryBuilders.matchQuery("foo", "baz"))));
            IndicesStatsResponse stats = indicesAdmin().prepareStats("index").setQueryCache(true).get();
            assertCumulativeQueryCacheStats(stats);
            assertThat(stats.getTotal().queryCache.getHitCount(), greaterThan(0L));
            assertThat(stats.getTotal().queryCache.getEvictions(), greaterThan(0L));
            assertThat(stats.getTotal().queryCache.getMissCount(), greaterThan(0L));
            assertThat(stats.getTotal().queryCache.getCacheSize(), greaterThan(0L));
            assertThat(stats.getTotal().queryCache.getMemorySizeInBytes(), greaterThan(0L));
        });

        assertAllSuccessful(indicesAdmin().prepareClearCache("index").setQueryCache(true).get());
        response = indicesAdmin().prepareStats("index").setQueryCache(true).get();
        assertCumulativeQueryCacheStats(response);
        assertThat(response.getTotal().queryCache.getHitCount(), greaterThan(0L));
        assertThat(response.getTotal().queryCache.getEvictions(), greaterThan(0L));
        assertThat(response.getTotal().queryCache.getMissCount(), greaterThan(0L));
        assertThat(response.getTotal().queryCache.getCacheSize(), equalTo(0L));
        assertThat(response.getTotal().queryCache.getMemorySizeInBytes(), equalTo(0L));
    }

    public void testBulkStats() throws Exception {
        final String index = "test";
        assertAcked(
            prepareCreate(index).setSettings(settingsBuilder().put("index.number_of_shards", 2).put("index.number_of_replicas", 1))
        );
        ensureGreen();
        final BulkRequest request1 = new BulkRequest();
        for (int i = 0; i < 20; ++i) {
            request1.add(new IndexRequest(index).source(Collections.singletonMap("key", "value" + i)))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        }
        BulkResponse bulkResponse = client().bulk(request1).get();
        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(20));
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            assertThat(bulkItemResponse.getIndex(), equalTo(index));
        }
        IndicesStatsResponse stats = indicesAdmin().prepareStats(index).setBulk(true).get();

        assertThat(stats.getTotal().bulk.getTotalOperations(), equalTo(4L));
        assertThat(stats.getTotal().bulk.getTotalTimeInMillis(), greaterThanOrEqualTo(0L));
        assertThat(stats.getTotal().bulk.getTotalSizeInBytes(), greaterThan(0L));
        assertThat(stats.getTotal().bulk.getAvgTimeInMillis(), greaterThanOrEqualTo(0L));
        assertThat(stats.getTotal().bulk.getAvgSizeInBytes(), greaterThan(0L));

        assertThat(stats.getPrimaries().bulk.getTotalOperations(), equalTo(2L));
        assertThat(stats.getPrimaries().bulk.getTotalTimeInMillis(), greaterThanOrEqualTo(0L));
        assertThat(stats.getPrimaries().bulk.getTotalSizeInBytes(), greaterThan(0L));
        assertThat(stats.getPrimaries().bulk.getAvgTimeInMillis(), greaterThanOrEqualTo(0L));
        assertThat(stats.getPrimaries().bulk.getAvgSizeInBytes(), greaterThan(0L));
    }

    /**
     * Test that we can safely concurrently index and get stats. This test was inspired by a serialization issue that arose due to a race
     * getting doc stats during heavy indexing. The race could lead to deleted docs being negative which would then be serialized as a
     * variable-length long. Since serialization of negative longs using a variable-length format was unsupported
     * ({@link org.elasticsearch.common.io.stream.StreamOutput#writeVLong(long)}), the stream would become corrupted. Here, we want to test
     * that we can continue to get stats while indexing.
     */
    public void testConcurrentIndexingAndStatsRequests() throws BrokenBarrierException, InterruptedException, ExecutionException {
        final AtomicInteger idGenerator = new AtomicInteger();
        final int numberOfIndexingThreads = Runtime.getRuntime().availableProcessors();
        final int numberOfStatsThreads = 4 * numberOfIndexingThreads;
        final CyclicBarrier barrier = new CyclicBarrier(1 + numberOfIndexingThreads + numberOfStatsThreads);
        final AtomicBoolean stop = new AtomicBoolean();
        final List<Thread> threads = new ArrayList<>(numberOfIndexingThreads + numberOfIndexingThreads);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean failed = new AtomicBoolean();
        final AtomicReference<List<DefaultShardOperationFailedException>> shardFailures = new AtomicReference<>(
            new CopyOnWriteArrayList<>()
        );
        final AtomicReference<List<Exception>> executionFailures = new AtomicReference<>(new CopyOnWriteArrayList<>());

        // increasing the number of shards increases the number of chances any one stats request will hit a race
        final CreateIndexRequest createIndexRequest = new CreateIndexRequest(
            "test",
            Settings.builder().put("index.number_of_shards", 10).build()
        );
        indicesAdmin().create(createIndexRequest).get();

        // start threads that will index concurrently with stats requests
        for (int i = 0; i < numberOfIndexingThreads; i++) {
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    failed.set(true);
                    executionFailures.get().add(e);
                    latch.countDown();
                }
                while (stop.get() == false) {
                    final String id = Integer.toString(idGenerator.incrementAndGet());
                    final DocWriteResponse response = prepareIndex("test").setId(id).setSource("{}", XContentType.JSON).get();
                    assertThat(response.getResult(), equalTo(DocWriteResponse.Result.CREATED));
                }
            });
            thread.setName("indexing-" + i);
            threads.add(thread);
            thread.start();
        }

        // start threads that will get stats concurrently with indexing
        for (int i = 0; i < numberOfStatsThreads; i++) {
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    failed.set(true);
                    executionFailures.get().add(e);
                    latch.countDown();
                }
                final IndicesStatsRequest request = new IndicesStatsRequest();
                request.all();
                request.indices(new String[0]);
                while (stop.get() == false) {
                    try {
                        final IndicesStatsResponse response = indicesAdmin().stats(request).get();
                        if (response.getFailedShards() > 0) {
                            failed.set(true);
                            shardFailures.get().addAll(Arrays.asList(response.getShardFailures()));
                            latch.countDown();
                        }
                    } catch (final ExecutionException | InterruptedException e) {
                        failed.set(true);
                        executionFailures.get().add(e);
                        latch.countDown();
                    }
                }
            });
            thread.setName("stats-" + i);
            threads.add(thread);
            thread.start();
        }

        // release the hounds
        barrier.await();

        // wait for a failure, or for fifteen seconds to elapse
        latch.await(15, TimeUnit.SECONDS);

        // stop all threads and wait for them to complete
        stop.set(true);
        for (final Thread thread : threads) {
            thread.join();
        }

        assertThat(shardFailures.get(), emptyCollectionOf(DefaultShardOperationFailedException.class));
        assertThat(executionFailures.get(), emptyCollectionOf(Exception.class));
    }

    public void testWriteLoadIsCaptured() throws Exception {
        final String indexName = "test-idx";
        createIndex(indexName);
        final IndicesStatsResponse statsResponseBeforeIndexing = indicesAdmin().prepareStats(indexName).get();
        final IndexStats indexStatsBeforeIndexing = statsResponseBeforeIndexing.getIndices().get(indexName);
        assertThat(indexStatsBeforeIndexing, is(notNullValue()));
        assertThat(indexStatsBeforeIndexing.getPrimaries().getIndexing().getTotal().getWriteLoad(), is(equalTo(0.0)));

        final AtomicInteger idGenerator = new AtomicInteger();
        assertBusy(() -> {
            final int numDocs = randomIntBetween(15, 25);
            final List<ActionFuture<DocWriteResponse>> indexRequestFutures = new ArrayList<>(numDocs);
            for (int i = 0; i < numDocs; i++) {
                indexRequestFutures.add(
                    prepareIndex(indexName).setId(Integer.toString(idGenerator.incrementAndGet()))
                        .setSource("{}", XContentType.JSON)
                        .execute()
                );
            }

            for (ActionFuture<DocWriteResponse> indexRequestFuture : indexRequestFutures) {
                assertThat(indexRequestFuture.get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            }

            final IndicesStatsResponse statsResponseAfterIndexing = indicesAdmin().prepareStats(indexName).get();
            final IndexStats indexStatsAfterIndexing = statsResponseAfterIndexing.getIndices().get(indexName);
            assertThat(indexStatsAfterIndexing, is(notNullValue()));
            assertThat(indexStatsAfterIndexing.getPrimaries().getIndexing().getTotal().getWriteLoad(), is(greaterThan(0.0)));
        });
    }

    /**
     * Persist the global checkpoint on all shards of the given index into disk.
     * This makes sure that the persisted global checkpoint on those shards will equal to the in-memory value.
     */
    private void persistGlobalCheckpoint(String index) throws Exception {
        final Set<String> nodes = internalCluster().nodesInclude(index);
        for (String node : nodes) {
            final IndicesService indexServices = internalCluster().getInstance(IndicesService.class, node);
            for (IndexService indexService : indexServices) {
                for (IndexShard indexShard : indexService) {
                    indexShard.sync();
                    assertThat(indexShard.getLastSyncedGlobalCheckpoint(), equalTo(indexShard.getLastKnownGlobalCheckpoint()));
                }
            }
        }
    }
}
