/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.memory.breaker;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.engine.MockEngineSupport;
import org.elasticsearch.test.engine.ThrowingLeafReaderWrapper;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the circuit breaker while random exceptions are happening
 */
public class RandomExceptionCircuitBreakerIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(RandomExceptionDirectoryReaderWrapper.TestPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testBreakerWithRandomExceptions() throws IOException, InterruptedException, ExecutionException {
        for (NodeStats node : client().admin().cluster().prepareNodesStats().clear().setBreaker(true).execute().actionGet().getNodes()) {
            assertThat("Breaker is not set to 0", node.getBreaker().getStats(CircuitBreaker.FIELDDATA).getEstimated(), equalTo(0L));
        }

        String mapping = Strings // {}
            .toString(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("test-str")
                    .field("type", "keyword")
                    .field("doc_values", randomBoolean())
                    .endObject() // test-str
                    .startObject("test-num")
                    // I don't use randomNumericType() here because I don't want "byte", and I want "float" and "double"
                    .field("type", randomFrom(Arrays.asList("float", "long", "double", "short", "integer")))
                    .endObject() // test-num
                    .endObject() // properties
                    .endObject()
            );
        final double topLevelRate;
        final double lowLevelRate;
        if (frequently()) {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    lowLevelRate = 1.0 / between(2, 10);
                    topLevelRate = 0.0d;
                } else {
                    topLevelRate = 1.0 / between(2, 10);
                    lowLevelRate = 0.0d;
                }
            } else {
                lowLevelRate = 1.0 / between(2, 10);
                topLevelRate = 1.0 / between(2, 10);
            }
        } else {
            // rarely no exception
            topLevelRate = 0d;
            lowLevelRate = 0d;
        }

        Settings.Builder settings = Settings.builder()
            .put(indexSettings())
            .put(EXCEPTION_TOP_LEVEL_RATIO_KEY, topLevelRate)
            .put(EXCEPTION_LOW_LEVEL_RATIO_KEY, lowLevelRate)
            .put(MockEngineSupport.WRAP_READER_RATIO.getKey(), 1.0d);
        logger.info("creating index: [test] using settings: [{}]", settings.build());
        CreateIndexResponse response = indicesAdmin().prepareCreate("test").setSettings(settings).setMapping(mapping).execute().actionGet();
        final int numDocs;
        if (response.isShardsAcknowledged() == false) {
            /* some seeds just won't let you create the index at all and we enter a ping-pong mode
             * trying one node after another etc. that is ok but we need to make sure we don't wait
             * forever when indexing documents so we set numDocs = 1 and expect all shards to fail
             * when we search below.*/
            if (response.isAcknowledged()) {
                logger.info("Index creation timed out waiting for primaries to start - only index one doc and expect searches to fail");
            } else {
                logger.info("Index creation failed - only index one doc and expect searches to fail");
            }
            numDocs = 1;
        } else {
            numDocs = between(10, 100);
        }
        for (int i = 0; i < numDocs; i++) {
            try {
                client().prepareIndex("test")
                    .setId("" + i)
                    .setTimeout(TimeValue.timeValueSeconds(1))
                    .setSource("test-str", randomUnicodeOfLengthBetween(5, 25), "test-num", i)
                    .get();
            } catch (ElasticsearchException ex) {}
        }
        logger.info("Start Refresh");
        // don't assert on failures here
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh("test").execute().get();
        final boolean refreshFailed = refreshResponse.getShardFailures().length != 0 || refreshResponse.getFailedShards() != 0;
        logger.info(
            "Refresh failed: [{}] numShardsFailed: [{}], shardFailuresLength: [{}], successfulShards: [{}], totalShards: [{}] ",
            refreshFailed,
            refreshResponse.getFailedShards(),
            refreshResponse.getShardFailures().length,
            refreshResponse.getSuccessfulShards(),
            refreshResponse.getTotalShards()
        );
        final int numSearches = scaledRandomIntBetween(50, 150);
        NodesStatsResponse resp = client().admin().cluster().prepareNodesStats().clear().setBreaker(true).execute().actionGet();
        for (NodeStats stats : resp.getNodes()) {
            assertThat("Breaker is set to 0", stats.getBreaker().getStats(CircuitBreaker.FIELDDATA).getEstimated(), equalTo(0L));
        }

        for (int i = 0; i < numSearches; i++) {
            SearchRequestBuilder searchRequestBuilder = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery());
            if (random().nextBoolean()) {
                searchRequestBuilder.addSort("test-str", SortOrder.ASC);
            }
            searchRequestBuilder.addSort("test-num", SortOrder.ASC);
            boolean success = false;
            try {
                // Sort by the string and numeric fields, to load them into field data
                searchRequestBuilder.get();
                success = true;
            } catch (SearchPhaseExecutionException ex) {
                logger.info("expected SearchPhaseException: [{}]", ex.getMessage());
            }

            if (frequently()) {
                // Now, clear the cache and check that the circuit breaker has been
                // successfully set back to zero. If there is a bug in the circuit
                // breaker adjustment code, it should show up here by the breaker
                // estimate being either positive or negative.
                ensureGreen("test");  // make sure all shards are there - there could be shards that are still starting up.
                assertAllSuccessful(client().admin().indices().prepareClearCache("test").setFieldDataCache(true).execute().actionGet());

                // Since .cleanUp() is no longer called on cache clear, we need to call it on each node manually
                for (String node : internalCluster().getNodeNames()) {
                    final IndicesFieldDataCache fdCache = internalCluster().getInstance(IndicesService.class, node)
                        .getIndicesFieldDataCache();
                    // Clean up the cache, ensuring that entries' listeners have been called
                    fdCache.getCache().refresh();
                }
                NodesStatsResponse nodeStats = client().admin()
                    .cluster()
                    .prepareNodesStats()
                    .clear()
                    .setBreaker(true)
                    .execute()
                    .actionGet();
                for (NodeStats stats : nodeStats.getNodes()) {
                    assertThat(
                        "Breaker reset to 0 last search success: " + success + " mapping: " + mapping,
                        stats.getBreaker().getStats(CircuitBreaker.FIELDDATA).getEstimated(),
                        equalTo(0L)
                    );
                }
            }
        }
    }

    public static final String EXCEPTION_TOP_LEVEL_RATIO_KEY = "index.engine.exception.ratio.top";
    public static final String EXCEPTION_LOW_LEVEL_RATIO_KEY = "index.engine.exception.ratio.low";

    // TODO: Generalize this class and add it as a utility
    public static class RandomExceptionDirectoryReaderWrapper extends MockEngineSupport.DirectoryReaderWrapper {

        public static final Setting<Double> EXCEPTION_TOP_LEVEL_RATIO_SETTING = Setting.doubleSetting(
            EXCEPTION_TOP_LEVEL_RATIO_KEY,
            0.1d,
            0.0d,
            Property.IndexScope
        );
        public static final Setting<Double> EXCEPTION_LOW_LEVEL_RATIO_SETTING = Setting.doubleSetting(
            EXCEPTION_LOW_LEVEL_RATIO_KEY,
            0.1d,
            0.0d,
            Property.IndexScope
        );

        public static class TestPlugin extends MockEngineFactoryPlugin {
            @Override
            public List<Setting<?>> getSettings() {
                List<Setting<?>> settings = new ArrayList<>();
                settings.addAll(super.getSettings());
                settings.add(EXCEPTION_TOP_LEVEL_RATIO_SETTING);
                settings.add(EXCEPTION_LOW_LEVEL_RATIO_SETTING);
                return settings;
            }

            @Override
            protected Class<? extends FilterDirectoryReader> getReaderWrapperClass() {
                return RandomExceptionDirectoryReaderWrapper.class;
            }
        }

        private final Settings settings;

        static class ThrowingSubReaderWrapper extends SubReaderWrapper implements ThrowingLeafReaderWrapper.Thrower {
            private final Random random;
            private final double topLevelRatio;
            private final double lowLevelRatio;

            ThrowingSubReaderWrapper(Settings settings) {
                final long seed = ESIntegTestCase.INDEX_TEST_SEED_SETTING.get(settings);
                this.topLevelRatio = EXCEPTION_TOP_LEVEL_RATIO_SETTING.get(settings);
                this.lowLevelRatio = EXCEPTION_LOW_LEVEL_RATIO_SETTING.get(settings);
                this.random = new Random(seed);
            }

            @Override
            public LeafReader wrap(LeafReader reader) {
                return new ThrowingLeafReaderWrapper(reader, this);
            }

            @Override
            public void maybeThrow(ThrowingLeafReaderWrapper.Flags flag) throws IOException {
                switch (flag) {
                    case Fields:
                        break;
                    case TermVectors:
                        break;
                    case Terms:
                    case TermsEnum:
                        if (random.nextDouble() < topLevelRatio) {
                            throw new IOException("Forced top level Exception on [" + flag.name() + "]");
                        }
                        break;
                    case Intersect:
                        break;
                    case Norms:
                        break;
                    case NumericDocValues:
                        break;
                    case BinaryDocValues:
                        break;
                    case SortedDocValues:
                        break;
                    case SortedSetDocValues:
                        break;
                    case DocsEnum:
                    case DocsAndPositionsEnum:
                        if (random.nextDouble() < lowLevelRatio) {
                            throw new IOException("Forced low level Exception on [" + flag.name() + "]");
                        }
                        break;
                }
            }

            @Override
            public boolean wrapTerms(String field) {
                return field.startsWith("test");
            }
        }

        public RandomExceptionDirectoryReaderWrapper(DirectoryReader in, Settings settings) throws IOException {
            super(in, new ThrowingSubReaderWrapper(settings));
            this.settings = settings;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new RandomExceptionDirectoryReaderWrapper(in, settings);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}
