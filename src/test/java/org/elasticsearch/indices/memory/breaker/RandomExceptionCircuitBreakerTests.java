/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.memory.breaker;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.engine.MockInternalEngine;
import org.elasticsearch.test.engine.ThrowingAtomicReaderWrapper;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the circuit breaker while random exceptions are happening
 */
public class RandomExceptionCircuitBreakerTests extends ElasticsearchIntegrationTest {

    @Test
    public void testBreakerWithRandomExceptions() throws IOException, InterruptedException, ExecutionException {
        for (NodeStats node : client().admin().cluster().prepareNodesStats()
                .clear().setBreaker(true).execute().actionGet().getNodes()) {
            assertThat("Breaker is not set to 0", node.getBreaker().getStats(CircuitBreaker.Name.FIELDDATA).getEstimated(), equalTo(0L));
        }

        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("properties")
                .startObject("test-str")
                .field("type", "string")
                .field("index", "not_analyzed")
                .startObject("fielddata")
                .field("format", randomBytesFieldDataFormat())
                .endObject() // fielddata
                .endObject() // test-str
                .startObject("test-num")
                // I don't use randomNumericType() here because I don't want "byte", and I want "float" and "double"
                .field("type", randomFrom(Arrays.asList("float", "long", "double", "short", "integer")))
                .startObject("fielddata")
                .field("format", randomNumericFieldDataFormat())
                .endObject() // fielddata
                .endObject() // test-num
                .endObject() // properties
                .endObject() // type
                .endObject() // {}
                .string();
        final double topLevelRate;
        final double lowLevelRate;
        if (frequently()) {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    lowLevelRate =  1.0/between(2, 10);
                    topLevelRate = 0.0d;
                } else {
                    topLevelRate =  1.0/between(2, 10);
                    lowLevelRate = 0.0d;
                }
            } else {
                lowLevelRate =  1.0/between(2, 10);
                topLevelRate =  1.0/between(2, 10);
            }
        } else {
            // rarely no exception
            topLevelRate = 0d;
            lowLevelRate = 0d;
        }

        ImmutableSettings.Builder settings = settingsBuilder()
                .put(indexSettings())
                .put(MockInternalEngine.READER_WRAPPER_TYPE, RandomExceptionDirectoryReaderWrapper.class.getName())
                .put(EXCEPTION_TOP_LEVEL_RATIO_KEY, topLevelRate)
                .put(EXCEPTION_LOW_LEVEL_RATIO_KEY, lowLevelRate)
                .put(MockInternalEngine.WRAP_READER_RATIO, 1.0d);
        logger.info("creating index: [test] using settings: [{}]", settings.build().getAsMap());
        client().admin().indices().prepareCreate("test")
                .setSettings(settings)
                .addMapping("type", mapping).execute().actionGet();
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster()
                .health(Requests.clusterHealthRequest().waitForYellowStatus().timeout(TimeValue.timeValueSeconds(5))).get(); // it's OK to timeout here
        final int numDocs;
        if (clusterHealthResponse.isTimedOut()) {
            /* some seeds just won't let you create the index at all and we enter a ping-pong mode
             * trying one node after another etc. that is ok but we need to make sure we don't wait
             * forever when indexing documents so we set numDocs = 1 and expect all shards to fail
             * when we search below.*/
            logger.info("ClusterHealth timed out - only index one doc and expect searches to fail");
            numDocs = 1;
        } else {
            numDocs = between(10, 100);
        }
        for (int i = 0; i < numDocs ; i++) {
            try {
                client().prepareIndex("test", "type", "" + i)
                        .setTimeout(TimeValue.timeValueSeconds(1)).setSource("test-str", randomUnicodeOfLengthBetween(5, 25), "test-num", i).get();
            } catch (ElasticsearchException ex) {
            }
        }
        logger.info("Start Refresh");
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh("test").execute().get(); // don't assert on failures here
        final boolean refreshFailed = refreshResponse.getShardFailures().length != 0 || refreshResponse.getFailedShards() != 0;
        logger.info("Refresh failed: [{}] numShardsFailed: [{}], shardFailuresLength: [{}], successfulShards: [{}], totalShards: [{}] ",
                refreshFailed, refreshResponse.getFailedShards(), refreshResponse.getShardFailures().length,
                refreshResponse.getSuccessfulShards(), refreshResponse.getTotalShards());
        final int numSearches = scaledRandomIntBetween(50, 150);
        NodesStatsResponse resp = client().admin().cluster().prepareNodesStats()
                .clear().setBreaker(true).execute().actionGet();
        for (NodeStats stats : resp.getNodes()) {
            assertThat("Breaker is set to 0", stats.getBreaker().getStats(CircuitBreaker.Name.FIELDDATA).getEstimated(), equalTo(0L));
        }

        for (int i = 0; i < numSearches; i++) {
            SearchRequestBuilder searchRequestBuilder = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery());
            switch(randomIntBetween(0, 5)) {
                case 5:
                case 4:
                case 3:
                    searchRequestBuilder.addSort("test-str", SortOrder.ASC);
                    // fall through - sometimes get both fields
                case 2:
                case 1:
                default:
                    searchRequestBuilder.addSort("test-num", SortOrder.ASC);

            }
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
                NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats()
                    .clear().setBreaker(true).execute().actionGet();
                for (NodeStats stats : nodeStats.getNodes()) {
                    assertThat("Breaker reset to 0 last search success: " + success + " mapping: " + mapping,
                            stats.getBreaker().getStats(CircuitBreaker.Name.FIELDDATA).getEstimated(), equalTo(0L));
                }
            }
        }
    }



    public static final String EXCEPTION_TOP_LEVEL_RATIO_KEY = "index.engine.exception.ratio.top";
    public static final String EXCEPTION_LOW_LEVEL_RATIO_KEY = "index.engine.exception.ratio.low";

    // TODO: Generalize this class and add it as a utility
    public static class RandomExceptionDirectoryReaderWrapper extends MockInternalEngine.DirectoryReaderWrapper {
        private final Settings settings;
        static class ThrowingSubReaderWrapper extends SubReaderWrapper implements ThrowingAtomicReaderWrapper.Thrower {
            private final Random random;
            private final double topLevelRatio;
            private final double lowLevelRatio;

            ThrowingSubReaderWrapper(Settings settings) {
                final long seed = settings.getAsLong(SETTING_INDEX_SEED, 0l);
                this.topLevelRatio = settings.getAsDouble(EXCEPTION_TOP_LEVEL_RATIO_KEY, 0.1d);
                this.lowLevelRatio = settings.getAsDouble(EXCEPTION_LOW_LEVEL_RATIO_KEY, 0.1d);
                this.random = new Random(seed);
            }

            @Override
            public AtomicReader wrap(AtomicReader reader) {
                return new ThrowingAtomicReaderWrapper(reader, this);
            }

            @Override
            public void maybeThrow(ThrowingAtomicReaderWrapper.Flags flag) throws IOException {
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

            public boolean wrapTerms(String field) {
                return field.startsWith("test");
            }
        }



        public RandomExceptionDirectoryReaderWrapper(DirectoryReader in, Settings settings) {
            super(in, new ThrowingSubReaderWrapper(settings));
            this.settings = settings;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
            return new RandomExceptionDirectoryReaderWrapper(in, settings);
        }
    }
}
