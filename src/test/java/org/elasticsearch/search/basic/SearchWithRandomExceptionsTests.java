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

package org.elasticsearch.search.basic;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.English;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.engine.MockEngineSupport;
import org.elasticsearch.test.engine.ThrowingLeafReaderWrapper;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.store.MockDirectoryHelper;
import org.elasticsearch.test.store.MockFSDirectoryService;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;

public class SearchWithRandomExceptionsTests extends ElasticsearchIntegrationTest {

    @Test
    @Slow // maybe due to all the logging?
    @TestLogging("action.search.type:TRACE,index.shard:TRACE")
    public void testRandomDirectoryIOExceptions() throws IOException, InterruptedException, ExecutionException {
        String mapping = XContentFactory.jsonBuilder().
                startObject().
                startObject("type").
                startObject("properties").
                startObject("test")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject().
                        endObject().
                        endObject()
                .endObject().string();
        final double exceptionRate;
        final double exceptionOnOpenRate;
        if (frequently()) {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    exceptionOnOpenRate = 1.0 / between(5, 100);
                    exceptionRate = 0.0d;
                } else {
                    exceptionRate = 1.0 / between(5, 100);
                    exceptionOnOpenRate = 0.0d;
                }
            } else {
                exceptionOnOpenRate = 1.0 / between(5, 100);
                exceptionRate = 1.0 / between(5, 100);
            }
        } else {
            // rarely no exception
            exceptionRate = 0d;
            exceptionOnOpenRate = 0d;
        }
        final boolean createIndexWithoutErrors = randomBoolean();
        int numInitialDocs = 0;

        if (createIndexWithoutErrors) {
            Builder settings = settingsBuilder()
                    .put("index.number_of_replicas", randomIntBetween(0, 1));
            logger.info("creating index: [test] using settings: [{}]", settings.build().getAsMap());
            client().admin().indices().prepareCreate("test")
                    .setSettings(settings)
                    .addMapping("type", mapping).execute().actionGet();
            numInitialDocs = between(10, 100);
            ensureGreen();
            for (int i = 0; i < numInitialDocs; i++) {
                client().prepareIndex("test", "type", "init" + i).setSource("test", "init").get();
            }
            client().admin().indices().prepareRefresh("test").execute().get();
            client().admin().indices().prepareFlush("test").setWaitIfOngoing(true).execute().get();
            client().admin().indices().prepareClose("test").execute().get();
            client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder()
                    .put(MockDirectoryHelper.RANDOM_IO_EXCEPTION_RATE, exceptionRate)
                    .put(MockDirectoryHelper.RANDOM_IO_EXCEPTION_RATE_ON_OPEN, exceptionOnOpenRate));
            client().admin().indices().prepareOpen("test").execute().get();
        } else {
            Builder settings = settingsBuilder()
                    .put("index.number_of_replicas", randomIntBetween(0, 1))
                    .put(MockFSDirectoryService.CHECK_INDEX_ON_CLOSE, false)
                    .put(MockDirectoryHelper.RANDOM_IO_EXCEPTION_RATE, exceptionRate)
                    .put(MockDirectoryHelper.RANDOM_IO_EXCEPTION_RATE_ON_OPEN, exceptionOnOpenRate); // we cannot expect that the index will be valid
            logger.info("creating index: [test] using settings: [{}]", settings.build().getAsMap());
            client().admin().indices().prepareCreate("test")
                    .setSettings(settings)
                    .addMapping("type", mapping).execute().actionGet();
        }
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster()
                .health(Requests.clusterHealthRequest().waitForYellowStatus().timeout(TimeValue.timeValueSeconds(5))).get(); // it's OK to timeout here 
        final int numDocs;
        final boolean expectAllShardsFailed;
        if (clusterHealthResponse.isTimedOut()) {
            /* some seeds just won't let you create the index at all and we enter a ping-pong mode
             * trying one node after another etc. that is ok but we need to make sure we don't wait
             * forever when indexing documents so we set numDocs = 1 and expecte all shards to fail
             * when we search below.*/
            logger.info("ClusterHealth timed out - only index one doc and expect searches to fail");
            numDocs = 1;
            expectAllShardsFailed = true;
        } else {
            numDocs = between(10, 100);
            expectAllShardsFailed = false;
        }
        int numCreated = 0;
        boolean[] added = new boolean[numDocs];
        for (int i = 0; i < numDocs; i++) {
            added[i] = false;
            try {
                IndexResponse indexResponse = client().prepareIndex("test", "type", Integer.toString(i)).setTimeout(TimeValue.timeValueSeconds(1)).setSource("test", English.intToEnglish(i)).get();
                if (indexResponse.isCreated()) {
                    numCreated++;
                    added[i] = true;
                }
            } catch (ElasticsearchException ex) {
            }

        }
        NumShards numShards = getNumShards("test");
        logger.info("Start Refresh");
        final RefreshResponse refreshResponse = client().admin().indices().prepareRefresh("test").execute().get(); // don't assert on failures here
        final boolean refreshFailed = refreshResponse.getShardFailures().length != 0 || refreshResponse.getFailedShards() != 0;
        logger.info("Refresh failed [{}] numShardsFailed: [{}], shardFailuresLength: [{}], successfulShards: [{}], totalShards: [{}] ", refreshFailed, refreshResponse.getFailedShards(), refreshResponse.getShardFailures().length, refreshResponse.getSuccessfulShards(), refreshResponse.getTotalShards());
        final int numSearches = scaledRandomIntBetween(10, 20);
        // we don't check anything here really just making sure we don't leave any open files or a broken index behind.
        for (int i = 0; i < numSearches; i++) {
            try {
                int docToQuery = between(0, numDocs - 1);
                int expectedResults = added[docToQuery] ? 1 : 0;
                logger.info("Searching for [test:{}]", English.intToEnglish(docToQuery));
                SearchResponse searchResponse = client().prepareSearch().setTypes("type").setQuery(QueryBuilders.matchQuery("test", English.intToEnglish(docToQuery)))
                        .setSize(expectedResults).get();
                logger.info("Successful shards: [{}]  numShards: [{}]", searchResponse.getSuccessfulShards(), numShards.numPrimaries);
                if (searchResponse.getSuccessfulShards() == numShards.numPrimaries && !refreshFailed) {
                    assertResultsAndLogOnFailure(expectedResults, searchResponse);
                }
                // check match all
                searchResponse = client().prepareSearch().setTypes("type").setQuery(QueryBuilders.matchAllQuery())
                        .setSize(numCreated + numInitialDocs).addSort("_uid", SortOrder.ASC).get();
                logger.info("Match all Successful shards: [{}]  numShards: [{}]", searchResponse.getSuccessfulShards(), numShards.numPrimaries);
                if (searchResponse.getSuccessfulShards() == numShards.numPrimaries && !refreshFailed) {
                    assertResultsAndLogOnFailure(numCreated + numInitialDocs, searchResponse);
                }
            } catch (SearchPhaseExecutionException ex) {
                logger.info("SearchPhaseException: [{}]", ex.getMessage());
                // if a scheduled refresh or flush fails all shards we see all shards failed here
                if (!(expectAllShardsFailed || refreshResponse.getSuccessfulShards() == 0 || ex.getMessage().contains("all shards failed"))) {
                    throw ex;
                }
            }
        }

        if (createIndexWithoutErrors) {
            // check the index still contains the records that we indexed without errors
            client().admin().indices().prepareClose("test").execute().get();
            client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder()
                    .put(MockDirectoryHelper.RANDOM_IO_EXCEPTION_RATE, 0)
                    .put(MockDirectoryHelper.RANDOM_IO_EXCEPTION_RATE_ON_OPEN, 0));
            client().admin().indices().prepareOpen("test").execute().get();
            ensureGreen();
            SearchResponse searchResponse = client().prepareSearch().setTypes("type").setQuery(QueryBuilders.matchQuery("test", "init")).get();
            assertNoFailures(searchResponse);
            assertHitCount(searchResponse, numInitialDocs);
        }
    }

    private void assertResultsAndLogOnFailure(long expectedResults, SearchResponse searchResponse) {
        if (searchResponse.getHits().getTotalHits() != expectedResults) {
            StringBuilder sb = new StringBuilder("search result contains [");
            sb.append(searchResponse.getHits().getTotalHits()).append("] results. expected [").append(expectedResults).append("]");
            String failMsg = sb.toString();
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                sb.append("\n-> _index: [").append(hit.getIndex()).append("] type [").append(hit.getType())
                        .append("] id [").append(hit.id()).append("]");
            }
            logger.warn(sb.toString());
            fail(failMsg);
        }
    }

    @Test
    public void testRandomExceptions() throws IOException, InterruptedException, ExecutionException {
        String mapping = XContentFactory.jsonBuilder().
                startObject().
                startObject("type").
                startObject("properties").
                startObject("test")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject().
                        endObject().
                        endObject()
                .endObject().string();
        final double lowLevelRate;
        final double topLevelRate;
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

        Builder settings = settingsBuilder()
                .put(indexSettings())
                .put(MockEngineSupport.READER_WRAPPER_TYPE, RandomExceptionDirectoryReaderWrapper.class.getName())
                .put(EXCEPTION_TOP_LEVEL_RATIO_KEY, topLevelRate)
                .put(EXCEPTION_LOW_LEVEL_RATIO_KEY, lowLevelRate)
                .put(MockEngineSupport.WRAP_READER_RATIO, 1.0d);
        logger.info("creating index: [test] using settings: [{}]", settings.build().getAsMap());
        assertAcked(prepareCreate("test")
                .setSettings(settings)
                .addMapping("type", mapping));
        ensureSearchable();
        final int numDocs = between(10, 100);
        int numCreated = 0;
        boolean[] added = new boolean[numDocs];
        for (int i = 0; i < numDocs; i++) {
            try {
                IndexResponse indexResponse = client().prepareIndex("test", "type", "" + i).setTimeout(TimeValue.timeValueSeconds(1)).setSource("test", English.intToEnglish(i)).get();
                if (indexResponse.isCreated()) {
                    numCreated++;
                    added[i] = true;
                }
            } catch (ElasticsearchException ex) {
            }
        }
        logger.info("Start Refresh");
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh("test").execute().get(); // don't assert on failures here
        final boolean refreshFailed = refreshResponse.getShardFailures().length != 0 || refreshResponse.getFailedShards() != 0;
        logger.info("Refresh failed [{}] numShardsFailed: [{}], shardFailuresLength: [{}], successfulShards: [{}], totalShards: [{}] ", refreshFailed, refreshResponse.getFailedShards(), refreshResponse.getShardFailures().length, refreshResponse.getSuccessfulShards(), refreshResponse.getTotalShards());

        NumShards test = getNumShards("test");
        final int numSearches = scaledRandomIntBetween(100, 200);
        // we don't check anything here really just making sure we don't leave any open files or a broken index behind.
        for (int i = 0; i < numSearches; i++) {
            try {
                int docToQuery = between(0, numDocs - 1);
                int expectedResults = added[docToQuery] ? 1 : 0;
                logger.info("Searching for [test:{}]", English.intToEnglish(docToQuery));
                SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.matchQuery("test", English.intToEnglish(docToQuery)))
                        .setSize(expectedResults).get();
                logger.info("Successful shards: [{}]  numShards: [{}]", searchResponse.getSuccessfulShards(), test.numPrimaries);
                if (searchResponse.getSuccessfulShards() == test.numPrimaries && !refreshFailed) {
                    assertResultsAndLogOnFailure(expectedResults, searchResponse);
                }
                // check match all
                searchResponse = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).setSize(numCreated).addSort("_id", SortOrder.ASC).get();
                logger.info("Match all Successful shards: [{}]  numShards: [{}]", searchResponse.getSuccessfulShards(), test.numPrimaries);
                if (searchResponse.getSuccessfulShards() == test.numPrimaries && !refreshFailed) {
                    assertResultsAndLogOnFailure(numCreated, searchResponse);
                }

            } catch (SearchPhaseExecutionException ex) {
                logger.info("expected SearchPhaseException: [{}]", ex.getMessage());
            }
        }
    }


    public static final String EXCEPTION_TOP_LEVEL_RATIO_KEY = "index.engine.exception.ratio.top";
    public static final String EXCEPTION_LOW_LEVEL_RATIO_KEY = "index.engine.exception.ratio.low";


    public static class RandomExceptionDirectoryReaderWrapper extends MockEngineSupport.DirectoryReaderWrapper {
        private final Settings settings;

        static class ThrowingSubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper implements ThrowingLeafReaderWrapper.Thrower {
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
            public LeafReader wrap(LeafReader reader) {
                return new ThrowingLeafReaderWrapper(reader, this);
            }

            @Override
            public void maybeThrow(ThrowingLeafReaderWrapper.Flags flag) throws IOException {
                switch (flag) {
                    case Fields:
                    case TermVectors:
                    case Terms:
                    case TermsEnum:
                    case Intersect:
                    case Norms:
                    case NumericDocValues:
                    case BinaryDocValues:
                    case SortedDocValues:
                    case SortedSetDocValues:
                        if (random.nextDouble() < topLevelRatio) {
                            throw new IOException("Forced top level Exception on [" + flag.name() + "]");
                        }
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
                return true;
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
    }


}
