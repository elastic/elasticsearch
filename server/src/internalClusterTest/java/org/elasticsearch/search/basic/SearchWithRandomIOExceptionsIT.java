/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.basic;

import org.apache.lucene.tests.util.English;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.store.MockFSDirectoryFactory;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public class SearchWithRandomIOExceptionsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockFSIndexStore.TestPlugin.class);
    }

    public void testRandomDirectoryIOExceptions() throws IOException, InterruptedException, ExecutionException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("test")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
        );
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
            Settings.Builder settings = Settings.builder().put("index.number_of_replicas", numberOfReplicas());
            logger.info("creating index: [test] using settings: [{}]", settings.build());
            indicesAdmin().prepareCreate("test").setSettings(settings).setMapping(mapping).get();
            numInitialDocs = between(10, 100);
            ensureGreen();
            for (int i = 0; i < numInitialDocs; i++) {
                prepareIndex("test").setId("init" + i).setSource("test", "init").get();
            }
            indicesAdmin().prepareRefresh("test").execute().get();
            indicesAdmin().prepareFlush("test").execute().get();
            indicesAdmin().prepareClose("test").execute().get();
            indicesAdmin().prepareOpen("test").execute().get();
        } else {
            Settings.Builder settings = Settings.builder()
                .put("index.number_of_replicas", randomIntBetween(0, 1))
                .put(MockFSIndexStore.INDEX_CHECK_INDEX_ON_CLOSE_SETTING.getKey(), false)
                .put(MockFSDirectoryFactory.RANDOM_IO_EXCEPTION_RATE_SETTING.getKey(), exceptionRate)
                // we cannot expect that the index will be valid
                .put(MockFSDirectoryFactory.RANDOM_IO_EXCEPTION_RATE_ON_OPEN_SETTING.getKey(), exceptionOnOpenRate);
            logger.info("creating index: [test] using settings: [{}]", settings.build());
            indicesAdmin().prepareCreate("test").setSettings(settings).setMapping(mapping).get();
        }
        ClusterHealthResponse clusterHealthResponse = clusterAdmin()
            // it's OK to timeout here
            .health(
                new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, new String[] {}).waitForYellowStatus()
                    .masterNodeTimeout(TimeValue.timeValueSeconds(5))
                    .timeout(TimeValue.timeValueSeconds(5))
            )
            .get();
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
                DocWriteResponse indexResponse = prepareIndex("test").setId(Integer.toString(i))
                    .setTimeout(TimeValue.timeValueSeconds(1))
                    .setSource("test", English.intToEnglish(i))
                    .get();
                if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    numCreated++;
                    added[i] = true;
                }
            } catch (ElasticsearchException ex) {}

        }
        ESIntegTestCase.NumShards numShards = getNumShards("test");
        logger.info("Start Refresh");
        // don't assert on failures here
        final BroadcastResponse refreshResponse = indicesAdmin().prepareRefresh("test").execute().get();
        final boolean refreshFailed = refreshResponse.getShardFailures().length != 0 || refreshResponse.getFailedShards() != 0;
        logger.info(
            "Refresh failed [{}] numShardsFailed: [{}], shardFailuresLength: [{}], successfulShards: [{}], totalShards: [{}] ",
            refreshFailed,
            refreshResponse.getFailedShards(),
            refreshResponse.getShardFailures().length,
            refreshResponse.getSuccessfulShards(),
            refreshResponse.getTotalShards()
        );
        final int numSearches = scaledRandomIntBetween(10, 20);
        final int finalNumCreated = numCreated;
        final int finalNumInitialDocs = numInitialDocs;
        // we don't check anything here really just making sure we don't leave any open files or a broken index behind.
        for (int i = 0; i < numSearches; i++) {
            try {
                int docToQuery = between(0, numDocs - 1);
                int expectedResults = added[docToQuery] ? 1 : 0;
                logger.info("Searching for [test:{}]", English.intToEnglish(docToQuery));
                assertResponse(
                    prepareSearch().setQuery(QueryBuilders.matchQuery("test", English.intToEnglish(docToQuery))).setSize(expectedResults),
                    response -> {
                        logger.info("Successful shards: [{}]  numShards: [{}]", response.getSuccessfulShards(), numShards.numPrimaries);
                        if (response.getSuccessfulShards() == numShards.numPrimaries && refreshFailed == false) {
                            assertResultsAndLogOnFailure(expectedResults, response);
                        }
                    }
                );
                // check match all
                assertResponse(
                    prepareSearch().setQuery(QueryBuilders.matchAllQuery())
                        .setSize(numCreated + numInitialDocs)
                        .addSort("_uid", SortOrder.ASC),
                    response -> {
                        logger.info(
                            "Match all Successful shards: [{}]  numShards: [{}]",
                            response.getSuccessfulShards(),
                            numShards.numPrimaries
                        );
                        if (response.getSuccessfulShards() == numShards.numPrimaries && refreshFailed == false) {
                            assertResultsAndLogOnFailure(finalNumCreated + finalNumInitialDocs, response);
                        }
                    }
                );
            } catch (SearchPhaseExecutionException ex) {
                logger.info("SearchPhaseException: [{}]", ex.getMessage());
                // if a scheduled refresh or flush fails all shards we see all shards failed here
                if ((expectAllShardsFailed
                    || refreshResponse.getSuccessfulShards() == 0
                    || ex.getMessage().contains("all shards failed")) == false) {
                    throw ex;
                }
            }
        }

        if (createIndexWithoutErrors) {
            // check the index still contains the records that we indexed without errors
            indicesAdmin().prepareClose("test").execute().get();
            indicesAdmin().prepareOpen("test").execute().get();
            ensureGreen();
            assertHitCountAndNoFailures(prepareSearch().setQuery(QueryBuilders.matchQuery("test", "init")), numInitialDocs);
        }
    }
}
