/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class MaxDocsLimitIT extends ESIntegTestCase {

    private static final AtomicInteger maxDocs = new AtomicInteger();

    public static class TestEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> {
                assert maxDocs.get() > 0 : "maxDocs is unset";
                return EngineTestCase.createEngine(config, maxDocs.get());
            });
        }
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestEnginePlugin.class);
    }

    @Before
    public void setMaxDocs() {
        maxDocs.set(randomIntBetween(10, 100)); // Do not set this too low as we can fail to write the cluster state
        setIndexWriterMaxDocs(maxDocs.get());
    }

    @After
    public void restoreMaxDocs() {
        restoreIndexWriterMaxDocs();
    }

    public void testMaxDocsLimit() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                )
        );
        IndexingResult indexingResult = indexDocs(maxDocs.get(), 1);
        assertThat(indexingResult.numSuccess, equalTo(maxDocs.get()));
        assertThat(indexingResult.numFailures, equalTo(0));
        int rejectedRequests = between(1, 10);
        indexingResult = indexDocs(rejectedRequests, between(1, 8));
        assertThat(indexingResult.numFailures, equalTo(rejectedRequests));
        assertThat(indexingResult.numSuccess, equalTo(0));
        final IllegalArgumentException deleteError = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareDelete("test", "any-id").get()
        );
        assertThat(deleteError.getMessage(), containsString("Number of documents in the index can't exceed [" + maxDocs.get() + "]"));
        client().admin().indices().prepareRefresh("test").get();
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(new MatchAllQueryBuilder())
            .setTrackTotalHitsUpTo(Integer.MAX_VALUE)
            .setSize(0)
            .get();
        ElasticsearchAssertions.assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) maxDocs.get()));
        if (randomBoolean()) {
            client().admin().indices().prepareFlush("test").get();
        }
        internalCluster().fullRestart();
        internalCluster().ensureAtLeastNumDataNodes(2);
        ensureGreen("test");
        searchResponse = client().prepareSearch("test")
            .setQuery(new MatchAllQueryBuilder())
            .setTrackTotalHitsUpTo(Integer.MAX_VALUE)
            .setSize(0)
            .get();
        ElasticsearchAssertions.assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) maxDocs.get()));
    }

    public void testMaxDocsLimitConcurrently() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        assertAcked(
            client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
        );
        IndexingResult indexingResult = indexDocs(between(maxDocs.get() + 1, maxDocs.get() * 2), between(2, 8));
        assertThat(indexingResult.numFailures, greaterThan(0));
        assertThat(indexingResult.numSuccess, both(greaterThan(0)).and(lessThanOrEqualTo(maxDocs.get())));
        client().admin().indices().prepareRefresh("test").get();
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(new MatchAllQueryBuilder())
            .setTrackTotalHitsUpTo(Integer.MAX_VALUE)
            .setSize(0)
            .get();
        ElasticsearchAssertions.assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) indexingResult.numSuccess));
        int totalSuccess = indexingResult.numSuccess;
        while (totalSuccess < maxDocs.get()) {
            indexingResult = indexDocs(between(1, 10), between(1, 8));
            assertThat(indexingResult.numSuccess, greaterThan(0));
            totalSuccess += indexingResult.numSuccess;
        }
        if (randomBoolean()) {
            indexingResult = indexDocs(between(1, 10), between(1, 8));
            assertThat(indexingResult.numSuccess, equalTo(0));
        }
        client().admin().indices().prepareRefresh("test").get();
        searchResponse = client().prepareSearch("test")
            .setQuery(new MatchAllQueryBuilder())
            .setTrackTotalHitsUpTo(Integer.MAX_VALUE)
            .setSize(0)
            .get();
        ElasticsearchAssertions.assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) totalSuccess));
    }

    record IndexingResult(int numSuccess, int numFailures) {}

    static IndexingResult indexDocs(int numRequests, int numThreads) throws Exception {
        final AtomicInteger completedRequests = new AtomicInteger();
        final AtomicInteger numSuccess = new AtomicInteger();
        final AtomicInteger numFailure = new AtomicInteger();
        Thread[] indexers = new Thread[numThreads];
        Phaser phaser = new Phaser(indexers.length);
        for (int i = 0; i < indexers.length; i++) {
            indexers[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                while (completedRequests.incrementAndGet() <= numRequests) {
                    try {
                        final IndexResponse resp = client().prepareIndex("test").setSource("{}", XContentType.JSON).get();
                        numSuccess.incrementAndGet();
                        assertThat(resp.status(), equalTo(RestStatus.CREATED));
                    } catch (IllegalArgumentException e) {
                        numFailure.incrementAndGet();
                        assertThat(e.getMessage(), containsString("Number of documents in the index can't exceed [" + maxDocs.get() + "]"));
                    }
                }
            });
            indexers[i].start();
        }
        for (Thread indexer : indexers) {
            indexer.join();
        }
        internalCluster().assertNoInFlightDocsInEngine();
        return new IndexingResult(numSuccess.get(), numFailure.get());
    }
}
