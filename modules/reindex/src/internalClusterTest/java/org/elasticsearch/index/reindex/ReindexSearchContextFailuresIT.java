/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.ReindexSettings;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for HTTP status codes when reindexing hits a ({@link SearchContextMissingException}).
 * Note that this test assumes we're using point-in-time searching rather than scroll search, since we only control the
 * keep-alive value for PIT. Scroll search, now used exclusively when reindexing from a remote cluster on a version &lt; 7.10,
 * uses a customer set keep-alive through the API, and so returning a 4XX error due to a timeout is always appropriate
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class ReindexSearchContextFailuresIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, MainRestPlugin.class, SearchContextFailureInjectionPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "*:*")
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0) // uncached millis for SearchContextKeepaliveDeadline
            .build();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            assertAcked(
                clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setPersistentSettings(Settings.builder().putNull(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey()).build())
            );
        } finally {
            SearchContextFailureInjectionPlugin.CONFIG.set(null);
            SearchContextFailureInjectionPlugin.PIT_SEARCH_COUNTER.set(0);
            SearchContextFailureInjectionPlugin.SCROLL_SEARCH_COUNTER.set(0);
            super.tearDown();
        }
    }

    /**
     * Failures that occur after the client-side keep-alive deadline are surfaced as {@link RestStatus#INTERNAL_SERVER_ERROR}
     */
    public void testPitKeepaliveExpiredReturns500StatusCode() {
        assumeTrue("reindex with point-in-time search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        TimeValue pitKeepAlive = TimeValue.timeValueMillis(200);
        assertAcked(
            clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(
                    Settings.builder().put(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey(), pitKeepAlive.getStringRep()).build()
                )
        );

        String source = randomAlphanumericOfLength(12).toLowerCase(Locale.ROOT);
        String dest = randomAlphanumericOfLength(12).toLowerCase(Locale.ROOT);
        assertAcked(prepareCreate(source));
        assertAcked(prepareCreate(dest));
        indexRandom(
            true,
            false,
            true,
            IntStream.range(0, 3).mapToObj(i -> prepareIndex(source).setId(Integer.toString(i)).setSource("n", i)).toList()
        );
        assertHitCount(prepareSearch(source).setSize(0).setTrackTotalHits(true), 3L);

        SearchContextFailureInjectionPlugin.PIT_SEARCH_COUNTER.set(0);
        SearchContextFailureInjectionPlugin.CONFIG.set(
            new SearchContextFailureInjectionPlugin.InjectionConfig(
                TimeValue.timeValueMillis(pitKeepAlive.millis() + 100),
                testMissingContext()
            )
        );

        // Use a HTTP request to hit the REST layer and assert the exception returned
        Request request = new Request("POST", "/_reindex");
        request.setJsonEntity(Strings.format("""
            {
              "source": { "index": "%s", "size": 1 },
              "dest": { "index": "%s" }
            }""", source, dest));

        ResponseException ex = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.INTERNAL_SERVER_ERROR.getStatus()));
    }

    /**
     * Failures that occur during the client-side keep-alive deadline are surfaced as {@link RestStatus#NOT_FOUND}
     */
    public void testPitSearchContextMissingInsideKeepaliveReturns404StatusCode() {
        assumeTrue("reindex with point-in-time search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        TimeValue pitKeepAlive = TimeValue.timeValueSeconds(5);
        assertAcked(
            clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(
                    Settings.builder().put(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey(), pitKeepAlive.getStringRep()).build()
                )
        );

        String source = randomAlphanumericOfLength(12).toLowerCase(Locale.ROOT);
        String dest = randomAlphanumericOfLength(12).toLowerCase(Locale.ROOT);
        assertAcked(prepareCreate(source));
        assertAcked(prepareCreate(dest));
        indexRandom(
            true,
            false,
            true,
            IntStream.range(0, 3).mapToObj(i -> prepareIndex(source).setId(Integer.toString(i)).setSource("n", i)).toList()
        );
        assertHitCount(prepareSearch(source).setSize(0).setTrackTotalHits(true), 3L);

        SearchContextFailureInjectionPlugin.PIT_SEARCH_COUNTER.set(0);
        SearchContextFailureInjectionPlugin.CONFIG.set(
            new SearchContextFailureInjectionPlugin.InjectionConfig(TimeValue.ZERO, testMissingContext())
        );

        // Use a HTTP request to hit the REST layer and assert the exception returned
        Request request = new Request("POST", "/_reindex");
        request.setJsonEntity(Strings.format("""
            {
              "source": { "index": "%s", "size": 1 },
              "dest": { "index": "%s" }
            }""", source, dest));

        ResponseException ex = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.NOT_FOUND.getStatus()));
    }

    private static SearchContextMissingException testMissingContext() {
        return new SearchContextMissingException(new ShardSearchContextId("it_session", 1L));
    }
}
