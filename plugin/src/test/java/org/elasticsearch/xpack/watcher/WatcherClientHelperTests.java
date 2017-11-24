/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.test.WatchExecutionContextMockBuilder;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME;
import static org.elasticsearch.xpack.ClientHelper.WATCHER_ORIGIN;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WatcherClientHelperTests extends ESTestCase {

    private Client client = mock(Client.class);

    @Before
    public void setupMocks() {
        PlainActionFuture<SearchResponse> searchFuture = PlainActionFuture.newFuture();
        searchFuture.onResponse(new SearchResponse());
        when(client.search(any())).thenReturn(searchFuture);

        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);
    }

    public void testEmptyHeaders() {
        WatchExecutionContext ctx = new WatchExecutionContextMockBuilder("_id").buildMock();
        when(ctx.watch().status().getHeaders()).thenReturn(Collections.emptyMap());

        assertExecutionWithOrigin(ctx);
    }

    public void testWithHeaders() {
        WatchExecutionContext ctx = new WatchExecutionContextMockBuilder("_id").buildMock();
        Map<String, String> watchStatusHeaders = MapBuilder.<String, String>newMapBuilder()
                .put("es-security-runas-user", "anything")
                .put("_xpack_security_authentication", "anything")
                .map();
        when(ctx.watch().status().getHeaders()).thenReturn(watchStatusHeaders);

        assertRunAsExecution(ctx, headers -> {
            assertThat(headers.keySet(), hasSize(2));
            assertThat(headers, hasEntry("es-security-runas-user", "anything"));
            assertThat(headers, hasEntry("_xpack_security_authentication", "anything"));
        });
    }

    public void testFilteredHeaders() {
        WatchExecutionContext ctx = new WatchExecutionContextMockBuilder("_id").buildMock();
        Map<String, String> watchStatusHeaders = MapBuilder.<String, String>newMapBuilder()
                .put(randomAlphaOfLength(10), "anything")
                .map();
        when(ctx.watch().status().getHeaders()).thenReturn(watchStatusHeaders);

        assertRunAsExecution(ctx, headers -> {
            assertThat(headers.keySet(), hasSize(0));
        });
    }

    /**
     * This method executes a search and checks if the thread context was enriched with the watcher origin
     */
    private void assertExecutionWithOrigin(WatchExecutionContext ctx) {
        WatcherClientHelper.execute(ctx.watch(), client, () -> {
            Object origin = client.threadPool().getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME);
            assertThat(origin, is(WATCHER_ORIGIN));

            // check that headers are not set
            Map<String, String> headers = client.threadPool().getThreadContext().getHeaders();
            assertThat(headers, not(hasEntry("es-security-runas-user", "anything")));
            assertThat(headers, not(hasEntry("_xpack_security_authentication", "anything")));

            return client.search(new SearchRequest()).actionGet();
        });

    }

    /**
     * This method executes a search and ensures no stashed origin thread context was created, so that the regular node
     * client was used, to emulate a run_as function
     */
    public void assertRunAsExecution(WatchExecutionContext ctx, Consumer<Map<String, String>> consumer) {
        WatcherClientHelper.execute(ctx.watch(), client, () -> {
            Object origin = client.threadPool().getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME);
            assertThat(origin, is(nullValue()));

            Map<String, String> headers = client.threadPool().getThreadContext().getHeaders();
            consumer.accept(headers);
            return client.search(new SearchRequest()).actionGet();
        });

    }
}