/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlClientHelperTests extends ESTestCase {

    private Client client = mock(Client.class);

    @Before
    public void setupMocks() {
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);

        PlainActionFuture<SearchResponse> searchFuture = PlainActionFuture.newFuture();
        searchFuture.onResponse(new SearchResponse());
        when(client.search(any())).thenReturn(searchFuture);
    }

    public void testEmptyHeaders() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed-foo", "foo");
        builder.setIndices(Collections.singletonList("foo-index"));

        assertExecutionWithOrigin(builder.build());
    }

    public void testWithHeaders() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed-foo", "foo");
        builder.setIndices(Collections.singletonList("foo-index"));
        Map<String, String> headers = MapBuilder.<String, String>newMapBuilder()
                .put(AuthenticationField.AUTHENTICATION_KEY, "anything")
                .put(AuthenticationServiceField.RUN_AS_USER_HEADER, "anything")
                .map();
        builder.setHeaders(headers);

        assertRunAsExecution(builder.build(), h -> {
            assertThat(h.keySet(), hasSize(2));
            assertThat(h, hasEntry(AuthenticationField.AUTHENTICATION_KEY, "anything"));
            assertThat(h, hasEntry(AuthenticationServiceField.RUN_AS_USER_HEADER, "anything"));
        });
    }

    public void testFilteredHeaders() {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed-foo", "foo");
        builder.setIndices(Collections.singletonList("foo-index"));
        Map<String, String> unrelatedHeaders = MapBuilder.<String, String>newMapBuilder()
                .put(randomAlphaOfLength(10), "anything")
                .map();
        builder.setHeaders(unrelatedHeaders);

        assertRunAsExecution(builder.build(), h -> assertThat(h.keySet(), hasSize(0)));
    }

    /**
     * This method executes a search and checks if the thread context was enriched with the ml origin
     */
    private void assertExecutionWithOrigin(DatafeedConfig datafeedConfig) {
        MlClientHelper.execute(datafeedConfig, client, () -> {
            Object origin = client.threadPool().getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME);
            assertThat(origin, is(ML_ORIGIN));

            // Check that headers are not set
            Map<String, String> headers = client.threadPool().getThreadContext().getHeaders();
            assertThat(headers, not(hasEntry(AuthenticationField.AUTHENTICATION_KEY, "anything")));
            assertThat(headers, not(hasEntry(AuthenticationServiceField.RUN_AS_USER_HEADER, "anything")));

            return client.search(new SearchRequest()).actionGet();
        });
    }

    /**
     * This method executes a search and ensures no stashed origin thread context was created, so that the regular node
     * client was used, to emulate a run_as function
     */
    public void assertRunAsExecution(DatafeedConfig datafeedConfig, Consumer<Map<String, String>> consumer) {
        MlClientHelper.execute(datafeedConfig, client, () -> {
            Object origin = client.threadPool().getThreadContext().getTransient(ACTION_ORIGIN_TRANSIENT_NAME);
            assertThat(origin, is(nullValue()));

            consumer.accept(client.threadPool().getThreadContext().getHeaders());
            return client.search(new SearchRequest()).actionGet();
        });
    }
}
