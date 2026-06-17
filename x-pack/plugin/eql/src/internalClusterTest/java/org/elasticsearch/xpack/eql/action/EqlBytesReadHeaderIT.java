/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.store.Store;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

public class EqlBytesReadHeaderIT extends AbstractEqlIntegTestCase {

    private static final String SEARCH_METRICS_HEADER = "X-Elasticsearch-Search-Metrics";

    @Before
    public void ensureDirectoryMetricsEnabled() {
        assumeTrue("directory metrics must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());
    }

    public void testEventQueryDoesNotEmitMetricsHeader() throws InterruptedException {
        final String indexName = createEqlIndex();
        final int numDocs = indexProcessEvents(indexName);

        EqlSearchRequest request = new EqlSearchRequest().indices(indexName).query("process where true").size(numDocs);

        SetOnce<Integer> matchedEvents = new SetOnce<>();
        List<String> headerValues = runEqlAndCaptureMetricsHeaders(request, response -> matchedEvents.set(response.hits().events().size()));

        assertThat(matchedEvents.get(), equalTo(numDocs));
        assertThat("EQL must not surface the per-search metrics header", headerValues, nullValue());
    }

    // sequence runs several queries, which we want to ensure to not create a header per query, so this is disabled for now
    public void testSequenceQueryDoesNotEmitMetricsHeader() throws InterruptedException {
        final String indexName = createEqlIndex();
        indexProcessEvents(indexName);

        EqlSearchRequest request = new EqlSearchRequest().indices(indexName).query("sequence [process where true] [process where true]");

        SetOnce<Integer> matchedSequences = new SetOnce<>();
        List<String> headerValues = runEqlAndCaptureMetricsHeaders(
            request,
            response -> matchedSequences.set(response.hits().sequences().size())
        );

        assertThat("the sequence query should have matched at least one pair of events", matchedSequences.get(), greaterThan(0));
        assertThat("EQL must not surface the per-search metrics header", headerValues, nullValue());
    }

    private String createEqlIndex() {
        final String indexName = randomIndexName();
        final int primaryShards = between(2, 5);
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(indexSettings(primaryShards, 0))
                .setMapping("@timestamp", "type=date", "event.category", "type=keyword")
        );
        return indexName;
    }

    private int indexProcessEvents(String indexName) {
        final int numDocs = between(50, 200);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex(indexName).setId(Integer.toString(i)).setSource("@timestamp", 100000 + i, "event.category", "process").get();
        }
        refresh(indexName);
        return numDocs;
    }

    private List<String> runEqlAndCaptureMetricsHeaders(EqlSearchRequest request, Consumer<EqlSearchResponse> responseConsumer)
        throws InterruptedException {
        SetOnce<List<String>> headerValues = new SetOnce<>();
        SetOnce<Exception> failure = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);
        final Client client = client();

        client.execute(EqlSearchAction.INSTANCE, request, new LatchedActionListener<>(ActionListener.assertOnce(new ActionListener<>() {
            @Override
            public void onResponse(EqlSearchResponse response) {
                responseConsumer.accept(response);
                headerValues.set(client.threadPool().getThreadContext().getResponseHeaders().get(SEARCH_METRICS_HEADER));
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
            }
        }), latch));

        assertTrue("EQL search did not complete in time", latch.await(30, TimeUnit.SECONDS));

        if (failure.get() != null) {
            throw new AssertionError("unexpected EQL search failure", failure.get());
        }
        return headerValues.get();
    }
}
