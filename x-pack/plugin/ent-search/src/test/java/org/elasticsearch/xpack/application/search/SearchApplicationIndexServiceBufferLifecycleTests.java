/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SearchApplicationIndexServiceBufferLifecycleTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testBufferNotReleasedUntilListenerCompletes() throws Exception {
        final var capturedIndexRequest = new AtomicReference<IndexRequest>();
        final var capturedIndexListener = new AtomicReference<ActionListener<DocWriteResponse>>();

        try (var threadPool = new TestThreadPool(getTestName())) {
            var client = new NoOpClient(threadPool) {
                @Override
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    if (request instanceof IndicesAliasesRequest) {
                        listener.onResponse((Response) IndicesAliasesResponse.ACKNOWLEDGED_NO_ERRORS);
                    } else if (request instanceof IndexRequest indexRequest) {
                        capturedIndexRequest.set(indexRequest);
                        capturedIndexListener.set((ActionListener<DocWriteResponse>) listener);
                    }
                }
            };

            BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

            try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool)) {
                var service = new SearchApplicationIndexService(client, clusterService, new NamedWriteableRegistry(List.of()), bigArrays);

                var app = new SearchApplication("test-app", new String[] { "test-index" }, null, System.currentTimeMillis(), null);

                final var resultRef = new AtomicReference<DocWriteResponse>();
                final var failureRef = new AtomicReference<Exception>();

                service.putSearchApplication(app, true, new ActionListener<>() {
                    @Override
                    public void onResponse(DocWriteResponse response) {
                        resultRef.set(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        failureRef.set(e);
                    }
                });

                assertThat("index request should have been captured", capturedIndexRequest.get(), notNullValue());
                assertThat("index listener should have been captured", capturedIndexListener.get(), notNullValue());

                // The buffer must still be alive after putSearchApplication returns.
                // MockBigArrays poisons byte arrays with random content on close,
                // so parsing the source bytes will fail if the buffer was prematurely released.
                Map<String, Object> parsed = XContentHelper.convertToMap(capturedIndexRequest.get().source(), false, XContentType.JSON)
                    .v2();
                assertThat(parsed.get("name"), equalTo("test-app"));

                // Complete the async index operation — this triggers buffer release via the listener.
                capturedIndexListener.get()
                    .onResponse(new IndexResponse(new ShardId(".search-app-1", "_na_", 0), "test-app", 1, 1, 1, true));

                assertNull(failureRef.get());
                assertThat(resultRef.get(), notNullValue());
            }
        }
    }
}
