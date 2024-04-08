/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.XContentType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link RestBulkAction}.
 */
public class RestBulkActionTests extends ESTestCase {

    public void testBulkPipelineUpsert() throws Exception {
        SetOnce<Boolean> bulkCalled = new SetOnce<>();
        try (var threadPool = createThreadPool()) {
            final var verifyingClient = new NoOpNodeClient(threadPool) {
                @Override
                public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
                    bulkCalled.set(true);
                    assertThat(request.requests(), hasSize(2));
                    UpdateRequest updateRequest = (UpdateRequest) request.requests().get(1);
                    assertThat(updateRequest.upsertRequest().getPipeline(), equalTo("timestamps"));
                }
            };
            final Map<String, String> params = new HashMap<>();
            params.put("pipeline", "timestamps");
            new RestBulkAction(settings(IndexVersion.current()).build()).handleRequest(
                new FakeRestRequest.Builder(xContentRegistry()).withPath("my_index/_bulk").withParams(params).withContent(new BytesArray("""
                    {"index":{"_id":"1"}}
                    {"field1":"val1"}
                    {"update":{"_id":"2"}}
                    {"script":{"source":"ctx._source.counter++;"},"upsert":{"field1":"upserted_val"}}
                    """), XContentType.JSON).withMethod(RestRequest.Method.POST).build(),
                mock(RestChannel.class),
                verifyingClient
            );
            assertThat(bulkCalled.get(), equalTo(true));
        }
    }

    public void testListExecutedPipelines() throws Exception {
        AtomicBoolean bulkCalled = new AtomicBoolean(false);
        AtomicBoolean listExecutedPipelinesRequest1 = new AtomicBoolean(false);
        AtomicBoolean listExecutedPipelinesRequest2 = new AtomicBoolean(false);
        try (var threadPool = createThreadPool()) {
            final var verifyingClient = new NoOpNodeClient(threadPool) {
                @Override
                public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
                    bulkCalled.set(true);
                    assertThat(request.requests(), hasSize(2));
                    IndexRequest indexRequest1 = (IndexRequest) request.requests().get(0);
                    listExecutedPipelinesRequest1.set(indexRequest1.getListExecutedPipelines());
                    IndexRequest indexRequest2 = (IndexRequest) request.requests().get(1);
                    listExecutedPipelinesRequest2.set(indexRequest2.getListExecutedPipelines());
                }
            };
            Map<String, String> params = new HashMap<>();
            {
                new RestBulkAction(settings(IndexVersion.current()).build()).handleRequest(
                    new FakeRestRequest.Builder(xContentRegistry()).withPath("my_index/_bulk")
                        .withParams(params)
                        .withContent(new BytesArray("""
                            {"index":{"_id":"1"}}
                            {"field1":"val1"}
                            {"index":{"_id":"2"}}
                            {"field1":"val2"}
                            """), XContentType.JSON)
                        .withMethod(RestRequest.Method.POST)
                        .build(),
                    mock(RestChannel.class),
                    verifyingClient
                );
                assertThat(bulkCalled.get(), equalTo(true));
                assertThat(listExecutedPipelinesRequest1.get(), equalTo(false));
                assertThat(listExecutedPipelinesRequest2.get(), equalTo(false));
            }
            {
                params.put("list_executed_pipelines", "true");
                bulkCalled.set(false);
                new RestBulkAction(settings(IndexVersion.current()).build()).handleRequest(
                    new FakeRestRequest.Builder(xContentRegistry()).withPath("my_index/_bulk")
                        .withParams(params)
                        .withContent(new BytesArray("""
                            {"index":{"_id":"1"}}
                            {"field1":"val1"}
                            {"index":{"_id":"2"}}
                            {"field1":"val2"}
                            """), XContentType.JSON)
                        .withMethod(RestRequest.Method.POST)
                        .build(),
                    mock(RestChannel.class),
                    verifyingClient
                );
                assertThat(bulkCalled.get(), equalTo(true));
                assertThat(listExecutedPipelinesRequest1.get(), equalTo(true));
                assertThat(listExecutedPipelinesRequest2.get(), equalTo(true));
            }
            {
                bulkCalled.set(false);
                new RestBulkAction(settings(IndexVersion.current()).build()).handleRequest(
                    new FakeRestRequest.Builder(xContentRegistry()).withPath("my_index/_bulk")
                        .withParams(params)
                        .withContent(new BytesArray("""
                            {"index":{"_id":"1", "list_executed_pipelines": "false"}}
                            {"field1":"val1"}
                            {"index":{"_id":"2"}}
                            {"field1":"val2"}
                            """), XContentType.JSON)
                        .withMethod(RestRequest.Method.POST)
                        .build(),
                    mock(RestChannel.class),
                    verifyingClient
                );
                assertThat(bulkCalled.get(), equalTo(true));
                assertThat(listExecutedPipelinesRequest1.get(), equalTo(false));
                assertThat(listExecutedPipelinesRequest2.get(), equalTo(true));
            }
            {
                params.remove("list_executed_pipelines");
                bulkCalled.set(false);
                new RestBulkAction(settings(IndexVersion.current()).build()).handleRequest(
                    new FakeRestRequest.Builder(xContentRegistry()).withPath("my_index/_bulk")
                        .withParams(params)
                        .withContent(new BytesArray("""
                            {"index":{"_id":"1", "list_executed_pipelines": "true"}}
                            {"field1":"val1"}
                            {"index":{"_id":"2"}}
                            {"field1":"val2"}
                            """), XContentType.JSON)
                        .withMethod(RestRequest.Method.POST)
                        .build(),
                    mock(RestChannel.class),
                    verifyingClient
                );
                assertThat(bulkCalled.get(), equalTo(true));
                assertThat(listExecutedPipelinesRequest1.get(), equalTo(true));
                assertThat(listExecutedPipelinesRequest2.get(), equalTo(false));
            }
        }
    }
}
