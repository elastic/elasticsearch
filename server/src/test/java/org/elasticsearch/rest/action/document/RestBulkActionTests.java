/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link RestBulkAction}.
 */
public class RestBulkActionTests extends ESTestCase {

    public void testBulkPipelineUpsert() throws Exception {
        SetOnce<Boolean> bulkCalled = new SetOnce<>();
        try (NodeClient verifyingClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
                bulkCalled.set(true);
                assertThat(request.requests(), hasSize(2));
                UpdateRequest updateRequest = (UpdateRequest) request.requests().get(1);
                assertThat(updateRequest.upsertRequest().getPipeline(), equalTo("timestamps"));
            }
        }) {
            final Map<String, String> params = new HashMap<>();
            params.put("pipeline", "timestamps");
            new RestBulkAction(settings(Version.CURRENT).build())
                .handleRequest(
                    new FakeRestRequest.Builder(
                        xContentRegistry()).withPath("my_index/_bulk").withParams(params)
                        .withContent(
                            new BytesArray(
                                "{\"index\":{\"_id\":\"1\"}}\n" +
                                    "{\"field1\":\"val1\"}\n" +
                                    "{\"update\":{\"_id\":\"2\"}}\n" +
                                    "{\"script\":{\"source\":\"ctx._source.counter++;\"},\"upsert\":{\"field1\":\"upserted_val\"}}\n"
                            ),
                            XContentType.JSON
                        ).withMethod(RestRequest.Method.POST).build(),
                    mock(RestChannel.class), verifyingClient
                );
            assertThat(bulkCalled.get(), equalTo(true));
        }
    }
}
