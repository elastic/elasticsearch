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

package org.elasticsearch.client;

import org.apache.http.Header;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

public abstract class ESRestHighLevelClientTestCase extends ESRestTestCase {

    private static RestHighLevelClient restHighLevelClient;

    @Before
    public void initHighLevelClient() throws IOException {
        super.initClient();
        if (restHighLevelClient == null) {
            restHighLevelClient = new HighLevelClient(client());
        }
    }

    @AfterClass
    public static void cleanupClient() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }

    protected static RestHighLevelClient highLevelClient() {
        return restHighLevelClient;
    }

    /**
     * Executes the provided request using either the sync method or its async variant, both provided as functions
     */
    protected static <Req, Resp> Resp execute(Req request, SyncMethod<Req, Resp> syncMethod,
                                       AsyncMethod<Req, Resp> asyncMethod) throws IOException {
        if (randomBoolean()) {
            return syncMethod.execute(request, RequestOptions.DEFAULT);
        } else {
            PlainActionFuture<Resp> future = PlainActionFuture.newFuture();
            asyncMethod.execute(request, RequestOptions.DEFAULT, future);
            return future.actionGet();
        }
    }

    @FunctionalInterface
    protected interface SyncMethod<Request, Response> {
        Response execute(Request request, RequestOptions options) throws IOException;
    }

    @FunctionalInterface
    protected interface AsyncMethod<Request, Response> {
        void execute(Request request, RequestOptions options, ActionListener<Response> listener);
    }

    /**
     * Executes the provided request using either the sync method or its async variant, both provided as functions
     */
    @Deprecated
    protected static <Req, Resp> Resp execute(Req request, SyncMethod<Req, Resp> syncMethod, AsyncMethod<Req, Resp> asyncMethod,
                                              SyncMethodWithHeaders<Req, Resp> syncMethodWithHeaders,
                                              AsyncMethodWithHeaders<Req, Resp> asyncMethodWithHeaders) throws IOException {
        switch(randomIntBetween(0, 3)) {
            case 0:
                return syncMethod.execute(request, RequestOptions.DEFAULT);
            case 1:
                PlainActionFuture<Resp> future = PlainActionFuture.newFuture();
                asyncMethod.execute(request, RequestOptions.DEFAULT, future);
                return future.actionGet();
            case 2:
                return syncMethodWithHeaders.execute(request);
            case 3:
                PlainActionFuture<Resp> futureWithHeaders = PlainActionFuture.newFuture();
                asyncMethodWithHeaders.execute(request, futureWithHeaders);
                return futureWithHeaders.actionGet();
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Deprecated
    @FunctionalInterface
    protected interface SyncMethodWithHeaders<Request, Response> {
        Response execute(Request request, Header... headers) throws IOException;
    }

    @Deprecated
    @FunctionalInterface
    protected interface AsyncMethodWithHeaders<Request, Response> {
        void execute(Request request, ActionListener<Response> listener, Header... headers);
    }

    private static class HighLevelClient extends RestHighLevelClient {
        private HighLevelClient(RestClient restClient) {
            super(restClient, (client) -> {}, Collections.emptyList());
        }
    }

    protected static XContentBuilder buildRandomXContentPipeline() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder pipelineBuilder = XContentBuilder.builder(xContentType.xContent());
        pipelineBuilder.startObject();
        {
            pipelineBuilder.field(Pipeline.DESCRIPTION_KEY, "some random set of processors");
            pipelineBuilder.startArray(Pipeline.PROCESSORS_KEY);
            {
                pipelineBuilder.startObject().startObject("set");
                {
                    pipelineBuilder
                        .field("field", "foo")
                        .field("value", "bar");
                }
                pipelineBuilder.endObject().endObject();
                pipelineBuilder.startObject().startObject("convert");
                {
                    pipelineBuilder
                        .field("field", "rank")
                        .field("type", "integer");
                }
                pipelineBuilder.endObject().endObject();
            }
            pipelineBuilder.endArray();
        }
        pipelineBuilder.endObject();
        return pipelineBuilder;
    }

    protected static void createPipeline(String pipelineId) throws IOException {
        XContentBuilder builder = buildRandomXContentPipeline();
        createPipeline(new PutPipelineRequest(pipelineId, BytesReference.bytes(builder), builder.contentType()));
    }

    protected static void createPipeline(PutPipelineRequest putPipelineRequest) throws IOException {
        assertOK(client().performRequest(RequestConverters.putPipeline(putPipelineRequest)));
    }
}
