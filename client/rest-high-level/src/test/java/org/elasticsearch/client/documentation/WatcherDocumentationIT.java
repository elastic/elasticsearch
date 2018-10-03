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
package org.elasticsearch.client.documentation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.watcher.AckWatchRequest;
import org.elasticsearch.client.watcher.AckWatchResponse;
import org.elasticsearch.client.watcher.ActionStatus;
import org.elasticsearch.client.watcher.ActionStatus.AckStatus;
import org.elasticsearch.client.watcher.WatchStatus;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.rest.RestStatus;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class WatcherDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testWatcher() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            //tag::x-pack-put-watch-execute
            // you can also use the WatchSourceBuilder from org.elasticsearch.plugin:x-pack-core to create a watch programmatically
            BytesReference watch = new BytesArray("{ \n" +
                "  \"trigger\": { \"schedule\": { \"interval\": \"10h\" } },\n" +
                "  \"input\": { \"simple\": { \"foo\" : \"bar\" } },\n" +
                "  \"actions\": { \"logme\": { \"logging\": { \"text\": \"{{ctx.payload}}\" } } }\n" +
                "}");
            PutWatchRequest request = new PutWatchRequest("my_watch_id", watch, XContentType.JSON);
            request.setActive(false); // <1>
            PutWatchResponse response = client.watcher().putWatch(request, RequestOptions.DEFAULT);
            //end::x-pack-put-watch-execute

            //tag::x-pack-put-watch-response
            String watchId = response.getId(); // <1>
            boolean isCreated = response.isCreated(); // <2>
            long version = response.getVersion(); // <3>
            //end::x-pack-put-watch-response
        }

        {
            BytesReference watch = new BytesArray("{ \n" +
                "  \"trigger\": { \"schedule\": { \"interval\": \"10h\" } },\n" +
                "  \"input\": { \"simple\": { \"foo\" : \"bar\" } },\n" +
                "  \"actions\": { \"logme\": { \"logging\": { \"text\": \"{{ctx.payload}}\" } } }\n" +
                "}");
            PutWatchRequest request = new PutWatchRequest("my_other_watch_id", watch, XContentType.JSON);
            // tag::x-pack-put-watch-execute-listener
            ActionListener<PutWatchResponse> listener = new ActionListener<PutWatchResponse>() {
                @Override
                public void onResponse(PutWatchResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::x-pack-put-watch-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-put-watch-execute-async
            client.watcher().putWatchAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-put-watch-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }

        {
            //tag::x-pack-delete-watch-execute
            DeleteWatchRequest request = new DeleteWatchRequest("my_watch_id");
            DeleteWatchResponse response = client.watcher().deleteWatch(request, RequestOptions.DEFAULT);
            //end::x-pack-delete-watch-execute

            //tag::x-pack-delete-watch-response
            String watchId = response.getId(); // <1>
            boolean found = response.isFound(); // <2>
            long version = response.getVersion(); // <3>
            //end::x-pack-delete-watch-response
        }

        {
            DeleteWatchRequest request = new DeleteWatchRequest("my_other_watch_id");
            // tag::x-pack-delete-watch-execute-listener
            ActionListener<DeleteWatchResponse> listener = new ActionListener<DeleteWatchResponse>() {
                @Override
                public void onResponse(DeleteWatchResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::x-pack-delete-watch-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-delete-watch-execute-async
            client.watcher().deleteWatchAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-delete-watch-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testAckWatch() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            BytesReference watch = new BytesArray("{ \n" +
                "  \"trigger\": { \"schedule\": { \"interval\": \"10h\" } },\n" +
                "  \"input\": { \"simple\": { \"foo\" : \"bar\" } },\n" +
                "  \"actions\": { \"logme\": { \"logging\": { \"text\": \"{{ctx.payload}}\" } } }\n" +
                "}");
            PutWatchRequest putWatchRequest = new PutWatchRequest("my_watch_id", watch, XContentType.JSON);
            client.watcher().putWatch(putWatchRequest, RequestOptions.DEFAULT);

            // TODO: use the high-level REST client here once it supports 'execute watch'.
            Request executeWatchRequest = new Request("POST", "_xpack/watcher/watch/my_watch_id/_execute");
            executeWatchRequest.setJsonEntity("{ \"record_execution\": true }");
            Response executeResponse = client().performRequest(executeWatchRequest);
            assertEquals(RestStatus.OK.getStatus(), executeResponse.getStatusLine().getStatusCode());
        }

        {
            //tag::ack-watch-execute
            AckWatchRequest request = new AckWatchRequest("my_watch_id", // <1>
                "logme", "emailme"); // <2>
            AckWatchResponse response = client.watcher().ackWatch(request, RequestOptions.DEFAULT);
            //end::ack-watch-execute

            //tag::ack-watch-response
            WatchStatus watchStatus = response.getStatus();
            ActionStatus actionStatus = watchStatus.actionStatus("logme"); // <1>
            AckStatus.State ackState = actionStatus.ackStatus().state(); // <2>
            //end::ack-watch-response

            assertEquals(AckStatus.State.ACKED, ackState);
        }

        {
            AckWatchRequest request = new AckWatchRequest("my_watch_id");
            // tag::ack-watch-execute-listener
            ActionListener<AckWatchResponse> listener = new ActionListener<AckWatchResponse>() {
                @Override
                public void onResponse(AckWatchResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::ack-watch-execute-listener

            // For testing, replace the empty listener by a blocking listener.
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::ack-watch-execute-async
            client.watcher().ackWatchAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::ack-watch-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

}
