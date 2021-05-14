/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.documentation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.watcher.ActivateWatchRequest;
import org.elasticsearch.client.watcher.ActivateWatchResponse;
import org.elasticsearch.client.watcher.AckWatchRequest;
import org.elasticsearch.client.watcher.AckWatchResponse;
import org.elasticsearch.client.watcher.ActionStatus;
import org.elasticsearch.client.watcher.ActionStatus.AckStatus;
import org.elasticsearch.client.watcher.DeactivateWatchRequest;
import org.elasticsearch.client.watcher.DeactivateWatchResponse;
import org.elasticsearch.client.watcher.ExecuteWatchRequest;
import org.elasticsearch.client.watcher.ExecuteWatchResponse;
import org.elasticsearch.client.watcher.GetWatchRequest;
import org.elasticsearch.client.watcher.GetWatchResponse;
import org.elasticsearch.client.watcher.StartWatchServiceRequest;
import org.elasticsearch.client.watcher.StopWatchServiceRequest;
import org.elasticsearch.client.watcher.WatchStatus;
import org.elasticsearch.client.watcher.WatcherStatsRequest;
import org.elasticsearch.client.watcher.WatcherStatsResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.client.watcher.DeleteWatchRequest;
import org.elasticsearch.client.watcher.DeleteWatchResponse;
import org.elasticsearch.client.watcher.PutWatchRequest;
import org.elasticsearch.client.watcher.PutWatchResponse;
import org.elasticsearch.rest.RestStatus;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

public class WatcherDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testStartStopWatchService() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            //tag::start-watch-service-request
            StartWatchServiceRequest request = new StartWatchServiceRequest();
            //end::start-watch-service-request

            //tag::start-watch-service-execute
            AcknowledgedResponse response = client.watcher().startWatchService(request, RequestOptions.DEFAULT);
            //end::start-watch-service-execute

            //tag::start-watch-service-response
            boolean isAcknowledged = response.isAcknowledged(); // <1>
            //end::start-watch-service-response
        }

        {
            //tag::stop-watch-service-request
            StopWatchServiceRequest request = new StopWatchServiceRequest();
            //end::stop-watch-service-request

            //tag::stop-watch-service-execute
            AcknowledgedResponse response = client.watcher().stopWatchService(request, RequestOptions.DEFAULT);
            //end::stop-watch-service-execute

            //tag::stop-watch-service-response
            boolean isAcknowledged = response.isAcknowledged(); // <1>
            //end::stop-watch-service-response
        }

        {
            StartWatchServiceRequest request = new StartWatchServiceRequest();

            // tag::start-watch-service-execute-listener
            ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::start-watch-service-execute-listener

            CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::start-watch-service-execute-async
            client.watcher().startWatchServiceAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::start-watch-service-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }

        {
            StopWatchServiceRequest request = new StopWatchServiceRequest();

            // tag::stop-watch-service-execute-listener
            ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::stop-watch-service-execute-listener

            CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::stop-watch-service-execute-async
            client.watcher().stopWatchServiceAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::stop-watch-service-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

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
            // tag::x-pack-execute-watch-by-id
            ExecuteWatchRequest request = ExecuteWatchRequest.byId("my_watch_id");
            request.setAlternativeInput("{ \"foo\" : \"bar\" }");                                         // <1>
            request.setActionMode("action1", ExecuteWatchRequest.ActionExecutionMode.SIMULATE);             // <2>
            request.setRecordExecution(true);                                                               // <3>
            request.setIgnoreCondition(true);                                                               // <4>
            request.setTriggerData("{\"triggered_time\":\"now\"}");                                         // <5>
            request.setDebug(true);                                                                         // <6>
            ExecuteWatchResponse response = client.watcher().executeWatch(request, RequestOptions.DEFAULT);
            // end::x-pack-execute-watch-by-id

            // tag::x-pack-execute-watch-by-id-response
            String id = response.getRecordId();                                         // <1>
            Map<String, Object> watch = response.getRecordAsMap();                      // <2>
            String watch_id = ObjectPath.eval("watch_record.watch_id", watch);          // <3>
            // end::x-pack-execute-watch-by-id-response
        }

        {
            ExecuteWatchRequest request = ExecuteWatchRequest.byId("my_watch_id");
            // tag::x-pack-execute-watch-by-id-execute-listener
            ActionListener<ExecuteWatchResponse> listener = new ActionListener<ExecuteWatchResponse>() {
                @Override
                public void onResponse(ExecuteWatchResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::x-pack-execute-watch-by-id-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-execute-watch-by-id-execute-async
            client.watcher().executeWatchAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-execute-watch-by-id-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }

        {
            //tag::get-watch-request
            GetWatchRequest request = new GetWatchRequest("my_watch_id");
            //end::get-watch-request

            //tag::get-watch-execute
            GetWatchResponse response = client.watcher().getWatch(request, RequestOptions.DEFAULT);
            //end::get-watch-execute

            //tag::get-watch-response
            String watchId = response.getId(); // <1>
            boolean found = response.isFound(); // <2>
            long version = response.getVersion(); // <3>
            WatchStatus status = response.getStatus(); // <4>
            BytesReference source = response.getSource(); // <5>
            //end::get-watch-response
        }

        {
            GetWatchRequest request = new GetWatchRequest("my_other_watch_id");
            // tag::get-watch-execute-listener
            ActionListener<GetWatchResponse> listener = new ActionListener<GetWatchResponse>() {
                @Override
                public void onResponse(GetWatchResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-watch-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-watch-execute-async
            client.watcher().getWatchAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-watch-execute-async

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

    public void testExecuteInlineWatch() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // tag::x-pack-execute-watch-inline
            String watchJson = "{ \n" +
                "  \"trigger\": { \"schedule\": { \"interval\": \"10h\" } },\n" +
                "  \"input\": { \"none\": {} },\n" +
                "  \"actions\": { \"logme\": { \"logging\": { \"text\": \"{{ctx.payload}}\" } } }\n" +
                "}";
            ExecuteWatchRequest request = ExecuteWatchRequest.inline(watchJson);
            request.setAlternativeInput("{ \"foo\" : \"bar\" }");                                         // <1>
            request.setActionMode("action1", ExecuteWatchRequest.ActionExecutionMode.SIMULATE);             // <2>
            request.setIgnoreCondition(true);                                                               // <3>
            request.setTriggerData("{\"triggered_time\":\"now\"}");                                         // <4>
            request.setDebug(true);                                                                         // <5>
            ExecuteWatchResponse response = client.watcher().executeWatch(request, RequestOptions.DEFAULT);
            // end::x-pack-execute-watch-inline

            // tag::x-pack-execute-watch-inline-response
            String id = response.getRecordId();                                         // <1>
            Map<String, Object> watch = response.getRecordAsMap();                      // <2>
            String watch_id = ObjectPath.eval("watch_record.watch_id", watch);          // <3>
            // end::x-pack-execute-watch-inline-response
        }

        {
            String watchJson = "{ \n" +
                "  \"trigger\": { \"schedule\": { \"interval\": \"10h\" } },\n" +
                "  \"input\": { \"none\": {} },\n" +
                "  \"actions\": { \"logme\": { \"logging\": { \"text\": \"{{ctx.payload}}\" } } }\n" +
                "}";
            ExecuteWatchRequest request = ExecuteWatchRequest.inline(watchJson);
            // tag::x-pack-execute-watch-inline-execute-listener
            ActionListener<ExecuteWatchResponse> listener = new ActionListener<ExecuteWatchResponse>() {
                @Override
                public void onResponse(ExecuteWatchResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::x-pack-execute-watch-inline-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-execute-watch-inline-execute-async
            client.watcher().executeWatchAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-execute-watch-inline-execute-async

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
            Request executeWatchRequest = new Request("POST", "_watcher/watch/my_watch_id/_execute");
            executeWatchRequest.setJsonEntity("{ \"record_execution\": true }");
            Response executeResponse = client().performRequest(executeWatchRequest);
            assertEquals(RestStatus.OK.getStatus(), executeResponse.getStatusLine().getStatusCode());
        }

        {
            //tag::ack-watch-request
            AckWatchRequest request = new AckWatchRequest("my_watch_id", // <1>
                "logme", "emailme"); // <2>
            //end::ack-watch-request

            //tag::ack-watch-execute
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

    public void testDeactivateWatch() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            BytesReference watch = new BytesArray("{ \n" +
                "  \"trigger\": { \"schedule\": { \"interval\": \"10h\" } },\n" +
                "  \"input\": { \"simple\": { \"foo\" : \"bar\" } },\n" +
                "  \"actions\": { \"logme\": { \"logging\": { \"text\": \"{{ctx.payload}}\" } } }\n" +
                "}");
            PutWatchRequest putWatchRequest = new PutWatchRequest("my_watch_id", watch, XContentType.JSON);
            client.watcher().putWatch(putWatchRequest, RequestOptions.DEFAULT);
        }

        {
            //tag::deactivate-watch-execute
            DeactivateWatchRequest request = new DeactivateWatchRequest("my_watch_id");
            DeactivateWatchResponse response = client.watcher().deactivateWatch(request, RequestOptions.DEFAULT);
            //end::deactivate-watch-execute

            assertThat(response.getStatus().state().isActive(), is(false));
        }

        {
            DeactivateWatchRequest request = new DeactivateWatchRequest("my_watch_id");
            // tag::deactivate-watch-execute-listener
            ActionListener<DeactivateWatchResponse> listener = new ActionListener<DeactivateWatchResponse>() {
                @Override
                public void onResponse(DeactivateWatchResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::deactivate-watch-execute-listener

            // For testing, replace the empty listener by a blocking listener.
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::deactivate-watch-execute-async
            client.watcher().deactivateWatchAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::deactivate-watch-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }


    public void testActivateWatch() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            BytesReference watch = new BytesArray("{ \n" +
                "  \"trigger\": { \"schedule\": { \"interval\": \"10h\" } },\n" +
                "  \"input\": { \"simple\": { \"foo\" : \"bar\" } },\n" +
                "  \"actions\": { \"logme\": { \"logging\": { \"text\": \"{{ctx.payload}}\" } } }\n" +
                "}");
            PutWatchRequest request = new PutWatchRequest("my_watch_id", watch, XContentType.JSON);
            request.setActive(false); // <1>
            PutWatchResponse response = client.watcher().putWatch(request, RequestOptions.DEFAULT);
        }

        {
            //tag::activate-watch-request
            ActivateWatchRequest request = new ActivateWatchRequest("my_watch_id");
            ActivateWatchResponse response = client.watcher().activateWatch(request, RequestOptions.DEFAULT);
            //end::activate-watch-request

            //tag::activate-watch-response
            WatchStatus watchStatus = response.getStatus(); // <1>
            //end::activate-watch-response

            assertTrue(watchStatus.state().isActive());
        }

        {
            ActivateWatchRequest request = new ActivateWatchRequest("my_watch_id");
            //tag::activate-watch-request-listener
            ActionListener<ActivateWatchResponse> listener = new ActionListener<ActivateWatchResponse>() {
                @Override
                public void onResponse(ActivateWatchResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::activate-watch-request-listener

            //Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            //tag::activate-watch-request-async
            client.watcher().activateWatchAsync(request, RequestOptions.DEFAULT, listener); // <1>
            //end::activate-watch-request-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));

        }
    }

    public void testWatcherStats() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            //tag::watcher-stats-request
            WatcherStatsRequest request = new WatcherStatsRequest(true, true);
            //end::watcher-stats-request

            //tag::watcher-stats-execute
            WatcherStatsResponse response = client.watcher().watcherStats(request, RequestOptions.DEFAULT);
            //end::watcher-stats-execute

            //tag::watcher-stats-response
            List<WatcherStatsResponse.Node> nodes = response.getNodes(); // <1>
            //end::watcher-stats-response
        }

        {
            WatcherStatsRequest request = new WatcherStatsRequest();

            // tag::watcher-stats-execute-listener
            ActionListener<WatcherStatsResponse> listener = new ActionListener<WatcherStatsResponse>() {
                @Override
                public void onResponse(WatcherStatsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::watcher-stats-execute-listener

            CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::watcher-stats-execute-async
            client.watcher().watcherStatsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::watcher-stats-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

}
