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
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.SimulateDocumentBaseResult;
import org.elasticsearch.action.ingest.SimulateDocumentResult;
import org.elasticsearch.action.ingest.SimulateDocumentVerboseResult;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.action.ingest.SimulateProcessorResult;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.PipelineConfiguration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This class is used to generate the Java Ingest API documentation.
 * You need to wrap your code between two tags like:
 * // tag::example
 * // end::example
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{doc-tests}/IngestClientDocumentationIT.java[example]
 * --------------------------------------------------
 *
 * The column width of the code block is 84. If the code contains a line longer
 * than 84, the line will be cut and a horizontal scroll bar will be displayed.
 * (the code indentation of the tag is not included in the width)
 */
public class IngestClientDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testPutPipeline() throws IOException {
        RestHighLevelClient client = highLevelClient();

        {
            // tag::put-pipeline-request
            String source =
                "{\"description\":\"my set of processors\"," +
                    "\"processors\":[{\"set\":{\"field\":\"foo\",\"value\":\"bar\"}}]}";
            PutPipelineRequest request = new PutPipelineRequest(
                "my-pipeline-id", // <1>
                new BytesArray(source.getBytes(StandardCharsets.UTF_8)), // <2>
                XContentType.JSON // <3>
            );
            // end::put-pipeline-request

            // tag::put-pipeline-request-timeout
            request.timeout(TimeValue.timeValueMinutes(2)); // <1>
            request.timeout("2m"); // <2>
            // end::put-pipeline-request-timeout

            // tag::put-pipeline-request-masterTimeout
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
            request.masterNodeTimeout("1m"); // <2>
            // end::put-pipeline-request-masterTimeout

            // tag::put-pipeline-execute
            AcknowledgedResponse response = client.ingest().putPipeline(request, RequestOptions.DEFAULT); // <1>
            // end::put-pipeline-execute

            // tag::put-pipeline-response
            boolean acknowledged = response.isAcknowledged(); // <1>
            // end::put-pipeline-response
            assertTrue(acknowledged);
        }
    }

    public void testPutPipelineAsync() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            String source =
                "{\"description\":\"my set of processors\"," +
                    "\"processors\":[{\"set\":{\"field\":\"foo\",\"value\":\"bar\"}}]}";
            PutPipelineRequest request = new PutPipelineRequest(
                "my-pipeline-id",
                new BytesArray(source.getBytes(StandardCharsets.UTF_8)),
                XContentType.JSON
            );

            // tag::put-pipeline-execute-listener
            ActionListener<AcknowledgedResponse> listener =
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse response) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::put-pipeline-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-pipeline-execute-async
            client.ingest().putPipelineAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-pipeline-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    @SuppressWarnings("unused")
    public void testGetPipeline() throws IOException {
        RestHighLevelClient client = highLevelClient();

        {
            createPipeline("my-pipeline-id");
        }

        {
            // tag::get-pipeline-request
            GetPipelineRequest request = new GetPipelineRequest("my-pipeline-id"); // <1>
            // end::get-pipeline-request

            // tag::get-pipeline-request-masterTimeout
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
            request.masterNodeTimeout("1m"); // <2>
            // end::get-pipeline-request-masterTimeout

            // tag::get-pipeline-execute
            GetPipelineResponse response = client.ingest().getPipeline(request, RequestOptions.DEFAULT); // <1>
            // end::get-pipeline-execute

            // tag::get-pipeline-response
            boolean successful = response.isFound(); // <1>
            List<PipelineConfiguration> pipelines = response.pipelines(); // <2>
            for(PipelineConfiguration pipeline: pipelines) {
                Map<String, Object> config = pipeline.getConfigAsMap(); // <3>
            }
            // end::get-pipeline-response

            assertTrue(successful);
        }
    }

    public void testGetPipelineAsync() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            createPipeline("my-pipeline-id");
        }

        {
            GetPipelineRequest request = new GetPipelineRequest("my-pipeline-id");

            // tag::get-pipeline-execute-listener
            ActionListener<GetPipelineResponse> listener =
                new ActionListener<GetPipelineResponse>() {
                    @Override
                    public void onResponse(GetPipelineResponse response) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::get-pipeline-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-pipeline-execute-async
            client.ingest().getPipelineAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-pipeline-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeletePipeline() throws IOException {
        RestHighLevelClient client = highLevelClient();

        {
            createPipeline("my-pipeline-id");
        }

        {
            // tag::delete-pipeline-request
            DeletePipelineRequest request = new DeletePipelineRequest("my-pipeline-id"); // <1>
            // end::delete-pipeline-request

            // tag::delete-pipeline-request-timeout
            request.timeout(TimeValue.timeValueMinutes(2)); // <1>
            request.timeout("2m"); // <2>
            // end::delete-pipeline-request-timeout

            // tag::delete-pipeline-request-masterTimeout
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
            request.masterNodeTimeout("1m"); // <2>
            // end::delete-pipeline-request-masterTimeout

            // tag::delete-pipeline-execute
            AcknowledgedResponse response = client.ingest().deletePipeline(request, RequestOptions.DEFAULT); // <1>
            // end::delete-pipeline-execute

            // tag::delete-pipeline-response
            boolean acknowledged = response.isAcknowledged(); // <1>
            // end::delete-pipeline-response
            assertTrue(acknowledged);
        }
    }

    public void testDeletePipelineAsync() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            createPipeline("my-pipeline-id");
        }

        {
            DeletePipelineRequest request = new DeletePipelineRequest("my-pipeline-id");

            // tag::delete-pipeline-execute-listener
            ActionListener<AcknowledgedResponse> listener =
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse response) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::delete-pipeline-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-pipeline-execute-async
            client.ingest().deletePipelineAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-pipeline-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testSimulatePipeline() throws IOException {
        RestHighLevelClient client = highLevelClient();

        {
            // tag::simulate-pipeline-request
            String source =
                "{\"" +
                    "pipeline\":{" +
                        "\"description\":\"_description\"," +
                        "\"processors\":[{\"set\":{\"field\":\"field2\",\"value\":\"_value\"}}]" +
                    "}," +
                    "\"docs\":[" +
                        "{\"_index\":\"index\",\"_id\":\"id\",\"_source\":{\"foo\":\"bar\"}}," +
                        "{\"_index\":\"index\",\"_id\":\"id\",\"_source\":{\"foo\":\"rab\"}}" +
                    "]" +
                "}";
            SimulatePipelineRequest request = new SimulatePipelineRequest(
                new BytesArray(source.getBytes(StandardCharsets.UTF_8)), // <1>
                XContentType.JSON // <2>
            );
            // end::simulate-pipeline-request

            // tag::simulate-pipeline-request-pipeline-id
            request.setId("my-pipeline-id"); // <1>
            // end::simulate-pipeline-request-pipeline-id

            // For testing we set this back to null
            request.setId(null);

            // tag::simulate-pipeline-request-verbose
            request.setVerbose(true); // <1>
            // end::simulate-pipeline-request-verbose

            // tag::simulate-pipeline-execute
            SimulatePipelineResponse response = client.ingest().simulate(request, RequestOptions.DEFAULT); // <1>
            // end::simulate-pipeline-execute

            // tag::simulate-pipeline-response
            for (SimulateDocumentResult result: response.getResults()) { // <1>
                if (request.isVerbose()) {
                    assert result instanceof SimulateDocumentVerboseResult;
                    SimulateDocumentVerboseResult verboseResult = (SimulateDocumentVerboseResult)result; // <2>
                    for (SimulateProcessorResult processorResult: verboseResult.getProcessorResults()) { // <3>
                        processorResult.getIngestDocument(); // <4>
                        processorResult.getFailure(); // <5>
                    }
                } else {
                    assert result instanceof SimulateDocumentBaseResult;
                    SimulateDocumentBaseResult baseResult = (SimulateDocumentBaseResult)result; // <6>
                    baseResult.getIngestDocument(); // <7>
                    baseResult.getFailure(); // <8>
                }
            }
            // end::simulate-pipeline-response
            assert(response.getResults().size() > 0);
        }
    }

    public void testSimulatePipelineAsync() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            String source =
                "{\"" +
                    "pipeline\":{" +
                    "\"description\":\"_description\"," +
                    "\"processors\":[{\"set\":{\"field\":\"field2\",\"value\":\"_value\"}}]" +
                    "}," +
                    "\"docs\":[" +
                    "{\"_index\":\"index\",\"_id\":\"id\",\"_source\":{\"foo\":\"bar\"}}," +
                    "{\"_index\":\"index\",\"_id\":\"id\",\"_source\":{\"foo\":\"rab\"}}" +
                    "]" +
                    "}";
            SimulatePipelineRequest request = new SimulatePipelineRequest(
                new BytesArray(source.getBytes(StandardCharsets.UTF_8)),
                XContentType.JSON
            );

            // tag::simulate-pipeline-execute-listener
            ActionListener<SimulatePipelineResponse> listener =
                new ActionListener<SimulatePipelineResponse>() {
                    @Override
                    public void onResponse(SimulatePipelineResponse response) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::simulate-pipeline-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::simulate-pipeline-execute-async
            client.ingest().simulateAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::simulate-pipeline-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

}
