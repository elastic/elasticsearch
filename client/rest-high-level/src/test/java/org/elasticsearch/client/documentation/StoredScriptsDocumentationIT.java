package org.elasticsearch.client.documentation;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.StoredScriptSource;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * This class is used to generate the Java Stored Scripts API documentation.
 * You need to wrap your code between two tags like:
 * // tag::example
 * // end::example
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{doc-tests}/StoredScriptsDocumentationIT.java[example]
 * --------------------------------------------------
 *
 * The column width of the code block is 84. If the code contains a line longer
 * than 84, the line will be cut and a horizontal scroll bar will be displayed.
 * (the code indentation of the tag is not included in the width)
 */
public class StoredScriptsDocumentationIT extends ESRestHighLevelClientTestCase {

    @SuppressWarnings("unused")
    public void testGetStoredScript() throws Exception {
        RestHighLevelClient client = highLevelClient();

        final StoredScriptSource scriptSource =
            new StoredScriptSource("painless",
                "Math.log(_score * 2) + params.my_modifier",
                Collections.singletonMap(Script.CONTENT_TYPE_OPTION, XContentType.JSON.mediaType()));

        putStoredScript("calculate-score", scriptSource);

        {
            // tag::get-stored-script-request
            GetStoredScriptRequest request = new GetStoredScriptRequest("calculate-score"); // <1>
            // end::get-stored-script-request

            // tag::get-stored-script-request-masterTimeout
            request.masterNodeTimeout(TimeValue.timeValueSeconds(50)); // <1>
            request.masterNodeTimeout("50s"); // <2>
            // end::get-stored-script-request-masterTimeout

            // tag::get-stored-script-execute
            GetStoredScriptResponse getResponse = client.getScript(request, RequestOptions.DEFAULT);
            // end::get-stored-script-execute

            // tag::get-stored-script-response
            StoredScriptSource storedScriptSource = getResponse.getSource(); // <1>

            String lang = storedScriptSource.getLang(); // <2>
            String source = storedScriptSource.getSource(); // <3>
            Map<String, String> options = storedScriptSource.getOptions(); // <4>
            // end::get-stored-script-response

            assertThat(storedScriptSource, equalTo(scriptSource));

            // tag::get-stored-script-execute-listener
            ActionListener<GetStoredScriptResponse> listener =
                new ActionListener<GetStoredScriptResponse>() {
                    @Override
                    public void onResponse(GetStoredScriptResponse response) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::get-stored-script-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-stored-script-execute-async
            client.getScriptAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-stored-script-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }

    }

    @SuppressWarnings("unused")
    public void testDeleteStoredScript() throws Exception {
        RestHighLevelClient client = highLevelClient();

        final StoredScriptSource scriptSource =
            new StoredScriptSource("painless",
                "Math.log(_score * 2) + params.my_modifier",
                Collections.singletonMap(Script.CONTENT_TYPE_OPTION, XContentType.JSON.mediaType()));

        putStoredScript("calculate-score", scriptSource);

        // tag::delete-stored-script-request
        DeleteStoredScriptRequest deleteRequest = new DeleteStoredScriptRequest("calculate-score"); // <1>
        // end::delete-stored-script-request

        // tag::delete-stored-script-request-masterTimeout
        deleteRequest.masterNodeTimeout(TimeValue.timeValueSeconds(50)); // <1>
        deleteRequest.masterNodeTimeout("50s"); // <2>
        // end::delete-stored-script-request-masterTimeout

        // tag::delete-stored-script-request-timeout
        deleteRequest.timeout(TimeValue.timeValueSeconds(60)); // <1>
        deleteRequest.timeout("60s"); // <2>
        // end::delete-stored-script-request-timeout

        // tag::delete-stored-script-execute
        AcknowledgedResponse deleteResponse = client.deleteScript(deleteRequest, RequestOptions.DEFAULT);
        // end::delete-stored-script-execute

        // tag::delete-stored-script-response
        boolean acknowledged = deleteResponse.isAcknowledged();// <1>
        // end::delete-stored-script-response

        putStoredScript("calculate-score", scriptSource);

        // tag::delete-stored-script-execute-listener
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
        // end::delete-stored-script-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::delete-stored-script-execute-async
        client.deleteScriptAsync(deleteRequest, RequestOptions.DEFAULT, listener); // <1>
        // end::delete-stored-script-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testPutScript() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // tag::put-stored-script-request
            PutStoredScriptRequest request = new PutStoredScriptRequest();
            request.id("id"); // <1>
            request.content(new BytesArray(
                "{\n" +
                    "\"script\": {\n" +
                    "\"lang\": \"painless\",\n" +
                    "\"source\": \"Math.log(_score * 2) + params.multiplier\"" +
                    "}\n" +
                    "}\n"
            ), XContentType.JSON); // <2>
            // end::put-stored-script-request

            // tag::put-stored-script-context
            request.context("context"); // <1>
            // end::put-stored-script-context

            // tag::put-stored-script-timeout
            request.timeout(TimeValue.timeValueMinutes(2)); // <1>
            request.timeout("2m"); // <2>
            // end::put-stored-script-timeout

            // tag::put-stored-script-masterTimeout
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
            request.masterNodeTimeout("1m"); // <2>
            // end::put-stored-script-masterTimeout
        }

        {
            PutStoredScriptRequest request = new PutStoredScriptRequest();
            request.id("id");

            // tag::put-stored-script-content-painless
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.startObject("script");
                {
                    builder.field("lang", "painless");
                    builder.field("source", "Math.log(_score * 2) + params.multiplier");
                }
                builder.endObject();
            }
            builder.endObject();
            request.content(BytesReference.bytes(builder), XContentType.JSON); // <1>
            // end::put-stored-script-content-painless


            // tag::put-stored-script-execute
            AcknowledgedResponse putStoredScriptResponse = client.putScript(request, RequestOptions.DEFAULT);
            // end::put-stored-script-execute

            // tag::put-stored-script-response
            boolean acknowledged = putStoredScriptResponse.isAcknowledged(); // <1>
            // end::put-stored-script-response

            assertTrue(acknowledged);

            // tag::put-stored-script-execute-listener
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
            // end::put-stored-script-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-stored-script-execute-async
            client.putScriptAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-stored-script-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }

        {
            PutStoredScriptRequest request = new PutStoredScriptRequest();
            request.id("id");

            // tag::put-stored-script-content-mustache
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.startObject("script");
                {
                    builder.field("lang", "mustache");
                    builder.field("source", "{\"query\":{\"match\":{\"title\":\"{{query_string}}\"}}}");
                }
                builder.endObject();
            }
            builder.endObject();
            request.content(BytesReference.bytes(builder), XContentType.JSON); // <1>
            // end::put-stored-script-content-mustache

            client.putScript(request, RequestOptions.DEFAULT);

            Map<String, Object> script = getAsMap("/_scripts/id");
            assertThat(extractValue("script.lang", script), equalTo("mustache"));
            assertThat(extractValue("script.source", script), equalTo("{\"query\":{\"match\":{\"title\":\"{{query_string}}\"}}}"));
        }
    }

    private void putStoredScript(String id, StoredScriptSource scriptSource) throws IOException {
        PutStoredScriptRequest request =
            new PutStoredScriptRequest(id, "score", new BytesArray("{}"), XContentType.JSON, scriptSource);
        assertAcked(execute(request, highLevelClient()::putScript, highLevelClient()::putScriptAsync));
    }
}
