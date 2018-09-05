package org.elasticsearch.client.documentation;/*
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

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.StoredScriptSource;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
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

    private void putStoredScript(String id, StoredScriptSource scriptSource) throws IOException {
        final String script = Strings.toString(scriptSource.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS));
        // TODO: change to HighLevel PutStoredScriptRequest when it will be ready
        // so far - using low-level REST API
        Request request = new Request("PUT", "/_scripts/" + id);
        request.setJsonEntity("{\"script\":" + script + "}");
        Response putResponse = adminClient().performRequest(request);
        assertEquals(putResponse.getStatusLine().getReasonPhrase(), 200, putResponse.getStatusLine().getStatusCode());
        assertEquals("{\"acknowledged\":true}", EntityUtils.toString(putResponse.getEntity()));
    }
}
