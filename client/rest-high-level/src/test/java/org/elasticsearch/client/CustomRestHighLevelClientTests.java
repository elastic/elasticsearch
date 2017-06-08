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
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.client.ESRestHighLevelClientTestCase.execute;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Test and demonstrates how {@link RestHighLevelClient} can be extended to support
 * custom requests and responses against custom endpoints.
 */
public class CustomRestHighLevelClientTests extends ESTestCase {

    private static final String ENDPOINT = "/_custom";

    private CustomRestClient restHighLevelClient;

    @Before
    @SuppressWarnings("unchecked")
    public void initClients() throws IOException {
        if (restHighLevelClient == null) {
            final RestClient restClient = mock(RestClient.class);
            restHighLevelClient = new CustomRestClient(restClient);

            doAnswer(mock -> mockPerformRequest((Map) mock.getArguments()[2]))
                    .when(restClient)
                    .performRequest(eq(HttpGet.METHOD_NAME), eq(ENDPOINT), anyMapOf(String.class, String.class), anyObject(), anyVararg());

            doAnswer(mock -> mockPerformRequestAsync((Map) mock.getArguments()[2], (ResponseListener) mock.getArguments()[4]))
                    .when(restClient)
                    .performRequestAsync(eq(HttpGet.METHOD_NAME), eq(ENDPOINT), anyMapOf(String.class, String.class),
                            any(HttpEntity.class), any(ResponseListener.class), anyVararg());
        }
    }

    public void testCustomRequest() throws IOException {
        final CustomRequest customRequest = new CustomRequest(randomAlphaOfLength(5));

        CustomResponse customResponse = execute(customRequest, restHighLevelClient::custom, restHighLevelClient::customAsync);
        assertEquals(customRequest.getValue(), customResponse.getValue());
    }

    /**
     * Mocks the asynchronous request execution by calling the {@link #mockPerformRequest(Map)} method.
     */
    private Void mockPerformRequestAsync(Map<String, String> httpHeaders, ResponseListener responseListener) {
        try {
            responseListener.onSuccess(mockPerformRequest(httpHeaders));
        } catch (IOException e) {
            responseListener.onFailure(e);
        }
        return null;
    }

    /**
     * Mocks the synchronous request execution like if it was executed by Elasticsearch.
     */
    private Response mockPerformRequest(Map<String, String> httpHeaders) throws IOException {
        assertEquals(1, httpHeaders.size());

        ProtocolVersion protocol = new ProtocolVersion("HTTP", 1, 1);
        HttpResponse httpResponse = new BasicHttpResponse(new BasicStatusLine(protocol, 200, "OK"));
        httpResponse.setHeader("custom", httpHeaders.get("custom"));

        RequestLine requestLine = new BasicRequestLine(HttpGet.METHOD_NAME, ENDPOINT, protocol);
        return new Response(requestLine, new HttpHost("localhost", 9200), httpResponse);
    }

    /**
     * A custom high level client that provides methods to execute a custom request and get its associate response back.
     */
    static class CustomRestClient extends RestHighLevelClient {

        private CustomRestClient(RestClient restClient) {
            super(restClient);
        }

        public CustomResponse custom(CustomRequest customRequest, Header... headers) throws IOException {
            return performRequest(customRequest, this::toRequest, this::toResponse, emptySet(), headers);
        }

        public void customAsync(CustomRequest customRequest, ActionListener<CustomResponse> listener, Header... headers) {
            performRequestAsync(customRequest, this::toRequest, this::toResponse, listener, emptySet(), headers);
        }

        Request toRequest(CustomRequest customRequest) throws IOException {
            return new Request(HttpGet.METHOD_NAME, ENDPOINT, singletonMap("custom", customRequest.getValue()), null);
        }

        CustomResponse toResponse(Response response) throws IOException {
            return new CustomResponse(response.getHeader("custom"));
        }
    }

    /**
     * A custom request
     */
    static class CustomRequest extends ActionRequest {

        private final String value;

        CustomRequest(String value) {
            this.value = value;
        }

        String getValue() {
            return value;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /**
     * A custom response
     */
    static class CustomResponse extends ActionResponse {

        private final String value;

        CustomResponse(String value) {
            this.value = value;
        }

        String getValue() {
            return value;
        }
    }
}