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
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.client.ESRestHighLevelClientTestCase.execute;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Test and demonstrates how {@link RestHighLevelClient} can be extended to support custom endpoints.
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

            doAnswer(mock -> mockPerformRequest((Header) mock.getArguments()[4]))
                    .when(restClient)
                    .performRequest(eq(HttpGet.METHOD_NAME), eq(ENDPOINT), anyMapOf(String.class, String.class), anyObject(), anyVararg());

            doAnswer(mock -> mockPerformRequestAsync((Header) mock.getArguments()[5], (ResponseListener) mock.getArguments()[4]))
                    .when(restClient)
                    .performRequestAsync(eq(HttpGet.METHOD_NAME), eq(ENDPOINT), anyMapOf(String.class, String.class),
                            any(HttpEntity.class), any(ResponseListener.class), anyVararg());
        }
    }

    public void testCustomEndpoint() throws IOException {
        final MainRequest request = new MainRequest();
        final Header header = new BasicHeader("node_name", randomAlphaOfLengthBetween(1, 10));

        MainResponse response = execute(request, restHighLevelClient::custom, restHighLevelClient::customAsync, header);
        assertEquals(header.getValue(), response.getNodeName());

        response = execute(request, restHighLevelClient::customAndParse, restHighLevelClient::customAndParseAsync, header);
        assertEquals(header.getValue(), response.getNodeName());
    }

    /**
     * The {@link RestHighLevelClient} must declare the following execution methods using the <code>protected</code> modifier
     * so that they can be used by subclasses to implement custom logic.
     */
    @SuppressForbidden(reason = "We're forced to uses Class#getDeclaredMethods() here because this test checks protected methods")
    public void testMethodsVisibility() throws ClassNotFoundException {
        String[] methodNames = new String[]{"performRequest", "performRequestAndParseEntity", "performRequestAsync",
                "performRequestAsyncAndParseEntity"};
        for (String methodName : methodNames) {
            boolean found = false;
            for (Method method : RestHighLevelClient.class.getDeclaredMethods()) {
                if (method.getName().equals(methodName)) {
                    assertTrue("Method " + methodName + " must be protected", Modifier.isProtected(method.getModifiers()));
                    found = true;
                }
            }
            assertTrue("Failed to find method " + methodName, found);
        }
    }

    /**
     * Mocks the asynchronous request execution by calling the {@link #mockPerformRequest(Header)} method.
     */
    private Void mockPerformRequestAsync(Header httpHeader, ResponseListener responseListener) {
        try {
            responseListener.onSuccess(mockPerformRequest(httpHeader));
        } catch (IOException e) {
            responseListener.onFailure(e);
        }
        return null;
    }

    /**
     * Mocks the synchronous request execution like if it was executed by Elasticsearch.
     */
    private Response mockPerformRequest(Header httpHeader) throws IOException {
        ProtocolVersion protocol = new ProtocolVersion("HTTP", 1, 1);
        HttpResponse httpResponse = new BasicHttpResponse(new BasicStatusLine(protocol, 200, "OK"));

        MainResponse response = new MainResponse(httpHeader.getValue(), Version.CURRENT, ClusterName.DEFAULT, "_na", Build.CURRENT, true);
        BytesRef bytesRef = XContentHelper.toXContent(response, XContentType.JSON, false).toBytesRef();
        httpResponse.setEntity(new ByteArrayEntity(bytesRef.bytes, ContentType.APPLICATION_JSON));

        RequestLine requestLine = new BasicRequestLine(HttpGet.METHOD_NAME, ENDPOINT, protocol);
        return new Response(requestLine, new HttpHost("localhost", 9200), httpResponse);
    }

    /**
     * A custom high level client that provides custom methods to execute a request and get its associate response back.
     */
    static class CustomRestClient extends RestHighLevelClient {

        private CustomRestClient(RestClient restClient) {
            super(restClient, RestClient::close, Collections.emptyList());
        }

        MainResponse custom(MainRequest mainRequest, Header... headers) throws IOException {
            return performRequest(mainRequest, this::toRequest, this::toResponse, emptySet(), headers);
        }

        MainResponse customAndParse(MainRequest mainRequest, Header... headers) throws IOException {
            return performRequestAndParseEntity(mainRequest, this::toRequest, MainResponse::fromXContent, emptySet(), headers);
        }

        void customAsync(MainRequest mainRequest, ActionListener<MainResponse> listener, Header... headers) {
            performRequestAsync(mainRequest, this::toRequest, this::toResponse, listener, emptySet(), headers);
        }

        void customAndParseAsync(MainRequest mainRequest, ActionListener<MainResponse> listener, Header... headers) {
            performRequestAsyncAndParseEntity(mainRequest, this::toRequest, MainResponse::fromXContent, listener, emptySet(), headers);
        }

        Request toRequest(MainRequest mainRequest) throws IOException {
            return new Request(HttpGet.METHOD_NAME, ENDPOINT, emptyMap(), null);
        }

        MainResponse toResponse(Response response) throws IOException {
            return parseEntity(response.getEntity(), MainResponse::fromXContent);
        }
    }
}
