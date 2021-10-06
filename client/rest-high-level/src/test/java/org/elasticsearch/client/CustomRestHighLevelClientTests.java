/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RequestMatcher;
import org.junit.Before;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test and demonstrates how {@link RestHighLevelClient} can be extended to support custom endpoints.
 */
public class CustomRestHighLevelClientTests extends ESTestCase {

    private static final String ENDPOINT = "/_custom";

    private CustomRestClient restHighLevelClient;

    @Before
    public void initClients() throws IOException {
        if (restHighLevelClient == null) {
            final RestClient restClient = mock(RestClient.class);
            RestHighLevelClientTests.mockGetRoot(restClient);
            restHighLevelClient = new CustomRestClient(restClient);

            doAnswer(inv -> mockPerformRequest((Request) inv.getArguments()[0]))
                    .when(restClient)
                    .performRequest(argThat(new RequestMatcher("GET", ENDPOINT)));

            doAnswer(inv -> mockPerformRequestAsync(
                        ((Request) inv.getArguments()[0]),
                        (ResponseListener) inv.getArguments()[1]))
                    .when(restClient)
                    .performRequestAsync(argThat(new RequestMatcher("GET", ENDPOINT)), any(ResponseListener.class));
        }
    }

    public void testCustomEndpoint() throws IOException {
        final MainRequest request = new MainRequest();
        String nodeName = randomAlphaOfLengthBetween(1, 10);

        MainResponse response = restHighLevelClient.custom(request, optionsForNodeName(nodeName));
        assertEquals(nodeName, response.getNodeName());

        response = restHighLevelClient.customAndParse(request, optionsForNodeName(nodeName));
        assertEquals(nodeName, response.getNodeName());
    }

    public void testCustomEndpointAsync() throws Exception {
        final MainRequest request = new MainRequest();
        String nodeName = randomAlphaOfLengthBetween(1, 10);

        PlainActionFuture<MainResponse> future = PlainActionFuture.newFuture();
        restHighLevelClient.customAsync(request, optionsForNodeName(nodeName), future);
        assertEquals(nodeName, future.get().getNodeName());

        future = PlainActionFuture.newFuture();
        restHighLevelClient.customAndParseAsync(request, optionsForNodeName(nodeName), future);
        assertEquals(nodeName, future.get().getNodeName());
    }

    private static RequestOptions optionsForNodeName(String nodeName) {
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.addHeader("node_name", nodeName);
        return options.build();
    }

    /**
     * The {@link RestHighLevelClient} must declare the following execution methods using the <code>protected</code> modifier
     * so that they can be used by subclasses to implement custom logic.
     */
    @SuppressForbidden(reason = "We're forced to uses Class#getDeclaredMethods() here because this test checks protected methods")
    public void testMethodsVisibility() {
        final String[] methodNames = new String[]{"convertExistsResponse",
                                                  "parseEntity",
                                                  "parseResponseException",
                                                  "performRequest",
                                                  "performRequestAndParseEntity",
                                                  "performRequestAndParseOptionalEntity",
                                                  "performRequestAsync",
                                                  "performRequestAsyncAndParseEntity",
                                                  "performRequestAsyncAndParseOptionalEntity"
                                                  };

        final Set<String> protectedMethods =  Arrays.stream(RestHighLevelClient.class.getDeclaredMethods())
                                                     .filter(method -> Modifier.isProtected(method.getModifiers()))
                                                     .map(Method::getName)
                                                     .collect(Collectors.toCollection(TreeSet::new));

        assertThat(protectedMethods, contains(methodNames));
    }

    /**
     * Mocks the asynchronous request execution by calling the {@link #mockPerformRequest(Request)} method.
     */
    private Void mockPerformRequestAsync(Request request, ResponseListener responseListener) {
        try {
            responseListener.onSuccess(mockPerformRequest(request));
        } catch (IOException e) {
            responseListener.onFailure(e);
        }
        return null;
    }

    /**
     * Mocks the synchronous request execution like if it was executed by Elasticsearch.
     */
    private Response mockPerformRequest(Request request) throws IOException {
        assertThat(request.getOptions().getHeaders(), hasSize(1));
        Header httpHeader = request.getOptions().getHeaders().get(0);
        final Response mockResponse = mock(Response.class);
        when(mockResponse.getHost()).thenReturn(new HttpHost("localhost", 9200));

        ProtocolVersion protocol = new ProtocolVersion("HTTP", 1, 1);
        when(mockResponse.getStatusLine()).thenReturn(new BasicStatusLine(protocol, 200, "OK"));

        MainResponse response = new MainResponse(httpHeader.getValue(), Version.CURRENT, ClusterName.DEFAULT, "_na", Build.CURRENT);
        BytesRef bytesRef = XContentHelper.toXContent(response, XContentType.JSON, false).toBytesRef();
        when(mockResponse.getEntity()).thenReturn(new NByteArrayEntity(bytesRef.bytes, ContentType.APPLICATION_JSON));

        RequestLine requestLine = new BasicRequestLine(HttpGet.METHOD_NAME, ENDPOINT, protocol);
        when(mockResponse.getRequestLine()).thenReturn(requestLine);

        return mockResponse;
    }

    /**
     * A custom high level client that provides custom methods to execute a request and get its associate response back.
     */
    static class CustomRestClient extends RestHighLevelClient {

        private CustomRestClient(RestClient restClient) {
            super(restClient, RestClient::close, Collections.emptyList());
        }

        MainResponse custom(MainRequest mainRequest, RequestOptions options) throws IOException {
            return performRequest(mainRequest, this::toRequest, options, this::toResponse, emptySet());
        }

        MainResponse customAndParse(MainRequest mainRequest, RequestOptions options) throws IOException {
            return performRequestAndParseEntity(mainRequest, this::toRequest, options, MainResponse::fromXContent, emptySet());
        }

        void customAsync(MainRequest mainRequest, RequestOptions options, ActionListener<MainResponse> listener) {
            performRequestAsync(mainRequest, this::toRequest, options, this::toResponse, listener, emptySet());
        }

        void customAndParseAsync(MainRequest mainRequest, RequestOptions options, ActionListener<MainResponse> listener) {
            performRequestAsyncAndParseEntity(mainRequest, this::toRequest, options, MainResponse::fromXContent, listener, emptySet());
        }

        Request toRequest(MainRequest mainRequest) throws IOException {
            return new Request(HttpGet.METHOD_NAME, ENDPOINT);
        }

        MainResponse toResponse(Response response) throws IOException {
            return parseEntity(response.getEntity(), MainResponse::fromXContent);
        }
    }
}
