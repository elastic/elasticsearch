/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.HttpHost;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockRestHighLevelTests extends ESTestCase {
    private RestHighLevelClient client;
    private static final List<String> WARNINGS = Collections.singletonList("Some Warning");

    @Before
    private void setupClient() throws IOException {
        final RestClient mockClient = mock(RestClient.class);
        RestHighLevelClientTests.mockGetRoot(mockClient);
        final Response mockResponse = mock(Response.class);

        when(mockResponse.getHost()).thenReturn(new HttpHost("localhost", 9200));
        when(mockResponse.getWarnings()).thenReturn(WARNINGS);

        ProtocolVersion protocol = new ProtocolVersion("HTTP", 1, 1);
        when(mockResponse.getStatusLine()).thenReturn(new BasicStatusLine(protocol, 200, "OK"));

        RequestLine requestLine = new BasicRequestLine(HttpGet.METHOD_NAME, "/_blah", protocol);
        when(mockResponse.getRequestLine()).thenReturn(requestLine);

        WarningFailureException expectedException = new WarningFailureException(mockResponse);
        doThrow(expectedException).when(mockClient).performRequest(any());

        client = new RestHighLevelClient(mockClient, RestClient::close, Collections.emptyList());
    }

    public void testWarningFailure() {
        WarningFailureException exception = expectThrows(WarningFailureException.class,
            () -> client.info(RequestOptions.DEFAULT));
        assertThat(exception.getMessage(), equalTo("method [GET], host [http://localhost:9200], URI [/_blah], " +
            "status line [HTTP/1.1 200 OK]"));
        assertNull(exception.getCause());
        assertThat(exception.getResponse().getWarnings(), equalTo(WARNINGS));
    }
}
