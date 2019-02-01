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
