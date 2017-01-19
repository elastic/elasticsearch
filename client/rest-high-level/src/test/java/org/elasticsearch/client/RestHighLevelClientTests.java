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
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicStatusLine;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.internal.matchers.ArrayEquals;
import org.mockito.internal.matchers.VarargMatcher;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Collections;

import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestHighLevelClientTests extends ESTestCase {

    private RestClient restClient;
    private RestHighLevelClient restHighLevelClient;

    @Before
    public void initClient() throws IOException {
        restClient = mock(RestClient.class);
        restHighLevelClient = new RestHighLevelClient(restClient);
    }

    public void testPingSuccessful() throws IOException {
        Header[] headers = RestClientTestUtil.randomHeaders(random(), "Header");
        Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(newStatusLine(200));
        when(restClient.performRequest(anyString(), anyString(), anyMapOf(String.class, String.class),
                anyObject(), anyVararg())).thenReturn(response);
        assertTrue(restHighLevelClient.ping(headers));
        verify(restClient).performRequest(eq("HEAD"), eq("/"), eq(Collections.emptyMap()),
                Matchers.isNull(HttpEntity.class), argThat(new HeadersVarargMatcher(headers)));
    }

    public void testPing404NotFound() throws IOException {
        Header[] headers = RestClientTestUtil.randomHeaders(random(), "Header");
        Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(newStatusLine(404));
        when(restClient.performRequest(anyString(), anyString(), anyMapOf(String.class, String.class),
                anyObject(), anyVararg())).thenReturn(response);
        assertFalse(restHighLevelClient.ping(headers));
        verify(restClient).performRequest(eq("HEAD"), eq("/"), eq(Collections.emptyMap()),
                Matchers.isNull(HttpEntity.class), argThat(new HeadersVarargMatcher(headers)));
    }

    public void testPingSocketTimeout() throws IOException {
        Header[] headers = RestClientTestUtil.randomHeaders(random(), "Header");
        when(restClient.performRequest(anyString(), anyString(), anyMapOf(String.class, String.class),
                anyObject(), anyVararg())).thenThrow(new SocketTimeoutException());
        expectThrows(SocketTimeoutException.class, () -> restHighLevelClient.ping(headers));
        verify(restClient).performRequest(eq("HEAD"), eq("/"), eq(Collections.emptyMap()),
                Matchers.isNull(HttpEntity.class), argThat(new HeadersVarargMatcher(headers)));
    }

    public void testParseEntity() throws IOException {
        {
            IllegalStateException ise = expectThrows(IllegalStateException.class, () -> RestHighLevelClient.parseEntity(null, null));
            assertEquals("Response body expected but not returned", ise.getMessage());
        }
        {
            IllegalStateException ise = expectThrows(IllegalStateException.class,
                    () -> RestHighLevelClient.parseEntity(new BasicHttpEntity(), null));
            assertEquals("Elasticsearch didn't return the Content-Type header, unable to parse response body", ise.getMessage());
        }
        {
            StringEntity entity = new StringEntity("", ContentType.APPLICATION_SVG_XML);
            IllegalStateException ise = expectThrows(IllegalStateException.class, () -> RestHighLevelClient.parseEntity(entity, null));
            assertEquals("Unsupported Content-Type: " + entity.getContentType().getValue(), ise.getMessage());
        }
        {
            StringEntity entity = new StringEntity("{\"field\":\"value\"}", ContentType.APPLICATION_JSON);
            assertEquals("value", RestHighLevelClient.parseEntity(entity, parser -> {
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertTrue(parser.nextToken().isValue());
                String value = parser.text();
                assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
                assertNull(parser.nextToken());
                return value;
            }));
        }
    }

    //TODO add unit tests for all of the private / package private methods in RestHighLevelClient

    private static class HeadersVarargMatcher extends ArgumentMatcher<Header[]> implements VarargMatcher {
        private Header[] expectedHeaders;

        HeadersVarargMatcher(Header... expectedHeaders) {
            this.expectedHeaders = expectedHeaders;
        }

        @Override
        public boolean matches(Object varargArgument) {
            if (varargArgument instanceof Header[]) {
                Header[] actualHeaders = (Header[]) varargArgument;
                return new ArrayEquals(expectedHeaders).matches(actualHeaders);
            }
            return false;
        }
    }

    private static StatusLine newStatusLine(int statusCode) {
        return new BasicStatusLine(new ProtocolVersion("http", 1, 1), statusCode, "");
    }
}
