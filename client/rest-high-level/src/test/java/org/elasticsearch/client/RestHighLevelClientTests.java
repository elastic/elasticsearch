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
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.internal.matchers.ArrayEquals;
import org.mockito.internal.matchers.VarargMatcher;

import java.io.IOException;
import java.net.SocketTimeoutException;

import static org.mockito.Matchers.any;
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

    public void testPing() throws IOException {
        assertTrue(restHighLevelClient.ping());
        verify(restClient).performRequest(eq("HEAD"), eq("/"), argThat(new HeadersVarargMatcher()));
    }

    public void testPingFailure() throws IOException {
        when(restClient.performRequest(any(), any())).thenThrow(new IllegalStateException());
        expectThrows(IllegalStateException.class, () -> restHighLevelClient.ping());
    }

    public void testPingFailed() throws IOException {
        when(restClient.performRequest(any(), any())).thenThrow(new SocketTimeoutException());
        assertFalse(restHighLevelClient.ping());
    }

    public void testPingWithHeaders() throws IOException {
        Header[] headers = RestClientTestUtil.randomHeaders(random(), "Header");
        assertTrue(restHighLevelClient.ping(headers));
        verify(restClient).performRequest(eq("HEAD"), eq("/"), argThat(new HeadersVarargMatcher(headers)));
    }

    private class HeadersVarargMatcher extends ArgumentMatcher<Header[]> implements VarargMatcher {
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
}
