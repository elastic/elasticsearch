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
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RestClientTests extends RestClientTestCase {

    public void testCloseIsIdempotent() throws IOException {
        HttpHost[] hosts = new HttpHost[]{new HttpHost("localhost", 9200)};
        CloseableHttpAsyncClient closeableHttpAsyncClient = mock(CloseableHttpAsyncClient.class);
        RestClient restClient =  new RestClient(closeableHttpAsyncClient, 1_000, new Header[0], hosts, null, null);
        restClient.close();
        verify(closeableHttpAsyncClient, times(1)).close();
        restClient.close();
        verify(closeableHttpAsyncClient, times(2)).close();
        restClient.close();
        verify(closeableHttpAsyncClient, times(3)).close();
    }

    public void testPerformAsyncWithUnsupportedMethod() throws Exception {
        RestClient.SyncResponseListener listener = new RestClient.SyncResponseListener(10000);
        try (RestClient restClient = createRestClient()) {
            restClient.performRequestAsync("unsupported", randomAsciiLettersOfLength(5), listener);
            listener.get();

            fail("should have failed because of unsupported method");
        } catch (UnsupportedOperationException exception) {
            assertEquals("http method not supported: unsupported", exception.getMessage());
        }
    }

    public void testPerformAsyncWithNullParams() throws Exception {
        RestClient.SyncResponseListener listener = new RestClient.SyncResponseListener(10000);
        try (RestClient restClient = createRestClient()) {
            restClient.performRequestAsync(randomAsciiLettersOfLength(5), randomAsciiLettersOfLength(5), null, listener);
            listener.get();

            fail("should have failed because of null parameters");
        } catch (NullPointerException exception) {
            assertEquals("params must not be null", exception.getMessage());
        }
    }

    public void testPerformAsyncWithNullHeaders() throws Exception {
        RestClient.SyncResponseListener listener = new RestClient.SyncResponseListener(10000);
        try (RestClient restClient = createRestClient()) {
            restClient.performRequestAsync("GET", randomAsciiLettersOfLength(5), listener, (Header) null);
            listener.get();

            fail("should have failed because of null headers");
        } catch (NullPointerException exception) {
            assertEquals("request header must not be null", exception.getMessage());
        }
    }

    public void testPerformAsyncWithWrongEndpoint() throws Exception {
        RestClient.SyncResponseListener listener = new RestClient.SyncResponseListener(10000);
        try (RestClient restClient = createRestClient()) {
            restClient.performRequestAsync("GET", "::http:///", listener);
            listener.get();

            fail("should have failed because of wrong endpoint");
        } catch (IllegalArgumentException exception) {
            assertEquals("Expected scheme name at index 0: ::http:///", exception.getMessage());
        }
    }

    public void testBuildUriLeavesPathUntouched() {
        {
            URI uri = RestClient.buildUri("/foo$bar", "/index/type/id", Collections.<String, String>emptyMap());
            assertEquals("/foo$bar/index/type/id", uri.getPath());
        }
        {
            URI uri = RestClient.buildUri(null, "/foo$bar/ty/pe/i/d", Collections.<String, String>emptyMap());
            assertEquals("/foo$bar/ty/pe/i/d", uri.getPath());
        }
        {
            URI uri = RestClient.buildUri(null, "/index/type/id", Collections.singletonMap("foo$bar", "x/y/z"));
            assertEquals("/index/type/id", uri.getPath());
            assertEquals("foo$bar=x/y/z", uri.getQuery());
        }
    }

    private static RestClient createRestClient() {
        HttpHost[] hosts = new HttpHost[]{new HttpHost("localhost", 9200)};
        return new RestClient(mock(CloseableHttpAsyncClient.class), randomLongBetween(1_000, 30_000), new Header[]{}, hosts, null, null);
    }
}
