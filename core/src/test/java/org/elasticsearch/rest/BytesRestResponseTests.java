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

package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.transport.RemoteTransportException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BytesRestResponseTests extends ESTestCase {

    class UnknownException extends Exception {

        public UnknownException(final String message, final Throwable cause) {
            super(message, cause);
        }

    }

    public void testWithHeaders() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = randomBoolean() ? new DetailedExceptionRestChannel(request) : new SimpleExceptionRestChannel(request);

        BytesRestResponse response = new BytesRestResponse(channel, new WithHeadersException());
        assertThat(response.getHeaders().get("n1"), notNullValue());
        assertThat(response.getHeaders().get("n1"), contains("v11", "v12"));
        assertThat(response.getHeaders().get("n2"), notNullValue());
        assertThat(response.getHeaders().get("n2"), contains("v21", "v22"));
    }

    public void testSimpleExceptionMessage() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new SimpleExceptionRestChannel(request);

        Exception t = new ElasticsearchException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
        BytesRestResponse response = new BytesRestResponse(channel, t);
        String text = response.content().utf8ToString();
        assertThat(text, containsString("ElasticsearchException[an error occurred reading data]"));
        assertThat(text, not(containsString("FileNotFoundException")));
        assertThat(text, not(containsString("/foo/bar")));
        assertThat(text, not(containsString("error_trace")));
    }

    public void testDetailedExceptionMessage() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new DetailedExceptionRestChannel(request);

        Exception t = new ElasticsearchException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
        BytesRestResponse response = new BytesRestResponse(channel, t);
        String text = response.content().utf8ToString();
        assertThat(text, containsString("{\"type\":\"exception\",\"reason\":\"an error occurred reading data\"}"));
        assertThat(text, containsString("{\"type\":\"file_not_found_exception\",\"reason\":\"/foo/bar\"}"));
    }

    public void testNonElasticsearchExceptionIsNotShownAsSimpleMessage() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new SimpleExceptionRestChannel(request);

        Exception t = new UnknownException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
        BytesRestResponse response = new BytesRestResponse(channel, t);
        String text = response.content().utf8ToString();
        assertThat(text, not(containsString("UnknownException[an error occurred reading data]")));
        assertThat(text, not(containsString("FileNotFoundException[/foo/bar]")));
        assertThat(text, not(containsString("error_trace")));
        assertThat(text, containsString("\"error\":\"No ElasticsearchException found\""));
    }

    public void testErrorTrace() throws Exception {
        RestRequest request = new FakeRestRequest();
        request.params().put("error_trace", "true");
        RestChannel channel = new DetailedExceptionRestChannel(request);

        Exception t = new UnknownException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
        BytesRestResponse response = new BytesRestResponse(channel, t);
        String text = response.content().utf8ToString();
        assertThat(text, containsString("\"type\":\"unknown_exception\",\"reason\":\"an error occurred reading data\""));
        assertThat(text, containsString("{\"type\":\"file_not_found_exception\""));
        assertThat(text, containsString("\"stack_trace\":\"[an error occurred reading data]"));
    }

    public void testGuessRootCause() throws IOException {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new DetailedExceptionRestChannel(request);
        {
            Exception e = new ElasticsearchException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
            BytesRestResponse response = new BytesRestResponse(channel, e);
            String text = response.content().utf8ToString();
            assertThat(text, containsString("{\"root_cause\":[{\"type\":\"exception\",\"reason\":\"an error occurred reading data\"}]"));
        }
        {
            Exception e = new FileNotFoundException("/foo/bar");
            BytesRestResponse response = new BytesRestResponse(channel, e);
            String text = response.content().utf8ToString();
            assertThat(text, containsString("{\"root_cause\":[{\"type\":\"file_not_found_exception\",\"reason\":\"/foo/bar\"}]"));
        }
    }

    public void testNullThrowable() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new SimpleExceptionRestChannel(request);

        BytesRestResponse response = new BytesRestResponse(channel, null);
        String text = response.content().utf8ToString();
        assertThat(text, containsString("\"error\":\"unknown\""));
        assertThat(text, not(containsString("error_trace")));
    }

    public void testConvert() throws IOException {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new DetailedExceptionRestChannel(request);
        ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                new SearchShardTarget("node_1", new Index("foo", "_na_"), 1));
        ShardSearchFailure failure1 = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                new SearchShardTarget("node_1", new Index("foo", "_na_"), 2));
        SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed",  new ShardSearchFailure[] {failure, failure1});
        BytesRestResponse response = new BytesRestResponse(channel, new RemoteTransportException("foo", ex));
        String text = response.content().utf8ToString();
        String expected = "{\"error\":{\"root_cause\":[{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2}],\"type\":\"search_phase_execution_exception\",\"reason\":\"all shards failed\",\"phase\":\"search\",\"grouped\":true,\"failed_shards\":[{\"shard\":1,\"index\":\"foo\",\"node\":\"node_1\",\"reason\":{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2}}]},\"status\":400}";
        assertEquals(expected.trim(), text.trim());
        String stackTrace = ExceptionsHelper.stackTrace(ex);
        assertTrue(stackTrace.contains("Caused by: ParsingException[foobar]"));
    }

    public void testResponseWhenPathContainsEncodingError() throws IOException {
        final String path = "%a";
        final RestRequest request = new RestRequest(Collections.emptyMap(), path) {
            @Override
            public Method method() {
                return null;
            }

            @Override
            public String uri() {
                return null;
            }

            @Override
            public boolean hasContent() {
                return false;
            }

            @Override
            public BytesReference content() {
                return null;
            }

            @Override
            public String header(String name) {
                return null;
            }

            @Override
            public Iterable<Map.Entry<String, String>> headers() {
                return null;
            }
        };
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RestUtils.decodeComponent(request.rawPath()));
        final RestChannel channel = new DetailedExceptionRestChannel(request);
        // if we try to decode the path, this will throw an IllegalArgumentException again
        final BytesRestResponse response = new BytesRestResponse(channel, e);
        assertNotNull(response.content());
        final String content = response.content().utf8ToString();
        assertThat(content, containsString("\"type\":\"illegal_argument_exception\""));
        assertThat(content, containsString("\"reason\":\"partial escape sequence at end of string: %a\""));
        assertThat(content, containsString("\"status\":" + 400));
    }

    public void testResponseWhenInternalServerError() throws IOException {
        final RestRequest request = new FakeRestRequest();
        final RestChannel channel = new DetailedExceptionRestChannel(request);
        final BytesRestResponse response = new BytesRestResponse(channel, new ElasticsearchException("simulated"));
        assertNotNull(response.content());
        final String content = response.content().utf8ToString();
        assertThat(content, containsString("\"type\":\"exception\""));
        assertThat(content, containsString("\"reason\":\"simulated\""));
        assertThat(content, containsString("\"status\":" + 500));
    }

    public static class WithHeadersException extends ElasticsearchException {

        WithHeadersException() {
            super("");
            this.addHeader("n1", "v11", "v12");
            this.addHeader("n2", "v21", "v22");
        }
    }

    private static class SimpleExceptionRestChannel extends AbstractRestChannel {

        SimpleExceptionRestChannel(RestRequest request) {
            super(request, false);
        }

        @Override
        public void sendResponse(RestResponse response) {
        }
    }

    private static class DetailedExceptionRestChannel extends AbstractRestChannel {

        DetailedExceptionRestChannel(RestRequest request) {
            super(request, true);
        }

        @Override
        public void sendResponse(RestResponse response) {
        }
    }
}
