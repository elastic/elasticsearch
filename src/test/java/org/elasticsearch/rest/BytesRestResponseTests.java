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
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.Test;

import java.io.FileNotFoundException;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class BytesRestResponseTests extends ElasticsearchTestCase {

    @Test
    public void testWithHeaders() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = randomBoolean() ? new DetailedExceptionRestChannel(request) : new SimpleExceptionRestChannel(request);

        BytesRestResponse response = new BytesRestResponse(channel, new ExceptionWithHeaders());
        assertThat(response.getHeaders().get("n1"), notNullValue());
        assertThat(response.getHeaders().get("n1"), contains("v11", "v12"));
        assertThat(response.getHeaders().get("n2"), notNullValue());
        assertThat(response.getHeaders().get("n2"), contains("v21", "v22"));
    }

    @Test
    public void testSimpleExceptionMessage() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new SimpleExceptionRestChannel(request);

        Throwable t = new ElasticsearchException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
        BytesRestResponse response = new BytesRestResponse(channel, t);
        String text = response.content().toUtf8();
        assertThat(text, containsString("ElasticsearchException[an error occurred reading data]"));
        assertThat(text, not(containsString("FileNotFoundException")));
        assertThat(text, not(containsString("/foo/bar")));
        assertThat(text, not(containsString("error_trace")));
    }

    @Test
    public void testDetailedExceptionMessage() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new DetailedExceptionRestChannel(request);

        Throwable t = new ElasticsearchException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
        BytesRestResponse response = new BytesRestResponse(channel, t);
        String text = response.content().toUtf8();
        assertThat(text, containsString("ElasticsearchException[an error occurred reading data]"));
        assertThat(text, containsString("FileNotFoundException[/foo/bar]"));
    }

    @Test
    public void testNonElasticsearchExceptionIsNotShownAsSimpleMessage() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new SimpleExceptionRestChannel(request);

        Throwable t = new Throwable("an error occurred reading data", new FileNotFoundException("/foo/bar"));
        BytesRestResponse response = new BytesRestResponse(channel, t);
        String text = response.content().toUtf8();
        assertThat(text, not(containsString("Throwable[an error occurred reading data]")));
        assertThat(text, not(containsString("FileNotFoundException[/foo/bar]")));
        assertThat(text, not(containsString("error_trace")));
        assertThat(text, containsString("\"error\":\"No ElasticsearchException found\""));
    }

    @Test
    public void testErrorTrace() throws Exception {
        RestRequest request = new FakeRestRequest();
        request.params().put("error_trace", "true");
        RestChannel channel = new DetailedExceptionRestChannel(request);

        Throwable t = new Throwable("an error occurred reading data", new FileNotFoundException("/foo/bar"));
        BytesRestResponse response = new BytesRestResponse(channel, t);
        String text = response.content().toUtf8();
        assertThat(text, containsString("\"error\":\"Throwable[an error occurred reading data]"));
        assertThat(text, containsString("FileNotFoundException[/foo/bar]"));
        assertThat(text, containsString("\"error_trace\":{\"message\":\"an error occurred reading data\""));
    }

    @Test
    public void testNullThrowable() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new SimpleExceptionRestChannel(request);

        BytesRestResponse response = new BytesRestResponse(channel, null);
        String text = response.content().toUtf8();
        assertThat(text, containsString("\"error\":\"Unknown\""));
        assertThat(text, not(containsString("error_trace")));
    }

    private static class ExceptionWithHeaders extends ElasticsearchException.WithRestHeaders {

        ExceptionWithHeaders() {
            super("", header("n1", "v11", "v12"), header("n2", "v21", "v22"));
        }
    }

    private static class SimpleExceptionRestChannel extends RestChannel {

        SimpleExceptionRestChannel(RestRequest request) {
            super(request, false);
        }

        @Override
        public void sendResponse(RestResponse response) {
        }
    }

    private static class DetailedExceptionRestChannel extends RestChannel {

        DetailedExceptionRestChannel(RestRequest request) {
            super(request, true);
        }

        @Override
        public void sendResponse(RestResponse response) {
        }
    }
}
