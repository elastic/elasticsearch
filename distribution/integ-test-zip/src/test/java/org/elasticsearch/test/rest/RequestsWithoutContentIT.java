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

package org.elasticsearch.test.rest;

import org.elasticsearch.client.ResponseException;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;

public class RequestsWithoutContentIT extends ESRestTestCase {

    public void testIndexMissingBody() throws IOException {
        ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(
                randomBoolean() ? "POST" : "PUT", "/idx/type/123"));
        assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(responseException.getMessage(), containsString("request body is required"));
    }

    public void testBulkMissingBody() throws IOException {
        ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(
                randomBoolean() ? "POST" : "PUT", "/_bulk"));
        assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(responseException.getMessage(), containsString("request body is required"));
    }

    public void testPutSettingsMissingBody() throws IOException {
        ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(
                "PUT", "/_settings"));
        assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(responseException.getMessage(), containsString("request body is required"));
    }

    public void testPutMappingsMissingBody() throws IOException {
        ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(
                randomBoolean() ? "POST" : "PUT", "/test_index/test_type/_mapping"));
        assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(responseException.getMessage(), containsString("request body is required"));
    }

    public void testPutIndexTemplateMissingBody() throws IOException {
        ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(
                randomBoolean() ? "PUT" : "POST", "/_template/my_template"));
        assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(responseException.getMessage(), containsString("request body is required"));
    }

    public void testMultiSearchMissingBody() throws IOException {
        ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(
                randomBoolean() ? "POST" : "GET", "/_msearch"));
        assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(responseException.getMessage(), containsString("request body or source parameter is required"));
    }

    public void testPutPipelineMissingBody() throws IOException {
        ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(
                "PUT", "/_ingest/pipeline/my_pipeline"));
        assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(responseException.getMessage(), containsString("request body or source parameter is required"));
    }

    public void testSimulatePipelineMissingBody() throws IOException {
        ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(
                randomBoolean() ? "POST" : "GET", "/_ingest/pipeline/my_pipeline/_simulate"));
        assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(responseException.getMessage(), containsString("request body or source parameter is required"));
    }

    public void testPutScriptMissingBody() throws IOException {
        ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(
                randomBoolean() ? "POST" : "PUT", "/_scripts/lang"));
        assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(responseException.getMessage(), containsString("request body is required"));
    }
}
