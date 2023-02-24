/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

public class Netty4HeadBodyIsEmptyIT extends ESRestTestCase {
    public void testHeadRoot() throws IOException {
        headTestCase("/", emptyMap(), greaterThan(0));
        headTestCase("/", singletonMap("pretty", ""), greaterThan(0));
        headTestCase("/", singletonMap("pretty", "true"), greaterThan(0));
    }

    private void createTestDoc() throws IOException {
        createTestDoc("test");
    }

    private void createTestDoc(final String indexName) throws IOException {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.field("test", "test");
            }
            builder.endObject();
            Request request = new Request("PUT", "/" + indexName + "/_doc/" + "1");
            request.setJsonEntity(Strings.toString(builder));
            client().performRequest(request);
        }
    }

    public void testDocumentExists() throws IOException {
        createTestDoc();
        headTestCase("/test/_doc/1", emptyMap(), greaterThan(0));
        headTestCase("/test/_doc/1", singletonMap("pretty", "true"), greaterThan(0));
        headTestCase("/test/_doc/2", emptyMap(), NOT_FOUND.getStatus(), greaterThan(0));
    }

    public void testIndexExists() throws IOException {
        createTestDoc();
        headTestCase("/test", emptyMap(), nullValue(Integer.class));
        headTestCase("/test", singletonMap("pretty", "true"), nullValue(Integer.class));
    }

    public void testAliasExists() throws IOException {
        createTestDoc();
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startArray("actions");
                {
                    builder.startObject();
                    {
                        builder.startObject("add");
                        {
                            builder.field("index", "test");
                            builder.field("alias", "test_alias");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();

            Request request = new Request("POST", "/_aliases");
            request.setJsonEntity(Strings.toString(builder));
            client().performRequest(request);
            headTestCase("/_alias/test_alias", emptyMap(), greaterThan(0));
            headTestCase("/test/_alias/test_alias", emptyMap(), greaterThan(0));
        }
    }

    public void testAliasDoesNotExist() throws IOException {
        createTestDoc();
        headTestCase("/_alias/test_alias", emptyMap(), NOT_FOUND.getStatus(), greaterThan(0));
        headTestCase("/test/_alias/test_alias", emptyMap(), NOT_FOUND.getStatus(), greaterThan(0));
    }

    public void testTemplateExists() throws IOException {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.array("index_patterns", "*");
                builder.startObject("settings");
                {
                    builder.field("number_of_replicas", 0);
                }
                builder.endObject();
            }
            builder.endObject();

            Request request = new Request("PUT", "/_template/template");
            request.setOptions(expectWarnings(RestPutIndexTemplateAction.DEPRECATION_WARNING));

            request.setJsonEntity(Strings.toString(builder));
            client().performRequest(request);
            headTestCase("/_template/template", emptyMap(), greaterThan(0));
        }
    }

    public void testGetSourceAction() throws IOException {
        createTestDoc();
        headTestCase("/test/_source/1", emptyMap(), greaterThan(0));
        headTestCase("/test/_source/2", emptyMap(), NOT_FOUND.getStatus(), greaterThan(0));

        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("mappings");
                {
                    builder.startObject("_source");
                    {
                        builder.field("enabled", false);
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();

            Request request = new Request("PUT", "/test-no-source");
            request.setJsonEntity(Strings.toString(builder));
            client().performRequest(request);
            createTestDoc("test-no-source");
            headTestCase("/test-no-source/_source/1", emptyMap(), NOT_FOUND.getStatus(), greaterThan(0));
        }
    }

    public void testException() throws IOException {
        /*
         * This will throw an index not found exception which will be sent on the channel; previously when handling HEAD requests that would
         * throw an exception, the content was swallowed and a content length header of zero was returned. Instead of swallowing the content
         * we now let it rise up to the upstream channel so that it can compute the content length that would be returned. This test case is
         * a test for this situation.
         */
        headTestCase("/index-not-found-exception", emptyMap(), NOT_FOUND.getStatus(), greaterThan(0));
    }

    private void headTestCase(final String url, final Map<String, String> params, final Matcher<Integer> matcher) throws IOException {
        headTestCase(url, params, OK.getStatus(), matcher);
    }

    private void headTestCase(
        final String url,
        final Map<String, String> params,
        final int expectedStatusCode,
        final Matcher<Integer> matcher,
        final String... expectedWarnings
    ) throws IOException {
        Request request = new Request("HEAD", url);
        for (Map.Entry<String, String> param : params.entrySet()) {
            request.addParameter(param.getKey(), param.getValue());
        }
        request.setOptions(expectWarnings(expectedWarnings));
        Response response = client().performRequest(request);
        assertEquals(expectedStatusCode, response.getStatusLine().getStatusCode());
        final var contentLength = response.getHeader("Content-Length");
        assertThat(contentLength == null ? null : Integer.valueOf(contentLength), matcher);
        assertNull("HEAD requests shouldn't have a response body but " + url + " did", response.getEntity());
    }

}
