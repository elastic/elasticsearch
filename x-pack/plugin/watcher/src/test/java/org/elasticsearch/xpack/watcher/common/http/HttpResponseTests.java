/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.common.http;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class HttpResponseTests extends ESTestCase {

    public void testParseSelfGenerated() throws Exception {
        int status = randomIntBetween(200, 600);
        Map<String, String[]> headers = emptyMap();
        if (randomBoolean()) {
            headers = singletonMap("key", new String[] { "value" });
        }
        String body = randomBoolean() ? "body" : null;
        final HttpResponse response;
        if (randomBoolean() && headers.isEmpty() && body == null) {
            response = new HttpResponse(status);
        } else if (body != null ){
            switch (randomIntBetween(0, 2)) {
                case 0:
                    response = new HttpResponse(status, body, headers);
                    break;
                case 1:
                    response = new HttpResponse(status, body.getBytes(StandardCharsets.UTF_8), headers);
                    break;
                default: // 2
                    response = new HttpResponse(status, new BytesArray(body), headers);
                    break;
            }
        } else { // body is null
            switch (randomIntBetween(0, 3)) {
                case 0:
                    response = new HttpResponse(status, (String) null, headers);
                    break;
                case 1:
                    response = new HttpResponse(status, (byte[]) null, headers);
                    break;
                case 2:
                    response = new HttpResponse(status, (BytesReference) null, headers);
                    break;
                default: //3
                    response = new HttpResponse(status, headers);
                    break;
            }
        }

        XContentBuilder builder = jsonBuilder().value(response);
        XContentParser parser = createParser(builder);
        parser.nextToken();
        HttpResponse parsedResponse = HttpResponse.parse(parser);
        assertThat(parsedResponse, notNullValue());
        assertThat(parsedResponse.status(), is(status));
        if (body == null) {
            assertThat(parsedResponse.body(), nullValue());
        } else {
            assertThat(parsedResponse.body().utf8ToString(), is(body));
        }
        for (Map.Entry<String, String[]> headerEntry : headers.entrySet()) {
            assertThat(headerEntry.getValue(), arrayContaining(parsedResponse.header(headerEntry.getKey())));
        }
    }

    public void testThatHeadersAreCaseInsensitive() {
        Map<String, String[]> headers = new HashMap<>();
        headers.put(randomFrom("key", "keY", "KEY", "Key"), new String[] { "value" });
        headers.put(randomFrom("content-type"), new String[] { "text/html" });
        HttpResponse response = new HttpResponse(200, headers);
        assertThat(response.header("key")[0], is("value"));
        assertThat(response.contentType(), is("text/html"));
    }

    public void testThatHeaderNamesDoNotContainDotsOnSerialization() throws Exception {
        Map<String, String[]> headers = new HashMap<>();
        headers.put("es.index", new String[] { "value" });
        headers.put("es.index.2", new String[] { "value" });

        HttpResponse response = new HttpResponse(200, headers);
        assertThat(response.header("es.index")[0], is("value"));
        assertThat(response.header("es.index.2")[0], is("value"));

        XContentBuilder builder = jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        XContentParser parser = createParser(builder);
        Map<String, Object> responseMap = parser.map();
        parser.close();

        assertThat(responseMap, hasKey("headers"));
        assertThat(responseMap.get("headers"), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> responseHeaders = (Map<String, Object>) responseMap.get("headers");

        assertThat(responseHeaders, not(hasKey("es.index")));
        assertThat(responseHeaders, hasEntry("es_index", Collections.singletonList("value")));

        assertThat(responseHeaders, not(hasKey("es.index.2")));
        assertThat(responseHeaders, hasEntry("es_index_2", Collections.singletonList("value")));
    }
}
