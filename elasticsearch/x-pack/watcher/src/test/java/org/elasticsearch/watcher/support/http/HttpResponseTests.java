/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.test.WatcherTestUtils.xContentParser;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
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
        XContentParser parser = xContentParser(builder);
        parser.nextToken();
        HttpResponse parsedResponse = HttpResponse.parse(parser);
        assertThat(parsedResponse, notNullValue());
        assertThat(parsedResponse.status(), is(status));
        if (body == null) {
            assertThat(parsedResponse.body(), nullValue());
        } else {
            assertThat(parsedResponse.body().toUtf8(), is(body));
        }
        assertThat(parsedResponse.headers().size(), is(headers.size()));
        for (Map.Entry<String, String[]> header : parsedResponse.headers().entrySet()) {
            assertThat(header.getValue(), arrayContaining(headers.get(header.getKey())));
        }
    }
}
