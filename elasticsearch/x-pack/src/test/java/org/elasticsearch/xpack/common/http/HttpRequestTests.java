/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.common.http;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.common.http.HttpRequest;
import org.elasticsearch.xpack.common.http.Scheme;
import org.elasticsearch.xpack.common.http.auth.HttpAuthRegistry;
import org.elasticsearch.xpack.common.http.auth.basic.BasicAuth;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class HttpRequestTests extends ESTestCase {

    public void testParsingFromUrl() throws Exception {
        HttpRequest.Builder builder = HttpRequest.builder("www.example.org", 1234);
        builder.path("/foo/bar/org");
        builder.setParam("param", "test");
        builder.scheme(Scheme.HTTPS);
        assertThatManualBuilderEqualsParsingFromUrl("https://www.example.org:1234/foo/bar/org?param=test", builder);

        // test without specifying port
        builder = HttpRequest.builder("www.example.org", 80);
        assertThatManualBuilderEqualsParsingFromUrl("http://www.example.org", builder);

        // encoded values
        builder = HttpRequest.builder("www.example.org", 80).setParam("foo", " white space");
        assertThatManualBuilderEqualsParsingFromUrl("http://www.example.org?foo=%20white%20space", builder);
    }

    public void testParsingEmptyUrl() throws Exception {
        try {
            HttpRequest.builder().fromUrl("");
            fail("Expected exception due to empty URL");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("Configured URL is empty, please configure a valid URL"));
        }
    }

    public void testInvalidUrlsWithMissingScheme() throws Exception {
        try {
            HttpRequest.builder().fromUrl("www.test.de");
            fail("Expected exception due to missing scheme");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("URL [www.test.de] does not contain a scheme"));
        }
    }

    public void testInvalidUrlsWithHost() throws Exception {
        try {
            HttpRequest.builder().fromUrl("https://");
            fail("Expected exception due to missing host");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("Malformed URL [https://]"));
        }
    }

    public void testXContentSerialization() throws Exception {
        final HttpRequest.Builder builder;
        if (randomBoolean()) {
            builder = HttpRequest.builder();
            builder.fromUrl("http://localhost:9200/generic/createevent");
        } else {
            builder = HttpRequest.builder("localhost", 9200);
            if (randomBoolean()) {
                builder.scheme(randomFrom(Scheme.values()));
                if (usually()) {
                    builder.path(randomAsciiOfLength(50));
                }
            }
        }
        if (usually()) {
            builder.method(randomFrom(HttpMethod.values()));
        }
        if (randomBoolean()) {
            builder.setParam(randomAsciiOfLength(10), randomAsciiOfLength(10));
            if (randomBoolean()) {
                builder.setParam(randomAsciiOfLength(10), randomAsciiOfLength(10));
            }
        }
        if (randomBoolean()) {
            builder.setHeader(randomAsciiOfLength(10), randomAsciiOfLength(10));
            if (randomBoolean()) {
                builder.setHeader(randomAsciiOfLength(10), randomAsciiOfLength(10));
            }
        }
        if (randomBoolean()) {
            builder.auth(new BasicAuth(randomAsciiOfLength(10), randomAsciiOfLength(20).toCharArray()));
        }
        if (randomBoolean()) {
            builder.body(randomAsciiOfLength(200));
        }
        if (randomBoolean()) {
            builder.connectionTimeout(TimeValue.parseTimeValue(randomTimeValue(), "my.setting"));
        }
        if (randomBoolean()) {
            builder.readTimeout(TimeValue.parseTimeValue(randomTimeValue(), "my.setting"));
        }
        if (randomBoolean()) {
            builder.proxy(new HttpProxy(randomAsciiOfLength(10), randomIntBetween(1024, 65000)));
        }

        builder.build().toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS);
    }

    private void assertThatManualBuilderEqualsParsingFromUrl(String url, HttpRequest.Builder builder) throws Exception {
        XContentBuilder urlContentBuilder = jsonBuilder().startObject().field("url", url).endObject();
        XContentParser urlContentParser = JsonXContent.jsonXContent.createParser(urlContentBuilder.bytes());
        urlContentParser.nextToken();

        HttpRequest.Parser parser = new HttpRequest.Parser(mock(HttpAuthRegistry.class));
        HttpRequest urlParsedRequest = parser.parse(urlContentParser);

        XContentBuilder xContentBuilder = builder.build().toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS);
        XContentParser xContentParser = JsonXContent.jsonXContent.createParser(xContentBuilder.bytes());
        xContentParser.nextToken();
        HttpRequest parsedRequest = parser.parse(xContentParser);

        assertThat(parsedRequest, is(urlParsedRequest));
    }
}
