/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.common.http;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherXContentParser;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xcontent.XContentFactory.cborBuilder;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xcontent.XContentFactory.smileBuilder;
import static org.elasticsearch.xcontent.XContentFactory.yamlBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

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
                    builder.path(randomAlphaOfLength(50));
                }
            }
        }
        if (usually()) {
            builder.method(randomFrom(HttpMethod.values()));
        }
        if (randomBoolean()) {
            builder.setParam(randomAlphaOfLength(10), randomAlphaOfLength(10));
            if (randomBoolean()) {
                builder.setParam(randomAlphaOfLength(10), randomAlphaOfLength(10));
            }
        }
        if (randomBoolean()) {
            builder.setHeader(randomAlphaOfLength(10), randomAlphaOfLength(10));
            if (randomBoolean()) {
                builder.setHeader(randomAlphaOfLength(10), randomAlphaOfLength(10));
            }
        }
        if (randomBoolean()) {
            builder.auth(new BasicAuth(randomAlphaOfLength(10), randomAlphaOfLength(20).toCharArray()));
        }
        if (randomBoolean()) {
            builder.body(randomAlphaOfLength(200));
        }
        if (randomBoolean()) {
            builder.connectionTimeout(randomTimeout());
        }
        if (randomBoolean()) {
            builder.readTimeout(randomTimeout());
        }
        if (randomBoolean()) {
            builder.proxy(new HttpProxy(randomAlphaOfLength(10), randomIntBetween(1024, 65000)));
        }

        final HttpRequest httpRequest = builder.build();
        assertNotNull(httpRequest);

        try (XContentBuilder xContentBuilder = randomFrom(jsonBuilder(), smileBuilder(), yamlBuilder(), cborBuilder())) {
            httpRequest.toXContent(xContentBuilder, WatcherParams.builder().hideSecrets(false).build());

            try (XContentParser parser = createParser(xContentBuilder)) {
                assertNull(parser.currentToken());
                parser.nextToken();

                HttpRequest parsedRequest = HttpRequest.Parser.parse(parser);
                assertEquals(httpRequest, parsedRequest);
            }
        }
    }

    private static TimeValue randomTimeout() {
        // micros and nanos don't round trip will full precision so exclude them from the test
        return randomTimeValue(0, 1000, TimeUnit.DAYS, TimeUnit.HOURS, TimeUnit.MINUTES, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    }

    public void testXContentRemovesAuthorization() throws Exception {
        HttpRequest request = HttpRequest.builder("localhost", 443).setHeader("Authorization", "Bearer Foo").build();
        try (XContentBuilder builder = jsonBuilder()) {
            WatcherParams params = WatcherParams.builder().hideSecrets(false).build();
            request.toXContent(builder, params);
            assertThat(Strings.toString(builder), containsString("Bearer Foo"));
        }
        try (XContentBuilder builder = jsonBuilder()) {
            request.toXContent(builder, WatcherParams.HIDE_SECRETS);
            assertThat(Strings.toString(builder), not(containsString("Bearer Foo")));
            assertThat(Strings.toString(builder), containsString(WatcherXContentParser.REDACTED_PASSWORD));
        }
    }

    public void testToStringDoesNotContainAuthorizationheader() {
        HttpRequest request = HttpRequest.builder("localhost", 443).setHeader("Authorization", "Bearer Foo").build();
        assertThat(request.toString(), not(containsString("Bearer Foo")));
        assertThat(request.toString(), containsString("Authorization: " + WatcherXContentParser.REDACTED_PASSWORD));
    }

    private void assertThatManualBuilderEqualsParsingFromUrl(String url, HttpRequest.Builder builder) throws Exception {
        XContentBuilder urlContentBuilder = jsonBuilder().startObject().field("url", url).endObject();
        XContentParser urlContentParser = createParser(urlContentBuilder);
        urlContentParser.nextToken();

        HttpRequest urlParsedRequest = HttpRequest.Parser.parse(urlContentParser);

        WatcherParams params = WatcherParams.builder().hideSecrets(false).build();
        XContentBuilder xContentBuilder = builder.build().toXContent(jsonBuilder(), params);
        XContentParser xContentParser = createParser(xContentBuilder);
        xContentParser.nextToken();
        HttpRequest parsedRequest = HttpRequest.Parser.parse(xContentParser);

        assertThat(parsedRequest, is(urlParsedRequest));
    }
}
