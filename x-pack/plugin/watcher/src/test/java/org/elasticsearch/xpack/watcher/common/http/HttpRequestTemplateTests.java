/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.common.http;

import io.netty.handler.codec.http.HttpHeaders;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.test.MockTextTemplateEngine;

import java.util.Collections;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class HttpRequestTemplateTests extends ESTestCase {

    public void testBodyWithXContent() throws Exception {
        XContentType type = randomFrom(XContentType.JSON, XContentType.YAML);
        HttpRequestTemplate template = HttpRequestTemplate.builder("_host", 1234)
            .body(XContentBuilder.builder(type.xContent()).startObject().endObject())
            .build();
        HttpRequest request = template.render(new MockTextTemplateEngine(), emptyMap());
        assertThat(request.headers, hasEntry(HttpHeaders.Names.CONTENT_TYPE, type.mediaType()));
    }

    public void testBody() throws Exception {
        HttpRequestTemplate template = HttpRequestTemplate.builder("_host", 1234).body("_body").build();
        HttpRequest request = template.render(new MockTextTemplateEngine(), emptyMap());
        assertThat(request.headers.size(), is(0));
    }

    public void testProxy() throws Exception {
        HttpRequestTemplate template = HttpRequestTemplate.builder("_host", 1234).proxy(new HttpProxy("localhost", 8080)).build();
        HttpRequest request = template.render(new MockTextTemplateEngine(), Collections.emptyMap());
        assertThat(request.proxy().getHost(), is("localhost"));
        assertThat(request.proxy().getPort(), is(8080));
    }

    public void testRender() {
        HttpRequestTemplate template = HttpRequestTemplate.builder("_host", 1234)
            .body(new TextTemplate("_body"))
            .path(new TextTemplate("_path"))
            .putParam("_key1", new TextTemplate("_value1"))
            .putHeader("_key2", new TextTemplate("_value2"))
            .build();

        HttpRequest result = template.render(new MockTextTemplateEngine(), Collections.emptyMap());
        assertThat(result.body(), equalTo("_body"));
        assertThat(result.path(), equalTo("_path"));
        assertThat(result.params(), equalTo(Collections.singletonMap("_key1", "_value1")));
        assertThat(result.headers(), equalTo(Collections.singletonMap("_key2", "_value2")));
    }

    public void testProxyParsing() throws Exception {
        HttpRequestTemplate.Builder builder = HttpRequestTemplate.builder("_host", 1234);
        builder.path("/path");
        builder.method(randomFrom(HttpMethod.values()));
        String proxyHost = randomAlphaOfLength(10);
        int proxyPort = randomIntBetween(1, 65534);
        builder.proxy(new HttpProxy(proxyHost, proxyPort));
        HttpRequestTemplate template = builder.build();

        XContentBuilder xContentBuilder = template.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS);
        XContentParser xContentParser = createParser(xContentBuilder);
        xContentParser.nextToken();

        HttpRequestTemplate parsedTemplate = HttpRequestTemplate.Parser.parse(xContentParser);
        assertThat(parsedTemplate.proxy().getPort(), is(proxyPort));
        assertThat(parsedTemplate.proxy().getHost(), is(proxyHost));
    }

    public void testParseSelfGenerated() throws Exception {
        HttpRequestTemplate.Builder builder = HttpRequestTemplate.builder("_host", 1234);

        if (randomBoolean()) {
            builder.method(randomFrom(HttpMethod.values()));
        }
        if (randomBoolean()) {
            builder.path("/path");
        }
        boolean xbody = randomBoolean();
        if (randomBoolean()) {
            if (xbody) {
                builder.body(jsonBuilder().startObject().endObject());
            } else {
                builder.body("_body");
            }
        }
        if (randomBoolean()) {
            builder.auth(new BasicAuth("_username", "_password".toCharArray()));
        }
        if (randomBoolean()) {
            builder.putParam("_key", new TextTemplate("_value"));
        }
        if (randomBoolean()) {
            builder.putHeader("_key", new TextTemplate("_value"));
        }
        long connectionTimeout = randomBoolean() ? 0 : randomIntBetween(5, 100000);
        if (connectionTimeout > 0) {
            builder.connectionTimeout(TimeValue.timeValueSeconds(connectionTimeout));
        }
        long readTimeout = randomBoolean() ? 0 : randomIntBetween(5, 100000);
        if (readTimeout > 0) {
            builder.readTimeout(TimeValue.timeValueSeconds(readTimeout));
        }
        boolean enableProxy = randomBoolean();
        if (enableProxy) {
            builder.proxy(new HttpProxy(randomAlphaOfLength(10), randomIntBetween(1, 65534)));
        }

        HttpRequestTemplate template = builder.build();

        XContentBuilder xContentBuilder = template.toXContent(jsonBuilder(), WatcherParams.builder().hideSecrets(false).build());
        XContentParser xContentParser = createParser(xContentBuilder);
        xContentParser.nextToken();
        HttpRequestTemplate parsed = HttpRequestTemplate.Parser.parse(xContentParser);

        assertEquals(template, parsed);
    }

    public void testParsingFromUrl() throws Exception {
        HttpRequestTemplate.Builder builder = HttpRequestTemplate.builder("www.example.org", 1234);
        builder.path("/foo/bar/org");
        builder.putParam("param", new TextTemplate("test"));
        builder.scheme(Scheme.HTTPS);
        assertThatManualBuilderEqualsParsingFromUrl("https://www.example.org:1234/foo/bar/org?param=test", builder);

        // ssl support, getting the default port right
        builder = HttpRequestTemplate.builder("www.example.org", 443).scheme(Scheme.HTTPS).path("/test");
        assertThatManualBuilderEqualsParsingFromUrl("https://www.example.org/test", builder);

        // test without specifying port
        builder = HttpRequestTemplate.builder("www.example.org", 80);
        assertThatManualBuilderEqualsParsingFromUrl("http://www.example.org", builder);

        // encoded values
        builder = HttpRequestTemplate.builder("www.example.org", 80).putParam("foo", new TextTemplate(" white space"));
        assertThatManualBuilderEqualsParsingFromUrl("http://www.example.org?foo=%20white%20space", builder);
    }

    public void testParsingEmptyUrl() throws Exception {
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> HttpRequestTemplate.builder().fromUrl(""));
        assertThat(e.getMessage(), containsString("Configured URL is empty, please configure a valid URL"));
    }

    public void testInvalidUrlsWithMissingScheme() throws Exception {
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> HttpRequestTemplate.builder().fromUrl("www.test.de")
        );
        assertThat(e.getMessage(), containsString("URL [www.test.de] does not contain a scheme"));
    }

    public void testInvalidUrlsWithHost() throws Exception {
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> HttpRequestTemplate.builder().fromUrl("https://")
        );
        assertThat(e.getMessage(), containsString("Malformed URL [https://]"));
    }

    public void testThatPartsFromUrlAreTemplatable() throws Exception {
        HttpRequestTemplate template = HttpRequestTemplate.builder().fromUrl("http://www.test.de/%7B%7Bfoo%7D%7D").build();
        HttpRequest request = template.render(new MockTextTemplateEngine(), emptyMap());
        assertThat(request.path(), is("/{{foo}}"));
    }

    private void assertThatManualBuilderEqualsParsingFromUrl(String url, HttpRequestTemplate.Builder builder) throws Exception {
        XContentBuilder urlContentBuilder = jsonBuilder().startObject().field("url", url).endObject();
        XContentParser urlContentParser = createParser(urlContentBuilder);
        urlContentParser.nextToken();

        HttpRequestTemplate urlParsedTemplate = HttpRequestTemplate.Parser.parse(urlContentParser);

        XContentBuilder xContentBuilder = builder.build().toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS);
        XContentParser xContentParser = createParser(xContentBuilder);
        xContentParser.nextToken();
        HttpRequestTemplate parsedTemplate = HttpRequestTemplate.Parser.parse(xContentParser);

        assertThat(parsedTemplate, is(urlParsedTemplate));
    }

}
