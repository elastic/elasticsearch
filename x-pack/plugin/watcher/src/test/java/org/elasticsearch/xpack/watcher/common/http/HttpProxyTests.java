/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.common.http;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class HttpProxyTests extends ESTestCase {

    public void testParser() throws Exception {
        int port = randomIntBetween(1, 65000);
        String host = randomAlphaOfLength(10);
        XContentBuilder builder = jsonBuilder().startObject().field("host", host).field("port", port);
        boolean isSchemeConfigured = randomBoolean();
        String scheme = null;
        if (isSchemeConfigured) {
            scheme = randomFrom(Scheme.values()).scheme();
            builder.field("scheme", scheme);
        }
        builder.endObject();
        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(builder).streamInput())
        ) {
            parser.nextToken();
            HttpProxy proxy = HttpProxy.parse(parser);
            assertThat(proxy.getHost(), is(host));
            assertThat(proxy.getPort(), is(port));
            if (isSchemeConfigured) {
                assertThat(proxy.getScheme().scheme(), is(scheme));
            } else {
                assertThat(proxy.getScheme(), is(nullValue()));
            }
        }
    }

    public void testParserValidScheme() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
            .field("host", "localhost")
            .field("port", 12345)
            .field("scheme", "invalid")
            .endObject();
        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(builder).streamInput())
        ) {
            parser.nextToken();
            expectThrows(IllegalArgumentException.class, () -> HttpProxy.parse(parser));
        }
    }

    public void testParserValidPortRange() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("host", "localhost").field("port", -1).endObject();
        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(builder).streamInput())
        ) {
            parser.nextToken();
            expectThrows(ElasticsearchParseException.class, () -> HttpProxy.parse(parser));
        }
    }

    public void testParserNoHost() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("port", -1).endObject();
        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(builder).streamInput())
        ) {
            parser.nextToken();
            expectThrows(ElasticsearchParseException.class, () -> HttpProxy.parse(parser));
        }
    }

    public void testParserNoPort() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("host", "localhost").endObject();
        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(builder).streamInput())
        ) {
            parser.nextToken();
            expectThrows(ElasticsearchParseException.class, () -> HttpProxy.parse(parser));
        }
    }

    public void testToXContent() throws Exception {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            HttpProxy proxy = new HttpProxy("localhost", 3128);
            proxy.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            assertThat(Strings.toString(builder), is("{\"proxy\":{\"host\":\"localhost\",\"port\":3128}}"));
        }

        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            HttpProxy httpsProxy = new HttpProxy("localhost", 3128, Scheme.HTTPS);
            httpsProxy.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            assertThat(Strings.toString(builder), is("{\"proxy\":{\"host\":\"localhost\",\"port\":3128,\"scheme\":\"https\"}}"));
        }
    }
}
