/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;

public final class Utils {

    public static String getUrl(MockWebServer webServer) {
        return format("http://%s:%s", webServer.getHostName(), webServer.getPort());
    }

    public static Map<String, Object> entityAsMap(String body) throws IOException {
        InputStream bodyStream = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));

        return entityAsMap(bodyStream);
    }

    public static Map<String, Object> entityAsMap(InputStream body) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(
                    XContentParserConfiguration.EMPTY.withRegistry(NamedXContentRegistry.EMPTY)
                        .withDeprecationHandler(DeprecationHandler.THROW_UNSUPPORTED_OPERATION),
                    body
                )
        ) {
            return parser.map();
        }
    }

    private Utils() {}
}
