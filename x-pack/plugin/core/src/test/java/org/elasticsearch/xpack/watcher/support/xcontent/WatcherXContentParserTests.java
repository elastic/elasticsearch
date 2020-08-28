/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support.xcontent;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherXContentParser;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.is;

public class WatcherXContentParserTests extends ESTestCase {

    public void testThatRedactedSecretsThrowException() throws Exception {
        String fieldName = randomAlphaOfLength(10);
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject().field(fieldName, "::es_redacted::").endObject();

            try (XContentParser xContentParser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, Strings.toString(builder))) {
                xContentParser.nextToken();
                xContentParser.nextToken();
                assertThat(xContentParser.currentName(), is(fieldName));
                xContentParser.nextToken();
                assertThat(xContentParser.currentToken(), is(XContentParser.Token.VALUE_STRING));
                ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

                WatcherXContentParser parser = new WatcherXContentParser(xContentParser, now, null, false);
                ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
                        () -> WatcherXContentParser.secretOrNull(parser));
                assertThat(e.getMessage(), is("found redacted password in field [" + fieldName + "]"));
            }
        }
    }
}
