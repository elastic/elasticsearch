/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CACHE_CONTROL_TTL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CACHE_CONTROL_TYPE_FIELD;
import static org.hamcrest.Matchers.is;

public class CacheControlTests extends AbstractBWCSerializationTestCase<CacheControl> {

    public void testParsingCacheControl_AllFields() throws IOException {
        String json = """
            {
                "type": "ephemeral",
                "ttl": "1h"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            var cacheControl = CacheControl.PARSER.apply(parser, null);
            assertThat(cacheControl, is(new CacheControl("ephemeral", TimeValue.timeValueHours(1))));
        }
    }

    public void testParsingCacheControl_TtlNull() throws IOException {
        String json = """
            {
                "type": "ephemeral",
                "ttl": null
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            var cacheControl = CacheControl.PARSER.apply(parser, null);
            assertThat(cacheControl, is(new CacheControl("ephemeral", null)));
        }
    }

    public void testParsingCacheControl_OnlyType() throws IOException {
        String json = """
            {
                "type": "ephemeral"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            var cacheControl = CacheControl.PARSER.apply(parser, null);
            assertThat(cacheControl, is(new CacheControl("ephemeral", null)));
        }
    }

    public void testParsingCacheControl_OnlyTtl() throws IOException {
        String json = """
            {
                "ttl": "30m"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            var cacheControl = CacheControl.PARSER.apply(parser, null);
            assertThat(cacheControl, is(new CacheControl(null, TimeValue.timeValueMinutes(30))));
        }
    }

    public void testParsingCacheControl_NoFields() throws IOException {
        try (var parser = createParser(JsonXContent.jsonXContent, "{}")) {
            var cacheControl = CacheControl.PARSER.apply(parser, null);
            assertThat(cacheControl, is(new CacheControl(null, null)));
        }
    }

    public void testParsingCacheControl_InvalidTtl_ThrowsException() throws IOException {
        String json = """
            {
                "ttl": "invalid"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            assertThrows(XContentParseException.class, () -> CacheControl.PARSER.apply(parser, null));
        }
    }

    public void testParsingCacheControl_UnknownField_ThrowsException() throws IOException {
        String json = """
            {
                "type": "ephemeral",
                "unknown_field": "some value"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            var exception = assertThrows(XContentParseException.class, () -> CacheControl.PARSER.apply(parser, null));
            assertThat(exception.getMessage(), is("[3:5] [CacheControl] unknown field [unknown_field]"));
        }
    }

    public void testParsingCacheControl_UnknownField_IgnoredByLenientParser() throws IOException {
        String json = """
            {
                "type": "ephemeral",
                "unknown_field": "some value"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, json)) {
            var cacheControl = CacheControl.LENIENT_PARSER.apply(parser, null);
            assertThat(cacheControl, is(new CacheControl("ephemeral", null)));
        }
    }

    @Override
    protected CacheControl mutateInstanceForVersion(CacheControl instance, TransportVersion version) {
        // No version-specific mutations
        return instance;
    }

    @Override
    protected CacheControl doParseInstance(XContentParser parser) throws IOException {
        return CacheControl.PARSER.apply(parser, null);
    }

    @Override
    protected Writeable.Reader<CacheControl> instanceReader() {
        return CacheControl::new;
    }

    @Override
    protected CacheControl createTestInstance() {
        return randomCacheControl();
    }

    @Override
    protected CacheControl mutateInstance(CacheControl instance) throws IOException {
        var type = instance.type();
        var ttl = instance.ttl();

        switch (randomFrom(Set.of(CACHE_CONTROL_TYPE_FIELD, CACHE_CONTROL_TTL_FIELD))) {
            case CACHE_CONTROL_TYPE_FIELD -> type = randomValueOtherThan(
                type,
                () -> randomBoolean() ? randomAlphanumericOfLength(8) : null
            );
            case CACHE_CONTROL_TTL_FIELD -> ttl = randomValueOtherThan(
                ttl,
                () -> randomBoolean() ? ESTestCase.randomPositiveTimeValue() : null
            );
            default -> throw new AssertionError("Illegal mutation branch");
        }

        return new CacheControl(type, ttl);
    }

    public static CacheControl randomCacheControl() {
        var type = randomBoolean() ? randomAlphanumericOfLength(8) : null;
        var ttl = randomBoolean() ? randomPositiveTimeValue() : null;

        return new CacheControl(type, ttl);
    }
}
