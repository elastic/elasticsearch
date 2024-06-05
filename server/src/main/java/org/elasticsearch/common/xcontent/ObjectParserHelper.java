/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * This class provides helpers for {@link ObjectParser} that allow dealing with
 * classes outside of the xcontent dependencies.
 */
public final class ObjectParserHelper<Value, Context> {

    /**
     * Helper to declare an object that will be parsed into a {@link BytesReference}
     */
    public static <Value, Context> void declareRawObject(
        final AbstractObjectParser<Value, Context> parser,
        final BiConsumer<Value, BytesReference> consumer,
        final ParseField field
    ) {
        final CheckedFunction<XContentParser, BytesReference, IOException> bytesParser = getBytesParser();
        parser.declareField(consumer, bytesParser, field, ValueType.OBJECT);
    }

    public static <Value, Context> void declareRawObjectOrNull(
        final AbstractObjectParser<Value, Context> parser,
        final BiConsumer<Value, BytesReference> consumer,
        final ParseField field
    ) {
        final CheckedFunction<XContentParser, BytesReference, IOException> bytesParser = getBytesParser();
        parser.declareField(consumer, bytesParser, field, ValueType.OBJECT_OR_NULL);
    }

    private static CheckedFunction<XContentParser, BytesReference, IOException> getBytesParser() {
        return p -> {
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                builder.copyCurrentStructure(p);
                return BytesReference.bytes(builder);
            }
        };
    }

}
