/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Superclass for {@link ObjectParser} and {@link ConstructingObjectParser}. Defines most of the "declare" methods so they can be shared.
 */
public abstract class AbstractObjectParser<Value, Context extends ParseFieldMatcherSupplier>
        implements BiFunction<XContentParser, Context, Value> {
    /**
     * Reads an object from a parser using some context.
     */
    @FunctionalInterface
    public interface ContextParser<Context, T> {
        T parse(XContentParser p, Context c) throws IOException;
    }

    /**
     * Reads an object right from the parser without any context.
     */
    @FunctionalInterface
    public interface NoContextParser<T> {
        T parse(XContentParser p) throws IOException;
    }

    /**
     * Declare some field. Usually it is easier to use {@link #declareString(BiConsumer, ParseField)} or
     * {@link #declareObject(BiConsumer, BiFunction, ParseField)} rather than call this directly.
     */
    public abstract <T> void declareField(BiConsumer<Value, T> consumer, ContextParser<Context, T> parser, ParseField parseField,
            ValueType type);

    public <T> void declareField(BiConsumer<Value, T> consumer, NoContextParser<T> parser, ParseField parseField, ValueType type) {
        declareField(consumer, (p, c) -> parser.parse(p), parseField, type);
    }

    public <T> void declareObject(BiConsumer<Value, T> consumer, BiFunction<XContentParser, Context, T> objectParser, ParseField field) {
        declareField(consumer, (p, c) -> objectParser.apply(p, c), field, ValueType.OBJECT);
    }

    public void declareFloat(BiConsumer<Value, Float> consumer, ParseField field) {
        // Using a method reference here angers some compilers
        declareField(consumer, p -> p.floatValue(), field, ValueType.FLOAT);
    }

    public void declareDouble(BiConsumer<Value, Double> consumer, ParseField field) {
        // Using a method reference here angers some compilers
        declareField(consumer, p -> p.doubleValue(), field, ValueType.DOUBLE);
    }

    public void declareLong(BiConsumer<Value, Long> consumer, ParseField field) {
        // Using a method reference here angers some compilers
        declareField(consumer, p -> p.longValue(), field, ValueType.LONG);
    }

    public void declareInt(BiConsumer<Value, Integer> consumer, ParseField field) {
     // Using a method reference here angers some compilers
        declareField(consumer, p -> p.intValue(), field, ValueType.INT);
    }

    public void declareString(BiConsumer<Value, String> consumer, ParseField field) {
        declareField(consumer, XContentParser::text, field, ValueType.STRING);
    }

    public void declareStringOrNull(BiConsumer<Value, String> consumer, ParseField field) {
        declareField(consumer, (p) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.text(), field,
                ValueType.STRING_OR_NULL);
    }

    public void declareBoolean(BiConsumer<Value, Boolean> consumer, ParseField field) {
        declareField(consumer, XContentParser::booleanValue, field, ValueType.BOOLEAN);
    }

    public <T> void declareObjectArray(BiConsumer<Value, List<T>> consumer, BiFunction<XContentParser, Context, T> objectParser,
            ParseField field) {
        declareField(consumer, (p, c) -> parseArray(p, () -> objectParser.apply(p, c)), field, ValueType.OBJECT_ARRAY);
    }

    public void declareStringArray(BiConsumer<Value, List<String>> consumer, ParseField field) {
        declareField(consumer, (p, c) -> parseArray(p, p::text), field, ValueType.STRING_ARRAY);
    }

    public void declareDoubleArray(BiConsumer<Value, List<Double>> consumer, ParseField field) {
        declareField(consumer, (p, c) -> parseArray(p, p::doubleValue), field, ValueType.DOUBLE_ARRAY);
    }

    public void declareFloatArray(BiConsumer<Value, List<Float>> consumer, ParseField field) {
        declareField(consumer, (p, c) -> parseArray(p, p::floatValue), field, ValueType.FLOAT_ARRAY);
    }

    public void declareLongArray(BiConsumer<Value, List<Long>> consumer, ParseField field) {
        declareField(consumer, (p, c) -> parseArray(p, p::longValue), field, ValueType.LONG_ARRAY);
    }

    public void declareIntArray(BiConsumer<Value, List<Integer>> consumer, ParseField field) {
        declareField(consumer, (p, c) -> parseArray(p, p::intValue), field, ValueType.INT_ARRAY);
    }

    public void declareRawObject(BiConsumer<Value, BytesReference> consumer, ParseField field) {
        NoContextParser<BytesReference> bytesParser = p -> {
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                builder.prettyPrint();
                builder.copyCurrentStructure(p);
                return builder.bytes();
            }
        };
        declareField(consumer, bytesParser, field, ValueType.OBJECT);
    }

    private interface IOSupplier<T> {
        T get() throws IOException;
    }
    private static <T> List<T> parseArray(XContentParser parser, IOSupplier<T> supplier) throws IOException {
        List<T> list = new ArrayList<>();
        if (parser.currentToken().isValue()) {
            list.add(supplier.get()); // single value
        } else {
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (parser.currentToken().isValue() || parser.currentToken() == XContentParser.Token.START_OBJECT) {
                    list.add(supplier.get());
                } else {
                    throw new IllegalStateException("expected value but got [" + parser.currentToken() + "]");
                }
            }
        }
        return list;
    }
}
