/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.xcontent.ObjectParser.NamedObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Superclass for {@link ObjectParser} and {@link ConstructingObjectParser}. Defines most of the "declare" methods so they can be shared.
 */
public abstract class AbstractObjectParser<Value, Context> {

    protected AbstractObjectParser() {}

    /**
     * Declare some field. Usually it is easier to use {@link #declareString(BiConsumer, ParseField)} or
     * {@link #declareObject(BiConsumer, ContextParser, ParseField)} rather than call this directly.
     */
    public abstract <T> void declareField(
        BiConsumer<Value, T> consumer,
        ContextParser<Context, T> parser,
        ParseField parseField,
        ValueType type
    );

    /**
     * Declares a single named object.
     *
     * <pre>
     * <code>
     * {
     *   "object_name": {
     *     "instance_name": { "field1": "value1", ... }
     *     }
     *   }
     * }
     * </code>
     * </pre>
     *
     * @param consumer
     *            sets the value once it has been parsed
     * @param namedObjectParser
     *            parses the named object
     * @param parseField
     *            the field to parse
     */
    public abstract <T> void declareNamedObject(
        BiConsumer<Value, T> consumer,
        NamedObjectParser<T, Context> namedObjectParser,
        ParseField parseField
    );

    /**
     * Declares named objects in the style of aggregations. These are named
     * inside and object like this:
     *
     * <pre>
     * <code>
     * {
     *   "aggregations": {
     *     "name_1": { "aggregation_type": {} },
     *     "name_2": { "aggregation_type": {} },
     *     "name_3": { "aggregation_type": {} }
     *     }
     *   }
     * }
     * </code>
     * </pre>
     *
     * Unlike the other version of this method, "ordered" mode (arrays of
     * objects) is not supported.
     *
     * See NamedObjectHolder in ObjectParserTests for examples of how to invoke
     * this.
     *
     * @param consumer
     *            sets the values once they have been parsed
     * @param namedObjectParser
     *            parses each named object
     * @param parseField
     *            the field to parse
     */
    public abstract <T> void declareNamedObjects(
        BiConsumer<Value, List<T>> consumer,
        NamedObjectParser<T, Context> namedObjectParser,
        ParseField parseField
    );

    /**
     * Declares named objects in the style of highlighting's field element.
     * These are usually named inside and object like this:
     *
     * <pre>
     * <code>
     * {
     *   "highlight": {
     *     "fields": {        &lt;------ this one
     *       "title": {},
     *       "body": {},
     *       "category": {}
     *     }
     *   }
     * }
     * </code>
     * </pre>
     *
     * but, when order is important, some may be written this way:
     *
     * <pre>
     * <code>
     * {
     *   "highlight": {
     *     "fields": [        &lt;------ this one
     *       {"title": {}},
     *       {"body": {}},
     *       {"category": {}}
     *     ]
     *   }
     * }
     * </code>
     * </pre>
     *
     * This is because json doesn't enforce ordering. Elasticsearch reads it in
     * the order sent but tools that generate json are free to put object
     * members in an unordered Map, jumbling them. Thus, if you care about order
     * you can send the object in the second way.
     *
     * See NamedObjectHolder in ObjectParserTests for examples of how to invoke
     * this.
     *
     * @param consumer
     *            sets the values once they have been parsed
     * @param namedObjectParser
     *            parses each named object
     * @param orderedModeCallback
     *            called when the named object is parsed using the "ordered"
     *            mode (the array of objects)
     * @param parseField
     *            the field to parse
     */
    public abstract <T> void declareNamedObjects(
        BiConsumer<Value, List<T>> consumer,
        NamedObjectParser<T, Context> namedObjectParser,
        Consumer<Value> orderedModeCallback,
        ParseField parseField
    );

    public abstract String getName();

    public <T> void declareField(
        BiConsumer<Value, T> consumer,
        CheckedFunction<XContentParser, T, IOException> parser,
        ParseField parseField,
        ValueType type
    ) {
        if (parser == null) {
            throw new IllegalArgumentException("[parser] is required");
        }
        declareField(consumer, (p, c) -> parser.apply(p), parseField, type);
    }

    public <T> void declareObject(BiConsumer<Value, T> consumer, ContextParser<Context, T> objectParser, ParseField field) {
        declareField(consumer, objectParser, field, ValueType.OBJECT);
    }

    /**
     * Declare an object field that parses explicit {@code null}s in the json to a default value.
     */
    public <T> void declareObjectOrNull(
        BiConsumer<Value, T> consumer,
        ContextParser<Context, T> objectParser,
        T nullValue,
        ParseField field
    ) {
        declareField(
            consumer,
            (p, c) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? nullValue : objectParser.parse(p, c),
            field,
            ValueType.OBJECT_OR_NULL
        );
    }

    public void declareFloat(BiConsumer<Value, Float> consumer, ParseField field) {
        // Using a method reference here angers some compilers
        declareField(consumer, p -> p.floatValue(), field, ValueType.FLOAT);
    }

    /**
     * Declare a float field that parses explicit {@code null}s in the json to a default value.
     */
    public void declareFloatOrNull(BiConsumer<Value, Float> consumer, float nullValue, ParseField field) {
        declareField(
            consumer,
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? nullValue : p.floatValue(),
            field,
            ValueType.FLOAT_OR_NULL
        );
    }

    public void declareDouble(BiConsumer<Value, Double> consumer, ParseField field) {
        // Using a method reference here angers some compilers
        declareField(consumer, p -> p.doubleValue(), field, ValueType.DOUBLE);
    }

    /**
     * Declare a double field that parses explicit {@code null}s in the json to a default value.
     */
    public void declareDoubleOrNull(BiConsumer<Value, Double> consumer, double nullValue, ParseField field) {
        declareField(
            consumer,
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? nullValue : p.doubleValue(),
            field,
            ValueType.DOUBLE_OR_NULL
        );
    }

    public void declareLong(BiConsumer<Value, Long> consumer, ParseField field) {
        // Using a method reference here angers some compilers
        declareField(consumer, p -> p.longValue(), field, ValueType.LONG);
    }

    public void declareLongOrNull(BiConsumer<Value, Long> consumer, long nullValue, ParseField field) {
        // Using a method reference here angers some compilers
        declareField(
            consumer,
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? nullValue : p.longValue(),
            field,
            ValueType.LONG_OR_NULL
        );
    }

    public void declareInt(BiConsumer<Value, Integer> consumer, ParseField field) {
        // Using a method reference here angers some compilers
        declareField(consumer, p -> p.intValue(), field, ValueType.INT);
    }

    /**
     * Declare an integer field that parses explicit {@code null}s in the json to a default value.
     */
    public void declareIntOrNull(BiConsumer<Value, Integer> consumer, int nullValue, ParseField field) {
        declareField(
            consumer,
            p -> p.currentToken() == XContentParser.Token.VALUE_NULL ? nullValue : p.intValue(),
            field,
            ValueType.INT_OR_NULL
        );
    }

    public void declareString(BiConsumer<Value, String> consumer, ParseField field) {
        declareField(consumer, XContentParser::text, field, ValueType.STRING);
    }

    /**
     * Declare a field of type {@code T} parsed from string and converted to {@code T} using provided function.
     * Throws if the next token is not a string.
     */
    public <T> void declareString(BiConsumer<Value, T> consumer, Function<String, T> fromStringFunction, ParseField field) {
        declareField(consumer, p -> fromStringFunction.apply(p.text()), field, ValueType.STRING);
    }

    public void declareStringOrNull(BiConsumer<Value, String> consumer, ParseField field) {
        declareField(
            consumer,
            (p) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.text(),
            field,
            ValueType.STRING_OR_NULL
        );
    }

    public void declareBoolean(BiConsumer<Value, Boolean> consumer, ParseField field) {
        declareField(consumer, XContentParser::booleanValue, field, ValueType.BOOLEAN);
    }

    public <T> void declareObjectArray(BiConsumer<Value, List<T>> consumer, ContextParser<Context, T> objectParser, ParseField field) {
        declareFieldArray(consumer, objectParser, field, ValueType.OBJECT_ARRAY);
    }

    /**
     * like {@link #declareObjectArray(BiConsumer, ContextParser, ParseField)}, but can also handle single null values,
     * in which case the consumer isn't called
     */
    public <T> void declareObjectArrayOrNull(
        BiConsumer<Value, List<T>> consumer,
        ContextParser<Context, T> objectParser,
        ParseField field
    ) {
        declareField(
            (value, list) -> { if (list != null) consumer.accept(value, list); },
            (p, c) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : parseArray(p, c, objectParser),
            field,
            ValueType.OBJECT_ARRAY_OR_NULL
        );
    }

    public void declareStringArray(BiConsumer<Value, List<String>> consumer, ParseField field) {
        declareFieldArray(consumer, (p, c) -> p.text(), field, ValueType.STRING_ARRAY);
    }

    public void declareDoubleArray(BiConsumer<Value, List<Double>> consumer, ParseField field) {
        declareFieldArray(consumer, (p, c) -> p.doubleValue(), field, ValueType.DOUBLE_ARRAY);
    }

    public void declareFloatArray(BiConsumer<Value, List<Float>> consumer, ParseField field) {
        declareFieldArray(consumer, (p, c) -> p.floatValue(), field, ValueType.FLOAT_ARRAY);
    }

    public void declareLongArray(BiConsumer<Value, List<Long>> consumer, ParseField field) {
        declareFieldArray(consumer, (p, c) -> p.longValue(), field, ValueType.LONG_ARRAY);
    }

    public void declareIntArray(BiConsumer<Value, List<Integer>> consumer, ParseField field) {
        declareFieldArray(consumer, (p, c) -> p.intValue(), field, ValueType.INT_ARRAY);
    }

    /**
     * Declares a field that can contain an array of elements listed in the type ValueType enum
     */
    public <T> void declareFieldArray(
        BiConsumer<Value, List<T>> consumer,
        ContextParser<Context, T> itemParser,
        ParseField field,
        ValueType type
    ) {
        declareField(consumer, (p, c) -> parseArray(p, c, itemParser), field, type);
    }

    /**
     * Declares a set of fields that are required for parsing to succeed. Only one of the values
     * provided per String[] must be matched.
     *
     * E.g. <code>declareRequiredFieldSet("foo", "bar");</code> means at least one of "foo" or
     * "bar" fields must be present.  If neither of those fields are present, an exception will be thrown.
     *
     * Multiple required sets can be configured:
     *
     * <pre><code>
     *   parser.declareRequiredFieldSet("foo", "bar");
     *   parser.declareRequiredFieldSet("bizz", "buzz");
     * </code></pre>
     *
     * requires that one of "foo" or "bar" fields are present, and also that one of "bizz" or
     * "buzz" fields are present.
     *
     * In JSON, it means any of these combinations are acceptable:
     *
     * <ul>
     *   <li><code>{"foo":"...", "bizz": "..."}</code></li>
     *   <li><code>{"bar":"...", "bizz": "..."}</code></li>
     *   <li><code>{"foo":"...", "buzz": "..."}</code></li>
     *   <li><code>{"bar":"...", "buzz": "..."}</code></li>
     *   <li><code>{"foo":"...", "bar":"...", "bizz": "..."}</code></li>
     *   <li><code>{"foo":"...", "bar":"...", "buzz": "..."}</code></li>
     *   <li><code>{"foo":"...", "bizz":"...", "buzz": "..."}</code></li>
     *   <li><code>{"bar":"...", "bizz":"...", "buzz": "..."}</code></li>
     *   <li><code>{"foo":"...", "bar":"...", "bizz": "...", "buzz": "..."}</code></li>
     * </ul>
     *
     * The following would however be rejected:
     *
     * <table>
     *   <caption>failure cases</caption>
     *   <tr><th>Provided JSON</th><th>Reason for failure</th></tr>
     *   <tr><td><code>{"foo":"..."}</code></td><td>Missing "bizz" or "buzz" field</td></tr>
     *   <tr><td><code>{"bar":"..."}</code></td><td>Missing "bizz" or "buzz" field</td></tr>
     *   <tr><td><code>{"bizz": "..."}</code></td><td>Missing "foo" or "bar" field</td></tr>
     *   <tr><td><code>{"buzz": "..."}</code></td><td>Missing "foo" or "bar" field</td></tr>
     *   <tr><td><code>{"foo":"...", "bar": "..."}</code></td><td>Missing "bizz" or "buzz" field</td></tr>
     *   <tr><td><code>{"bizz":"...", "buzz": "..."}</code></td><td>Missing "foo" or "bar" field</td></tr>
     *   <tr><td><code>{"unrelated":"..."}</code></td>  <td>Missing "foo" or "bar" field, and missing "bizz" or "buzz" field</td></tr>
     * </table>
     *
     * @param requiredSet
     *          A set of required fields, where at least one of the fields in the array _must_ be present
     */
    public abstract void declareRequiredFieldSet(String... requiredSet);

    /**
     * Declares a set of fields of which at most one must appear for parsing to succeed
     *
     * E.g. <code>declareExclusiveFieldSet("foo", "bar");</code> means that only one of 'foo'
     * or 'bar' must be present, and if both appear then an exception will be thrown.  Note
     * that this does not make 'foo' or 'bar' required - see {@link #declareRequiredFieldSet(String...)}
     * for required fields.
     *
     * Multiple exclusive sets may be declared
     *
     * @param exclusiveSet a set of field names, at most one of which must appear
     */
    public abstract void declareExclusiveFieldSet(String... exclusiveSet);

    public static <T, Context> List<T> parseArray(XContentParser parser, Context context, ContextParser<Context, T> itemParser)
        throws IOException {
        final XContentParser.Token currentToken = parser.currentToken();
        if (currentToken.isValue()
            || currentToken == XContentParser.Token.VALUE_NULL
            || currentToken == XContentParser.Token.START_OBJECT) {
            return Collections.singletonList(itemParser.parse(parser, context)); // single value
        }
        final List<T> list = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token.isValue() || token == XContentParser.Token.VALUE_NULL || token == XContentParser.Token.START_OBJECT) {
                list.add(itemParser.parse(parser, context));
            } else {
                throw new IllegalStateException("expected value but got [" + token + "]");
            }
        }
        return list;
    }
}
