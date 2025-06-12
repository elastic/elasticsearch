/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.xcontent;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RestApiVersion;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.xcontent.XContentParser.Token.START_ARRAY;
import static org.elasticsearch.xcontent.XContentParser.Token.START_OBJECT;
import static org.elasticsearch.xcontent.XContentParser.Token.VALUE_BOOLEAN;
import static org.elasticsearch.xcontent.XContentParser.Token.VALUE_EMBEDDED_OBJECT;
import static org.elasticsearch.xcontent.XContentParser.Token.VALUE_NULL;
import static org.elasticsearch.xcontent.XContentParser.Token.VALUE_NUMBER;
import static org.elasticsearch.xcontent.XContentParser.Token.VALUE_STRING;

/**
 * A declarative, stateless parser that turns XContent into setter calls. A single parser should be defined for each object being parsed,
 * nested elements can be added via {@link #declareObject(BiConsumer, ContextParser, ParseField)} which should be satisfied where possible
 * by passing another instance of {@link ObjectParser}, this one customized for that Object.
 * <p>
 * This class works well for object that do have a constructor argument or that can be built using information available from earlier in the
 * XContent. For objects that have constructors with required arguments that are specified on the same level as other fields see
 * {@link ConstructingObjectParser}.
 * </p>
 * <p>
 * Instances of {@link ObjectParser} should be setup by declaring a constant field for the parsers and declaring all fields in a static
 * block just below the creation of the parser. Like this:
 * </p>
 * <pre>{@code
 *   private static final ObjectParser<Thing, SomeContext> PARSER = new ObjectParser<>("thing", Thing::new));
 *   static {
 *       PARSER.declareInt(Thing::setMineral, new ParseField("mineral"));
 *       PARSER.declareInt(Thing::setFruit, new ParseField("fruit"));
 *   }
 * }</pre>
 * It's highly recommended to use the high level declare methods like {@link #declareString(BiConsumer, ParseField)} instead of
 * {@link #declareField} which can be used to implement exceptional parsing operations not covered by the high level methods.
 */
public final class ObjectParser<Value, Context> extends AbstractObjectParser<Value, Context>
    implements
        BiFunction<XContentParser, Context, Value>,
        ContextParser<Context, Value> {

    private final List<String[]> requiredFieldSets = new ArrayList<>();
    private final List<String[]> exclusiveFieldSets = new ArrayList<>();

    /**
     * Adapts an array (or varags) setter into a list setter.
     */
    public static <Value, ElementValue> BiConsumer<Value, List<ElementValue>> fromList(
        Class<ElementValue> c,
        BiConsumer<Value, ElementValue[]> consumer
    ) {
        return (Value v, List<ElementValue> l) -> {
            @SuppressWarnings("unchecked")
            ElementValue[] array = (ElementValue[]) Array.newInstance(c, l.size());
            consumer.accept(v, l.toArray(array));
        };
    }

    private interface UnknownFieldParser<Value, Context> {
        void acceptUnknownField(
            ObjectParser<Value, Context> objectParser,
            String field,
            XContentLocation location,
            XContentParser parser,
            Value value,
            Context context
        ) throws IOException;
    }

    private static <Value, Context> UnknownFieldParser<Value, Context> ignoreUnknown() {
        return (op, f, l, p, v, c) -> p.skipChildren();
    }

    private static <Value, Context> UnknownFieldParser<Value, Context> errorOnUnknown() {
        return (op, f, l, p, v, c) -> {
            throw new XContentParseException(
                l,
                ErrorOnUnknown.IMPLEMENTATION.errorMessage(
                    op.name,
                    f,
                    op.fieldParserMap.getOrDefault(p.getRestApiVersion(), Collections.emptyMap()).keySet()
                )
            );
        };
    }

    /**
     * Defines how to consume a parsed undefined field
     */
    public interface UnknownFieldConsumer<Value> {
        void accept(Value target, String field, Object value);
    }

    private static <Value, Context> UnknownFieldParser<Value, Context> consumeUnknownField(UnknownFieldConsumer<Value> consumer) {
        return (objectParser, field, location, parser, value, context) -> {
            XContentParser.Token t = parser.currentToken();
            switch (t) {
                case VALUE_STRING -> consumer.accept(value, field, parser.text());
                case VALUE_NUMBER -> consumer.accept(value, field, parser.numberValue());
                case VALUE_BOOLEAN -> consumer.accept(value, field, parser.booleanValue());
                case VALUE_NULL -> consumer.accept(value, field, null);
                case START_OBJECT -> consumer.accept(value, field, parser.map());
                case START_ARRAY -> consumer.accept(value, field, parser.list());
                default -> throw new XContentParseException(
                    parser.getTokenLocation(),
                    "[" + objectParser.name + "] cannot parse field [" + field + "] with value type [" + t + "]"
                );
            }
        };
    }

    private static <Value, Category, Context> UnknownFieldParser<Value, Context> unknownIsNamedXContent(
        Class<Category> categoryClass,
        BiConsumer<Value, ? super Category> consumer
    ) {
        return (objectParser, field, location, parser, value, context) -> {
            Category o;
            try {
                o = parser.namedObject(categoryClass, field, context);
            } catch (NamedObjectNotFoundException e) {
                Set<String> candidates = new HashSet<>(
                    objectParser.fieldParserMap.getOrDefault(parser.getRestApiVersion(), Collections.emptyMap()).keySet()
                );
                e.getCandidates().forEach(candidates::add);
                String message = ErrorOnUnknown.IMPLEMENTATION.errorMessage(objectParser.name, field, candidates);
                throw new XContentParseException(location, message, e);
            }
            consumer.accept(value, o);
        };
    }

    private final Map<RestApiVersion, Map<String, FieldParser>> fieldParserMap = new EnumMap<>(RestApiVersion.class);
    private final String name;
    private final Function<Context, Value> valueBuilder;
    private final UnknownFieldParser<Value, Context> unknownFieldParser;

    /**
     * Creates a new ObjectParser.
     * @param name the parsers name, used to reference the parser in exceptions and messages.
     */
    public ObjectParser(String name) {
        this(name, errorOnUnknown(), null);
    }

    /**
     * Creates a new ObjectParser.
     * @param name the parsers name, used to reference the parser in exceptions and messages.
     * @param valueSupplier A supplier that creates a new Value instance. Used when the parser is used as an inner object parser.
     */
    public ObjectParser(String name, @Nullable Supplier<Value> valueSupplier) {
        this(name, errorOnUnknown(), wrapValueSupplier(valueSupplier));
    }

    /**
     * Creates a new ObjectParser.
     * @param name the parsers name, used to reference the parser in exceptions and messages.
     * @param valueBuilder A function that creates a new Value from the parse Context. Used
     *                     when the parser is used as an inner object parser.
     */
    public static <Value, Context> ObjectParser<Value, Context> fromBuilder(String name, Function<Context, Value> valueBuilder) {
        requireNonNull(valueBuilder, "Use the single argument ctor instead");
        return new ObjectParser<Value, Context>(name, errorOnUnknown(), valueBuilder);
    }

    /**
     * Creates a new ObjectParser.
     * @param name the parsers name, used to reference the parser in exceptions and messages.
     * @param ignoreUnknownFields Should this parser ignore unknown fields? This should generally be set to true only when parsing
     *      responses from external systems, never when parsing requests from users.
     * @param valueSupplier a supplier that creates a new Value instance used when the parser is used as an inner object parser.
     */
    public ObjectParser(String name, boolean ignoreUnknownFields, @Nullable Supplier<Value> valueSupplier) {
        this(name, ignoreUnknownFields ? ignoreUnknown() : errorOnUnknown(), wrapValueSupplier(valueSupplier));
    }

    private static <C, V> Function<C, V> wrapValueSupplier(@Nullable Supplier<V> valueSupplier) {
        return valueSupplier == null ? c -> { throw new NullPointerException(); } : c -> valueSupplier.get();
    }

    /**
     * Creates a new ObjectParser that consumes unknown fields as generic Objects.
     * @param name the parsers name, used to reference the parser in exceptions and messages.
     * @param unknownFieldConsumer how to consume parsed unknown fields
     * @param valueSupplier a supplier that creates a new Value instance used when the parser is used as an inner object parser.
     */
    public ObjectParser(String name, UnknownFieldConsumer<Value> unknownFieldConsumer, @Nullable Supplier<Value> valueSupplier) {
        this(name, consumeUnknownField(unknownFieldConsumer), wrapValueSupplier(valueSupplier));
    }

    /**
     * Creates a new ObjectParser that attempts to resolve unknown fields as {@link XContentParser#namedObject namedObjects}.
     * @param <C> the type of named object that unknown fields are expected to be
     * @param name the parsers name, used to reference the parser in exceptions and messages.
     * @param categoryClass the type of named object that unknown fields are expected to be
     * @param unknownFieldConsumer how to consume parsed unknown fields
     * @param valueSupplier a supplier that creates a new Value instance used when the parser is used as an inner object parser.
     */
    public <C> ObjectParser(
        String name,
        Class<C> categoryClass,
        BiConsumer<Value, C> unknownFieldConsumer,
        @Nullable Supplier<Value> valueSupplier
    ) {
        this(name, unknownIsNamedXContent(categoryClass, unknownFieldConsumer), wrapValueSupplier(valueSupplier));
    }

    /**
     * Creates a new ObjectParser instance with a name.
     * @param name the parsers name, used to reference the parser in exceptions and messages.
     * @param unknownFieldParser how to parse unknown fields
     * @param valueBuilder builds the value from the context. Used when the ObjectParser is not passed a value.
     */
    private ObjectParser(
        String name,
        UnknownFieldParser<Value, Context> unknownFieldParser,
        @Nullable Function<Context, Value> valueBuilder
    ) {
        this.name = name;
        this.unknownFieldParser = unknownFieldParser;
        this.valueBuilder = valueBuilder == null ? c -> { throw new NullPointerException("valueBuilder is not set"); } : valueBuilder;
    }

    /**
     * Parses a Value from the given {@link XContentParser}
     * @param parser the parser to build a value from
     * @param context context needed for parsing
     * @return a new value instance drawn from the provided value supplier on {@link #ObjectParser(String, Supplier)}
     * @throws IOException if an IOException occurs.
     */
    @Override
    public Value parse(XContentParser parser, Context context) throws IOException {
        return parse(parser, valueBuilder.apply(context), context);
    }

    /**
     * Parses a Value from the given {@link XContentParser}
     * @param parser the parser to build a value from
     * @param value the value to fill from the parser
     * @param context a context that is passed along to all declared field parsers
     * @return the parsed value
     * @throws IOException if an IOException occurs.
     */
    public Value parse(XContentParser parser, Value value, Context context) throws IOException {
        XContentParser.Token token;
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throwExpectedStartObject(parser, token);
            }
        }

        final List<String[]> requiredFields = this.requiredFieldSets.isEmpty() ? null : new ArrayList<>(this.requiredFieldSets);
        final List<List<String>> exclusiveFields;
        if (exclusiveFieldSets.isEmpty()) {
            exclusiveFields = null;
        } else {
            exclusiveFields = new ArrayList<>();
            for (int i = 0; i < this.exclusiveFieldSets.size(); i++) {
                exclusiveFields.add(new ArrayList<>());
            }
        }

        FieldParser fieldParser;
        String currentFieldName;
        XContentLocation currentPosition;
        final Map<String, FieldParser> parsers = fieldParserMap.getOrDefault(parser.getRestApiVersion(), Collections.emptyMap());
        while ((currentFieldName = parser.nextFieldName()) != null) {
            currentPosition = parser.getTokenLocation();
            fieldParser = parsers.get(currentFieldName);
            token = parser.nextToken();
            if (fieldParser == null) {
                unknownFieldParser.acceptUnknownField(this, currentFieldName, currentPosition, parser, value, context);
            } else {
                fieldParser.assertSupports(name, parser, token, currentFieldName);

                if (requiredFields != null) {
                    // Check to see if this field is a required field, if it is we can
                    // remove the entry as the requirement is satisfied
                    maybeMarkRequiredField(currentFieldName, requiredFields);
                }

                if (exclusiveFields != null) {
                    // Check if this field is in an exclusive set, if it is then mark
                    // it as seen.
                    maybeMarkExclusiveField(currentFieldName, exclusiveFields);
                }

                parseSub(parser, fieldParser, token, currentFieldName, value, context);
            }
        }

        // Check for a) multiple entries appearing in exclusive field sets and b) empty required field entries
        if (exclusiveFields != null) {
            ensureExclusiveFields(exclusiveFields);
        }
        if (requiredFields != null && requiredFields.isEmpty() == false) {
            throwMissingRequiredFields(requiredFields);
        }
        return value;
    }

    private void throwExpectedStartObject(XContentParser parser, XContentParser.Token token) {
        throw new XContentParseException(parser.getTokenLocation(), "[" + name + "] Expected START_OBJECT but was: " + token);
    }

    private static void throwMissingRequiredFields(List<String[]> requiredFields) {
        final StringBuilder message = new StringBuilder();
        for (int i = 0; i < requiredFields.size(); i++) {
            if (i > 0) {
                message.append(" ");
            }
            message.append("Required one of fields ").append(Arrays.toString(requiredFields.get(i))).append(", but none were specified.");
        }
        throw new IllegalArgumentException(message.toString());
    }

    private static void ensureExclusiveFields(List<List<String>> exclusiveFields) {
        StringBuilder message = null;
        for (List<String> fieldset : exclusiveFields) {
            if (fieldset.size() > 1) {
                if (message == null) {
                    message = new StringBuilder();
                }
                message.append("The following fields are not allowed together: ").append(fieldset).append(" ");
            }
        }
        if (message != null && message.length() > 0) {
            throw new IllegalArgumentException(message.toString());
        }
    }

    private void maybeMarkExclusiveField(String currentFieldName, List<List<String>> exclusiveFields) {
        for (int i = 0; i < this.exclusiveFieldSets.size(); i++) {
            for (String field : this.exclusiveFieldSets.get(i)) {
                if (field.equals(currentFieldName)) {
                    exclusiveFields.get(i).add(currentFieldName);
                }
            }
        }
    }

    private static void maybeMarkRequiredField(String currentFieldName, List<String[]> requiredFields) {
        Iterator<String[]> iter = requiredFields.iterator();
        while (iter.hasNext()) {
            String[] requiredFieldNames = iter.next();
            for (String field : requiredFieldNames) {
                if (field.equals(currentFieldName)) {
                    iter.remove();
                    break;
                }
            }
        }
    }

    @Override
    public Value apply(XContentParser parser, Context context) {
        try {
            return parse(parser, context);
        } catch (IOException e) {
            throw new XContentParseException(parser.getTokenLocation(), "[" + name + "] failed to parse object", e);
        }
    }

    public interface Parser<Value, Context> {
        void parse(XContentParser parser, Value value, Context context) throws IOException;
    }

    public void declareField(Parser<Value, Context> p, ParseField parseField, ValueType type) {
        if (parseField == null) {
            throw new IllegalArgumentException("[parseField] is required");
        }
        if (type == null) {
            throw new IllegalArgumentException("[type] is required");
        }
        FieldParser fieldParser = new FieldParser(p, type.supportedTokens(), parseField, type);
        for (String fieldValue : parseField.getAllNamesIncludedDeprecated()) {

            if (RestApiVersion.minimumSupported().matches(parseField.getForRestApiVersion())) {
                Map<String, FieldParser> nameToParserMap = fieldParserMap.computeIfAbsent(
                    RestApiVersion.minimumSupported(),
                    (v) -> new HashMap<>()
                );
                FieldParser previousValue = nameToParserMap.putIfAbsent(fieldValue, fieldParser);
                if (previousValue != null) {
                    throw new IllegalArgumentException("Parser already registered for name=[" + fieldValue + "]. " + previousValue);
                }
            }
            if (RestApiVersion.current().matches(parseField.getForRestApiVersion())) {
                Map<String, FieldParser> nameToParserMap = fieldParserMap.computeIfAbsent(RestApiVersion.current(), (v) -> new HashMap<>());
                FieldParser previousValue = nameToParserMap.putIfAbsent(fieldValue, fieldParser);
                if (previousValue != null) {
                    throw new IllegalArgumentException("Parser already registered for name=[" + fieldValue + "]. " + previousValue);
                }
            }
        }

    }

    @Override
    public <T> void declareField(BiConsumer<Value, T> consumer, ContextParser<Context, T> parser, ParseField parseField, ValueType type) {
        if (consumer == null) {
            throw new IllegalArgumentException("[consumer] is required");
        }
        if (parser == null) {
            throw new IllegalArgumentException("[parser] is required");
        }
        declareField((p, v, c) -> consumer.accept(v, parser.parse(p, c)), parseField, type);
    }

    public <T> void declareObjectOrDefault(
        BiConsumer<Value, T> consumer,
        BiFunction<XContentParser, Context, T> objectParser,
        Supplier<T> defaultValue,
        ParseField field
    ) {
        declareField((p, v, c) -> {
            if (p.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                if (p.booleanValue()) {
                    consumer.accept(v, defaultValue.get());
                }
            } else {
                consumer.accept(v, objectParser.apply(p, c));
            }
        }, field, ValueType.OBJECT_OR_BOOLEAN);
    }

    @Override
    public <T> void declareNamedObject(BiConsumer<Value, T> consumer, NamedObjectParser<T, Context> namedObjectParser, ParseField field) {

        BiFunction<XContentParser, Context, T> objectParser = (XContentParser p, Context c) -> {
            try {
                XContentParser.Token token = p.nextToken();
                assert token == XContentParser.Token.FIELD_NAME;
                String currentName = p.currentName();
                try {
                    T namedObject = namedObjectParser.parse(p, c, currentName);
                    // consume the end object token
                    token = p.nextToken();
                    assert token == XContentParser.Token.END_OBJECT;
                    return namedObject;
                } catch (Exception e) {
                    throw rethrowFieldParseFailure(field, p, currentName, e);
                }
            } catch (IOException e) {
                throw wrapParseError(field, p, e, "error while parsing named object");
            }
        };

        declareField((XContentParser p, Value v, Context c) -> consumer.accept(v, objectParser.apply(p, c)), field, ValueType.OBJECT);
    }

    @Override
    public <T> void declareNamedObjects(
        BiConsumer<Value, List<T>> consumer,
        NamedObjectParser<T, Context> namedObjectParser,
        Consumer<Value> orderedModeCallback,
        ParseField field
    ) {
        // This creates and parses the named object
        BiFunction<XContentParser, Context, T> objectParser = (XContentParser p, Context c) -> {
            if (p.currentToken() != XContentParser.Token.FIELD_NAME) {
                throw wrapCanBeObjectOrArrayOfObjects(field, p);
            }
            // This messy exception nesting has the nice side effect of telling the user which field failed to parse
            try {
                String currentName = p.currentName();
                try {
                    return namedObjectParser.parse(p, c, currentName);
                } catch (Exception e) {
                    throw rethrowFieldParseFailure(field, p, currentName, e);
                }
            } catch (IOException e) {
                throw wrapParseError(field, p, e, "error while parsing");
            }
        };
        declareField((XContentParser p, Value v, Context c) -> {
            List<T> fields = new ArrayList<>();
            if (p.currentToken() == XContentParser.Token.START_OBJECT) {
                // Fields are just named entries in a single object
                while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                    fields.add(objectParser.apply(p, c));
                }
            } else if (p.currentToken() == XContentParser.Token.START_ARRAY) {
                // Fields are objects in an array. Each object contains a named field.
                parseObjectsInArray(orderedModeCallback, field, objectParser, p, v, c, fields);
            }
            consumer.accept(v, fields);
        }, field, ValueType.OBJECT_ARRAY);
    }

    private static <Value, Context, T> void parseObjectsInArray(
        Consumer<Value> orderedModeCallback,
        ParseField field,
        BiFunction<XContentParser, Context, T> objectParser,
        XContentParser p,
        Value v,
        Context c,
        List<T> fields
    ) throws IOException {
        orderedModeCallback.accept(v);
        XContentParser.Token token;
        while ((token = p.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token != XContentParser.Token.START_OBJECT) {
                throw wrapCanBeObjectOrArrayOfObjects(field, p);
            }
            p.nextToken(); // Move to the first field in the object
            fields.add(objectParser.apply(p, c));
            p.nextToken(); // Move past the object, should be back to into the array
            if (p.currentToken() != XContentParser.Token.END_OBJECT) {
                throw wrapCanBeObjectOrArrayOfObjects(field, p);
            }
        }
    }

    private static XContentParseException wrapCanBeObjectOrArrayOfObjects(ParseField field, XContentParser p) {
        return new XContentParseException(
            p.getTokenLocation(),
            "["
                + field
                + "] can be a single object with any number of "
                + "fields or an array where each entry is an object with a single field"
        );
    }

    private static XContentParseException wrapParseError(ParseField field, XContentParser p, IOException e, String s) {
        return new XContentParseException(p.getTokenLocation(), "[" + field + "] " + s, e);
    }

    private static XContentParseException rethrowFieldParseFailure(ParseField field, XContentParser p, String currentName, Exception e) {
        return new XContentParseException(p.getTokenLocation(), "[" + field + "] failed to parse field [" + currentName + "]", e);
    }

    @Override
    public <T> void declareNamedObjects(
        BiConsumer<Value, List<T>> consumer,
        NamedObjectParser<T, Context> namedObjectParser,
        ParseField field
    ) {
        Consumer<Value> orderedModeCallback = (v) -> {
            throw new IllegalArgumentException("[" + field + "] doesn't support arrays. Use a single object with multiple fields.");
        };
        declareNamedObjects(consumer, namedObjectParser, orderedModeCallback, field);
    }

    /**
     * Functional interface for instantiating and parsing named objects. See ObjectParserTests#NamedObject for the canonical way to
     * implement this for objects that themselves have a parser.
     */
    @FunctionalInterface
    public interface NamedObjectParser<T, Context> {
        T parse(XContentParser p, Context c, String name) throws IOException;
    }

    /**
     * Get the name of the parser.
     */
    @Override
    public String getName() {
        return name;
    }

    @Override
    public void declareRequiredFieldSet(String... requiredSet) {
        if (requiredSet.length == 0) {
            return;
        }
        this.requiredFieldSets.add(requiredSet);
    }

    @Override
    public void declareExclusiveFieldSet(String... exclusiveSet) {
        if (exclusiveSet.length == 0) {
            return;
        }
        this.exclusiveFieldSets.add(exclusiveSet);
    }

    private void parseArray(XContentParser parser, FieldParser fieldParser, String currentFieldName, Value value, Context context) {
        assert parser.currentToken() == XContentParser.Token.START_ARRAY : "Token was: " + parser.currentToken();
        parseValue(parser, fieldParser, currentFieldName, value, context);
    }

    private void parseValue(XContentParser parser, FieldParser fieldParser, String currentFieldName, Value value, Context context) {
        try {
            fieldParser.parser.parse(parser, value, context);
        } catch (Exception ex) {
            throwFailedToParse(parser, currentFieldName, ex);
        }
    }

    private void throwFailedToParse(XContentParser parser, String currentFieldName, Exception ex) {
        throw new XContentParseException(parser.getTokenLocation(), "[" + name + "] failed to parse field [" + currentFieldName + "]", ex);
    }

    private void parseSub(
        XContentParser parser,
        FieldParser fieldParser,
        XContentParser.Token token,
        String currentFieldName,
        Value value,
        Context context
    ) {
        switch (token) {
            case START_OBJECT -> {
                parseValue(parser, fieldParser, currentFieldName, value, context);
                /*
                 * Well behaving parsers should consume the entire object but
                 * asserting that they do that is not something we can do
                 * efficiently here. Instead we can check that they end on an
                 * END_OBJECT. They could end on the *wrong* end object and
                 * this test won't catch them, but that is the price that we pay
                 * for having a cheap test.
                 */
                if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
                    throwMustEndOn(currentFieldName, XContentParser.Token.END_OBJECT);
                }
            }
            case START_ARRAY -> {
                parseArray(parser, fieldParser, currentFieldName, value, context);
                /*
                 * Well behaving parsers should consume the entire array but
                 * asserting that they do that is not something we can do
                 * efficiently here. Instead we can check that they end on an
                 * END_ARRAY. They could end on the *wrong* end array and
                 * this test won't catch them, but that is the price that we pay
                 * for having a cheap test.
                 */
                if (parser.currentToken() != XContentParser.Token.END_ARRAY) {
                    throwMustEndOn(currentFieldName, XContentParser.Token.END_ARRAY);
                }
            }
            case END_OBJECT, END_ARRAY, FIELD_NAME -> throw throwUnexpectedToken(parser, token);
            case VALUE_STRING, VALUE_NUMBER, VALUE_BOOLEAN, VALUE_EMBEDDED_OBJECT, VALUE_NULL -> parseValue(
                parser,
                fieldParser,
                currentFieldName,
                value,
                context
            );
        }
    }

    private static void throwMustEndOn(String currentFieldName, XContentParser.Token token) {
        throw new IllegalStateException("parser for [" + currentFieldName + "] did not end on " + token);
    }

    private XContentParseException throwUnexpectedToken(XContentParser parser, XContentParser.Token token) {
        return new XContentParseException(parser.getTokenLocation(), "[" + name + "]" + token + " is unexpected");
    }

    private class FieldParser {
        private final Parser<Value, Context> parser;
        private final EnumSet<XContentParser.Token> supportedTokens;
        private final ParseField parseField;
        private final ValueType type;

        FieldParser(Parser<Value, Context> parser, EnumSet<XContentParser.Token> supportedTokens, ParseField parseField, ValueType type) {
            this.parser = parser;
            this.supportedTokens = supportedTokens;
            this.parseField = parseField;
            this.type = type;
        }

        void assertSupports(String parserName, XContentParser xContentParser, XContentParser.Token currentToken, String currentFieldName) {
            boolean match = parseField.match(
                parserName,
                xContentParser::getTokenLocation,
                currentFieldName,
                xContentParser.getDeprecationHandler()
            );
            if (match == false) {
                throw new XContentParseException(
                    xContentParser.getTokenLocation(),
                    "[" + parserName + "] parsefield doesn't accept: " + currentFieldName
                );
            }
            if (supportedTokens.contains(xContentParser.currentToken()) == false) {
                throw new XContentParseException(
                    xContentParser.getTokenLocation(),
                    "[" + parserName + "] " + currentFieldName + " doesn't support values of type: " + currentToken
                );
            }
        }

        @Override
        public String toString() {
            String[] deprecatedNames = parseField.getDeprecatedNames();
            String allReplacedWith = parseField.getAllReplacedWith();
            String deprecated = "";
            if (deprecatedNames != null && deprecatedNames.length > 0) {
                deprecated = ", deprecated_names=" + Arrays.toString(deprecatedNames);
            }
            return "FieldParser{"
                + "preferred_name="
                + parseField.getPreferredName()
                + ", supportedTokens="
                + supportedTokens
                + deprecated
                + (allReplacedWith == null ? "" : ", replaced_with=" + allReplacedWith)
                + ", type="
                + type.name()
                + '}';
        }
    }

    public enum ValueType {
        STRING(VALUE_STRING),
        STRING_OR_NULL(VALUE_STRING, VALUE_NULL),
        FLOAT(VALUE_NUMBER, VALUE_STRING),
        FLOAT_OR_NULL(VALUE_NUMBER, VALUE_STRING, VALUE_NULL),
        DOUBLE(VALUE_NUMBER, VALUE_STRING),
        DOUBLE_OR_NULL(VALUE_NUMBER, VALUE_STRING, VALUE_NULL),
        LONG(VALUE_NUMBER, VALUE_STRING),
        LONG_OR_NULL(VALUE_NUMBER, VALUE_STRING, VALUE_NULL),
        INT(VALUE_NUMBER, VALUE_STRING),
        INT_OR_NULL(VALUE_NUMBER, VALUE_STRING, VALUE_NULL),
        BOOLEAN(VALUE_BOOLEAN, VALUE_STRING),
        BOOLEAN_OR_NULL(VALUE_BOOLEAN, VALUE_STRING, VALUE_NULL),
        STRING_ARRAY(START_ARRAY, VALUE_STRING),
        FLOAT_ARRAY(START_ARRAY, VALUE_NUMBER, VALUE_STRING),
        DOUBLE_ARRAY(START_ARRAY, VALUE_NUMBER, VALUE_STRING),
        LONG_ARRAY(START_ARRAY, VALUE_NUMBER, VALUE_STRING),
        INT_ARRAY(START_ARRAY, VALUE_NUMBER, VALUE_STRING),
        BOOLEAN_ARRAY(START_ARRAY, VALUE_BOOLEAN),
        OBJECT(START_OBJECT),
        OBJECT_OR_NULL(START_OBJECT, VALUE_NULL),
        OBJECT_ARRAY(START_OBJECT, START_ARRAY),
        OBJECT_ARRAY_OR_NULL(START_OBJECT, START_ARRAY, VALUE_NULL),
        OBJECT_OR_BOOLEAN(START_OBJECT, VALUE_BOOLEAN),
        OBJECT_OR_STRING(START_OBJECT, VALUE_STRING),
        OBJECT_OR_NUMBER(START_OBJECT, VALUE_NUMBER),
        OBJECT_ARRAY_BOOLEAN_OR_STRING(START_OBJECT, START_ARRAY, VALUE_BOOLEAN, VALUE_STRING),
        OBJECT_ARRAY_OR_STRING(START_OBJECT, START_ARRAY, VALUE_STRING),
        OBJECT_ARRAY_STRING_OR_NUMBER(START_OBJECT, START_ARRAY, VALUE_STRING, VALUE_NUMBER),
        VALUE(VALUE_BOOLEAN, VALUE_NULL, VALUE_EMBEDDED_OBJECT, VALUE_NUMBER, VALUE_STRING),
        VALUE_OBJECT_ARRAY(VALUE_BOOLEAN, VALUE_NULL, VALUE_EMBEDDED_OBJECT, VALUE_NUMBER, VALUE_STRING, START_OBJECT, START_ARRAY),
        VALUE_ARRAY(VALUE_BOOLEAN, VALUE_NULL, VALUE_NUMBER, VALUE_STRING, START_ARRAY);

        private final EnumSet<XContentParser.Token> tokens;

        ValueType(XContentParser.Token first, XContentParser.Token... rest) {
            this.tokens = EnumSet.of(first, rest);
        }

        public EnumSet<XContentParser.Token> supportedTokens() {
            return this.tokens;
        }
    }

    @Override
    public String toString() {
        return "ObjectParser{" + "name='" + name + '\'' + ", fields=" + fieldParserMap + '}';
    }
}
