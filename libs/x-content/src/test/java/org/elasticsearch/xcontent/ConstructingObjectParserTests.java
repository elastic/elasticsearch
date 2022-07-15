/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ObjectParserTests.NamedObject;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matcher;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class ConstructingObjectParserTests extends ESTestCase {
    public void testNullDeclares() {
        ConstructingObjectParser<Void, Void> objectParser = new ConstructingObjectParser<>("foo", a -> null);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> objectParser.declareField(null, (r, c) -> null, new ParseField("test"), ObjectParser.ValueType.STRING)
        );
        assertEquals("[consumer] is required", e.getMessage());
        e = expectThrows(
            IllegalArgumentException.class,
            () -> objectParser.declareField(
                (o, v) -> {},
                (ContextParser<Void, Object>) null,
                new ParseField("test"),
                ObjectParser.ValueType.STRING
            )
        );
        assertEquals("[parser] is required", e.getMessage());
        e = expectThrows(
            IllegalArgumentException.class,
            () -> objectParser.declareField(
                (o, v) -> {},
                (CheckedFunction<XContentParser, Object, IOException>) null,
                new ParseField("test"),
                ObjectParser.ValueType.STRING
            )
        );
        assertEquals("[parser] is required", e.getMessage());
        e = expectThrows(
            IllegalArgumentException.class,
            () -> objectParser.declareField((o, v) -> {}, (r, c) -> null, null, ObjectParser.ValueType.STRING)
        );
        assertEquals("[parseField] is required", e.getMessage());
        e = expectThrows(
            IllegalArgumentException.class,
            () -> objectParser.declareField((o, v) -> {}, (r, c) -> null, new ParseField("test"), null)
        );
        assertEquals("[type] is required", e.getMessage());
    }

    /**
     * Builds the object in random order and parses it.
     */
    public void testRandomOrder() throws Exception {
        HasCtorArguments expected = new HasCtorArguments(randomAlphaOfLength(5), randomInt());
        expected.setMineral(randomInt());
        expected.setFruit(randomInt());
        expected.setA(randomBoolean() ? null : randomAlphaOfLength(5));
        expected.setB(randomBoolean() ? null : randomAlphaOfLength(5));
        expected.setC(randomBoolean() ? null : randomAlphaOfLength(5));
        expected.setD(randomBoolean());
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        expected.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder = shuffleXContent(builder);
        builder.flush();
        byte[] bytes = ((ByteArrayOutputStream) builder.getOutputStream()).toByteArray();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, bytes)) {
            HasCtorArguments parsed = randomFrom(HasCtorArguments.ALL_PARSERS).apply(parser, null);
            assertEquals(expected.animal, parsed.animal);
            assertEquals(expected.vegetable, parsed.vegetable);
            assertEquals(expected.mineral, parsed.mineral);
            assertEquals(expected.fruit, parsed.fruit);
            assertEquals(expected.a, parsed.a);
            assertEquals(expected.b, parsed.b);
            assertEquals(expected.c, parsed.c);
            assertEquals(expected.d, parsed.d);
        }
    }

    public void testMissingAllConstructorArgs() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"mineral\": 1 }");
        ConstructingObjectParser<HasCtorArguments, Void> objectParser = randomBoolean()
            ? HasCtorArguments.PARSER
            : HasCtorArguments.PARSER_VEGETABLE_OPTIONAL;
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> objectParser.apply(parser, null));
        if (objectParser == HasCtorArguments.PARSER) {
            assertEquals("Required [animal, vegetable]", e.getMessage());
        } else {
            assertEquals("Required [animal]", e.getMessage());
        }
    }

    public void testMissingAllConstructorArgsButNotRequired() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"mineral\": 1 }");
        HasCtorArguments parsed = HasCtorArguments.PARSER_ALL_OPTIONAL.apply(parser, null);
        assertEquals(1, parsed.mineral);
    }

    public void testMissingSecondConstructorArg() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"mineral\": 1, \"animal\": \"cat\" }");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> HasCtorArguments.PARSER.apply(parser, null));
        assertEquals("Required [vegetable]", e.getMessage());
    }

    public void testMissingSecondConstructorArgButNotRequired() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"mineral\": 1, \"animal\": \"cat\" }");
        @SuppressWarnings("unchecked")
        HasCtorArguments parsed = randomFrom(HasCtorArguments.PARSER_VEGETABLE_OPTIONAL, HasCtorArguments.PARSER_ALL_OPTIONAL).apply(
            parser,
            null
        );
        assertEquals(1, parsed.mineral);
        assertEquals("cat", parsed.animal);
    }

    public void testMissingFirstConstructorArg() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"mineral\": 1, \"vegetable\": 2 }");
        @SuppressWarnings("unchecked")
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> randomFrom(HasCtorArguments.PARSER, HasCtorArguments.PARSER_VEGETABLE_OPTIONAL).apply(parser, null)
        );
        assertEquals("Required [animal]", e.getMessage());
    }

    public void testMissingFirstConstructorArgButNotRequired() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"mineral\": 1, \"vegetable\": 2 }");
        HasCtorArguments parsed = HasCtorArguments.PARSER_ALL_OPTIONAL.apply(parser, null);
        assertEquals(1, parsed.mineral);
        assertEquals((Integer) 2, parsed.vegetable);
    }

    public void testBadParam() throws IOException {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            // The following JSON needs to include newlines, in order to affect the line numbers
            // included in the exception
            """
                {
                  "animal": "cat",
                  "vegetable": 2,
                  "a": "supercalifragilisticexpialidocious"
                }
                """
        );
        XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> randomFrom(HasCtorArguments.ALL_PARSERS).apply(parser, null)
        );
        assertThat(e.getMessage(), containsString("[has_required_arguments] failed to parse field [a]"));
        assertEquals(4, e.getLineNumber());
        assertEquals(
            "[a] must be less than 10 characters in length but was [supercalifragilisticexpialidocious]",
            e.getCause().getMessage()
        );
    }

    public void testBadParamBeforeObjectBuilt() throws IOException {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            // The following JSON needs to include newlines, in order to affect the line numbers
            // included in the exception
            """
                {
                  "a": "supercalifragilisticexpialidocious",
                  "animal": "cat",
                  "vegetable": 2
                }
                """
        );
        XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> randomFrom(HasCtorArguments.ALL_PARSERS).apply(parser, null)
        );
        assertThat(e.getMessage(), containsString("[has_required_arguments] failed to parse field [vegetable]"));
        assertEquals(4, e.getLineNumber());
        e = (XContentParseException) e.getCause();
        assertThat(e.getMessage(), containsString("failed to build [has_required_arguments] after last required field arrived"));
        assertEquals(2, e.getLineNumber());
        e = (XContentParseException) e.getCause();
        assertThat(e.getMessage(), containsString("[has_required_arguments] failed to parse field [a]"));
        assertEquals(2, e.getLineNumber());
        assertEquals(
            "[a] must be less than 10 characters in length but was [supercalifragilisticexpialidocious]",
            e.getCause().getMessage()
        );
    }

    public void testConstructorArgsMustBeConfigured() throws IOException {
        class NoConstructorArgs {}
        ConstructingObjectParser<NoConstructorArgs, Void> parser = new ConstructingObjectParser<>(
            "constructor_args_required",
            (a) -> new NoConstructorArgs()
        );
        try {
            parser.apply(createParser(JsonXContent.jsonXContent, "{}"), null);
            fail("Expected AssertionError");
        } catch (AssertionError e) {
            assertEquals(
                "[constructor_args_required] must configure at least one constructor argument. If it doesn't have any it should "
                    + "use ObjectParser instead of ConstructingObjectParser. This is a bug in the parser declaration.",
                e.getMessage()
            );
        }
    }

    /**
     * Tests the non-constructor fields are only set on time.
     */
    public void testCalledOneTime() throws IOException {
        boolean ctorArgOptional = randomBoolean();
        class CalledOneTime {
            CalledOneTime(String yeah) {
                Matcher<String> yeahMatcher = equalTo("!");
                if (ctorArgOptional) {
                    // either(yeahMatcher).or(nullValue) is broken by https://github.com/hamcrest/JavaHamcrest/issues/49
                    yeahMatcher = anyOf(yeahMatcher, nullValue());
                }
                assertThat(yeah, yeahMatcher);
            }

            boolean fooSet = false;

            void setFoo(String foo) {
                assertFalse(fooSet);
                fooSet = true;
            }
        }
        ConstructingObjectParser<CalledOneTime, Void> parser = new ConstructingObjectParser<>(
            "one_time_test",
            (a) -> new CalledOneTime((String) a[0])
        );
        parser.declareString(CalledOneTime::setFoo, new ParseField("foo"));
        parser.declareString(ctorArgOptional ? optionalConstructorArg() : constructorArg(), new ParseField("yeah"));

        // ctor arg first so we can test for the bug we found one time
        XContentParser xcontent = createParser(JsonXContent.jsonXContent, "{ \"yeah\": \"!\", \"foo\": \"foo\" }");
        CalledOneTime result = parser.apply(xcontent, null);
        assertTrue(result.fooSet);

        // and ctor arg second just in case
        xcontent = createParser(JsonXContent.jsonXContent, "{ \"foo\": \"foo\",  \"yeah\": \"!\" }");
        result = parser.apply(xcontent, null);
        assertTrue(result.fooSet);

        if (ctorArgOptional) {
            // and without the constructor arg if we've made it optional
            xcontent = createParser(JsonXContent.jsonXContent, "{ \"foo\": \"foo\" }");
            result = parser.apply(xcontent, null);
        }
        assertTrue(result.fooSet);
    }

    public void testIgnoreUnknownFields() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            { "test" : "foo", "junk" : 2 }""");
        class TestStruct {
            public final String test;

            TestStruct(String test) {
                this.test = test;
            }
        }
        ConstructingObjectParser<TestStruct, Void> objectParser = new ConstructingObjectParser<>(
            "foo",
            true,
            a -> new TestStruct((String) a[0])
        );
        objectParser.declareString(constructorArg(), new ParseField("test"));
        TestStruct s = objectParser.apply(parser, null);
        assertEquals(s.test, "foo");
    }

    public void testConstructObjectUsingContext() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            { "animal": "dropbear", "mineral": -8 }""");
        HasCtorArguments parsed = HasCtorArguments.PARSER_INT_CONTEXT.apply(parser, 42);
        assertEquals(Integer.valueOf(42), parsed.vegetable);
        assertEquals("dropbear", parsed.animal);
        assertEquals(-8, parsed.mineral);
    }

    private static class HasCtorArguments implements ToXContentObject {
        @Nullable
        final String animal;
        @Nullable
        final Integer vegetable;
        int mineral;
        int fruit;
        String a;
        String b;
        String c;
        boolean d;

        HasCtorArguments(@Nullable String animal, @Nullable Integer vegetable) {
            this.animal = animal;
            this.vegetable = vegetable;
        }

        public void setMineral(int mineral) {
            this.mineral = mineral;
        }

        public void setFruit(int fruit) {
            this.fruit = fruit;
        }

        public void setA(String a) {
            if (a != null && a.length() > 9) {
                throw new IllegalArgumentException("[a] must be less than 10 characters in length but was [" + a + "]");
            }
            this.a = a;
        }

        public void setB(String b) {
            this.b = b;
        }

        public void setC(String c) {
            this.c = c;
        }

        public void setD(boolean d) {
            this.d = d;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("animal", animal);
            builder.field("vegetable", vegetable);
            if (mineral != 0) { // We're just using 0 as the default because it is easy for testing
                builder.field("mineral", mineral);
            }
            if (fruit != 0) {
                builder.field("fruit", fruit);
            }
            if (a != null) {
                builder.field("a", a);
            }
            if (b != null) {
                builder.field("b", b);
            }
            if (c != null) {
                builder.field("c", c);
            }
            if (d) {
                builder.field("d", d);
            }
            builder.endObject();
            return builder;
        }

        /*
         * It is normal just to declare a single PARSER but we use a couple of different parsers for testing so we have all of these. Don't
         * this this style is normal just because it is in the test.
         */
        public static final ConstructingObjectParser<HasCtorArguments, Void> PARSER = buildParser(true, true);
        public static final ConstructingObjectParser<HasCtorArguments, Void> PARSER_VEGETABLE_OPTIONAL = buildParser(true, false);
        public static final ConstructingObjectParser<HasCtorArguments, Void> PARSER_ALL_OPTIONAL = buildParser(false, false);

        public static final List<ConstructingObjectParser<HasCtorArguments, Void>> ALL_PARSERS = List.of(
            PARSER,
            PARSER_VEGETABLE_OPTIONAL,
            PARSER_ALL_OPTIONAL
        );

        public static final ConstructingObjectParser<HasCtorArguments, Integer> PARSER_INT_CONTEXT = buildContextParser();

        private static ConstructingObjectParser<HasCtorArguments, Void> buildParser(boolean animalRequired, boolean vegetableRequired) {
            ConstructingObjectParser<HasCtorArguments, Void> parser = new ConstructingObjectParser<>(
                "has_required_arguments",
                args -> new HasCtorArguments((String) args[0], (Integer) args[1])
            );
            parser.declareString(animalRequired ? constructorArg() : optionalConstructorArg(), new ParseField("animal"));
            parser.declareInt(vegetableRequired ? constructorArg() : optionalConstructorArg(), new ParseField("vegetable"));
            declareSetters(parser);
            return parser;
        }

        private static ConstructingObjectParser<HasCtorArguments, Integer> buildContextParser() {
            ConstructingObjectParser<HasCtorArguments, Integer> parser = new ConstructingObjectParser<>(
                "has_required_arguments",
                false,
                (args, ctx) -> new HasCtorArguments((String) args[0], ctx)
            );
            parser.declareString(constructorArg(), new ParseField("animal"));
            declareSetters(parser);
            return parser;
        }

        private static void declareSetters(ConstructingObjectParser<HasCtorArguments, ?> parser) {
            parser.declareInt(HasCtorArguments::setMineral, new ParseField("mineral"));
            parser.declareInt(HasCtorArguments::setFruit, new ParseField("fruit"));
            parser.declareString(HasCtorArguments::setA, new ParseField("a"));
            parser.declareString(HasCtorArguments::setB, new ParseField("b"));
            parser.declareString(HasCtorArguments::setC, new ParseField("c"));
            parser.declareBoolean(HasCtorArguments::setD, new ParseField("d"));
        }
    }

    public void testParseNamedObject() throws IOException {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{\"named\": { \"a\": {} }, \"named_in_constructor\": { \"b\": {} } }"
        );
        NamedObjectHolder h = NamedObjectHolder.PARSER.apply(parser, null);
        assertThat(h.named, hasSize(1));
        assertEquals("a", h.named.get(0).name);
        assertThat(h.namedInConstructor, hasSize(1));
        assertEquals("b", h.namedInConstructor.get(0).name);
        assertFalse(h.namedSuppliedInOrder);
    }

    public void testParseNamedObjectInOrder() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {"named": [ {"a": {}} ], "named_in_constructor": [ {"b": {}} ]}""");
        NamedObjectHolder h = NamedObjectHolder.PARSER.apply(parser, null);
        assertThat(h.named, hasSize(1));
        assertEquals("a", h.named.get(0).name);
        assertThat(h.namedInConstructor, hasSize(1));
        assertEquals("b", h.namedInConstructor.get(0).name);
        assertTrue(h.namedSuppliedInOrder);
    }

    public void testParseNamedObjectTwoFieldsInArray() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {"named": [ {"a": {}, "b": {}}], "named_in_constructor": [ {"c": {}} ]}""");
        XContentParseException e = expectThrows(XContentParseException.class, () -> NamedObjectHolder.PARSER.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named]"));
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "[named] can be a single object with any number of fields "
                    + "or an array where each entry is an object with a single field"
            )
        );
    }

    public void testParseNamedObjectTwoFieldsInArrayConstructorArg() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {"named": [ {"a": {}}], "named_in_constructor": [ {"c": {}, "d": {}} ]}""");
        XContentParseException e = expectThrows(XContentParseException.class, () -> NamedObjectHolder.PARSER.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named_in_constructor]"));
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "[named_in_constructor] can be a single object with any number of fields "
                    + "or an array where each entry is an object with a single field"
            )
        );
    }

    public void testParseNamedObjectNoFieldsInArray() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"named\": [ {} ], \"named_in_constructor\": [ {\"a\": {}} ]}");
        XContentParseException e = expectThrows(XContentParseException.class, () -> NamedObjectHolder.PARSER.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named]"));
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "[named] can be a single object with any number of fields "
                    + "or an array where each entry is an object with a single field"
            )
        );
    }

    public void testParseNamedObjectNoFieldsInArrayConstructorArg() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"named\": [ {\"a\": {}} ], \"named_in_constructor\": [ {} ]}");
        XContentParseException e = expectThrows(XContentParseException.class, () -> NamedObjectHolder.PARSER.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named_in_constructor]"));
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "[named_in_constructor] can be a single object with any number of fields "
                    + "or an array where each entry is an object with a single field"
            )
        );
    }

    public void testParseNamedObjectJunkInArray() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {"named": [ "junk" ], "named_in_constructor": [ {"a": {}} ]}""");
        XContentParseException e = expectThrows(XContentParseException.class, () -> NamedObjectHolder.PARSER.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named]"));
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "[named] can be a single object with any number of fields "
                    + "or an array where each entry is an object with a single field"
            )
        );
    }

    public void testParseNamedObjectJunkInArrayConstructorArg() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {"named": [ {"a": {}} ], "named_in_constructor": [ "junk" ]}""");
        XContentParseException e = expectThrows(XContentParseException.class, () -> NamedObjectHolder.PARSER.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named_in_constructor]"));
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "[named_in_constructor] can be a single object with any number of fields "
                    + "or an array where each entry is an object with a single field"
            )
        );
    }

    public void testParseNamedObjectInOrderNotSupported() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
                "named": [ { "a": {} } ],
                "named_in_constructor": {
                  "b": {}
                }
              }""");

        // Create our own parser for this test so we can disable support for the "ordered" mode specified by the array above
        @SuppressWarnings("unchecked")
        ConstructingObjectParser<NamedObjectHolder, Void> objectParser = new ConstructingObjectParser<>(
            "named_object_holder",
            a -> new NamedObjectHolder(((List<NamedObject>) a[0]))
        );
        objectParser.declareNamedObjects(
            ConstructingObjectParser.constructorArg(),
            NamedObject.PARSER,
            new ParseField("named_in_constructor")
        );
        objectParser.declareNamedObjects(NamedObjectHolder::setNamed, NamedObject.PARSER, new ParseField("named"));

        // Now firing the xml through it fails
        XContentParseException e = expectThrows(XContentParseException.class, () -> objectParser.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named]"));
        assertEquals("[named] doesn't support arrays. Use a single object with multiple fields.", e.getCause().getMessage());
    }

    public void testParseNamedObjectInOrderNotSupportedConstructorArg() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
              "named": {
                "a": {}
              },
              "named_in_constructor": [ { "b": {} } ]
            }""");

        // Create our own parser for this test so we can disable support for the "ordered" mode specified by the array above
        @SuppressWarnings("unchecked")
        ConstructingObjectParser<NamedObjectHolder, Void> objectParser = new ConstructingObjectParser<>(
            "named_object_holder",
            a -> new NamedObjectHolder(((List<NamedObject>) a[0]))
        );
        objectParser.declareNamedObjects(
            ConstructingObjectParser.constructorArg(),
            NamedObject.PARSER,
            new ParseField("named_in_constructor")
        );
        objectParser.declareNamedObjects(NamedObjectHolder::setNamed, NamedObject.PARSER, new ParseField("named"));

        // Now firing the xml through it fails
        XContentParseException e = expectThrows(XContentParseException.class, () -> objectParser.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named_in_constructor]"));
        assertThat(
            e.getCause().getMessage(),
            containsString("[named_in_constructor] doesn't support arrays. Use a single object with multiple fields.")
        );
    }

    static class NamedObjectHolder {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<NamedObjectHolder, Void> PARSER = new ConstructingObjectParser<>(
            "named_object_holder",
            a -> new NamedObjectHolder(((List<NamedObject>) a[0]))
        );
        static {
            PARSER.declareNamedObjects(
                ConstructingObjectParser.constructorArg(),
                NamedObject.PARSER,
                NamedObjectHolder::keepNamedInOrder,
                new ParseField("named_in_constructor")
            );
            PARSER.declareNamedObjects(
                NamedObjectHolder::setNamed,
                NamedObject.PARSER,
                NamedObjectHolder::keepNamedInOrder,
                new ParseField("named")
            );
        }

        private List<NamedObject> named;
        private List<NamedObject> namedInConstructor;
        private boolean namedSuppliedInOrder = false;

        NamedObjectHolder(List<NamedObject> namedInConstructor) {
            this.namedInConstructor = namedInConstructor;
        }

        public void setNamed(List<NamedObject> named) {
            this.named = named;
        }

        public void keepNamedInOrder() {
            namedSuppliedInOrder = true;
        }
    }

    public void testRequiredAndExclusiveFields() throws IOException {

        class TestStruct {
            final String a;
            final long b;

            TestStruct(String a) {
                this.a = a;
                this.b = 0;
            }

            TestStruct(long b) {
                this.a = null;
                this.b = b;
            }
        }

        XContentParser ok = createParser(JsonXContent.jsonXContent, "{ \"a\" : \"a\" }");
        XContentParser toomany = createParser(JsonXContent.jsonXContent, "{ \"a\" : \"a\", \"b\" : 1 }");
        XContentParser notenough = createParser(JsonXContent.jsonXContent, "{ }");

        ConstructingObjectParser<TestStruct, Void> parser = new ConstructingObjectParser<>("teststruct", args -> {
            if (args[0] != null) {
                return new TestStruct((String) args[0]);
            }
            return new TestStruct((Long) args[1]);
        });
        parser.declareString(optionalConstructorArg(), new ParseField("a"));
        parser.declareLong(optionalConstructorArg(), new ParseField("b"));
        parser.declareExclusiveFieldSet("a", "b");
        parser.declareRequiredFieldSet("a", "b");

        TestStruct actual = parser.parse(ok, null);
        assertThat(actual.a, equalTo("a"));
        assertThat(actual.b, equalTo(0L));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parser.parse(toomany, null));
        assertThat(e.getMessage(), containsString("allowed together: [a, b]"));

        e = expectThrows(IllegalArgumentException.class, () -> parser.parse(notenough, null));
        assertThat(e.getMessage(), containsString("Required one of fields [a, b], but none were specified."));
    }

    // migrating name and type from old_string_name:String to new_int_name:int
    public static class StructWithCompatibleFields {
        // real usage would have RestApiVersion.V_7 instead of currentVersion or minimumSupported
        static final ConstructingObjectParser<StructWithCompatibleFields, Void> PARSER = new ConstructingObjectParser<>(
            "struct_with_compatible_fields",
            a -> new StructWithCompatibleFields((Integer) a[0])
        );

        static {
            // declare a field with `new_name` being preferable, and old_name deprecated.
            // The declaration is only available for lookup when parser has compatibility set
            PARSER.declareInt(
                constructorArg(),
                new ParseField("new_name", "old_name").forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.minimumSupported()))
            );

            // declare `new_name` to be parsed when compatibility is NOT used
            PARSER.declareInt(
                constructorArg(),
                new ParseField("new_name").forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.current()))
            );

            // declare `old_name` to throw exception when compatibility is NOT used
            PARSER.declareInt(
                (r, s) -> failWithException(),
                new ParseField("old_name").forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.current()))
            );
        }
        private int intField;

        public StructWithCompatibleFields(int intField) {
            this.intField = intField;
        }

        private static void failWithException() {
            throw new IllegalArgumentException("invalid parameter [old_name], use [new_name] instead");
        }
    }

    public void testCompatibleFieldDeclarations() throws IOException {
        {
            // new_name is the only way to parse when compatibility is not set
            XContentParser parser = createParserWithCompatibilityFor(
                JsonXContent.jsonXContent,
                "{\"new_name\": 1}",
                RestApiVersion.current()
            );
            StructWithCompatibleFields o = StructWithCompatibleFields.PARSER.parse(parser, null);
            assertEquals(1, o.intField);
        }

        {
            // old_name results with an exception when compatibility is not set
            XContentParser parser = createParserWithCompatibilityFor(
                JsonXContent.jsonXContent,
                "{\"old_name\": 1}",
                RestApiVersion.current()
            );
            expectThrows(IllegalArgumentException.class, () -> StructWithCompatibleFields.PARSER.parse(parser, null));
        }
        {
            // new_name is allowed to be parsed with compatibility
            XContentParser parser = createParserWithCompatibilityFor(
                JsonXContent.jsonXContent,
                "{\"new_name\": 1}",
                RestApiVersion.minimumSupported()
            );
            StructWithCompatibleFields o = StructWithCompatibleFields.PARSER.parse(parser, null);
            assertEquals(1, o.intField);
        }
        {

            // old_name is allowed to be parsed with compatibility, but results in deprecation
            XContentParser parser = createParserWithCompatibilityFor(
                JsonXContent.jsonXContent,
                "{\"old_name\": 1}",
                RestApiVersion.minimumSupported()
            );
            StructWithCompatibleFields o = StructWithCompatibleFields.PARSER.parse(parser, null);
            assertEquals(1, o.intField);
            assertWarnings(
                false,
                new DeprecationWarning(
                    DeprecationLogger.CRITICAL,
                    "[struct_with_compatible_fields][1:14] " + "Deprecated field [old_name] used, expected [new_name] instead"
                )
            );
        }
    }

    // an example on how to support a removed field
    public static class StructRemovalField {
        private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(StructRemovalField.class);

        // real usage would have RestApiVersion.V_7 instead of currentVersion or minimumSupported
        static final ConstructingObjectParser<StructRemovalField, Void> PARSER = new ConstructingObjectParser<>(
            "struct_removal",
            a -> new StructRemovalField((String) a[0])
        );

        static {
            // we still need to have something to pass to a constructor. Otherwise use ObjectParser
            PARSER.declareString(constructorArg(), new ParseField("second_field"));

            // declare a field with `old_name` being preferable, no deprecated name.
            // deprecated field name results in a deprecation warning with a suggestion what field to use.
            // the field was removed so there is nothing to suggest.
            // The deprecation shoudl be done manually
            PARSER.declareInt(
                logWarningDoNothing("old_name"),
                new ParseField("old_name").forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.minimumSupported()))
            );

            // declare `old_name` to throw exception when compatibility is NOT used
            PARSER.declareInt(
                (r, s) -> failWithException(),
                new ParseField("old_name").forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.current()))
            );
        }

        private final String secondField;

        public StructRemovalField(String secondField) {
            this.secondField = secondField;
        }

        private static BiConsumer<StructRemovalField, Integer> logWarningDoNothing(String old_name) {
            return (struct, value) -> deprecationLogger.compatibleCritical(
                "struct_removal",
                "The field old_name has been removed and is being ignored"
            );
        }

        private static void failWithException() {
            throw new IllegalArgumentException("invalid parameter [old_name], use [new_name] instead");
        }
    }

    public void testRemovalOfField() throws IOException {
        {
            // old_name with NO compatibility is resulting in an exception
            XContentParser parser = createParserWithCompatibilityFor(JsonXContent.jsonXContent, """
                {"old_name": 1, "second_field": "someString"}""", RestApiVersion.current());
            expectThrows(XContentParseException.class, () -> StructRemovalField.PARSER.parse(parser, null));
        }

        {
            // old_name with compatibility is still parsed, but ignored and results in a warning
            XContentParser parser = createParserWithCompatibilityFor(JsonXContent.jsonXContent, """
                {"old_name": 1, "second_field": "someString"}""", RestApiVersion.minimumSupported());
            StructRemovalField parse = StructRemovalField.PARSER.parse(parser, null);

            assertCriticalWarnings("The field old_name has been removed and is being ignored");
        }
    }

    public void testDoubleDeclarationThrowsException() throws IOException {
        class DoubleFieldDeclaration {
            private int intField;

            DoubleFieldDeclaration(int intField) {
                this.intField = intField;
            }
        }
        ConstructingObjectParser<DoubleFieldDeclaration, Void> PARSER = new ConstructingObjectParser<>(
            "double_field_declaration",
            a -> new DoubleFieldDeclaration((int) a[0])
        );
        PARSER.declareInt(constructorArg(), new ParseField("name"));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> PARSER.declareInt(constructorArg(), new ParseField("name"))
        );

        assertThat(exception, instanceOf(IllegalArgumentException.class));
        assertThat(exception.getMessage(), startsWith("Parser already registered for name=[name]"));
    }
}
