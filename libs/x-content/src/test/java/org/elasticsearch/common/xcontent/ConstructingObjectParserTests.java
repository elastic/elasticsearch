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

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParserTests.NamedObject;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class ConstructingObjectParserTests extends ESTestCase {
    public void testNullDeclares() {
        ConstructingObjectParser<Void, Void> objectParser = new ConstructingObjectParser<>("foo", a -> null);
        Exception e = expectThrows(IllegalArgumentException.class,
                () -> objectParser.declareField(null, (r, c) -> null, new ParseField("test"), ObjectParser.ValueType.STRING));
        assertEquals("[consumer] is required", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> objectParser.declareField(
                (o, v) -> {}, (ContextParser<Void, Object>) null,
                new ParseField("test"), ObjectParser.ValueType.STRING));
        assertEquals("[parser] is required", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> objectParser.declareField(
                (o, v) -> {}, (CheckedFunction<XContentParser, Object, IOException>) null,
                new ParseField("test"), ObjectParser.ValueType.STRING));
        assertEquals("[parser] is required", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> objectParser.declareField(
                (o, v) -> {}, (r, c) -> null, null, ObjectParser.ValueType.STRING));
        assertEquals("[parseField] is required", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> objectParser.declareField(
                (o, v) -> {}, (r, c) -> null, new ParseField("test"), null));
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
        ConstructingObjectParser<HasCtorArguments, Void> objectParser = randomBoolean() ? HasCtorArguments.PARSER
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
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> HasCtorArguments.PARSER.apply(parser, null));
        assertEquals("Required [vegetable]", e.getMessage());
    }

    public void testMissingSecondConstructorArgButNotRequired() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"mineral\": 1, \"animal\": \"cat\" }");
        @SuppressWarnings("unchecked")
        HasCtorArguments parsed = randomFrom(HasCtorArguments.PARSER_VEGETABLE_OPTIONAL, HasCtorArguments.PARSER_ALL_OPTIONAL).apply(parser,
                null);
        assertEquals(1, parsed.mineral);
        assertEquals("cat", parsed.animal);
    }

    public void testMissingFirstConstructorArg() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"mineral\": 1, \"vegetable\": 2 }");
        @SuppressWarnings("unchecked")
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> randomFrom(HasCtorArguments.PARSER, HasCtorArguments.PARSER_VEGETABLE_OPTIONAL).apply(parser, null));
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
            "{\n" + "  \"animal\": \"cat\",\n" + "  \"vegetable\": 2,\n" + "  \"a\": \"supercalifragilisticexpialidocious\"\n" + "}"
        );
        XContentParseException e = expectThrows(XContentParseException.class,
                () -> randomFrom(HasCtorArguments.ALL_PARSERS).apply(parser, null));
        assertThat(e.getMessage(), containsString("[has_required_arguments] failed to parse field [a]"));
        assertEquals(4, e.getLineNumber());
        assertEquals("[a] must be less than 10 characters in length but was [supercalifragilisticexpialidocious]",
                e.getCause().getMessage());
    }

    public void testBadParamBeforeObjectBuilt() throws IOException {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            // The following JSON needs to include newlines, in order to affect the line numbers
            // included in the exception
            "{\n" + "  \"a\": \"supercalifragilisticexpialidocious\",\n" + "  \"animal\": \"cat\"\n," + "  \"vegetable\": 2\n" + "}"
        );
        XContentParseException e = expectThrows(XContentParseException.class,
                () -> randomFrom(HasCtorArguments.ALL_PARSERS).apply(parser, null));
        assertThat(e.getMessage(), containsString("[has_required_arguments] failed to parse field [vegetable]"));
        assertEquals(4, e.getLineNumber());
        e = (XContentParseException) e.getCause();
        assertThat(e.getMessage(), containsString("failed to build [has_required_arguments] after last required field arrived"));
        assertEquals(2, e.getLineNumber());
        e = (XContentParseException) e.getCause();
        assertThat(e.getMessage(), containsString("[has_required_arguments] failed to parse field [a]"));
        assertEquals(2, e.getLineNumber());
        assertEquals("[a] must be less than 10 characters in length but was [supercalifragilisticexpialidocious]",
                e.getCause().getMessage());
    }

    public void testConstructorArgsMustBeConfigured() throws IOException {
        class NoConstructorArgs {
        }
        ConstructingObjectParser<NoConstructorArgs, Void> parser = new ConstructingObjectParser<>(
                "constructor_args_required", (a) -> new NoConstructorArgs());
        try {
            parser.apply(createParser(JsonXContent.jsonXContent, "{}"), null);
            fail("Expected AssertionError");
        } catch (AssertionError e) {
            assertEquals("[constructor_args_required] must configure at least one constructor argument. If it doesn't have any it should "
                    + "use ObjectParser instead of ConstructingObjectParser. This is a bug in the parser declaration.", e.getMessage());
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
        ConstructingObjectParser<CalledOneTime, Void> parser = new ConstructingObjectParser<>("one_time_test",
                (a) -> new CalledOneTime((String) a[0]));
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
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"test\" : \"foo\", \"junk\" : 2 }");
        class TestStruct {
            public final String test;
            TestStruct(String test) {
                this.test = test;
            }
        }
        ConstructingObjectParser<TestStruct, Void> objectParser = new ConstructingObjectParser<>("foo", true, a ->
                new TestStruct((String) a[0]));
        objectParser.declareString(constructorArg(), new ParseField("test"));
        TestStruct s = objectParser.apply(parser, null);
        assertEquals(s.test, "foo");
    }

    public void testConstructObjectUsingContext() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"animal\": \"dropbear\", \"mineral\": -8 }");
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

        public static final List<ConstructingObjectParser<HasCtorArguments, Void>> ALL_PARSERS =
                List.of(PARSER, PARSER_VEGETABLE_OPTIONAL, PARSER_ALL_OPTIONAL);

        public static final ConstructingObjectParser<HasCtorArguments, Integer> PARSER_INT_CONTEXT = buildContextParser();

        private static ConstructingObjectParser<HasCtorArguments, Void> buildParser(boolean animalRequired,
                boolean vegetableRequired) {
            ConstructingObjectParser<HasCtorArguments, Void> parser = new ConstructingObjectParser<>(
                    "has_required_arguments", a -> new HasCtorArguments((String) a[0], (Integer) a[1]));
            parser.declareString(animalRequired ? constructorArg() : optionalConstructorArg(), new ParseField("animal"));
            parser.declareInt(vegetableRequired ? constructorArg() : optionalConstructorArg(), new ParseField("vegetable"));
            declareSetters(parser);
            return parser;
        }

        private static ConstructingObjectParser<HasCtorArguments, Integer> buildContextParser() {
            ConstructingObjectParser<HasCtorArguments, Integer> parser = new ConstructingObjectParser<>(
                    "has_required_arguments", false, (args, ctx) -> new HasCtorArguments((String) args[0], ctx));
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
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{\"named\": [ {\"a\": {}} ], \"named_in_constructor\": [ {\"b\": {}} ]}"
        );
        NamedObjectHolder h = NamedObjectHolder.PARSER.apply(parser, null);
        assertThat(h.named, hasSize(1));
        assertEquals("a", h.named.get(0).name);
        assertThat(h.namedInConstructor, hasSize(1));
        assertEquals("b", h.namedInConstructor.get(0).name);
        assertTrue(h.namedSuppliedInOrder);
    }

    public void testParseNamedObjectTwoFieldsInArray() throws IOException {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{\"named\": [ {\"a\": {}, \"b\": {}}], \"named_in_constructor\": [ {\"c\": {}} ]}"
        );
        XContentParseException e = expectThrows(XContentParseException.class, () -> NamedObjectHolder.PARSER.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named]"));
        assertThat(e.getCause().getMessage(),
                containsString("[named] can be a single object with any number of fields " +
                    "or an array where each entry is an object with a single field"));
    }

    public void testParseNamedObjectTwoFieldsInArrayConstructorArg() throws IOException {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{\"named\": [ {\"a\": {}}], \"named_in_constructor\": [ {\"c\": {}, \"d\": {}} ]}"
        );
        XContentParseException e = expectThrows(XContentParseException.class, () -> NamedObjectHolder.PARSER.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named_in_constructor]"));
        assertThat(e.getCause().getMessage(),
                containsString("[named_in_constructor] can be a single object with any number of fields "
                        + "or an array where each entry is an object with a single field"));
    }

    public void testParseNamedObjectNoFieldsInArray() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"named\": [ {} ], \"named_in_constructor\": [ {\"a\": {}} ]}");
        XContentParseException e = expectThrows(XContentParseException.class, () -> NamedObjectHolder.PARSER.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named]"));
        assertThat(e.getCause().getMessage(),
            containsString("[named] can be a single object with any number of fields " +
                "or an array where each entry is an object with a single field"));
    }

    public void testParseNamedObjectNoFieldsInArrayConstructorArg() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"named\": [ {\"a\": {}} ], \"named_in_constructor\": [ {} ]}");
        XContentParseException e = expectThrows(XContentParseException.class, () -> NamedObjectHolder.PARSER.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named_in_constructor]"));
        assertThat(e.getCause().getMessage(),
                containsString("[named_in_constructor] can be a single object with any number of fields "
                        + "or an array where each entry is an object with a single field"));
    }

    public void testParseNamedObjectJunkInArray() throws IOException {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{\"named\": [ \"junk\" ], \"named_in_constructor\": [ {\"a\": {}} ]}"
        );
        XContentParseException e = expectThrows(XContentParseException.class, () -> NamedObjectHolder.PARSER.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named]"));
        assertThat(e.getCause().getMessage(),
                containsString("[named] can be a single object with any number of fields " +
                    "or an array where each entry is an object with a single field"));
    }

    public void testParseNamedObjectJunkInArrayConstructorArg() throws IOException {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{\"named\": [ {\"a\": {}} ], \"named_in_constructor\": [ \"junk\" ]}"
        );
        XContentParseException e = expectThrows(XContentParseException.class, () -> NamedObjectHolder.PARSER.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named_in_constructor]"));
        assertThat(e.getCause().getMessage(),
                containsString("[named_in_constructor] can be a single object with any number of fields "
                        + "or an array where each entry is an object with a single field"));
    }

    public void testParseNamedObjectInOrderNotSupported() throws IOException {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{\"named\": [\n" + "  {\"a\": {}}" + "],\"named_in_constructor\": {\"b\": {}}" + "}"
        );

        // Create our own parser for this test so we can disable support for the "ordered" mode specified by the array above
        @SuppressWarnings("unchecked")
        ConstructingObjectParser<NamedObjectHolder, Void> objectParser = new ConstructingObjectParser<>("named_object_holder",
                a -> new NamedObjectHolder(((List<NamedObject>) a[0])));
        objectParser.declareNamedObjects(ConstructingObjectParser.constructorArg(), NamedObject.PARSER,
                new ParseField("named_in_constructor"));
        objectParser.declareNamedObjects(NamedObjectHolder::setNamed, NamedObject.PARSER, new ParseField("named"));

        // Now firing the xml through it fails
        XContentParseException e = expectThrows(XContentParseException.class, () -> objectParser.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named]"));
        assertEquals("[named] doesn't support arrays. Use a single object with multiple fields.", e.getCause().getMessage());
    }

    public void testParseNamedObjectInOrderNotSupportedConstructorArg() throws IOException {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{\"named\": {\"a\": {}}, \"named_in_constructor\": [ {\"b\": {}} ]}"
        );

        // Create our own parser for this test so we can disable support for the "ordered" mode specified by the array above
        @SuppressWarnings("unchecked")
        ConstructingObjectParser<NamedObjectHolder, Void> objectParser = new ConstructingObjectParser<>("named_object_holder",
                a -> new NamedObjectHolder(((List<NamedObject>) a[0])));
        objectParser.declareNamedObjects(ConstructingObjectParser.constructorArg(), NamedObject.PARSER,
                new ParseField("named_in_constructor"));
        objectParser.declareNamedObjects(NamedObjectHolder::setNamed, NamedObject.PARSER, new ParseField("named"));

        // Now firing the xml through it fails
        XContentParseException e = expectThrows(XContentParseException.class, () -> objectParser.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named_in_constructor]"));
        assertThat(e.getCause().getMessage(),
            containsString("[named_in_constructor] doesn't support arrays. Use a single object with multiple fields."));
    }

    static class NamedObjectHolder {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<NamedObjectHolder, Void> PARSER = new ConstructingObjectParser<>("named_object_holder",
                a -> new NamedObjectHolder(((List<NamedObject>) a[0])));
        static {
            PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(), NamedObject.PARSER, NamedObjectHolder::keepNamedInOrder,
                    new ParseField("named_in_constructor"));
            PARSER.declareNamedObjects(NamedObjectHolder::setNamed, NamedObject.PARSER, NamedObjectHolder::keepNamedInOrder,
                    new ParseField("named"));
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
}
