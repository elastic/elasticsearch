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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class ConstructingObjectParserTests extends ESTestCase {
    private static final ParseFieldMatcherSupplier MATCHER = () -> ParseFieldMatcher.STRICT;

    /**
     * Builds the object in random order and parses it.
     */
    public void testRandomOrder() throws Exception {
        HasCtorArguments expected = new HasCtorArguments(randomAsciiOfLength(5), randomInt());
        expected.setMineral(randomInt());
        expected.setFruit(randomInt());
        expected.setA(randomBoolean() ? null : randomAsciiOfLength(5));
        expected.setB(randomBoolean() ? null : randomAsciiOfLength(5));
        expected.setC(randomBoolean() ? null : randomAsciiOfLength(5));
        expected.setD(randomBoolean());
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        expected.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder = shuffleXContent(builder);
        BytesReference bytes = builder.bytes();
        try (XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes)) {
            HasCtorArguments parsed = randomFrom(HasCtorArguments.ALL_PARSERS).apply(parser, MATCHER);
            assertEquals(expected.animal, parsed.animal);
            assertEquals(expected.vegetable, parsed.vegetable);
            assertEquals(expected.mineral, parsed.mineral);
            assertEquals(expected.fruit, parsed.fruit);
            assertEquals(expected.a, parsed.a);
            assertEquals(expected.b, parsed.b);
            assertEquals(expected.c, parsed.c);
            assertEquals(expected.d, parsed.d);
        } catch (Throwable e) {
            // It is convenient to decorate the error message with the json
            throw new Exception("Error parsing: [" + builder.string() + "]", e);
        }
    }

    public void testMissingAllConstructorArgs() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(
                  "{\n"
                + "  \"mineral\": 1\n"
                + "}");
        ConstructingObjectParser<HasCtorArguments, ParseFieldMatcherSupplier> objectParser = randomBoolean() ? HasCtorArguments.PARSER
                : HasCtorArguments.PARSER_VEGETABLE_OPTIONAL;
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> objectParser.apply(parser, MATCHER));
        if (objectParser == HasCtorArguments.PARSER) {
            assertEquals("Required [animal, vegetable]", e.getMessage());
        } else {
            assertEquals("Required [animal]", e.getMessage());
        }
    }

    public void testMissingAllConstructorArgsButNotRequired() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(
                "{\n"
              + "  \"mineral\": 1\n"
              + "}");
        HasCtorArguments parsed = HasCtorArguments.PARSER_ALL_OPTIONAL.apply(parser, MATCHER);
        assertEquals(1, parsed.mineral);
    }

    public void testMissingSecondConstructorArg() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(
                  "{\n"
                + "  \"mineral\": 1,\n"
                + "  \"animal\": \"cat\"\n"
                + "}");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> HasCtorArguments.PARSER.apply(parser, MATCHER));
        assertEquals("Required [vegetable]", e.getMessage());
    }

    public void testMissingSecondConstructorArgButNotRequired() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(
                "{\n"
              + "  \"mineral\": 1,\n"
              + "  \"animal\": \"cat\"\n"
              + "}");
        @SuppressWarnings("unchecked")
        HasCtorArguments parsed = randomFrom(HasCtorArguments.PARSER_VEGETABLE_OPTIONAL, HasCtorArguments.PARSER_ALL_OPTIONAL).apply(parser,
                MATCHER);
        assertEquals(1, parsed.mineral);
        assertEquals("cat", parsed.animal);
    }

    public void testMissingFirstConstructorArg() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(
                  "{\n"
                + "  \"mineral\": 1,\n"
                + "  \"vegetable\": 2\n"
                + "}");
        @SuppressWarnings("unchecked")
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> randomFrom(HasCtorArguments.PARSER, HasCtorArguments.PARSER_VEGETABLE_OPTIONAL).apply(parser, MATCHER));
        assertEquals("Required [animal]", e.getMessage());
    }

    public void testMissingFirstConstructorArgButNotRequired() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(
                "{\n"
              + "  \"mineral\": 1,\n"
              + "  \"vegetable\": 2\n"
              + "}");
        HasCtorArguments parsed = HasCtorArguments.PARSER_ALL_OPTIONAL.apply(parser, MATCHER);
        assertEquals(1, parsed.mineral);
        assertEquals((Integer) 2, parsed.vegetable);
    }

    public void testRepeatedConstructorParam() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(
                  "{\n"
                + "  \"vegetable\": 1,\n"
                + "  \"vegetable\": 2\n"
                + "}");
        Throwable e = expectThrows(ParsingException.class, () -> randomFrom(HasCtorArguments.ALL_PARSERS).apply(parser, MATCHER));
        assertEquals("[has_required_arguments] failed to parse field [vegetable]", e.getMessage());
        e = e.getCause();
        assertThat(e, instanceOf(IllegalArgumentException.class));
        assertEquals("Can't repeat param [vegetable]", e.getMessage());
    }

    public void testBadParam() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(
                  "{\n"
                + "  \"animal\": \"cat\",\n"
                + "  \"vegetable\": 2,\n"
                + "  \"a\": \"supercalifragilisticexpialidocious\"\n"
                + "}");
        ParsingException e = expectThrows(ParsingException.class, () -> randomFrom(HasCtorArguments.ALL_PARSERS).apply(parser, MATCHER));
        assertEquals("[has_required_arguments] failed to parse field [a]", e.getMessage());
        assertEquals(4, e.getLineNumber());
        assertEquals("[a] must be less than 10 characters in length but was [supercalifragilisticexpialidocious]",
                e.getCause().getMessage());
    }

    public void testBadParamBeforeObjectBuilt() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(
                  "{\n"
                + "  \"a\": \"supercalifragilisticexpialidocious\",\n"
                + "  \"animal\": \"cat\"\n,"
                + "  \"vegetable\": 2\n"
                + "}");
        ParsingException e = expectThrows(ParsingException.class, () -> randomFrom(HasCtorArguments.ALL_PARSERS).apply(parser, MATCHER));
        assertEquals("[has_required_arguments] failed to parse field [vegetable]", e.getMessage());
        assertEquals(4, e.getLineNumber());
        e = (ParsingException) e.getCause();
        assertEquals("failed to build [has_required_arguments] after last required field arrived", e.getMessage());
        assertEquals(2, e.getLineNumber());
        e = (ParsingException) e.getCause();
        assertEquals("[has_required_arguments] failed to parse field [a]", e.getMessage());
        assertEquals(2, e.getLineNumber());
        assertEquals("[a] must be less than 10 characters in length but was [supercalifragilisticexpialidocious]",
                e.getCause().getMessage());
    }

    public void testConstructorArgsMustBeConfigured() throws IOException {
        class NoConstructorArgs {
        }
        ConstructingObjectParser<NoConstructorArgs, ParseFieldMatcherSupplier> parser = new ConstructingObjectParser<>(
                "constructor_args_required", (a) -> new NoConstructorArgs());
        try {
            parser.apply(XContentType.JSON.xContent().createParser("{}"), null);
            fail("Expected AssertionError");
        } catch (AssertionError e) {
            assertEquals("[constructor_args_required] must configure at least on constructor argument. If it doesn't have any it should "
                    + "use ObjectParser instead of ConstructingObjectParser. This is a bug in the parser declaration.", e.getMessage());
        }
    }

    /**
     * Tests the non-constructor fields are only set on time.
     */
    public void testCalledOneTime() throws IOException {
        boolean ctorArgOptional = randomBoolean();
        class CalledOneTime {
            public CalledOneTime(String yeah) {
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
        ConstructingObjectParser<CalledOneTime, ParseFieldMatcherSupplier> parser = new ConstructingObjectParser<>("one_time_test",
                (a) -> new CalledOneTime((String) a[0]));
        parser.declareString(CalledOneTime::setFoo, new ParseField("foo"));
        parser.declareString(ctorArgOptional ? optionalConstructorArg() : constructorArg(), new ParseField("yeah"));

        // ctor arg first so we can test for the bug we found one time
        XContentParser xcontent = XContentType.JSON.xContent().createParser(
                "{\n"
              + "  \"yeah\": \"!\",\n"
              + "  \"foo\": \"foo\"\n"
              + "}");
        CalledOneTime result = parser.apply(xcontent, MATCHER);
        assertTrue(result.fooSet);

        // and ctor arg second just in case
        xcontent = XContentType.JSON.xContent().createParser(
                "{\n"
              + "  \"foo\": \"foo\",\n"
              + "  \"yeah\": \"!\"\n"
              + "}");
        result = parser.apply(xcontent, MATCHER);
        assertTrue(result.fooSet);

        if (ctorArgOptional) {
            // and without the constructor arg if we've made it optional
            xcontent = XContentType.JSON.xContent().createParser(
                    "{\n"
                  + "  \"foo\": \"foo\"\n"
                  + "}");
            result = parser.apply(xcontent, MATCHER);
        }
        assertTrue(result.fooSet);
    }

    private static class HasCtorArguments implements ToXContent {
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

        public HasCtorArguments(@Nullable String animal, @Nullable Integer vegetable) {
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
        public static final ConstructingObjectParser<HasCtorArguments, ParseFieldMatcherSupplier> PARSER = buildParser(true, true);
        public static final ConstructingObjectParser<HasCtorArguments, ParseFieldMatcherSupplier> PARSER_VEGETABLE_OPTIONAL = buildParser(
                true, false);
        public static final ConstructingObjectParser<HasCtorArguments, ParseFieldMatcherSupplier> PARSER_ALL_OPTIONAL = buildParser(false,
                false);
        public static final List<ConstructingObjectParser<HasCtorArguments, ParseFieldMatcherSupplier>> ALL_PARSERS = unmodifiableList(
                Arrays.asList(PARSER, PARSER_VEGETABLE_OPTIONAL, PARSER_ALL_OPTIONAL));

        private static ConstructingObjectParser<HasCtorArguments, ParseFieldMatcherSupplier> buildParser(boolean animalRequired,
                boolean vegetableRequired) {
            ConstructingObjectParser<HasCtorArguments, ParseFieldMatcherSupplier> parser = new ConstructingObjectParser<>(
                    "has_required_arguments", a -> new HasCtorArguments((String) a[0], (Integer) a[1]));
            parser.declareString(animalRequired ? constructorArg() : optionalConstructorArg(), new ParseField("animal"));
            parser.declareInt(vegetableRequired ? constructorArg() : optionalConstructorArg(), new ParseField("vegetable"));
            parser.declareInt(HasCtorArguments::setMineral, new ParseField("mineral"));
            parser.declareInt(HasCtorArguments::setFruit, new ParseField("fruit"));
            parser.declareString(HasCtorArguments::setA, new ParseField("a"));
            parser.declareString(HasCtorArguments::setB, new ParseField("b"));
            parser.declareString(HasCtorArguments::setC, new ParseField("c"));
            parser.declareBoolean(HasCtorArguments::setD, new ParseField("d"));
            return parser;
        }
    }
}
