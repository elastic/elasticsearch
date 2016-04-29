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
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static java.util.Collections.emptySet;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.hamcrest.Matchers.instanceOf;

public class ConstructingObjectParserTests extends ESTestCase {
    private static final ParseFieldMatcherSupplier MATCHER = () -> ParseFieldMatcher.STRICT;

    /**
     * Builds the object in random order and parses it.
     */
    public void testRandomOrder() throws Exception {
        HasRequiredArguments expected = new HasRequiredArguments(randomAsciiOfLength(5), randomInt());
        expected.setMineral(randomInt());
        expected.setFruit(randomInt());
        expected.setA(randomBoolean() ? null : randomAsciiOfLength(5));
        expected.setB(randomBoolean() ? null : randomAsciiOfLength(5));
        expected.setC(randomBoolean() ? null : randomAsciiOfLength(5));
        expected.setD(randomBoolean());
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        expected.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder = shuffleXContent(builder, emptySet());
        BytesReference bytes = builder.bytes();
        XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes);
        try {
            HasRequiredArguments parsed = HasRequiredArguments.PARSER.apply(parser, MATCHER);
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

    public void testMissingAllConstructorParams() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(
                  "{\n"
                + "  \"mineral\": 1\n"
                + "}");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> HasRequiredArguments.PARSER.apply(parser, MATCHER));
        assertEquals("Required [animal, vegetable]", e.getMessage());
    }

    public void testMissingSecondConstructorParam() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(
                  "{\n"
                + "  \"mineral\": 1,\n"
                + "  \"animal\": \"cat\"\n"
                + "}");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> HasRequiredArguments.PARSER.apply(parser, MATCHER));
        assertEquals("Required [vegetable]", e.getMessage());
    }

    public void testMissingFirstConstructorParam() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(
                  "{\n"
                + "  \"mineral\": 1,\n"
                + "  \"vegetable\": 2\n"
                + "}");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> HasRequiredArguments.PARSER.apply(parser, MATCHER));
        assertEquals("Required [animal]", e.getMessage());
    }

    public void testRepeatedConstructorParam() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(
                  "{\n"
                + "  \"vegetable\": 1,\n"
                + "  \"vegetable\": 2\n"
                + "}");
        Throwable e = expectThrows(ParsingException.class, () -> HasRequiredArguments.PARSER.apply(parser, MATCHER));
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
        ParsingException e = expectThrows(ParsingException.class, () -> HasRequiredArguments.PARSER.apply(parser, MATCHER));
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
        ParsingException e = expectThrows(ParsingException.class, () -> HasRequiredArguments.PARSER.apply(parser, MATCHER));
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
        Exception e = expectThrows(IllegalStateException.class, () -> parser.apply(XContentType.JSON.xContent().createParser("{}"), null));
        assertEquals("[constructor_args_required] must configure at least on constructor argument. If it doens't have any it "
                + "should use ObjectParser instead of ConstructingObjectParser. This is a bug in the parser declaration.", e.getMessage());
    }

    /**
     * Tests the non-constructor fields are only set on time.
     */
    public void testCalledOneTime() throws IOException {
        class CalledOneTime {
            public CalledOneTime(String yeah) {
                assertEquals("!", yeah);
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
        parser.declareString(constructorArg(), new ParseField("yeah"));

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
    }


    private static class HasRequiredArguments implements ToXContent {
        final String animal;
        final int vegetable;
        int mineral;
        int fruit;
        String a;
        String b;
        String c;
        boolean d;

        public HasRequiredArguments(String animal, int vegetable) {
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

        public static final ConstructingObjectParser<HasRequiredArguments, ParseFieldMatcherSupplier> PARSER =
                new ConstructingObjectParser<>("has_required_arguments", a -> new HasRequiredArguments((String) a[0], (Integer) a[1]));
        static {
            PARSER.declareString(constructorArg(), new ParseField("animal"));
            PARSER.declareInt(constructorArg(), new ParseField("vegetable"));
            PARSER.declareInt(HasRequiredArguments::setMineral, new ParseField("mineral"));
            PARSER.declareInt(HasRequiredArguments::setFruit, new ParseField("fruit"));
            PARSER.declareString(HasRequiredArguments::setA, new ParseField("a"));
            PARSER.declareString(HasRequiredArguments::setB, new ParseField("b"));
            PARSER.declareString(HasRequiredArguments::setC, new ParseField("c"));
            PARSER.declareBoolean(HasRequiredArguments::setD, new ParseField("d"));
        }
    }
}
