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

package org.elasticsearch.search.highlight;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder.Field;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class HighlightBuilderTests extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    private static NamedWriteableRegistry namedWriteableRegistry;

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() {
        if (namedWriteableRegistry == null) {
            namedWriteableRegistry = new NamedWriteableRegistry();
            namedWriteableRegistry.registerPrototype(QueryBuilder.class, new MatchAllQueryBuilder());
            namedWriteableRegistry.registerPrototype(QueryBuilder.class, new IdsQueryBuilder());
            namedWriteableRegistry.registerPrototype(QueryBuilder.class, new TermQueryBuilder("field", "value"));
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
    }

    /**
     * Test serialization and deserialization of the highlighter builder
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            HighlightBuilder original = randomHighlighterBuilder();
            HighlightBuilder deserialized = serializedCopy(original);
            assertEquals(deserialized, original);
            assertEquals(deserialized.hashCode(), original.hashCode());
            assertNotSame(deserialized, original);
        }
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            HighlightBuilder firstBuilder = randomHighlighterBuilder();
            assertFalse("highlighter is equal to null", firstBuilder.equals(null));
            assertFalse("highlighter is equal to incompatible type", firstBuilder.equals(""));
            assertTrue("highlighter is not equal to self", firstBuilder.equals(firstBuilder));
            assertThat("same highlighter's hashcode returns different values if called multiple times", firstBuilder.hashCode(),
                    equalTo(firstBuilder.hashCode()));
            assertThat("different highlighters should not be equal", mutate(firstBuilder), not(equalTo(firstBuilder)));

            HighlightBuilder secondBuilder = serializedCopy(firstBuilder);
            assertTrue("highlighter is not equal to self", secondBuilder.equals(secondBuilder));
            assertTrue("highlighter is not equal to its copy", firstBuilder.equals(secondBuilder));
            assertTrue("equals is not symmetric", secondBuilder.equals(firstBuilder));
            assertThat("highlighter copy's hashcode is different from original hashcode", secondBuilder.hashCode(), equalTo(firstBuilder.hashCode()));

            HighlightBuilder thirdBuilder = serializedCopy(secondBuilder);
            assertTrue("highlighter is not equal to self", thirdBuilder.equals(thirdBuilder));
            assertTrue("highlighter is not equal to its copy", secondBuilder.equals(thirdBuilder));
            assertThat("highlighter copy's hashcode is different from original hashcode", secondBuilder.hashCode(), equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not transitive", firstBuilder.equals(thirdBuilder));
            assertThat("highlighter copy's hashcode is different from original hashcode", firstBuilder.hashCode(), equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not symmetric", thirdBuilder.equals(secondBuilder));
            assertTrue("equals is not symmetric", thirdBuilder.equals(firstBuilder));
        }
    }

    /**
     * create random shape that is put under test
     */
    private static HighlightBuilder randomHighlighterBuilder() {
        HighlightBuilder testHighlighter = new HighlightBuilder();
        setRandomCommonOptions(testHighlighter);
        testHighlighter.useExplicitFieldOrder(randomBoolean());
        if (randomBoolean()) {
            testHighlighter.encoder(randomFrom(Arrays.asList(new String[]{"default", "html"})));
        }
        int numberOfFields = randomIntBetween(1,5);
        for (int i = 0; i < numberOfFields; i++) {
            Field field = new Field(randomAsciiOfLengthBetween(1, 10));
            setRandomCommonOptions(field);
            if (randomBoolean()) {
                field.fragmentOffset(randomIntBetween(1, 100));
            }
            if (randomBoolean()) {
                field.matchedFields(randomStringArray(0, 4));
            }
            testHighlighter.field(field);
        }
        return testHighlighter;
    }

    private static void setRandomCommonOptions(AbstractHighlighterBuilder highlightBuilder) {
        if (randomBoolean()) {
            highlightBuilder.preTags(randomStringArray(0, 3));
        }
        if (randomBoolean()) {
            highlightBuilder.postTags(randomStringArray(0, 3));
        }
        if (randomBoolean()) {
            highlightBuilder.fragmentSize(randomIntBetween(0, 100));
        }
        if (randomBoolean()) {
            highlightBuilder.numOfFragments(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            highlightBuilder.highlighterType(randomAsciiOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            highlightBuilder.fragmenter(randomAsciiOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            QueryBuilder highlightQuery;
            switch (randomInt(2)) {
            case 0:
                highlightQuery = new MatchAllQueryBuilder();
                break;
            case 1:
                highlightQuery = new IdsQueryBuilder();
                break;
            default:
            case 2:
                highlightQuery = new TermQueryBuilder(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10));
                break;
            }
            highlightQuery.boost((float) randomDoubleBetween(0, 10, false));
            highlightBuilder.highlightQuery(highlightQuery);
        }
        if (randomBoolean()) {
            highlightBuilder.order(randomAsciiOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            highlightBuilder.highlightFilter(randomBoolean());
        }
        if (randomBoolean()) {
            highlightBuilder.forceSource(randomBoolean());
        }
        if (randomBoolean()) {
            highlightBuilder.boundaryMaxScan(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            highlightBuilder.boundaryChars(randomAsciiOfLengthBetween(1, 10).toCharArray());
        }
        if (randomBoolean()) {
            highlightBuilder.noMatchSize(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            highlightBuilder.phraseLimit(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            int items = randomIntBetween(0, 5);
            Map<String, Object> options = new HashMap<String, Object>(items);
            for (int i = 0; i < items; i++) {
                Object value = null;
                switch (randomInt(2)) {
                case 0:
                    value = randomAsciiOfLengthBetween(1, 10);
                    break;
                case 1:
                    value = new Integer(randomInt(1000));
                    break;
                case 2:
                    value = new Boolean(randomBoolean());
                    break;
                }
                options.put(randomAsciiOfLengthBetween(1, 10), value);
            }
        }
        if (randomBoolean()) {
            highlightBuilder.requireFieldMatch(randomBoolean());
        }
    }

    @SuppressWarnings("unchecked")
    private static void mutateCommonOptions(AbstractHighlighterBuilder highlightBuilder) {
        switch (randomIntBetween(1, 16)) {
        case 1:
            highlightBuilder.preTags(randomStringArray(4, 6));
            break;
        case 2:
            highlightBuilder.postTags(randomStringArray(4, 6));
            break;
        case 3:
            highlightBuilder.fragmentSize(randomIntBetween(101, 200));
            break;
        case 4:
            highlightBuilder.numOfFragments(randomIntBetween(11, 20));
            break;
        case 5:
            highlightBuilder.highlighterType(randomAsciiOfLengthBetween(11, 20));
            break;
        case 6:
            highlightBuilder.fragmenter(randomAsciiOfLengthBetween(11, 20));
            break;
        case 7:
            highlightBuilder.highlightQuery(new TermQueryBuilder(randomAsciiOfLengthBetween(11, 20), randomAsciiOfLengthBetween(11, 20)));
            break;
        case 8:
            highlightBuilder.order(randomAsciiOfLengthBetween(11, 20));
            break;
        case 9:
            highlightBuilder.highlightFilter(toggleOrSet(highlightBuilder.highlightFilter()));
        case 10:
            highlightBuilder.forceSource(toggleOrSet(highlightBuilder.forceSource()));
            break;
        case 11:
            highlightBuilder.boundaryMaxScan(randomIntBetween(11, 20));
            break;
        case 12:
            highlightBuilder.boundaryChars(randomAsciiOfLengthBetween(11, 20).toCharArray());
            break;
        case 13:
            highlightBuilder.noMatchSize(randomIntBetween(11, 20));
            break;
        case 14:
            highlightBuilder.phraseLimit(randomIntBetween(11, 20));
            break;
        case 15:
            int items = 6;
            Map<String, Object> options = new HashMap<String, Object>(items);
            for (int i = 0; i < items; i++) {
                options.put(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10));
            }
            highlightBuilder.options(options);
            break;
        case 16:
            highlightBuilder.requireFieldMatch(toggleOrSet(highlightBuilder.requireFieldMatch()));
            break;
        }
    }

    private static Boolean toggleOrSet(Boolean flag) {
        if (flag == null) {
            return randomBoolean();
        } else {
            return !flag.booleanValue();
        }
    }

    private static String[] randomStringArray(int minSize, int maxSize) {
        int size = randomIntBetween(minSize, maxSize);
        String[] randomStrings = new String[size];
        for (int f = 0; f < size; f++) {
            randomStrings[f] = randomAsciiOfLengthBetween(1, 10);
        }
        return randomStrings;
    }

    /**
     * mutate the given highlighter builder so the returned one is different in one aspect
     */
    private static HighlightBuilder mutate(HighlightBuilder original) throws IOException {
        HighlightBuilder mutation = serializedCopy(original);
        if (randomBoolean()) {
            mutateCommonOptions(mutation);
        } else {
            switch (randomIntBetween(0, 2)) {
                // change settings that only exists on top level
                case 0:
                    mutation.useExplicitFieldOrder(!original.useExplicitFieldOrder()); break;
                case 1:
                    mutation.encoder(original.encoder() + randomAsciiOfLength(2)); break;
                case 2:
                    if (randomBoolean()) {
                        // add another field
                        mutation.field(new Field(randomAsciiOfLength(10)));
                    } else {
                        // change existing fields
                        List<Field> originalFields = original.fields();
                        Field fieldToChange = originalFields.get(randomInt(originalFields.size() - 1));
                        if (randomBoolean()) {
                            fieldToChange.fragmentOffset(randomIntBetween(101, 200));
                        } else {
                            fieldToChange.matchedFields(randomStringArray(5, 10));
                        }
                    }
            }
        }
        return mutation;
    }

    private static HighlightBuilder serializedCopy(HighlightBuilder original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                return HighlightBuilder.PROTOTYPE.readFrom(in);
            }
        }
    }
}
