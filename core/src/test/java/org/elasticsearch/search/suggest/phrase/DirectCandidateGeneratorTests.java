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

package org.elasticsearch.search.suggest.phrase;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionContext.DirectCandidateGenerator;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import static org.hamcrest.Matchers.equalTo;

public class DirectCandidateGeneratorTests extends ESTestCase{

    private static final IndicesQueriesRegistry mockRegistry = new IndicesQueriesRegistry();
    private static final int NUMBER_OF_RUNS = 20;



    /**
     * Test serialization and deserialization of the generator
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_RUNS; runs++) {
            DirectCandidateGeneratorBuilder original = randomCandidateGenerator();
            DirectCandidateGeneratorBuilder deserialized = serializedCopy(original);
            assertEquals(deserialized, original);
            assertEquals(deserialized.hashCode(), original.hashCode());
            assertNotSame(deserialized, original);
        }
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_RUNS; runs++) {
            DirectCandidateGeneratorBuilder first = randomCandidateGenerator();
            assertFalse("generator is equal to null", first.equals(null));
            assertFalse("generator is equal to incompatible type", first.equals(""));
            assertTrue("generator is not equal to self", first.equals(first));
            assertThat("same generator's hashcode returns different values if called multiple times", first.hashCode(),
                    equalTo(first.hashCode()));

            DirectCandidateGeneratorBuilder second = serializedCopy(first);
            assertTrue("generator is not equal to self", second.equals(second));
            assertTrue("generator is not equal to its copy", first.equals(second));
            assertTrue("equals is not symmetric", second.equals(first));
            assertThat("generator copy's hashcode is different from original hashcode", second.hashCode(), equalTo(first.hashCode()));

            DirectCandidateGeneratorBuilder third = serializedCopy(second);
            assertTrue("generator is not equal to self", third.equals(third));
            assertTrue("generator is not equal to its copy", second.equals(third));
            assertThat("generator copy's hashcode is different from original hashcode", second.hashCode(), equalTo(third.hashCode()));
            assertTrue("equals is not transitive", first.equals(third));
            assertThat("generator copy's hashcode is different from original hashcode", first.hashCode(), equalTo(third.hashCode()));
            assertTrue("equals is not symmetric", third.equals(second));
            assertTrue("equals is not symmetric", third.equals(first));

            // test for non-equality, check that all fields are covered by changing one by one
            first = new DirectCandidateGeneratorBuilder("aaa");
            assertEquals(first, serializedCopy(first));
            second = new DirectCandidateGeneratorBuilder("bbb");
            assertNotEquals(first, second);
            assertNotEquals(first.accuracy(0.1f), serializedCopy(first).accuracy(0.2f));
            assertNotEquals(first.maxEdits(1), serializedCopy(first).maxEdits(2));
            assertNotEquals(first.maxInspections(1), serializedCopy(first).maxInspections(2));
            assertNotEquals(first.maxTermFreq(0.1f), serializedCopy(first).maxTermFreq(0.2f));
            assertNotEquals(first.minDocFreq(0.1f), serializedCopy(first).minDocFreq(0.2f));
            assertNotEquals(first.minWordLength(1), serializedCopy(first).minWordLength(2));
            assertNotEquals(first.postFilter("postFilter"), serializedCopy(first).postFilter("postFilter_other"));
            assertNotEquals(first.preFilter("preFilter"), serializedCopy(first).preFilter("preFilter_other"));
            assertNotEquals(first.prefixLength(1), serializedCopy(first).prefixLength(2));
            assertNotEquals(first.size(1), serializedCopy(first).size(2));
            assertNotEquals(first.sort("score"), serializedCopy(first).sort("frequency"));
            assertNotEquals(first.stringDistance("levenstein"), serializedCopy(first).sort("ngram"));
            assertNotEquals(first.suggestMode("missing"), serializedCopy(first).suggestMode("always"));
        }
    }

    /**
     *  creates random candidate generator, renders it to xContent and back to new instance that should be equal to original
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_RUNS; runs++) {
            DirectCandidateGeneratorBuilder generator = randomCandidateGenerator();
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                builder.prettyPrint();
            }
            generator.toXContent(builder, ToXContent.EMPTY_PARAMS);
            XContentParser parser = XContentHelper.createParser(shuffleXContent(builder).bytes());
            QueryParseContext context = new QueryParseContext(mockRegistry, parser, ParseFieldMatcher.STRICT);
            parser.nextToken();
            DirectCandidateGeneratorBuilder secondGenerator = DirectCandidateGeneratorBuilder.fromXContent(context);
            assertNotSame(generator, secondGenerator);
            assertEquals(generator, secondGenerator);
            assertEquals(generator.hashCode(), secondGenerator.hashCode());
        }
    }

    public static void assertEqualGenerators(DirectCandidateGenerator first, DirectCandidateGenerator second) {
        assertEquals(first.field(), second.field());
        assertEquals(first.accuracy(), second.accuracy(), Float.MIN_VALUE);
        assertEquals(first.maxTermFreq(), second.maxTermFreq(), Float.MIN_VALUE);
        assertEquals(first.maxEdits(), second.maxEdits());
        assertEquals(first.maxInspections(), second.maxInspections());
        assertEquals(first.minDocFreq(), second.minDocFreq(), Float.MIN_VALUE);
        assertEquals(first.minWordLength(), second.minWordLength());
        assertEquals(first.postFilter(), second.postFilter());
        assertEquals(first.prefixLength(), second.prefixLength());
        assertEquals(first.preFilter(), second.preFilter());
        assertEquals(first.sort(), second.sort());
        assertEquals(first.size(), second.size());
        // some instances of StringDistance don't support equals, just checking the class here
        assertEquals(first.stringDistance().getClass(), second.stringDistance().getClass());
        assertEquals(first.suggestMode(), second.suggestMode());
    }

    /**
     * test that bad xContent throws exception
     */
    public void testIllegalXContent() throws IOException {
        // test missing fieldname
        String directGenerator = "{ }";
        assertIllegalXContent(directGenerator, IllegalArgumentException.class,
                "[direct_generator] expects exactly one field parameter, but found []");

        // test two fieldnames
        directGenerator = "{ \"field\" : \"f1\", \"field\" : \"f2\" }";
        assertIllegalXContent(directGenerator, IllegalArgumentException.class,
                "[direct_generator] expects exactly one field parameter, but found [f2, f1]");

        // test unknown field
        directGenerator = "{ \"unknown_param\" : \"f1\" }";
        assertIllegalXContent(directGenerator, IllegalArgumentException.class,
                "[direct_generator] unknown field [unknown_param], parser not found");

        // test bad value for field (e.g. size expects an int)
        directGenerator = "{ \"size\" : \"xxl\" }";
        assertIllegalXContent(directGenerator, ParsingException.class,
                "[direct_generator] failed to parse field [size]");

        // test unexpected token
        directGenerator = "{ \"size\" : [ \"xxl\" ] }";
        assertIllegalXContent(directGenerator, IllegalArgumentException.class,
                "[direct_generator] size doesn't support values of type: START_ARRAY");
    }

    private static void assertIllegalXContent(String directGenerator, Class<? extends Exception> exceptionClass, String exceptionMsg)
            throws IOException {
        XContentParser parser = XContentFactory.xContent(directGenerator).createParser(directGenerator);
        QueryParseContext context = new QueryParseContext(mockRegistry, parser, ParseFieldMatcher.STRICT);
        Exception e = expectThrows(exceptionClass, () -> DirectCandidateGeneratorBuilder.fromXContent(context));
        assertEquals(exceptionMsg, e.getMessage());
    }

    /**
     * create random {@link DirectCandidateGeneratorBuilder}
     */
    public static DirectCandidateGeneratorBuilder randomCandidateGenerator() {
        DirectCandidateGeneratorBuilder generator = new DirectCandidateGeneratorBuilder(randomAsciiOfLength(10));
        maybeSet(generator::accuracy, randomFloat());
        maybeSet(generator::maxEdits, randomIntBetween(1, 2));
        maybeSet(generator::maxInspections, randomIntBetween(1, 20));
        maybeSet(generator::maxTermFreq, randomFloat());
        maybeSet(generator::minDocFreq, randomFloat());
        maybeSet(generator::minWordLength, randomIntBetween(1, 20));
        maybeSet(generator::prefixLength, randomIntBetween(1, 20));
        maybeSet(generator::preFilter, randomAsciiOfLengthBetween(1, 20));
        maybeSet(generator::postFilter, randomAsciiOfLengthBetween(1, 20));
        maybeSet(generator::size, randomIntBetween(1, 20));
        maybeSet(generator::sort, randomFrom("score", "frequency"));
        maybeSet(generator::stringDistance, randomFrom("internal", "damerau_levenshtein", "levenstein", "jarowinkler", "ngram"));
        maybeSet(generator::suggestMode, randomFrom("missing", "popular", "always"));
        return generator;
    }

    private static DirectCandidateGeneratorBuilder serializedCopy(DirectCandidateGeneratorBuilder original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = StreamInput.wrap(output.bytes())) {
                return new DirectCandidateGeneratorBuilder(in);
            }
        }
    }
}
