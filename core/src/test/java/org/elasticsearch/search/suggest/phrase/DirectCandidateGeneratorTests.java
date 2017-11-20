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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionContext.DirectCandidateGenerator;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class DirectCandidateGeneratorTests extends ESTestCase {
    private static final int NUMBER_OF_RUNS = 20;

    /**
     * Test serialization and deserialization of the generator
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_RUNS; runs++) {
            DirectCandidateGeneratorBuilder original = randomCandidateGenerator();
            DirectCandidateGeneratorBuilder deserialized = copy(original);
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
            final DirectCandidateGeneratorBuilder original = randomCandidateGenerator();
            checkEqualsAndHashCode(original, DirectCandidateGeneratorTests::copy, DirectCandidateGeneratorTests::mutate);
        }
    }

    private static DirectCandidateGeneratorBuilder mutate(DirectCandidateGeneratorBuilder original) throws IOException {
        DirectCandidateGeneratorBuilder mutation = copy(original);
        List<Supplier<DirectCandidateGeneratorBuilder>> mutators = new ArrayList<>();
        mutators.add(() -> new DirectCandidateGeneratorBuilder(original.field() + "_other"));
        mutators.add(() -> mutation.accuracy(original.accuracy() == null ? 0.1f : original.accuracy() + 0.1f));
        mutators.add(() -> {
            Integer maxEdits = original.maxEdits() == null ? 1 : original.maxEdits();
            if (maxEdits == 1) {
                maxEdits = 2;
            } else {
                maxEdits = 1;
            }
            return mutation.maxEdits(maxEdits);
        });
        mutators.add(() -> mutation.maxInspections(original.maxInspections() == null ? 1 : original.maxInspections() + 1));
        mutators.add(() -> mutation.minWordLength(original.minWordLength() == null ? 1 : original.minWordLength() + 1));
        mutators.add(() -> mutation.prefixLength(original.prefixLength() == null ? 1 : original.prefixLength() + 1));
        mutators.add(() -> mutation.size(original.size() == null ? 1 : original.size() + 1));
        mutators.add(() -> mutation.maxTermFreq(original.maxTermFreq() == null ? 0.1f : original.maxTermFreq() + 0.1f));
        mutators.add(() -> mutation.minDocFreq(original.minDocFreq() == null ? 0.1f : original.minDocFreq() + 0.1f));
        mutators.add(() -> mutation.postFilter(original.postFilter() == null ? "postFilter" : original.postFilter() + "_other"));
        mutators.add(() -> mutation.preFilter(original.preFilter() == null ? "preFilter" : original.preFilter() + "_other"));
        mutators.add(() -> mutation.sort(original.sort() == null ? "score" : original.sort() + "_other"));
        mutators.add(
                () -> mutation.stringDistance(original.stringDistance() == null ? "levenstein" : original.stringDistance() + "_other"));
        mutators.add(() -> mutation.suggestMode(original.suggestMode() == null ? "missing" : original.suggestMode() + "_other"));
        return randomFrom(mutators).get();
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
            XContentParser parser = createParser(shuffleXContent(builder));
            parser.nextToken();
            DirectCandidateGeneratorBuilder secondGenerator = DirectCandidateGeneratorBuilder.PARSER.apply(parser, null);
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
                "Required [field]");

        // test two fieldnames
        if (XContent.isStrictDuplicateDetectionEnabled()) {
            logger.info("Skipping test as it uses a custom duplicate check that is obsolete when strict duplicate checks are enabled.");
        } else {
            directGenerator = "{ \"field\" : \"f1\", \"field\" : \"f2\" }";
            assertIllegalXContent(directGenerator, IllegalArgumentException.class,
                "[direct_generator] failed to parse field [field]");
        }

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
        assertIllegalXContent(directGenerator, ParsingException.class,
                "[direct_generator] size doesn't support values of type: START_ARRAY");
    }

    private void assertIllegalXContent(String directGenerator, Class<? extends Exception> exceptionClass, String exceptionMsg)
            throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, directGenerator);
        Exception e = expectThrows(exceptionClass, () -> DirectCandidateGeneratorBuilder.PARSER.apply(parser, null));
        assertEquals(exceptionMsg, e.getMessage());
    }

    /**
     * create random {@link DirectCandidateGeneratorBuilder}
     */
    public static DirectCandidateGeneratorBuilder randomCandidateGenerator() {
        DirectCandidateGeneratorBuilder generator = new DirectCandidateGeneratorBuilder(randomAlphaOfLength(10));
        maybeSet(generator::accuracy, randomFloat());
        maybeSet(generator::maxEdits, randomIntBetween(1, 2));
        maybeSet(generator::maxInspections, randomIntBetween(1, 20));
        maybeSet(generator::maxTermFreq, randomFloat());
        maybeSet(generator::minDocFreq, randomFloat());
        maybeSet(generator::minWordLength, randomIntBetween(1, 20));
        maybeSet(generator::prefixLength, randomIntBetween(1, 20));
        maybeSet(generator::preFilter, randomAlphaOfLengthBetween(1, 20));
        maybeSet(generator::postFilter, randomAlphaOfLengthBetween(1, 20));
        maybeSet(generator::size, randomIntBetween(1, 20));
        maybeSet(generator::sort, randomFrom("score", "frequency"));
        maybeSet(generator::stringDistance, randomFrom("internal", "damerau_levenshtein", "levenstein", "jarowinkler", "ngram"));
        maybeSet(generator::suggestMode, randomFrom("missing", "popular", "always"));
        return generator;
    }

    private static DirectCandidateGeneratorBuilder copy(DirectCandidateGeneratorBuilder original) throws IOException {
        return copyWriteable(original, new NamedWriteableRegistry(Collections.emptyList()), DirectCandidateGeneratorBuilder::new);
    }
}
