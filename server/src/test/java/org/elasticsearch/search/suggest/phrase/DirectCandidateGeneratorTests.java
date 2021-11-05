/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.JaroWinklerDistance;
import org.apache.lucene.search.spell.LevenshteinDistance;
import org.apache.lucene.search.spell.LuceneLevenshteinDistance;
import org.apache.lucene.search.spell.NGramDistance;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

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

    public void testFromString() {
        assertThat(DirectCandidateGeneratorBuilder.resolveDistance("internal"), equalTo(DirectSpellChecker.INTERNAL_LEVENSHTEIN));
        assertThat(DirectCandidateGeneratorBuilder.resolveDistance("damerau_levenshtein"), instanceOf(LuceneLevenshteinDistance.class));
        assertThat(DirectCandidateGeneratorBuilder.resolveDistance("levenshtein"), instanceOf(LevenshteinDistance.class));
        assertThat(DirectCandidateGeneratorBuilder.resolveDistance("jaro_winkler"), instanceOf(JaroWinklerDistance.class));
        assertThat(DirectCandidateGeneratorBuilder.resolveDistance("ngram"), instanceOf(NGramDistance.class));

        expectThrows(IllegalArgumentException.class, () -> DirectCandidateGeneratorBuilder.resolveDistance("doesnt_exist"));
        expectThrows(NullPointerException.class, () -> DirectCandidateGeneratorBuilder.resolveDistance(null));
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
            () -> mutation.stringDistance(original.stringDistance() == null ? "levenshtein" : original.stringDistance() + "_other")
        );
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
            try (XContentParser parser = createParser(shuffleXContent(builder))) {
                parser.nextToken();
                DirectCandidateGeneratorBuilder secondGenerator = DirectCandidateGeneratorBuilder.PARSER.apply(parser, null);
                assertNotSame(generator, secondGenerator);
                assertEquals(generator, secondGenerator);
                assertEquals(generator.hashCode(), secondGenerator.hashCode());
            }
        }
    }

    public static void assertEqualGenerators(
        PhraseSuggestionContext.DirectCandidateGenerator first,
        PhraseSuggestionContext.DirectCandidateGenerator second
    ) {
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
        assertIllegalXContent(directGenerator, IllegalArgumentException.class, "Required [field]");

        // test unknown field
        directGenerator = "{ \"unknown_param\" : \"f1\" }";
        assertIllegalXContent(directGenerator, IllegalArgumentException.class, "[direct_generator] unknown field [unknown_param]");

        // test bad value for field (e.g. size expects an int)
        directGenerator = "{ \"size\" : \"xxl\" }";
        assertIllegalXContent(directGenerator, XContentParseException.class, "[direct_generator] failed to parse field [size]");

        // test unexpected token
        directGenerator = "{ \"size\" : [ \"xxl\" ] }";
        assertIllegalXContent(
            directGenerator,
            XContentParseException.class,
            "[direct_generator] size doesn't support values of type: START_ARRAY"
        );
    }

    public void testFrequencyThreshold() throws Exception {
        try (Directory dir = newDirectory()) {
            IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
            int numDocs = randomIntBetween(10, 20);
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                if (i == 0) {
                    for (int j = 0; j < numDocs; j++) {
                        doc.add(new TextField("field", "fooz", Field.Store.NO));
                    }
                } else {
                    doc.add(new TextField("field", "foo", Field.Store.NO));
                }
                writer.addDocument(doc);
            }
            try (IndexReader reader = DirectoryReader.open(writer)) {
                writer.close();
                DirectSpellChecker spellchecker = new DirectSpellChecker();
                DirectCandidateGenerator generator = new DirectCandidateGenerator(
                    spellchecker,
                    "field",
                    SuggestMode.SUGGEST_MORE_POPULAR,
                    reader,
                    0f,
                    10
                );
                DirectCandidateGenerator.CandidateSet candidateSet = generator.drawCandidates(
                    new DirectCandidateGenerator.CandidateSet(
                        DirectCandidateGenerator.Candidate.EMPTY,
                        generator.createCandidate(new BytesRef("fooz"), false)
                    )
                );
                assertThat(candidateSet.candidates.length, equalTo(1));
                assertThat(candidateSet.candidates[0].termStats.docFreq, equalTo(numDocs - 1));
                assertThat(candidateSet.candidates[0].termStats.totalTermFreq, equalTo((long) numDocs - 1));

                // test that it doesn't overflow
                assertThat(generator.thresholdTermFrequency(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

                spellchecker = new DirectSpellChecker();
                spellchecker.setThresholdFrequency(0.5f);
                generator = new DirectCandidateGenerator(spellchecker, "field", SuggestMode.SUGGEST_MORE_POPULAR, reader, 0f, 10);
                candidateSet = generator.drawCandidates(
                    new DirectCandidateGenerator.CandidateSet(
                        DirectCandidateGenerator.Candidate.EMPTY,
                        generator.createCandidate(new BytesRef("fooz"), false)
                    )
                );
                assertThat(candidateSet.candidates.length, equalTo(1));
                assertThat(candidateSet.candidates[0].termStats.docFreq, equalTo(numDocs - 1));
                assertThat(candidateSet.candidates[0].termStats.totalTermFreq, equalTo((long) numDocs - 1));

                // test that it doesn't overflow
                assertThat(generator.thresholdTermFrequency(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

                spellchecker = new DirectSpellChecker();
                spellchecker.setThresholdFrequency(0.5f);
                generator = new DirectCandidateGenerator(spellchecker, "field", SuggestMode.SUGGEST_ALWAYS, reader, 0f, 10);
                candidateSet = generator.drawCandidates(
                    new DirectCandidateGenerator.CandidateSet(
                        DirectCandidateGenerator.Candidate.EMPTY,
                        generator.createCandidate(new BytesRef("fooz"), false)
                    )
                );
                assertThat(candidateSet.candidates.length, equalTo(01));

                // test that it doesn't overflow
                assertThat(generator.thresholdTermFrequency(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));
            }
        }

    }

    private void assertIllegalXContent(String directGenerator, Class<? extends Exception> exceptionClass, String exceptionMsg)
        throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, directGenerator)) {
            Exception e = expectThrows(exceptionClass, () -> DirectCandidateGeneratorBuilder.PARSER.apply(parser, null));
            assertThat(e.getMessage(), containsString(exceptionMsg));
        }
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
        maybeSet(generator::stringDistance, randomFrom("internal", "damerau_levenshtein", "levenshtein", "jaro_winkler", "ngram"));
        maybeSet(generator::suggestMode, randomFrom("missing", "popular", "always"));
        return generator;
    }

    private static DirectCandidateGeneratorBuilder copy(DirectCandidateGeneratorBuilder original) throws IOException {
        return copyWriteable(original, new NamedWriteableRegistry(Collections.emptyList()), DirectCandidateGeneratorBuilder::new);
    }
}
