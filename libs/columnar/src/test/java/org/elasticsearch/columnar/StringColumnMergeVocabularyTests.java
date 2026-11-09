/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesConsumer.MergedVocabulary;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.BestCoverage;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.DictionaryStringColumnReader;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnMetadata;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.SummaryPolicy;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class StringColumnMergeVocabularyTests extends ESTestCase {

    public void testMergeSurveyAfterDeletionsExcludesDeletedTerm() throws IOException {
        final DictionaryPolicy policy = new DictionaryPolicy(4, 0.5, 1.0);
        final SummaryPolicy summaryPolicy = SummaryPolicy.sized(4);
        try (Directory dir = newDirectory()) {
            flushAndDeleteTheDeadTerm(dir, policy, summaryPolicy);
            forceMerge(dir, policy, summaryPolicy);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final StringColumnReader column = stringColumn(reader.leaves().get(0).reader());
                assertEquals(200, reader.numDocs());
                assertEquals("the live values are named, not escaping a dead dictionary", 0, column.escapeCount());
                final List<BytesRef> terms = new ArrayList<>();
                final List<Long> counts = new ArrayList<>();
                column.readSummary(terms, counts);
                assertEquals("and only the living term is recorded", List.of(new BytesRef("a")), terms);
                assertEquals(List.of(200L), counts);
            }
        }
    }

    public void testExpungingDeletionsLeavesNoStaleBoundBehind() throws IOException {
        final DictionaryPolicy policy = new DictionaryPolicy(4, 0.5, 1.0);
        final SummaryPolicy summaryPolicy = SummaryPolicy.sized(4);
        try (Directory dir = newDirectory()) {
            flushAndDeleteTheDeadTerm(dir, policy, summaryPolicy);
            forceMerge(dir, policy, summaryPolicy);
            final DictionaryPolicy smaller = new DictionaryPolicy(1, 0.5, 1.0);
            flushSegments(dir, List.of(repeated("a", 100)), smaller, summaryPolicy);
            forceMerge(dir, smaller, summaryPolicy);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final StringColumnReader column = stringColumn(reader.leaves().get(0).reader());
                assertEquals(300, reader.numDocs());
                final BestCoverage bound = column.bestCoverage();
                assertTrue(
                    "one byte names all three hundred, so a bound below that is wrong: " + bound,
                    bound.known() == false || bound.namedValues() >= 300
                );
                assertTrue("and the one live term is named", column.hasDictionary());
                assertEquals("and it is the only one the dictionary holds", List.of("a"), dictionaryTerms(column));
            }
        }
    }

    private static final String MARKER = "marker";
    private static final String FIELD = "keyword";
    private static final int SEGMENTS = 10;
    private static final long SEED = 42;

    private static final Set<String> COLUMNAR_EXTENSIONS = Set.of(
        ColumNARDocValuesFormat.DATA_EXTENSION,
        ColumNARDocValuesFormat.META_EXTENSION,
        ColumNARDocValuesFormat.SKIP_EXTENSION
    );

    /** A bar high enough to refuse a column half of whose reads would still escape. */
    private static final DictionaryPolicy STRICT_COVERAGE_BAR = new DictionaryPolicy(
        StringColumnOptions.DEFAULT_DICTIONARY.maxBytes(),
        0.9,
        StringColumnOptions.DEFAULT_DICTIONARY.maxShareOfColumn()
    );

    /**
     * A steady rate emitter: one head term held many times by every segment, and identifiers held once each. It is what a flush
     * holding about one collection round leaves behind.
     */
    private static final String STEADY_HEAD_TERM = "web-frontend-canary";
    private static final int STEADY_HEAD_VALUES_PER_SEGMENT = 500;
    private static final int STEADY_TAIL_TERMS_PER_SEGMENT = 4_500;

    /** A short head beside values held once in the whole index, which no dictionary should ever name. */
    private static final String SHORT_HEAD_TERM = "INFO";
    private static final int SHORT_HEAD_PAIRS_PER_SEGMENT = 1_000;

    /** A Zipf law over {@link #ZIPF_TERMS} terms sharing {@link #ZIPF_VALUES} values, every term spread evenly over the segments. */
    private static final int ZIPF_TERMS = 2_000;
    private static final int ZIPF_VALUES = 50_000;

    private static final int IDENTIFIER_LENGTH = 32;
    private static final char[] IDENTIFIER_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
    private static final String[] PATH_SECTIONS = { "api", "search", "checkout", "account", "catalog", "assets" };
    private static final String[] PATH_RESOURCES = { "orders", "items", "users", "sessions", "invoices", "products", "recommendations" };

    // NOTE: the bar is stated here rather than taken from the default, which is the subject of its own change.
    public void testShortHeadWithUniqueTailStaysPlain() throws IOException {
        final MergedColumn merged = merge("short head", shortHeadSegments(), STRICT_COVERAGE_BAR);
        assertTrue("no segment names enough of its own values", merged.segmentsArePlain());
        assertFalse("the head term names too few of the values to earn a dictionary", merged.hasDictionary());
    }

    // NOTE: the two shapes are the same distribution and differ only in whether their bytes compress.
    public void testZipfPathsBelowTheHeadAreNamedAfterMerge() throws IOException {
        assertTermsBelowTheHeadAreNamed("url.path", StringColumnMergeVocabularyTests::path);
    }

    public void testZipfIdentifiersBelowTheHeadAreNamedAfterMerge() throws IOException {
        assertTermsBelowTheHeadAreNamed("session.id", StringColumnMergeVocabularyTests::identifier);
    }

    public void testTermsHeldOncePerSegmentEarnADictionaryAfterMerge() throws IOException {
        final MergedColumn merged = merge("steady rate", steadyRateSegments(), StringColumnOptions.DEFAULT_DICTIONARY);
        assertTrue("no segment names enough of its own values", merged.segmentsArePlain());
        assertTrue("a dictionary", merged.hasDictionary());
        assertEquals("the head and every identifier", STEADY_TAIL_TERMS_PER_SEGMENT + 1, merged.terms());
        assertEquals("no value escapes", 0, merged.escapes());
    }

    public void testMergedSummaryKeepsWhatMoreThanOneSegmentHeld() throws IOException {
        final List<List<String>> segments = headAndOneTailPerSegment(2);
        segments.get(0).add("shared");
        segments.get(1).add("shared");
        try (Directory dir = newDirectory()) {
            flushSegments(dir, segments, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            forceMerge(dir, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            assertEquals("everything the inputs held", List.of("head", "shared", "tail0", "tail1"), summaryTerms(dir));
        }
    }

    // NOTE: a null is named by an ordinal of its own in every layout, so it is not a value a dictionary has
    // to name. Counting slots instead would give the merge a denominator its inputs never used, and a column
    // that kept a dictionary at flush could lose it on merging without anything about it having changed.
    public void testNullSlotsAreNotCountedAgainstAMergedDictionary() throws IOException {
        final List<List<String>> segments = new ArrayList<>(2);
        for (int segment = 0; segment < 2; segment++) {
            final List<String> values = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                values.add(SHORT_HEAD_TERM);
                values.add(null);
            }
            values.add(identifier(segment));
            segments.add(values);
        }
        try (Directory dir = newDirectory()) {
            flushSegments(dir, segments, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            try (DirectoryReader flushed = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : flushed.leaves()) {
                    assertTrue("each flush named its head term", stringColumn(leaf.reader()).hasDictionary());
                }
            }
            forceMerge(dir, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            withMergedColumn(dir, column -> {
                assertTrue("and the merge kept one", column.hasDictionary());
                assertEquals("the values a dictionary has to name, the twenty nulls aside", 22, column.summaryValues());
                assertEquals("the two identifiers no segment repeats", 2, column.escapeCount());
            });
        }
    }

    public void testMergedSummaryHonoursItsOwnCap() throws IOException {
        final SummaryPolicy cap = SummaryPolicy.sized(between(1, 32));
        final List<List<String>> segments = headAndOneTailPerSegment(between(2, 6));
        try (Directory dir = newDirectory()) {
            flushSegments(dir, segments, StringColumnOptions.DEFAULT_DICTIONARY, cap);
            forceMerge(dir, StringColumnOptions.DEFAULT_DICTIONARY, cap);
            final long bytes = summaryTerms(dir).stream().mapToLong(term -> Math.max(1, term.length())).sum();
            assertThat("a merge is bound by the cap a flush is bound by", bytes, lessThanOrEqualTo((long) cap.maxBytes()));
        }
    }

    public void testMergedSummaryIsSuppressedWhenDisabled() throws IOException {
        final List<List<String>> segments = headAndOneTailPerSegment(2);
        try (Directory dir = newDirectory()) {
            flushSegments(dir, segments, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            forceMerge(dir, StringColumnOptions.DEFAULT_DICTIONARY, SummaryPolicy.NONE);
            withMergedColumn(dir, column -> {
                assertFalse("NONE summarises no terms", column.hasSummaryTerms());
                assertFalse("nor any numbers", column.hasSummary());
            });
        }
    }

    // NOTE: a truncated summary leaves the merge building its dictionary from a partial picture, and everything
    // that picture misses has to escape correctly, so this reads every value back rather than only the sizes.
    public void testValuesSurviveAMergeFromATruncatedSummary() throws IOException {
        final int segmentCount = between(2, SEGMENTS);
        final int pairsPerSegment = between(50, 200);
        final List<List<String>> segments = new ArrayList<>(segmentCount);
        for (int segment = 0; segment < segmentCount; segment++) {
            final List<String> values = new ArrayList<>();
            for (int i = 0; i < pairsPerSegment; i++) {
                values.add(identifier(segment * pairsPerSegment + i));
                values.add(STEADY_HEAD_TERM);
            }
            segments.add(values);
        }
        // Far less than the identifiers alone would need, so the summary the merge reads is always short of them.
        final SummaryPolicy tinyCap = SummaryPolicy.sized(between(0, 128));
        try (Directory dir = newDirectory()) {
            flushSegments(dir, segments, StringColumnOptions.DEFAULT_DICTIONARY, tinyCap);
            forceMerge(dir, StringColumnOptions.DEFAULT_DICTIONARY, tinyCap);
            assertThat(
                "a merge is bound by the cap a flush is bound by",
                summaryTerms(dir).stream().mapToLong(term -> Math.max(1, term.length())).sum(),
                lessThanOrEqualTo((long) tinyCap.maxBytes())
            );
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("force-merged to one segment", 1, reader.leaves().size());
                assertValues(segments, reader.leaves().get(0).reader());
            }
        }
    }

    private static void assertValues(List<List<String>> segments, LeafReader leaf) throws IOException {
        final List<String> expected = new ArrayList<>();
        segments.forEach(expected::addAll);
        final BinaryDocValues values = leaf.getBinaryDocValues(FIELD);
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        for (int doc = 0; doc < expected.size(); doc++) {
            assertEquals("doc " + doc, doc, values.advance(doc));
            assertEquals("slots at doc " + doc, 1, decoder.reset(values.binaryValue()));
            assertEquals("value at doc " + doc, expected.get(doc), decoder.next().utf8ToString());
        }
    }

    // NOTE: a term any input holds twice reaches the same column either way; one held once by two inputs does not,
    // which the test below pins.
    public void testStagedMergesReachTheSameColumnForTermsAnInputRepeats() throws IOException {
        final List<List<String>> segments = headAndOneTailPerSegment(4);
        try (Directory oneStep = newDirectory(); Directory staged = newDirectory(); Directory half = newDirectory()) {
            flushSegments(oneStep, segments, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            forceMerge(oneStep, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);

            flushSegments(staged, segments.subList(0, 2), StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            forceMerge(staged, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            flushSegments(half, segments.subList(2, 4), StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            forceMerge(half, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            try (
                IndexWriter writer = new IndexWriter(
                    staged,
                    writerConfig(NoMergePolicy.INSTANCE, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY)
                )
            ) {
                writer.addIndexes(half);
            }
            forceMerge(staged, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);

            assertEquals("the same terms either way", dictionarySize(oneStep), dictionarySize(staged));
            assertEquals("nothing escapes either way", escapeCount(oneStep), escapeCount(staged));
        }
    }

    public void testStagedMergesKeepATermEachHalfHeldOnce() throws IOException {
        final List<List<String>> segments = headAndOneTailPerSegment(4);
        segments.get(0).add("shared");
        segments.get(2).add("shared");
        try (Directory oneStep = newDirectory(); Directory staged = newDirectory(); Directory half = newDirectory()) {
            flushSegments(oneStep, segments, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            forceMerge(oneStep, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            assertEquals("the head and the term two segments held", 2, dictionarySize(oneStep));

            flushSegments(staged, segments.subList(0, 2), StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            forceMerge(staged, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            flushSegments(half, segments.subList(2, 4), StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            forceMerge(half, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            try (
                IndexWriter writer = new IndexWriter(
                    staged,
                    writerConfig(NoMergePolicy.INSTANCE, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY)
                )
            ) {
                writer.addIndexes(half);
            }
            forceMerge(staged, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            assertEquals("the same column a single merge reaches", dictionarySize(oneStep), dictionarySize(staged));
        }
    }

    /** One head term repeated in every segment, and one tail term per segment held once, which no segment can name. */
    private static List<List<String>> headAndOneTailPerSegment(int segments) {
        final List<List<String>> values = new ArrayList<>(segments);
        for (int segment = 0; segment < segments; segment++) {
            final List<String> segmentValues = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                segmentValues.add("head");
            }
            segmentValues.add("tail" + segment);
            values.add(segmentValues);
        }
        return values;
    }

    /** The terms the merged column summarised, which is none where the cap paid for none. */
    /** The one column a force-merged directory holds, for a check that needs nothing but the column. */
    private void withMergedColumn(Directory dir, CheckedConsumer<StringColumnReader, IOException> check) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("force-merged to one segment", 1, reader.leaves().size());
            check.accept(stringColumn(reader.leaves().get(0).reader()));
        }
    }

    private List<String> summaryTerms(Directory dir) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            final StringColumnReader column = stringColumn(reader.leaves().get(0).reader());
            if (column.hasSummary() == false) {
                return List.of();
            }
            final List<BytesRef> terms = new ArrayList<>();
            column.readSummary(terms, new ArrayList<>());
            return terms.stream().map(BytesRef::utf8ToString).sorted().toList();
        }
    }

    private int dictionarySize(Directory dir) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            return stringColumn(reader.leaves().get(0).reader()).dictionarySize();
        }
    }

    private long escapeCount(Directory dir) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            return stringColumn(reader.leaves().get(0).reader()).escapeCount();
        }
    }

    public void testColumnNoDictionaryCanReachStillRecordsItsTerms() throws IOException {
        final DictionaryPolicy tinyCap = new DictionaryPolicy(1024, 0.5, StringColumnOptions.DEFAULT_DICTIONARY.maxShareOfColumn());
        final MergedColumn merged = merge("out of reach", unreachableSegments(), tinyCap);
        assertTrue("every segment records its terms", merged.segmentsSummariseTerms());
        assertFalse("no dictionary", merged.hasDictionary());
        assertTrue("and it records what it saw for the next merge", merged.hasSummaryTerms());
    }

    private static final int UNREACHABLE_TERMS_PER_SEGMENT = 2_000;

    private static List<List<String>> unreachableSegments() {
        return unreachableSegments(UNREACHABLE_TERMS_PER_SEGMENT);
    }

    private static List<List<String>> unreachableSegments(int termsPerSegment) {
        final List<List<String>> segments = new ArrayList<>(SEGMENTS);
        for (int segment = 0; segment < SEGMENTS; segment++) {
            final List<String> values = new ArrayList<>(termsPerSegment);
            for (int i = 0; i < termsPerSegment; i++) {
                values.add(identifier(segment * termsPerSegment + i));
            }
            segments.add(values);
        }
        return segments;
    }

    private void assertTermsBelowTheHeadAreNamed(String shape, IntFunction<String> term) throws IOException {
        final long[] counts = zipfCounts();
        final MergedColumn merged = merge(shape, zipfSegments(counts, term), StringColumnOptions.DEFAULT_DICTIONARY);
        assertTrue("a dictionary", merged.hasDictionary());
        assertEquals("every term the merged column holds more than once", termsHeldMoreThanOnce(counts), merged.terms());
        assertEquals("no value escapes", termsHeldOnce(counts), merged.escapes());
    }

    private static List<List<String>> steadyRateSegments() {
        return steadyRateSegments(STEADY_TAIL_TERMS_PER_SEGMENT);
    }

    private static List<List<String>> steadyRateSegments(int tails) {
        final List<List<String>> segments = new ArrayList<>(SEGMENTS);
        for (int segment = 0; segment < SEGMENTS; segment++) {
            final List<String> values = new ArrayList<>(STEADY_HEAD_VALUES_PER_SEGMENT + tails);
            for (int i = 0; i < STEADY_HEAD_VALUES_PER_SEGMENT; i++) {
                values.add(STEADY_HEAD_TERM);
            }
            for (int i = 0; i < tails; i++) {
                values.add(identifier(i));
            }
            Collections.shuffle(values, new Random(SEED + segment));
            segments.add(values);
        }
        return segments;
    }

    private static List<List<String>> shortHeadSegments() {
        return shortHeadSegments(SHORT_HEAD_PAIRS_PER_SEGMENT);
    }

    private static List<List<String>> shortHeadSegments(int pairs) {
        final List<List<String>> segments = new ArrayList<>(SEGMENTS);
        for (int segment = 0; segment < SEGMENTS; segment++) {
            final List<String> values = new ArrayList<>(2 * pairs);
            for (int i = 0; i < pairs; i++) {
                values.add(SHORT_HEAD_TERM);
                values.add(identifier(segment * pairs + i));
            }
            segments.add(values);
        }
        return segments;
    }

    private static List<List<String>> zipfSegments(long[] counts, IntFunction<String> term) {
        final List<List<String>> segments = new ArrayList<>(SEGMENTS);
        for (int segment = 0; segment < SEGMENTS; segment++) {
            segments.add(new ArrayList<>());
        }
        // Spreading each term evenly is what an emitter served at a steady rate leaves behind, and it is what puts a term held
        // ten times in the index into ten segments holding it once.
        for (int rank = 0; rank < counts.length; rank++) {
            final String value = term.apply(rank);
            for (long i = 0; i < counts[rank]; i++) {
                segments.get((int) (i % SEGMENTS)).add(value);
            }
        }
        final Random random = new Random(SEED);
        for (List<String> values : segments) {
            Collections.shuffle(values, random);
        }
        return segments;
    }

    private static long[] zipfCounts() {
        return zipfCounts(ZIPF_TERMS);
    }

    private static long[] zipfCounts(int terms) {
        double harmonic = 0;
        for (int rank = 1; rank <= terms; rank++) {
            harmonic += 1.0 / rank;
        }
        final long[] counts = new long[terms];
        for (int rank = 1; rank <= terms; rank++) {
            counts[rank - 1] = Math.max(1, Math.round(ZIPF_VALUES / (harmonic * rank)));
        }
        return counts;
    }

    private static int termsHeldMoreThanOnce(long[] counts) {
        int terms = 0;
        for (long count : counts) {
            if (count > 1) {
                terms++;
            }
        }
        return terms;
    }

    private static int termsHeldOnce(long[] counts) {
        int terms = 0;
        for (long count : counts) {
            if (count == 1) {
                terms++;
            }
        }
        return terms;
    }

    /** A path, which shares long prefixes with its neighbours and so costs the plain layout little. */
    private static String path(int rank) {
        final String section = PATH_SECTIONS[rank % PATH_SECTIONS.length];
        final String resource = PATH_RESOURCES[(rank / PATH_SECTIONS.length) % PATH_RESOURCES.length];
        return "/" + section + "/v1/" + resource + "/" + (100_000 + rank);
    }

    /** An opaque identifier, whose bytes compress no better in the escape stream than in a dictionary. */
    private static String identifier(int rank) {
        final Random random = new Random(SEED * 31 + rank);
        final char[] chars = new char[IDENTIFIER_LENGTH];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = IDENTIFIER_ALPHABET[random.nextInt(IDENTIFIER_ALPHABET.length)];
        }
        return new String(chars);
    }

    /** What the merged column turned out to be, beside what the segments it was merged from held and summarised. */
    private record MergedColumn(
        boolean segmentsArePlain,
        boolean segmentsSummariseTerms,
        long segmentsReachableValues,
        boolean hasDictionary,
        boolean hasSummaryTerms,
        int terms,
        long escapes,
        long boundNamedValues,
        long bytes
    ) {}

    public void testABoundNeverRefusesADictionaryTheInputsAlreadyBuilt() throws IOException {
        final DictionaryPolicy cappedAtOneHundred = new DictionaryPolicy(100, 0.5, 0.2);
        final List<String> first = repeated("b".repeat(100), 10);
        first.add("a");
        final List<String> second = repeated("b".repeat(100), 10);
        second.add("c");
        try (Directory dir = newDirectory()) {
            flushSegments(dir, List.of(first, second), cappedAtOneHundred, StringColumnOptions.DEFAULT_SUMMARY);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    assertTrue("each input holds a dictionary worth keeping", stringColumn(leaf.reader()).hasDictionary());
                }
            }
            forceMerge(dir, cappedAtOneHundred, StringColumnOptions.DEFAULT_SUMMARY);
            withMergedColumn(dir, column -> {
                assertTrue("and the term naming twenty of twenty two survives", column.hasDictionary());
                assertEquals("the repeated term, not the two held once", List.of("b".repeat(100)), dictionaryTerms(column));
            });
        }
    }

    public void testAnInputBelowTheBarStillContributesItsTerms() throws IOException {
        final DictionaryPolicy cappedAtFour = new DictionaryPolicy(4, 0.5, 0.2);
        final List<String> diluted = repeated("head", 100);
        for (int i = 0; i < 200; i++) {
            diluted.add("tail-" + i);
        }
        try (Directory dir = newDirectory()) {
            flushSegments(dir, List.of(diluted, repeated("head", 100)), cappedAtFour, StringColumnOptions.DEFAULT_SUMMARY);
            forceMerge(dir, cappedAtFour, StringColumnOptions.DEFAULT_SUMMARY);
            withMergedColumn(dir, column -> {
                assertTrue("head names two hundred of four hundred", column.hasDictionary());
                assertEquals("and it is the only term four bytes buy", List.of("head"), dictionaryTerms(column));
            });
        }
    }

    public void testALargerMergeCapIsNotBoundedByWhatTheInputsRecorded() throws IOException {
        final List<String> identifiers = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            identifiers.add("id-" + i);
        }
        try (Directory dir = newDirectory()) {
            flushSegments(dir, List.of(identifiers, identifiers), new DictionaryPolicy(8, 0.5, 1.0), StringColumnOptions.DEFAULT_SUMMARY);
            forceMerge(dir, new DictionaryPolicy(1024, 0.5, 1.0), StringColumnOptions.DEFAULT_SUMMARY);
            withMergedColumn(dir, column -> {
                assertTrue("every term repeats and fits the larger cap", column.hasDictionary());
                assertEquals("all hundred of them", identifiers.stream().distinct().sorted().toList(), dictionaryTerms(column));
            });
        }
    }

    // NOTE: the same segments twice, so the deletions are the only difference between the two merges.
    public void testDeletionsAreWhatSendTheMergeToTheValues() throws IOException {
        final DictionaryPolicy policy = new DictionaryPolicy(64, 0.5, 0.2);
        final List<String> segment = repeated("head", 100);
        for (int i = 0; i < 200; i++) {
            segment.add("tail-" + i);
        }
        try (Directory dir = newDirectory()) {
            flushMarked(dir, List.of(segment, segment), policy, StringColumnOptions.DEFAULT_SUMMARY);
            forceMerge(dir, policy, StringColumnOptions.DEFAULT_SUMMARY);
            withMergedColumn(dir, column -> assertFalse("no dictionary reaches the bar", column.hasDictionary()));
        }
        try (Directory dir = newDirectory()) {
            flushMarked(dir, List.of(segment, segment), policy, StringColumnOptions.DEFAULT_SUMMARY);
            deleteTails(dir, policy, StringColumnOptions.DEFAULT_SUMMARY);
            forceMerge(dir, policy, StringColumnOptions.DEFAULT_SUMMARY);
            withMergedColumn(dir, column -> {
                assertTrue("and the surviving values all earn one", column.hasDictionary());
                assertEquals("naming the term the deletions left behind", List.of("head"), dictionaryTerms(column));
            });
        }
    }

    // NOTE: the tails escape each input's dictionary, so the union cannot stand for the merged column and
    // the decision really does come from the summaries rather than from the cheaper exact path.
    public void testAMergeFromSummariesSurveysNothing() throws IOException {
        final DictionaryPolicy policy = StringColumnOptions.DEFAULT_DICTIONARY;
        final List<List<String>> segments = new ArrayList<>();
        for (int segment = 0; segment < 2; segment++) {
            final List<String> values = new ArrayList<>();
            for (int i = 0; i < 400; i++) {
                values.add(i % 4 == 0 ? "INFO" : "DEBUG");
            }
            for (int i = 0; i < 10; i++) {
                values.add("one-off-" + segment + "-" + i);
            }
            segments.add(values);
        }
        try (Directory dir = newDirectory()) {
            flushSegments(dir, segments, policy, StringColumnOptions.DEFAULT_SUMMARY);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    assertThat(
                        "the inputs let values escape, so no union is available",
                        stringColumn(leaf.reader()).escapeCount(),
                        greaterThan(0L)
                    );
                }
            }
            assertEquals(
                "so the summaries settle it",
                MergedVocabulary.Source.COMBINED_SUMMARIES,
                settledBy(dir, policy, StringColumnOptions.DEFAULT_SUMMARY)
            );
            forceMerge(dir, policy, StringColumnOptions.DEFAULT_SUMMARY);
            withMergedColumn(dir, column -> assertTrue("and they admitted a dictionary", column.hasDictionary()));
        }
    }

    public void testAMergeThatNeitherBoundSettlesWritesPlainFromTheSummaries() throws IOException {
        final DictionaryPolicy policy = new DictionaryPolicy(100, 0.5, 1.0);
        final List<List<String>> segments = new ArrayList<>();
        for (int segment = 0; segment < 2; segment++) {
            final List<String> values = repeated("head", 40);
            for (int i = 0; i < 60; i++) {
                values.add(Integer.toString(segment * 60 + i, 36) + "z");
            }
            segments.add(values);
        }
        try (Directory dir = newDirectory()) {
            flushSegments(dir, segments, policy, StringColumnOptions.DEFAULT_SUMMARY);
            assertEquals(
                "neither summed counts nor the bound settle it, and the summaries decide anyway",
                MergedVocabulary.Source.SUMMARY_REFUSAL,
                settledBy(dir, policy, StringColumnOptions.DEFAULT_SUMMARY)
            );
            forceMerge(dir, policy, StringColumnOptions.DEFAULT_SUMMARY);
            withMergedColumn(
                dir,
                column -> assertFalse("and the values themselves say no dictionary is worth keeping", column.hasDictionary())
            );
        }
    }

    public void testSummaryRefusalSkipsVocabularySurvey() throws IOException {
        final DictionaryPolicy policy = new DictionaryPolicy(16, 0.5, 0.2);
        final List<String> unique = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            unique.add("globally-unique-identifier-" + i);
        }
        try (Directory dir = newDirectory()) {
            flushSegments(dir, List.of(unique, unique), policy, StringColumnOptions.DEFAULT_SUMMARY);
            assertEquals(
                "the bound rules one out without reading a value",
                MergedVocabulary.Source.SUMMARY_REFUSAL,
                settledBy(dir, policy, StringColumnOptions.DEFAULT_SUMMARY)
            );
            forceMerge(dir, policy, StringColumnOptions.DEFAULT_SUMMARY);
            withMergedColumn(dir, column -> {
                assertFalse("no dictionary is worth keeping", column.hasDictionary());
                assertTrue("but the terms are still there for the next merge", column.hasSummaryTerms());
            });
        }
    }

    // NOTE: the bound only bites where the inputs lost nothing to their own summaries, since mass a summary
    // dropped is credited in full. A 512 KiB dictionary buys 524 of these terms, just over half of what two
    // segments hold and a third of what three do.
    public void testTheDefaultOptionsRuleOutADictionaryOnceTheVocabulariesOutgrowTheCap() throws IOException {
        final DictionaryPolicy policy = StringColumnOptions.DEFAULT_DICTIONARY;
        final List<List<String>> segments = new ArrayList<>();
        for (int segment = 0; segment < 3; segment++) {
            final List<String> values = new ArrayList<>();
            for (int i = 0; i < TERMS_PER_SEGMENT; i++) {
                final String term = paddedTerm(segment, i);
                // NOTE: the dictionary may hold a fifth of the column's bytes, so a term held n times puts
                // at most n fifths of the values within reach. Three leaves the shipped bar unreachable.
                for (int held = 0; held < 5; held++) {
                    values.add(term);
                }
            }
            // NOTE: a segment whose own dictionary names every value lets the merge union the dictionaries
            // instead of reading the summaries, which is not the path this test is about.
            for (int i = 0; i < ESCAPES_PER_SEGMENT; i++) {
                values.add(paddedTerm(segment, TERMS_PER_SEGMENT + i));
            }
            segments.add(values);
        }
        try (Directory dir = newDirectory()) {
            flushSegments(dir, segments.subList(0, 2), policy, StringColumnOptions.DEFAULT_SUMMARY);
            assertEquals(
                "two segments' worth is still within reach",
                MergedVocabulary.Source.COMBINED_SUMMARIES,
                settledBy(dir, policy, StringColumnOptions.DEFAULT_SUMMARY)
            );
        }
        try (Directory dir = newDirectory()) {
            flushSegments(dir, segments, policy, StringColumnOptions.DEFAULT_SUMMARY);
            assertTrue("with nothing lost from any input summary", every(dir, StringColumnReader::hasSummaryTerms));
            assertEquals(
                "a third puts the same dictionary under the bar",
                MergedVocabulary.Source.SUMMARY_REFUSAL,
                settledBy(dir, policy, StringColumnOptions.DEFAULT_SUMMARY)
            );
            forceMerge(dir, policy, StringColumnOptions.DEFAULT_SUMMARY);
            withMergedColumn(dir, column -> {
                assertFalse("so the merged column stays plain", column.hasDictionary());
                assertTrue("and still records what it saw for the next merge", column.hasSummaryTerms());
            });
        }
    }

    private static final int TERMS_PER_SEGMENT = 256;
    private static final int ESCAPES_PER_SEGMENT = 16;

    /** A term of a kilobyte, so a segment's vocabulary lands just under the default summary cap. */
    private static String paddedTerm(int segment, int index) {
        final StringBuilder term = new StringBuilder("s" + segment + "-t" + index + "-");
        while (term.length() < 1000) {
            term.append('x');
        }
        return term.toString();
    }

    public void testStatisticsAreFreshOnceDeletionsAreExpunged() throws IOException {
        final DictionaryPolicy policy = new DictionaryPolicy(64, 0.5, 0.2);
        final List<String> segment = repeated("head", 100);
        for (int i = 0; i < 200; i++) {
            segment.add("tail-" + i);
        }
        try (Directory dir = newDirectory()) {
            flushMarked(dir, List.of(segment, segment), policy, StringColumnOptions.DEFAULT_SUMMARY);
            deleteTails(dir, policy, StringColumnOptions.DEFAULT_SUMMARY);
            forceMerge(dir, policy, StringColumnOptions.DEFAULT_SUMMARY);
            withMergedColumn(dir, column -> {
                final StringColumnReader merged = column;
                assertTrue("the surviving values earn a dictionary", merged.hasDictionary());
                assertTrue("and the column records what it now holds", merged.bestCoverage().known());
                assertEquals("which is the values that survived", 200, merged.bestCoverage().numValues());
            });
        }
    }

    public void testABoundSurvivesAGenerationOfMerging() throws IOException {
        final DictionaryPolicy policy = new DictionaryPolicy(16, 0.5, 0.2);
        final List<String> unique = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            unique.add("one-of-a-kind-value-" + i);
        }
        try (Directory dir = newDirectory()) {
            flushSegments(dir, List.of(unique, unique), policy, StringColumnOptions.DEFAULT_SUMMARY);
            forceMerge(dir, policy, StringColumnOptions.DEFAULT_SUMMARY);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final BestCoverage afterOne = stringColumn(reader.leaves().get(0).reader()).bestCoverage();
                assertTrue("the merged column carries a bound of its own", afterOne.known());
                assertTrue("taken under a cap this policy does not exceed", afterOne.validFor(policy.maxBytes()));
            }
            flushSegments(dir, List.of(unique), policy, StringColumnOptions.DEFAULT_SUMMARY);
            forceMerge(dir, policy, StringColumnOptions.DEFAULT_SUMMARY);
            withMergedColumn(dir, column -> { assertTrue("and still carries a bound", column.bestCoverage().known()); });
        }
    }

    public void testASummaryPolicyOfNoneRecordsNothingAtAll() throws IOException {
        try (Directory dir = newDirectory()) {
            flushSegments(dir, List.of(repeated("head", 50)), StringColumnOptions.DEFAULT_DICTIONARY, SummaryPolicy.NONE);
            withMergedColumn(dir, column -> {
                assertFalse("no summary", column.hasSummary());
                assertFalse("and so no bound", column.bestCoverage().known());
            });
        }
    }

    /** Two segments of {@code a} a hundred times then {@code dead} a hundred and fifty, with the dead gone. */
    private void flushAndDeleteTheDeadTerm(Directory dir, DictionaryPolicy policy, SummaryPolicy summaryPolicy) throws IOException {
        final List<String> segment = repeated("a", 100);
        segment.addAll(repeated("dead", 150));
        flushMarked(dir, List.of(segment, segment), policy, summaryPolicy);
        try (
            IndexWriter writer = new IndexWriter(dir, writerConfig(NoMergePolicy.INSTANCE, policy, summaryPolicy));
            DirectoryReader reader = DirectoryReader.open(writer)
        ) {
            for (LeafReaderContext leaf : reader.leaves()) {
                for (int doc = 100; doc < 250; doc++) {
                    assertTrue(writer.tryDeleteDocument(reader, leaf.docBase + doc) >= 0);
                }
            }
            writer.commit();
        }
    }

    private static List<String> repeated(String value, int times) {
        final List<String> values = new ArrayList<>();
        for (int i = 0; i < times; i++) {
            values.add(value);
        }
        return values;
    }

    private MergedColumn merge(String shape, List<List<String>> segments, DictionaryPolicy dictionaryPolicy) throws IOException {
        try (Directory dir = newDirectory()) {
            flushSegments(dir, segments, dictionaryPolicy, StringColumnOptions.DEFAULT_SUMMARY);
            assertEquals("segments before the merge", SEGMENTS, segmentCount(dir));
            final boolean segmentsArePlain = every(dir, column -> column.hasDictionary() == false);
            final boolean segmentsSummariseTerms = every(dir, StringColumnReader::hasSummaryTerms);
            final long segmentsReachableValues = sum(dir, column -> column.bestCoverage().namedValues());
            forceMerge(dir, dictionaryPolicy, StringColumnOptions.DEFAULT_SUMMARY);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("force-merged to one segment", 1, reader.leaves().size());
                final SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();
                final StringColumnReader column = stringColumn(segment);
                final MergedColumn merged = new MergedColumn(
                    segmentsArePlain,
                    segmentsSummariseTerms,
                    segmentsReachableValues,
                    column.hasDictionary(),
                    column.hasSummaryTerms(),
                    column.hasDictionary() ? column.dictionarySize() : 0,
                    column.escapeCount(),
                    column.bestCoverage().namedValues(),
                    columnarBytes(dir, segment)
                );
                logger.info("{}: merged {} segments: {}", shape, SEGMENTS, merged);
                return merged;
            }
        }
    }

    /** Flushes segments whose documents carry a deletable marker, so a test can remove a class of values. */
    private static void flushMarked(
        Directory dir,
        List<List<String>> segments,
        DictionaryPolicy dictionaryPolicy,
        SummaryPolicy summaryPolicy
    ) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, writerConfig(NoMergePolicy.INSTANCE, dictionaryPolicy, summaryPolicy))) {
            for (List<String> values : segments) {
                for (String value : values) {
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, payload(value), columnarBinaryFieldType()));
                    doc.add(new StringField(MARKER, value.startsWith("tail-") ? "tail" : "head", Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
        }
    }

    private static void deleteTails(Directory dir, DictionaryPolicy dictionaryPolicy, SummaryPolicy summaryPolicy) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, writerConfig(NoMergePolicy.INSTANCE, dictionaryPolicy, summaryPolicy))) {
            writer.deleteDocuments(new Term(MARKER, "tail"));
            writer.commit();
        }
    }

    private static void flushSegments(
        Directory dir,
        List<List<String>> segments,
        DictionaryPolicy dictionaryPolicy,
        SummaryPolicy summaryPolicy
    ) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, writerConfig(NoMergePolicy.INSTANCE, dictionaryPolicy, summaryPolicy))) {
            for (List<String> values : segments) {
                for (String value : values) {
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, payload(value), columnarBinaryFieldType()));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
        }
    }

    /** What the flushed segments' columns add up to for {@code number}. */
    private long sum(Directory dir, ToLongFunction<StringColumnReader> number) throws IOException {
        long total = 0;
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            for (LeafReaderContext leaf : reader.leaves()) {
                total += number.applyAsLong(stringColumn(leaf.reader()));
            }
        }
        return total;
    }

    /** Whether every flushed segment's column answers {@code question} the same way. */
    private boolean every(Directory dir, Predicate<StringColumnReader> question) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            for (LeafReaderContext leaf : reader.leaves()) {
                if (question.test(stringColumn(leaf.reader())) == false) {
                    return false;
                }
            }
            return true;
        }
    }

    private static void forceMerge(Directory dir, DictionaryPolicy dictionaryPolicy, SummaryPolicy summaryPolicy) throws IOException {
        forceMerge(dir, dictionaryPolicy, summaryPolicy, LogDocMergePolicy.DEFAULT_MERGE_FACTOR);
    }

    // NOTE: the merge factor bounds how many segments one merge reads, so a force-merge of more than that
    // cascades and the run holds several decisions rather than the one a census row stands for.
    private static void forceMerge(Directory dir, DictionaryPolicy dictionaryPolicy, SummaryPolicy summaryPolicy, int mergeFactor)
        throws IOException {
        final LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
        mergePolicy.setMergeFactor(mergeFactor);
        mergePolicy.setNoCFSRatio(0.0);
        try (IndexWriter writer = new IndexWriter(dir, writerConfig(mergePolicy, dictionaryPolicy, summaryPolicy))) {
            writer.forceMerge(1);
        }
    }

    private static IndexWriterConfig writerConfig(MergePolicy mergePolicy, DictionaryPolicy dictionaryPolicy, SummaryPolicy summaryPolicy) {
        final ColumNARDocValuesFormat format = new ColumNARDocValuesFormat(
            (f, t) -> NumericPipeline::defaultPipeline,
            field -> ColumnarFieldType.STRING,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            dictionaryPolicy,
            summaryPolicy
        );
        return new IndexWriterConfig().setCodec(columnarCodec(format)).setUseCompoundFile(false).setMergePolicy(mergePolicy);
    }

    /** A slot holding {@code value}, or a null slot where it is null, which a dictionary never has to name. */
    private static BytesRef payload(String value) {
        final List<BytesRef> slots = new ArrayList<>(1);
        slots.add(value == null ? null : new BytesRef(value));
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(slots));
    }

    /** Which way the merge settles its vocabulary for the segments flushed so far. */
    private static MergedVocabulary.Source settledBy(Directory dir, DictionaryPolicy dictionaryPolicy, SummaryPolicy summaryPolicy)
        throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            final List<StringColumnReader> columns = new ArrayList<>(reader.leaves().size());
            boolean hasDeletions = false;
            for (LeafReaderContext leaf : reader.leaves()) {
                columns.add(stringColumn(leaf.reader()));
                hasDeletions |= leaf.reader().hasDeletions();
            }
            return ColumNARDocValuesConsumer.vocabularyFrom(columns, hasDeletions, dictionaryPolicy, summaryPolicy).source();
        }
    }

    private static StringColumnReader stringColumn(LeafReader leaf) throws IOException {
        final BinaryDocValues values = leaf.getBinaryDocValues(FIELD);
        assertTrue("expected a columnar column, got " + values, values instanceof ColumnarStringBinaryDocValues);
        return ((ColumnarStringBinaryDocValues) values).reader();
    }

    private record Census(
        String shape,
        MergedVocabulary.Source settled,
        int inputSegments,
        long values,
        double summedBoundShare,
        boolean hasDictionary,
        int dictionaryTerms,
        long escapes,
        long bytes
    ) {}

    private static final int OVERLAP_TERMS = 2_000;
    private static final int OVERLAP_OCCURRENCES = 10;

    private static List<List<String>> overlapSegments(int spread) {
        final List<List<String>> segments = new ArrayList<>(SEGMENTS);
        for (int segment = 0; segment < SEGMENTS; segment++) {
            segments.add(new ArrayList<>());
        }
        for (int t = 0; t < OVERLAP_TERMS; t++) {
            final String term = identifier(t);
            for (int i = 0; i < OVERLAP_OCCURRENCES; i++) {
                segments.get((t + i % spread) % SEGMENTS).add(term);
            }
        }
        final Random random = new Random(SEED);
        for (List<String> values : segments) {
            Collections.shuffle(values, random);
        }
        return segments;
    }

    private int segmentCount(Directory dir) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            return reader.leaves().size();
        }
    }

    private static final int PRIVATE_VOCABULARY_SEGMENTS = 40;
    private static final int PRIVATE_VOCABULARY_TERMS_PER_SEGMENT = 200;
    private static final int PRIVATE_VOCABULARY_TERM_LENGTH = 100;
    private static final int PRIVATE_VOCABULARY_REPEATS = 5;

    /** Segments sharing no term, each holding more vocabulary than a dictionary buys but less than a survey remembers. */
    private static List<List<String>> privateVocabularySegments() {
        final List<List<String>> segments = new ArrayList<>(PRIVATE_VOCABULARY_SEGMENTS);
        for (int segment = 0; segment < PRIVATE_VOCABULARY_SEGMENTS; segment++) {
            final List<String> values = new ArrayList<>();
            for (int t = 0; t < PRIVATE_VOCABULARY_TERMS_PER_SEGMENT; t++) {
                final int rank = segment * PRIVATE_VOCABULARY_TERMS_PER_SEGMENT + t;
                final String term = identifier(rank).repeat(PRIVATE_VOCABULARY_TERM_LENGTH / IDENTIFIER_LENGTH + 1)
                    .substring(0, PRIVATE_VOCABULARY_TERM_LENGTH);
                for (int i = 0; i < PRIVATE_VOCABULARY_REPEATS; i++) {
                    values.add(term);
                }
            }
            Collections.shuffle(values, new Random(SEED + segment));
            segments.add(values);
        }
        return segments;
    }

    // NOTE: the merge trims the combined counts to `SummaryPolicy.mergeBudgetBytes`, so across forty segments
    // sharing no term it keeps a fraction of them and credits the rest as unaccounted mass. What each input
    // recorded about itself is untouched by that trim, which is the case the persisted bound exists for.
    public void testAPersistedBoundRefusesWhereTheTrimmedCountsCannot() throws IOException {
        final DictionaryPolicy dictionaryPolicy = new DictionaryPolicy(8 * 1024, 0.5, 0.2);
        final SummaryPolicy summaryPolicy = SummaryPolicy.sized(64 * 1024);
        final Census census = settle("private vocabularies", privateVocabularySegments(), dictionaryPolicy, summaryPolicy);

        logger.info(
            "private vocabularies: {} segments, {} values -> {}, summed input bound={}",
            census.inputSegments(),
            census.values(),
            census.settled(),
            census.summedBoundShare()
        );
        assertThat("each input recorded a bound of its own", census.summedBoundShare(), lessThan(dictionaryPolicy.minCoverage()));
        assertEquals("so the merge refuses without reading a value", MergedVocabulary.Source.SUMMARY_REFUSAL, census.settled());
    }

    private static final DictionaryPolicy ROOMY_DICTIONARY = new DictionaryPolicy(64 * 1024, 0.5, 1.0);

    // NOTE: a term held once by every input is held ten times by the merged column, and a dictionary naming
    // all of them leaves nothing to escape. A summary bounded below what the column holds carries too few of
    // them to show that, so the summed counts fall far short of the bar while the values clear it. The
    // survey is what sees the difference, and skipping it would write the whole column plain.
    public void testASurveyNamesWhatAThinSummaryCannotShow() throws IOException {
        final List<List<String>> segments = overlapSegments(SEGMENTS);

        final Census carried = settle("thin summary", segments, ROOMY_DICTIONARY, SummaryPolicy.sized(64 * 1024));
        assertEquals(
            "a summary wide enough settles it without reading a value",
            MergedVocabulary.Source.COMBINED_SUMMARIES,
            carried.settled()
        );
        assertEquals(OVERLAP_TERMS, carried.dictionaryTerms());

        for (int summaryCap : new int[] { 16 * 1024, 4 * 1024, 1024 }) {
            final Census thin = settle("thin summary", segments, ROOMY_DICTIONARY, SummaryPolicy.sized(summaryCap));
            logger.info(
                "summary cap={}: {} values -> {}, terms={}, escapes={}, bytes={}",
                summaryCap,
                thin.values(),
                thin.settled(),
                thin.dictionaryTerms(),
                thin.escapes(),
                thin.bytes()
            );
            assertEquals("cap " + summaryCap + ": only the values can show it", MergedVocabulary.Source.SURVEY, thin.settled());
            assertEquals("cap " + summaryCap + ": and they name every term", OVERLAP_TERMS, thin.dictionaryTerms());
            assertEquals("cap " + summaryCap + ": leaving nothing to escape", 0, thin.escapes());
        }
    }

    /** Terms held once by every segment, at vocabularies that outgrow what one flush may survey and record. */
    private static List<List<String>> heldOncePerSegment(int terms) {
        final List<List<String>> segments = new ArrayList<>(SEGMENTS);
        for (int segment = 0; segment < SEGMENTS; segment++) {
            final List<String> values = new ArrayList<>(terms);
            for (int t = 0; t < terms; t++) {
                values.add(identifier(t));
            }
            Collections.shuffle(values, new Random(SEED + segment));
            segments.add(values);
        }
        return segments;
    }

    // NOTE: a survey holds `surveyBudgetBytes` while a merge holds four times that, so past the smaller of the
    // two the values are a weaker witness than the summaries that sent the merge to them. At 24000 terms the
    // merged column would name two thirds of its values, the summaries cannot show it, and the survey cannot
    // either, so the column is written plain.
    public void testSharedVocabularyBeyondSurveyCapacityProducesPlainOutput() throws IOException {
        final Census carried = settle("held once", heldOncePerSegment(16_000));
        assertEquals(
            "a vocabulary one flush can record settles without a survey",
            MergedVocabulary.Source.COMBINED_SUMMARIES,
            carried.settled()
        );
        assertEquals(16_000, carried.dictionaryTerms());

        final Census outgrown = settle("held once", heldOncePerSegment(24_000));
        logger.info(
            "held once terms=24000: {} values -> {}, dictionary terms={}, bytes={}",
            outgrown.values(),
            outgrown.settled(),
            outgrown.dictionaryTerms(),
            outgrown.bytes()
        );
        assertEquals("past it the summaries cannot show it", MergedVocabulary.Source.SUMMARY_REFUSAL, outgrown.settled());
        assertEquals("and the column goes plain without reading a value", 0, outgrown.dictionaryTerms());
    }

    private Census settle(String shape, List<List<String>> segments) throws IOException {
        return settle(shape, segments, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
    }

    private Census settle(String shape, List<List<String>> segments, DictionaryPolicy dictionaryPolicy, SummaryPolicy summaryPolicy)
        throws IOException {
        try (Directory dir = newDirectory()) {
            flushSegments(dir, segments, dictionaryPolicy, summaryPolicy);
            final long values = sum(dir, column -> column.numValues() - column.numNullSlots());
            final long summedBound = sum(dir, column -> column.bestCoverage().namedValues());
            final int inputSegments = segmentCount(dir);
            // NOTE: taken from the same inputs the merge below reads, so it is the decision that merge makes
            // rather than one inferred from the column it leaves.
            final MergedVocabulary.Source settled = settledBy(dir, dictionaryPolicy, summaryPolicy);
            forceMerge(dir, dictionaryPolicy, summaryPolicy, inputSegments);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(shape + ": force-merged to one segment", 1, reader.leaves().size());
                final SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();
                final StringColumnReader column = stringColumn(segment);
                return new Census(
                    shape,
                    settled,
                    inputSegments,
                    values,
                    (double) summedBound / values,
                    column.hasDictionary(),
                    column.hasDictionary() ? column.dictionarySize() : 0,
                    column.escapeCount(),
                    columnarBytes(dir, segment)
                );
            }
        }
    }

    /** The terms the column's dictionary actually holds, in term order. */
    private static List<String> dictionaryTerms(StringColumnReader column) throws IOException {
        final DictionaryStringColumnReader dictionary = (DictionaryStringColumnReader) column;
        final List<String> terms = new ArrayList<>(dictionary.dictionarySize());
        final BytesRef term = new BytesRef();
        for (int t = 0; t < dictionary.dictionarySize(); t++) {
            dictionary.termAt(StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL + t, term);
            terms.add(term.utf8ToString());
        }
        return terms;
    }

    private static final int LONG_TERM_LENGTH = 200;
    private static final int LONG_TERMS_PER_SEGMENT = 800;
    private static final int LONG_TERM_REPEATS = 5;

    private static List<List<String>> longTermSegments() {
        return longTermSegments(LONG_TERMS_PER_SEGMENT);
    }

    private static List<List<String>> longTermSegments(int termsPerSegment) {
        final List<List<String>> segments = new ArrayList<>(SEGMENTS);
        for (int segment = 0; segment < SEGMENTS; segment++) {
            final List<String> values = new ArrayList<>(termsPerSegment * LONG_TERM_REPEATS);
            for (int t = 0; t < termsPerSegment; t++) {
                final String term = identifier(segment * termsPerSegment + t).repeat(LONG_TERM_LENGTH / IDENTIFIER_LENGTH);
                for (int i = 0; i < LONG_TERM_REPEATS; i++) {
                    values.add(term);
                }
            }
            segments.add(values);
        }
        return segments;
    }

    private static final int SHARED_VOCABULARY_TERMS = 20;
    private static final int SHARED_VOCABULARY_VALUES_PER_SEGMENT = 1_000;

    private static List<List<String>> sharedVocabularySegments() {
        return sharedVocabularySegments(SHARED_VOCABULARY_TERMS);
    }

    private static List<List<String>> sharedVocabularySegments(int vocabulary) {
        final Random random = new Random(SEED);
        final List<List<String>> segments = new ArrayList<>(SEGMENTS);
        for (int segment = 0; segment < SEGMENTS; segment++) {
            final List<String> values = new ArrayList<>(SHARED_VOCABULARY_VALUES_PER_SEGMENT);
            for (int i = 0; i < SHARED_VOCABULARY_VALUES_PER_SEGMENT; i++) {
                values.add("status-" + random.nextInt(vocabulary));
            }
            segments.add(values);
        }
        return segments;
    }

    private static long columnarBytes(Directory dir, SegmentReader segment) throws IOException {
        long bytes = 0;
        for (String file : segment.getSegmentInfo().files()) {
            if (COLUMNAR_EXTENSIONS.contains(IndexFileNames.getExtension(file))) {
                bytes += dir.fileLength(file);
            }
        }
        return bytes;
    }
}
