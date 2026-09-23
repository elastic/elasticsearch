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
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.SummaryPolicy;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.IntFunction;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.elasticsearch.columnar.ColumnarTestUtils.stringPayload;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class StringColumnMergeVocabularyTests extends ESTestCase {

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

    // NOTE: a merge has put every term to the question once per input, so one still held once has failed it and
    // is dropped, while one held once by two inputs has not. A flush cannot make that distinction and admits both.
    public void testMergedSummaryKeepsWhatMoreThanOneSegmentHeld() throws IOException {
        final List<List<String>> segments = headAndOneTailPerSegment(2);
        segments.get(0).add("shared");
        segments.get(1).add("shared");
        try (Directory dir = newDirectory()) {
            flushSegments(dir, segments, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            forceMerge(dir, StringColumnOptions.DEFAULT_DICTIONARY, StringColumnOptions.DEFAULT_SUMMARY);
            assertEquals("the head and the term two segments held, and neither tail", List.of("head", "shared"), summaryTerms(dir));
        }
    }

    public void testMergedSummaryHonoursItsOwnCap() throws IOException {
        final SummaryPolicy cap = new SummaryPolicy(between(1, 32));
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
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertFalse("NONE summarises nothing", stringColumn(reader.leaves().get(0).reader()).hasSummary());
            }
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
        final SummaryPolicy tinyCap = new SummaryPolicy(between(0, 128));
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

    // NOTE: the known limit of not summarising what a merge holds once. Each half holds `shared` once, so
    // neither half summarises it and the merge of the halves never learns that the index holds it twice,
    // where one merge of all four segments would have named it. Keeping it would mean summarising every
    // value unique to the index at every generation, which costs more than this loses.
    public void testStagedMergesLoseATermEachHalfHeldOnce() throws IOException {
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
            assertEquals("the head alone", 1, dictionarySize(staged));
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

    private void assertTermsBelowTheHeadAreNamed(String shape, IntFunction<String> term) throws IOException {
        final long[] counts = zipfCounts();
        final MergedColumn merged = merge(shape, zipfSegments(counts, term), StringColumnOptions.DEFAULT_DICTIONARY);
        assertTrue("a dictionary", merged.hasDictionary());
        assertEquals("every term the merged column holds more than once", termsHeldMoreThanOnce(counts), merged.terms());
        assertEquals("no value escapes", termsHeldOnce(counts), merged.escapes());
    }

    private static List<List<String>> steadyRateSegments() {
        final List<List<String>> segments = new ArrayList<>(SEGMENTS);
        for (int segment = 0; segment < SEGMENTS; segment++) {
            final List<String> values = new ArrayList<>(STEADY_HEAD_VALUES_PER_SEGMENT + STEADY_TAIL_TERMS_PER_SEGMENT);
            for (int i = 0; i < STEADY_HEAD_VALUES_PER_SEGMENT; i++) {
                values.add(STEADY_HEAD_TERM);
            }
            for (int i = 0; i < STEADY_TAIL_TERMS_PER_SEGMENT; i++) {
                values.add(identifier(i));
            }
            Collections.shuffle(values, new Random(SEED + segment));
            segments.add(values);
        }
        return segments;
    }

    private static List<List<String>> shortHeadSegments() {
        final List<List<String>> segments = new ArrayList<>(SEGMENTS);
        for (int segment = 0; segment < SEGMENTS; segment++) {
            final List<String> values = new ArrayList<>(2 * SHORT_HEAD_PAIRS_PER_SEGMENT);
            for (int i = 0; i < SHORT_HEAD_PAIRS_PER_SEGMENT; i++) {
                values.add(SHORT_HEAD_TERM);
                values.add(identifier(segment * SHORT_HEAD_PAIRS_PER_SEGMENT + i));
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
        double harmonic = 0;
        for (int rank = 1; rank <= ZIPF_TERMS; rank++) {
            harmonic += 1.0 / rank;
        }
        final long[] counts = new long[ZIPF_TERMS];
        for (int rank = 1; rank <= ZIPF_TERMS; rank++) {
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

    /** What the merged column turned out to be, and whether the segments it was merged from held dictionaries of their own. */
    private record MergedColumn(boolean segmentsArePlain, boolean hasDictionary, int terms, long escapes, long bytes) {}

    private MergedColumn merge(String shape, List<List<String>> segments, DictionaryPolicy dictionaryPolicy) throws IOException {
        try (Directory dir = newDirectory()) {
            flushSegments(dir, segments, dictionaryPolicy, StringColumnOptions.DEFAULT_SUMMARY);
            final boolean segmentsArePlain = segmentsArePlain(dir);
            forceMerge(dir, dictionaryPolicy, StringColumnOptions.DEFAULT_SUMMARY);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("force-merged to one segment", 1, reader.leaves().size());
                final SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();
                final StringColumnReader column = stringColumn(segment);
                final MergedColumn merged = new MergedColumn(
                    segmentsArePlain,
                    column.hasDictionary(),
                    column.hasDictionary() ? column.dictionarySize() : 0,
                    column.escapeCount(),
                    columnarBytes(dir, segment)
                );
                logger.info("{}: merged {} segments: {}", shape, SEGMENTS, merged);
                return merged;
            }
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
                    doc.add(new Field(FIELD, stringPayload(value), columnarBinaryFieldType()));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
        }
    }

    private boolean segmentsArePlain(Directory dir) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("segments before the merge", SEGMENTS, reader.leaves().size());
            for (LeafReaderContext leaf : reader.leaves()) {
                if (stringColumn(leaf.reader()).hasDictionary()) {
                    return false;
                }
            }
            return true;
        }
    }

    private static void forceMerge(Directory dir, DictionaryPolicy dictionaryPolicy, SummaryPolicy summaryPolicy) throws IOException {
        final LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
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

    private static StringColumnReader stringColumn(LeafReader leaf) throws IOException {
        final BinaryDocValues values = leaf.getBinaryDocValues(FIELD);
        assertTrue("expected a columnar column, got " + values, values instanceof ColumnarStringBinaryDocValues);
        return ((ColumnarStringBinaryDocValues) values).reader();
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
