/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.string.DictionaryStringColumnReader;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.fielddata.ColumnarPayloadSortableBinaryDocValues;
import org.elasticsearch.index.fielddata.ValueDeduplicator;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.terms.MapStringTermsAggregator.BucketCollectConsumer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Counting a single-valued columnar keyword field through its dictionary ordinals has to give the counts of reading it a
 * document at a time. Each test searches real columnar segments, so the codec decides what the column holds, and compares
 * the collector with a count made from the documents. A document is its slot: a value, a null, or no field. Several
 * values in one document are written as {@code a|b}.
 */
public class ColumnarOrdinalTermsCollectorTests extends ESTestCase {

    private static final String FIELD = "kw";
    /** A document whose one slot is null, which has no value. */
    private static final String NULL_SLOT = "<null>";
    private static final String[] COMMON = { "alpha", "bravo", "charlie", "delta", "echo" };

    /** What a search counted, and how the collector went about it. */
    private record Outcome(
        Map<Long, Map<String, Long>> countsByOwner,
        int hashed,
        int knownBucket,
        int leaves,
        int ordinalLeaves,
        long escaped
    ) {
        /** The counts of one owning bucket. */
        Map<String, Long> counts(long owningBucketOrd) {
            return countsByOwner.getOrDefault(owningBucketOrd, Map.of());
        }
    }

    /**
     * Counts into maps, and gives each term a bucket the way the aggregator does. The real consumer is the aggregator,
     * which needs a whole search context.
     */
    private static final class CountingConsumer implements BucketCollectConsumer {
        final Map<Long, Map<String, Long>> countsByOwner = new TreeMap<>();
        private final Map<String, Long> bucketOf = new HashMap<>();
        private final List<Long> ownerOf = new ArrayList<>();
        private final List<String> termOf = new ArrayList<>();
        int hashed;
        int knownBucket;

        @Override
        public long accept(LeafBucketCollector sub, int doc, long owningBucketOrd, BytesRef bytes) {
            hashed++;
            final String term = bytes.utf8ToString();
            final Long known = bucketOf.get(owningBucketOrd + "/" + term);
            final long bucket;
            if (known == null) {
                bucket = termOf.size();
                bucketOf.put(owningBucketOrd + "/" + term, bucket);
                ownerOf.add(owningBucketOrd);
                termOf.add(term);
            } else {
                bucket = known;
            }
            count(owningBucketOrd, term);
            return bucket;
        }

        @Override
        public void acceptExisting(LeafBucketCollector sub, int doc, long bucketOrdinal) {
            knownBucket++;
            final int bucket = Math.toIntExact(bucketOrdinal);
            count(ownerOf.get(bucket), termOf.get(bucket));
        }

        private void count(long owningBucketOrd, String term) {
            countsByOwner.computeIfAbsent(owningBucketOrd, o -> new TreeMap<>()).merge(term, 1L, Long::sum);
        }
    }

    /** The aggregator's ordinary per-document collector, which is also the one the ordinal collector falls back to. */
    private static final class PerDocumentCollector extends LeafBucketCollector {
        private final ColumnarPayloadSortableBinaryDocValues values;
        private final IncludeExclude.StringFilter filter;
        private final CountingConsumer consumer;
        private final ValueDeduplicator duplicates;

        PerDocumentCollector(ColumnarPayloadSortableBinaryDocValues values, IncludeExclude.StringFilter filter, CountingConsumer consumer) {
            this.values = values;
            this.filter = filter;
            this.consumer = consumer;
            this.duplicates = new ValueDeduplicator(values);
        }

        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
            if (values.advanceExact(doc)) {
                final int count = values.docValueCount();
                duplicates.reset(count);
                for (int i = 0; i < count; i++) {
                    final BytesRef value = values.nextValue();
                    if (filter != null && filter.accept(value) == false) {
                        continue;
                    }
                    if (duplicates.seen(value) == false) {
                        consumer.accept(NO_OP_COLLECTOR, doc, owningBucketOrd, value);
                    }
                }
            }
        }
    }

    /** What to search for, and how. */
    private record Search(
        List<String[]> segments,
        Set<Integer> deleted,
        Set<Integer> tagged,
        IncludeExclude filter,
        boolean alternateOwners
    ) {
        Search(String[]... segments) {
            this(List.of(segments), Set.of(), null, null, false);
        }

        Search withFilter(IncludeExclude filter) {
            return new Search(segments, deleted, tagged, filter, alternateOwners);
        }

        Search withOwners() {
            return new Search(segments, deleted, tagged, filter, true);
        }

        String[] docs() {
            return segments.stream().flatMap(Arrays::stream).toArray(String[]::new);
        }
    }

    /** A column that repeats a lot is counted from its ordinals, and each term is hashed once. */
    public void testFewTermsAreHashedOnce() throws IOException {
        final String[] docs = randomDocs(5000, COMMON.length, 0, 0);
        final Outcome outcome = search(new Search(docs));
        assertThat(outcome.counts(0), equalTo(expected(new Search(docs), null)));
        assertThat("the dictionary is read", outcome.ordinalLeaves, equalTo(1));
        assertThat("each term once", outcome.hashed, equalTo(distinct(docs).size()));
        assertThat("every other document went to its bucket", outcome.knownBucket, equalTo(valued(docs) - outcome.hashed));
    }

    /** The bucket of an ordinal is for one segment, so each segment starts over, whatever the last one held. */
    public void testEachSegmentHashesItsTermsAgain() throws IOException {
        final List<String[]> segments = new ArrayList<>();
        int hashed = 0;
        final int segmentCount = randomIntBetween(2, 5);
        for (int segment = 0; segment < segmentCount; segment++) {
            // Overlapping vocabularies, so a term has a different ordinal from one segment to the next.
            final String[] docs = new String[randomIntBetween(500, 2000)];
            final int from = randomIntBetween(0, 3);
            for (int i = 0; i < docs.length; i++) {
                docs[i] = "term-" + (from + randomIntBetween(0, 4));
            }
            segments.add(docs);
            hashed += distinct(docs).size();
        }
        final Search search = new Search(segments, Set.of(), null, null, false);
        final Outcome outcome = search(search);
        assertThat(outcome.counts(0), equalTo(expected(search, null)));
        assertThat(outcome.leaves, equalTo(segments.size()));
        assertThat(outcome.ordinalLeaves, equalTo(segments.size()));
        assertThat("each term once for each segment", outcome.hashed, equalTo(hashed));
    }

    /** A document with no field, and one whose slot is null, hold no value. */
    public void testMissingAndNullDocumentsAreSkipped() throws IOException {
        final String[] docs = randomDocs(3000, 4, 0.2, 0.2);
        assertThat(Arrays.stream(docs).filter(d -> d == null).count(), greaterThan(0L));
        assertThat(Arrays.stream(docs).filter(NULL_SLOT::equals).count(), greaterThan(0L));
        final Outcome outcome = search(new Search(docs));
        assertThat(outcome.counts(0), equalTo(expected(new Search(docs), null)));
    }

    /**
     * The dictionary is capped relative to the column's size, so when some values are long and rare it names only some
     * of them and the rest escape. An escaped value is not in the dictionary, so it is read and hashed for each document.
     */
    public void testEscapedValuesAreCounted() throws IOException {
        final String[] docs = new String[5000];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = i % 12 == 1 ? "unique-" + i + "-" + "x".repeat(120) : COMMON[randomIntBetween(0, COMMON.length - 1)];
        }
        final Outcome outcome = search(new Search(docs));
        assertThat(outcome.counts(0), equalTo(expected(new Search(docs), null)));
        assertThat("the dictionary is read", outcome.ordinalLeaves, equalTo(1));
        assertThat("some values escaped, or this does not test them", outcome.escaped, greaterThan(0L));
        assertThat("an escaped value is hashed for each document", outcome.hashed, greaterThan(COMMON.length));
        assertThat("and the rest are not", outcome.knownBucket, greaterThan(0));
    }

    /** A term the filter refuses is refused once and then skipped, in the dictionary or escaped. */
    public void testIncludeAndExclude() throws IOException {
        final String[] docs = new String[4000];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = i % 12 == 1 ? "unique-" + i + "-" + "x".repeat(120) : "term-" + randomIntBetween(0, 9);
        }
        for (IncludeExclude filter : List.of(
            new IncludeExclude(null, "term-1.*|unique-1.*", null, null),
            new IncludeExclude("term-[2-4]|unique-.*", null, null, null),
            new IncludeExclude("term-.*", "term-5", null, null)
        )) {
            final Search search = new Search(docs).withFilter(filter);
            final Outcome outcome = search(search);
            assertThat(outcome.counts(0), equalTo(expected(search, filter)));
            assertThat(outcome.counts(0).size(), greaterThan(0));
        }
    }

    /** Deleted documents are left out. */
    public void testDeletedDocumentsAreLeftOut() throws IOException {
        final String[] docs = randomDocs(5000, 6, 0.05, 0.05);
        final Set<Integer> deleted = new HashSet<>();
        for (int doc = 0; doc < docs.length; doc += randomIntBetween(1, 30)) {
            deleted.add(doc);
        }
        final Search search = new Search(List.<String[]>of(docs), deleted, null, null, false);
        assertThat(search(search).counts(0), equalTo(expected(search, null)));
    }

    /** Owning bucket zero is read from ordinals, and any other goes to the per-document collector. */
    public void testOtherOwningBucketsUseTheFallback() throws IOException {
        final String[] docs = randomDocs(3000, 5, 0.05, 0.05);
        final Search search = new Search(docs).withOwners();
        final Outcome outcome = search(search);
        assertThat(outcome.counts(0), equalTo(expected(search, null, 0)));
        assertThat(outcome.counts(1), equalTo(expected(search, null, 1)));
        assertThat(outcome.counts(1).size(), greaterThan(0));
    }

    /**
     * A document with several values goes to the per-document collector, and the rest of the segment keeps the cache.
     * A document of several slots with one value, the others null, is one value.
     */
    public void testMultiValuedDocumentsGoToTheFallback() throws IOException {
        final String[] docs = new String[3000];
        for (int i = 0; i < docs.length; i++) {
            final String value = COMMON[i % COMMON.length];
            docs[i] = switch (i % 6) {
                case 0 -> value + "|" + COMMON[(i + 1) % COMMON.length];
                case 1 -> NULL_SLOT + "|" + value;
                case 2 -> NULL_SLOT + "|" + NULL_SLOT;
                case 3, 4, 5 -> value;
                default -> throw new IllegalStateException();
            };
        }
        final Search search = new Search(docs);
        final Outcome outcome = search(search);
        assertThat("the segment is still read from its ordinals", outcome.ordinalLeaves, equalTo(1));
        assertThat(outcome.counts(0), equalTo(expected(search, null)));
        assertThat("the documents of one value used the cache", outcome.knownBucket, greaterThan(0));
    }

    /**
     * A value a document holds more than once counts once. Such a document holds several non-null slots, so it goes to the
     * per-document collector, which drops the repeats.
     */
    public void testAValueRepeatedInADocumentCountsOnce() throws IOException {
        final String[] docs = new String[3000];
        for (int i = 0; i < docs.length; i++) {
            final String value = COMMON[i % COMMON.length];
            final String other = COMMON[(i + 1) % COMMON.length];
            docs[i] = switch (i % 5) {
                case 0 -> value + "|" + value;
                case 1 -> value + "|" + other + "|" + value;
                case 2 -> NULL_SLOT + "|" + value + "|" + value;
                case 3, 4 -> value;
                default -> throw new IllegalStateException();
            };
        }
        final Search search = new Search(docs);
        final Outcome outcome = search(search);
        assertThat("the segment is still read from its ordinals", outcome.ordinalLeaves, equalTo(1));
        final Map<String, Long> expected = expected(search, null);
        assertThat(outcome.counts(0), equalTo(expected));
        final long slots = Arrays.stream(docs)
            .flatMap(doc -> Arrays.stream(doc.split("\\|")))
            .filter(term -> term.equals(NULL_SLOT) == false)
            .count();
        assertThat("the documents do hold repeats", slots, greaterThan(expected.values().stream().mapToLong(count -> count).sum()));
        assertThat("the documents of one value used the cache", outcome.knownBucket, greaterThan(0));
    }

    /** A column with no dictionary is left to the per-document collector. */
    public void testColumnWithoutADictionaryIsNotReadByOrdinal() throws IOException {
        final String[] docs = new String[3000];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = "unique-" + i;
        }
        final Search search = new Search(docs);
        final Outcome outcome = search(search);
        assertThat(outcome.ordinalLeaves, equalTo(0));
        assertThat(outcome.counts(0), equalTo(expected(search, null)));
    }

    /** Segments, deletes, filters, tags, nulls and missing documents, in any mix. */
    public void testRandomLayouts() throws IOException {
        for (int round = 0; round < 10; round++) {
            final List<String[]> segments = new ArrayList<>();
            final int vocabulary = randomFrom(2, 10, 200, 20000);
            for (int segment = 0; segment < randomIntBetween(1, 4); segment++) {
                segments.add(randomDocs(randomIntBetween(1, 3000), vocabulary, 0.05, 0.05));
            }
            final int numDocs = segments.stream().mapToInt(s -> s.length).sum();
            final Set<Integer> deleted = new HashSet<>();
            if (randomBoolean()) {
                // From the second document: a segment with no live documents is dropped.
                for (int doc = 1; doc < numDocs; doc += randomIntBetween(1, 30)) {
                    deleted.add(doc);
                }
            }
            Set<Integer> tagged = null;
            if (randomBoolean()) {
                tagged = new HashSet<>();
                final double share = randomFrom(0.02, 0.3, 0.9);
                for (int doc = 0; doc < numDocs; doc++) {
                    if (random().nextDouble() < share) {
                        tagged.add(doc);
                    }
                }
            }
            final IncludeExclude filter = randomBoolean() ? null : new IncludeExclude(null, "term-1.*", null, null);
            final Search search = new Search(segments, deleted, tagged, filter, randomBoolean());
            final Outcome outcome = search(search);
            assertThat(outcome.counts(0), equalTo(expected(search, filter, 0)));
            assertThat(outcome.counts(1), equalTo(search.alternateOwners ? expected(search, filter, 1) : Map.of()));
        }
    }

    /** The cache is charged for its growth only, so segments do not add up. */
    public void testBucketsAreChargedForTheirGrowth() {
        final long[] charged = new long[1];
        final ColumnarOrdinalTermsCollector.OrdinalBuckets buckets = new ColumnarOrdinalTermsCollector.OrdinalBuckets();

        buckets.reset(100, bytes -> charged[0] += bytes);
        assertThat(charged[0], equalTo(100L * Long.BYTES));
        buckets.reset(50, bytes -> charged[0] += bytes);
        buckets.reset(100, bytes -> charged[0] += bytes);
        assertThat("big enough already", charged[0], equalTo(100L * Long.BYTES));
        buckets.reset(1000, bytes -> charged[0] += bytes);
        assertThat("the growth, not the whole size again", charged[0], equalTo(100L * Long.BYTES + 900L * Long.BYTES));
    }

    /** A segment never sees what the last one cached. */
    public void testBucketsAreClearedForEachSegment() {
        final ColumnarOrdinalTermsCollector.OrdinalBuckets buckets = new ColumnarOrdinalTermsCollector.OrdinalBuckets();
        long[] cache = buckets.reset(100, bytes -> {});
        Arrays.fill(cache, 7);
        cache = buckets.reset(60, bytes -> {});
        for (int ordinal = 0; ordinal < 60; ordinal++) {
            assertThat("no bucket known yet, which the collector marks as -1", cache[ordinal], equalTo(-1L));
        }
    }

    /** A document is {@code null} for no field, {@link #NULL_SLOT} for a null value, or its value. */
    private static String[] randomDocs(int numDocs, int vocabularySize, double missing, double nulls) {
        final String[] docs = new String[numDocs];
        for (int i = 0; i < numDocs; i++) {
            final double shape = random().nextDouble();
            if (shape >= missing) {
                docs[i] = shape < missing + nulls ? NULL_SLOT : "term-" + randomIntBetween(0, vocabularySize - 1);
            }
        }
        return docs;
    }

    private static Set<String> distinct(String[] docs) {
        final Set<String> distinct = new HashSet<>();
        for (String doc : docs) {
            if (doc != null && doc.equals(NULL_SLOT) == false) {
                distinct.add(doc);
            }
        }
        return distinct;
    }

    private static int valued(String[] docs) {
        return (int) Arrays.stream(docs).filter(d -> d != null && d.equals(NULL_SLOT) == false).count();
    }

    private static boolean accepts(IncludeExclude filter, String term) {
        if (filter == null) {
            return true;
        }
        return filter.convertToStringFilter(DocValueFormat.RAW).accept(new BytesRef(term));
    }

    private static Map<String, Long> expected(Search search, IncludeExclude filter) {
        return expected(search, filter, 0);
    }

    /** The counts of reading a document at a time, for the documents of one owning bucket when owners alternate. */
    private static Map<String, Long> expected(Search search, IncludeExclude filter, int owner) {
        final String[] docs = search.docs();
        final Map<String, Long> counts = new TreeMap<>();
        for (int doc = 0; doc < docs.length; doc++) {
            final boolean live = search.deleted.contains(doc) == false && (search.tagged == null || search.tagged.contains(doc));
            if (live == false || docs[doc] == null || docs[doc].equals(NULL_SLOT) || (search.alternateOwners && doc % 2 != owner)) {
                continue;
            }
            // A value a document holds twice counts once, as it does in the aggregator.
            for (String term : new LinkedHashSet<>(Arrays.asList(docs[doc].split("\\|")))) {
                if (term.equals(NULL_SLOT) == false && accepts(filter, term)) {
                    counts.merge(term, 1L, Long::sum);
                }
            }
        }
        return counts;
    }

    /**
     * Writes each segment of documents, deletes some, searches, and counts through the collector under test, the way the
     * aggregator builds it for each leaf: from ordinals when it can, and per document when it cannot.
     */
    private Outcome search(Search search) throws IOException {
        final CountingConsumer consumer = new CountingConsumer();
        final ColumnarOrdinalTermsCollector.OrdinalBuckets ordinalBuckets = new ColumnarOrdinalTermsCollector.OrdinalBuckets();
        final IncludeExclude.StringFilter filter = search.filter == null ? null : search.filter.convertToStringFilter(DocValueFormat.RAW);
        final int[] leaves = new int[2];
        final long[] escaped = new long[1];
        final Query query = search.tagged == null ? Queries.ALL_DOCS_INSTANCE : new TermQuery(new Term("tag", "yes"));
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig config = new IndexWriterConfig().setCodec(columnarCodec()).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                int id = 0;
                for (String[] segment : search.segments) {
                    for (String value : segment) {
                        final Document doc = new Document();
                        doc.add(new StringField("id", Integer.toString(id), Field.Store.NO));
                        if (search.tagged != null) {
                            doc.add(new StringField("tag", search.tagged.contains(id) ? "yes" : "no", Field.Store.NO));
                        }
                        if (value != null) {
                            doc.add(new Field(FIELD, encode(value), binaryDocValuesType()));
                        }
                        writer.addDocument(doc);
                        id++;
                    }
                    writer.commit(); // one segment each
                }
                for (int doc : search.deleted) {
                    writer.deleteDocuments(new Term("id", Integer.toString(doc)));
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                new IndexSearcher(reader).search(query, new CollectorManager<Collector, Void>() {
                    @Override
                    public Collector newCollector() {
                        return new Collector() {
                            @Override
                            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                                final ColumnarPayloadSortableBinaryDocValues values = ColumnarPayloadSortableBinaryDocValues.from(
                                    context.reader(),
                                    FIELD
                                );
                                final LeafBucketCollector perDocument = new PerDocumentCollector(values, filter, consumer);
                                final LeafBucketCollector byOrdinal = ColumnarOrdinalTermsCollector.create(
                                    values,
                                    filter,
                                    LeafBucketCollector.NO_OP_COLLECTOR,
                                    consumer,
                                    ordinalBuckets,
                                    bytes -> {},
                                    perDocument
                                );
                                leaves[0]++;
                                if (byOrdinal != null) {
                                    leaves[1]++;
                                    escaped[0] += ((DictionaryStringColumnReader) values.columnSource().reader()).escapeCount();
                                }
                                final LeafBucketCollector collector = byOrdinal != null ? byOrdinal : perDocument;
                                final int docBase = context.docBase;
                                return new LeafCollector() {
                                    @Override
                                    public void setScorer(Scorable scorer) {}

                                    @Override
                                    public void collect(int doc) throws IOException {
                                        collector.collect(doc, search.alternateOwners ? (docBase + doc) % 2 : 0);
                                    }
                                };
                            }

                            @Override
                            public ScoreMode scoreMode() {
                                return ScoreMode.COMPLETE_NO_SCORES;
                            }
                        };
                    }

                    @Override
                    public Void reduce(Collection<Collector> collectors) {
                        return null;
                    }
                });
            }
        }
        return new Outcome(consumer.countsByOwner, consumer.hashed, consumer.knownBucket, leaves[0], leaves[1], escaped[0]);
    }

    private static BytesRef encode(String value) {
        final List<BytesRef> slots = new ArrayList<>();
        for (String slot : value.split("\\|")) {
            slots.add(slot.equals(NULL_SLOT) ? null : new BytesRef(slot));
        }
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(slots));
    }

    private static FieldType binaryDocValuesType() {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.freeze();
        return type;
    }

    private static Codec columnarCodec() {
        final Codec base = TestUtil.getDefaultCodec();
        final DocValuesFormat columnar = new ColumNARDocValuesFormat(field -> ColumnarFieldType.STRING);
        return new FilterCodec(base.getName(), base) {
            private final DocValuesFormat perField = new PerFieldDocValuesFormat() {
                @Override
                public DocValuesFormat getDocValuesFormatForField(String field) {
                    return columnar;
                }
            };

            @Override
            public DocValuesFormat docValuesFormat() {
                return perField;
            }
        };
    }
}
