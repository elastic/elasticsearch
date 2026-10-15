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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.string.PageBudget;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.index.fielddata.ColumnarPayloadSortableBinaryDocValues;
import org.elasticsearch.index.fielddata.ValueDeduplicator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Counting a columnar field a page at a time has to give the counts that reading it a document at a time gives:
 * a document counts once for each distinct value it holds, however it is laid out in the column. Each test builds
 * a real columnar segment and compares the collector with a count made straight from the documents.
 */
public class ColumnarPageTermsCollectorTests extends ESTestCase {

    private static final String FIELD = "kw";

    /** The collector the last {@link #collect} built, so a test can ask which shapes of page it was given. */
    private ColumnarPageTermsCollector lastCollector;
    private PerDocumentCollector lastPerDocument;

    /**
     * The fewest documents a batch needs to be read as a page. One reads every batch as a page, so that the small
     * columns these tests use still exercise what a page does. Zero uses what the collector uses for the column.
     */
    private int minPageDocs = 1;

    private ColumnarPageTermsCollector pageCollector(
        ColumnarPayloadSortableBinaryDocValues values,
        CountingTarget target,
        PerDocumentCollector perDocument
    ) {
        final StringColumnReader reader = values.columnSource().reader();
        return minPageDocs == 0
            ? new ColumnarPageTermsCollector(reader, target, perDocument)
            : new ColumnarPageTermsCollector(reader, target, perDocument, minPageDocs);
    }

    /**
     * Counts into a map. The real target is the aggregator, which cannot be built without a whole search context,
     * so this stands in for it and does nothing but add up what it is given.
     */
    private static final class CountingTarget implements ColumnarPageTermsCollector.Target {
        final Map<String, Long> counts = new TreeMap<>();
        private final int[] scratch = new int[ColumnarPageTermsCollector.PAGE_DOCS];
        private final ColumnarPageTermsCollector.SlotScratch slots = new ColumnarPageTermsCollector.SlotScratch(PageBudget.UNLIMITED);

        @Override
        public void count(BytesRef term, long docCount) {
            counts.merge(term.utf8ToString(), docCount, Long::sum);
        }

        @Override
        public PageBudget budget() {
            return PageBudget.UNLIMITED;
        }

        @Override
        public int[] scratchDocs() {
            return scratch;
        }

        @Override
        public ColumnarPageTermsCollector.SlotScratch slotScratch() {
            return slots;
        }
    }

    /**
     * What the aggregator's ordinary collector does for a document, which a page cannot: advance, take the values,
     * leave out a repeat, count each. Here rather than the real one for the same reason as the target.
     */
    private static final class PerDocumentCollector extends LeafBucketCollector {
        private final ColumnarPayloadSortableBinaryDocValues values;
        private final ValueDeduplicator duplicates;
        private final ColumnarPageTermsCollector.Target target;
        int docs;

        PerDocumentCollector(ColumnarPayloadSortableBinaryDocValues values, ColumnarPageTermsCollector.Target target) {
            this.values = values;
            this.duplicates = new ValueDeduplicator(values);
            this.target = target;
        }

        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
            docs++;
            if (values.advanceExact(doc) == false) {
                return;
            }
            final int valueCount = values.docValueCount();
            duplicates.reset(valueCount);
            for (int i = 0; i < valueCount; i++) {
                final BytesRef value = values.nextValue();
                if (duplicates.seen(value) == false) {
                    target.count(value, 1);
                }
            }
        }
    }

    /** A column that repeats a lot, which is the case a dictionary page is for. Several pages' worth of documents. */
    public void testFewTermsOverSeveralPages() throws IOException {
        final String[] vocabulary = { "7.17.0", "8.4.1", "8.11.0", "8.12.2", "9.0.0" };
        final int numDocs = ColumnarPageTermsCollector.PAGE_DOCS * 2 + randomIntBetween(1, 1000);
        final String[][] docs = new String[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = new String[] { vocabulary[randomIntBetween(0, vocabulary.length - 1)] };
        }
        final CountingTarget target = collectAll(docs);
        assertThat(target.counts, equalTo(expected(docs, null)));
        assertThat("no document went the ordinary way", lastPerDocument.docs, equalTo(0));
        assertThat("every page is a dictionary and ordinals", lastCollector.ordinalPages, equalTo(3));
        assertThat(lastCollector.flatPages, equalTo(0));
    }

    /** A document that holds a value twice still counts once for it. */
    public void testADocumentCountsOncePerDistinctValue() throws IOException {
        final String[][] docs = { { "a", "a", "b" }, { "b" }, { "a", "b", "a", "b" }, { "c", "c", "c" }, { "a" } };
        final CountingTarget target = collectAll(docs);
        assertThat(target.counts, equalTo(expected(docs, null)));
        assertThat(target.counts.get("a"), equalTo(3L));
        assertThat(target.counts.get("c"), equalTo(1L));
    }

    /** As above with several values a document, some repeated within the document. */
    public void testNoRepeatsAcrossDocumentsButWithinThem() throws IOException {
        final int numDocs = 3000;
        final String[][] docs = new String[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = new String[] { "u-" + i, "v-" + i, "u-" + i };
        }
        final CountingTarget target = collectAll(docs);
        assertThat(target.counts, equalTo(expected(docs, null)));
        assertThat("the page is flat, so a repeat within a document is found by its bytes", lastCollector.flatPages, equalTo(1));
    }

    /** Documents that arrive one at a time, as a sparse match does, go the ordinary way and count the same. */
    public void testDocumentsOneAtATimeAreNotCountedAsPages() throws IOException {
        final String[][] docs = randomDocs(5000, 6, 3);
        final Set<Integer> chosen = new HashSet<>();
        for (int i = 0; i < docs.length; i++) {
            if (randomIntBetween(0, 3) == 0) {
                chosen.add(i);
            }
        }
        final CountingTarget target = collect(docs, chosen, false);
        assertThat(target.counts, equalTo(expected(docs, chosen)));
        assertThat("every document went the ordinary way", lastPerDocument.docs, equalTo(chosen.size()));
        assertThat(lastCollector.ordinalPages + lastCollector.flatPages, equalTo(0));
    }

    /** Runs of consecutive documents are pages, the documents between them are not, and the order is kept. */
    public void testRangesAndSingleDocumentsMixed() throws IOException {
        final String[][] docs = randomDocs(9000, 8, 2);
        final Set<Integer> chosen = new HashSet<>();
        final List<int[]> runs = new ArrayList<>();
        for (int doc = 0; doc < docs.length;) {
            final int length = randomIntBetween(1, 3000);
            final int end = Math.min(docs.length, doc + length);
            if (randomBoolean()) {
                runs.add(new int[] { doc, end });
                for (int d = doc; d < end; d++) {
                    chosen.add(d);
                }
            }
            doc = end + randomIntBetween(0, 5);
        }
        final CountingTarget target = new CountingTarget();
        withValues(docs, values -> {
            final PerDocumentCollector perDocument = new PerDocumentCollector(values, target);
            final ColumnarPageTermsCollector collector = pageCollector(values, target, perDocument);
            for (int[] run : runs) {
                if (run[1] - run[0] == 1 || randomBoolean()) {
                    collector.collectRange(run[0], run[1]);
                } else {
                    for (int doc = run[0]; doc < run[1]; doc++) {
                        collector.collect(doc, 0);
                    }
                }
            }
        });
        assertThat(target.counts, equalTo(expected(docs, chosen)));
    }

    /**
     * What a search does with a dense match: Lucene gives the collector the documents as a range, so they are all
     * counted as pages. The collector sees what a real bulk scorer sends it, not what these tests send.
     */
    public void testALuceneSearchOverADenseMatchCountsPages() throws IOException {
        final String[][] docs = randomDocs(5000, 6, 2);
        final CountingTarget target = new CountingTarget();
        final PerDocumentCollector[] perDocument = new PerDocumentCollector[1];
        searchAll(docs, Set.of(), target, perDocument);
        assertThat(target.counts, equalTo(expected(docs, null)));
        assertThat("every document arrived in a range, so none went the ordinary way", perDocument[0].docs, equalTo(0));
    }

    /**
     * With documents deleted Lucene sends the collector windows of matches found in a bit set, not a range. They arrive
     * as a stream of doc IDs too, so they are counted as pages as well.
     */
    public void testALuceneSearchWithDeletionsCountsTheSame() throws IOException {
        final String[][] docs = randomDocs(5000, 6, 2);
        final Set<Integer> deleted = new HashSet<>();
        for (int doc = 0; doc < docs.length; doc += randomIntBetween(1, 30)) {
            deleted.add(doc);
        }
        final Set<Integer> live = new HashSet<>();
        for (int doc = 0; doc < docs.length; doc++) {
            if (deleted.contains(doc) == false) {
                live.add(doc);
            }
        }
        final CountingTarget target = new CountingTarget();
        final PerDocumentCollector[] perDocument = new PerDocumentCollector[1];
        searchAll(docs, deleted, target, perDocument);
        assertThat(target.counts, equalTo(expected(docs, live)));
        assertThat("the live documents arrived as streams, so none went the ordinary way", perDocument[0].docs, equalTo(0));
    }

    /** A filter matching some of the documents, however Lucene chooses to deliver them, counts what it matches. */
    public void testALuceneSearchOverASubsetCountsTheSame() throws IOException {
        final String[][] docs = randomDocs(6000, 6, 3);
        minPageDocs = randomFrom(0, 1);
        final double share = randomFrom(0.02, 0.3, 0.9);
        final Set<Integer> tagged = new HashSet<>();
        for (int doc = 0; doc < docs.length; doc++) {
            if (random().nextDouble() < share) {
                tagged.add(doc);
            }
        }
        final CountingTarget target = new CountingTarget();
        search(docs, Set.of(), tagged, new TermQuery(new Term("tag", "yes")), target, new PerDocumentCollector[1]);
        assertThat("share " + share, target.counts, equalTo(expected(docs, tagged)));
    }

    /** A column with one value a document is read as a page only from a batch of this many documents. */
    public void testSmallBatchesAreNotReadAsPagesForASingleValuedColumn() throws IOException {
        minPageDocs = 0;
        final int min = ColumnarPageTermsCollector.MIN_PAGE_DOCS_SINGLE_VALUED;
        final String[][] docs = new String[min * 2][];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = new String[] { "term-" + randomIntBetween(0, 3) };
        }
        final Outcome below = collectRange(docs, 0, min - 1);
        assertThat(below.counts, equalTo(expected(docs, range(0, min - 1))));
        assertThat("a batch one short is read a document at a time", below.perDocumentDocs, equalTo(min - 1));
        assertThat(below.pages, equalTo(0));

        final Outcome enough = collectRange(docs, 0, min);
        assertThat(enough.counts, equalTo(expected(docs, range(0, min))));
        assertThat("a batch of the minimum is a page", enough.perDocumentDocs, equalTo(0));
        assertThat(enough.pages, greaterThan(0));
    }

    /** Documents holding several values are read more slowly as a page, so the batch has to be larger. */
    public void testMultiValuedColumnsNeedLargerBatches() throws IOException {
        minPageDocs = 0;
        final int min = ColumnarPageTermsCollector.MIN_PAGE_DOCS_MULTI_VALUED;
        assertThat(min, greaterThan(ColumnarPageTermsCollector.MIN_PAGE_DOCS_SINGLE_VALUED));
        final String[][] docs = new String[min * 2][];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = new String[] { "term-" + randomIntBetween(0, 3) };
        }
        docs[0] = new String[] { "term-0", "term-1" }; // one document with two values is enough to say so
        final Outcome below = collectRange(docs, 0, min - 1);
        assertThat(below.counts, equalTo(expected(docs, range(0, min - 1))));
        assertThat(below.perDocumentDocs, equalTo(min - 1));
        assertThat(below.pages, equalTo(0));

        final Outcome enough = collectRange(docs, 0, min);
        assertThat(enough.counts, equalTo(expected(docs, range(0, min))));
        assertThat(enough.perDocumentDocs, equalTo(0));
        assertThat(enough.pages, greaterThan(0));
    }

    /** The slot arrays are charged for what they grow by, and for nothing once they are big enough. */
    public void testSlotArraysAreChargedForTheirGrowth() {
        final long[] charged = new long[1];
        final PageBudget budget = bytes -> charged[0] += bytes;
        final ColumnarPageTermsCollector.SlotScratch slots = new ColumnarPageTermsCollector.SlotScratch(budget);

        slots.ensure(100);
        final long first = charged[0];
        assertThat("two int arrays of at least a slot each", first, greaterThanOrEqualTo(100L * 2 * Integer.BYTES));

        slots.ensure(50);
        assertThat("big enough already", charged[0], equalTo(first));

        slots.ensure(10_000);
        assertThat(charged[0], greaterThanOrEqualTo(10_000L * 2 * Integer.BYTES));
        assertThat("the growth, not the whole size again", charged[0], lessThan(2 * 10_000L * 2 * Integer.BYTES));
    }

    /**
     * A reader is garbage when its leaf is done, so what it was charged is returned for the next leaf. Without that the
     * charge of every leaf of a shard would stay until the aggregation closed.
     */
    public void testALeafBudgetIsReturnedWhenTheLeafIsDone() {
        final long[] held = new long[1];
        final long[] mostHeld = new long[1];
        final ColumnarPageTermsCollector.LeafBudget budget = new ColumnarPageTermsCollector.LeafBudget(bytes -> {
            held[0] += bytes;
            mostHeld[0] = Math.max(mostHeld[0], held[0]);
        });
        for (int leaf = 0; leaf < 20; leaf++) {
            budget.release(); // as the next leaf starts
            budget.charge(1000);
            budget.charge(500);
            assertThat("what this leaf holds, and no more", held[0], equalTo(1500L));
        }
        assertThat("twenty leaves never held more than one", mostHeld[0], equalTo(1500L));
        budget.release();
        assertThat("and the last is returned when the aggregation closes", held[0], equalTo(0L));
    }

    private record Outcome(Map<String, Long> counts, int perDocumentDocs, int pages) {}

    /** Collects {@code [from, to)} as one range, and reports how many documents went the ordinary way and how many pages. */
    private Outcome collectRange(String[][] docs, int from, int to) throws IOException {
        final CountingTarget target = new CountingTarget();
        final int[] perDocumentDocs = new int[1];
        final int[] pages = new int[1];
        withValues(docs, values -> {
            final PerDocumentCollector perDocument = new PerDocumentCollector(values, target);
            final ColumnarPageTermsCollector collector = pageCollector(values, target, perDocument);
            collector.collectRange(from, to);
            perDocumentDocs[0] = perDocument.docs;
            pages[0] = collector.ordinalPages + collector.flatPages;
        });
        return new Outcome(target.counts, perDocumentDocs[0], pages[0]);
    }

    private static Set<Integer> range(int from, int to) {
        final Set<Integer> docs = new HashSet<>();
        for (int doc = from; doc < to; doc++) {
            docs.add(doc);
        }
        return docs;
    }

    /** Random shapes: how many distinct values, how many a document, how much missing. */
    public void testRandomLayouts() throws IOException {
        for (int round = 0; round < 10; round++) {
            minPageDocs = randomFrom(0, 1);
            final int numDocs = randomIntBetween(1, ColumnarPageTermsCollector.PAGE_DOCS * 2 + 500);
            final int vocabularySize = randomFrom(2, 10, 200, 20000);
            final int maxSlots = randomFrom(1, 1, 3, 12);
            final String[][] docs = randomDocs(numDocs, vocabularySize, maxSlots);
            final CountingTarget target = collectAll(docs);
            assertThat(
                "vocabulary " + vocabularySize + ", up to " + maxSlots + " slots, " + numDocs + " docs",
                target.counts,
                equalTo(expected(docs, null))
            );
        }
    }

    private static String[][] randomDocs(int numDocs, int vocabularySize, int maxSlots) {
        final String[][] docs = new String[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            if (randomIntBetween(0, 19) == 0) {
                continue; // the field is absent
            }
            final String[] slots = new String[randomIntBetween(1, maxSlots)];
            for (int s = 0; s < slots.length; s++) {
                slots[s] = randomIntBetween(0, 19) == 0 ? null : "term-" + randomIntBetween(0, vocabularySize - 1);
            }
            docs[i] = slots;
        }
        return docs;
    }

    /** The counts a document-at-a-time reading gives: each document once for each distinct non-null value. */
    private static Map<String, Long> expected(String[][] docs, Set<Integer> only) {
        final Map<String, Long> counts = new TreeMap<>();
        for (int doc = 0; doc < docs.length; doc++) {
            if (docs[doc] == null || (only != null && only.contains(doc) == false)) {
                continue;
            }
            final Set<String> distinct = new HashSet<>();
            for (String slot : docs[doc]) {
                if (slot != null) {
                    distinct.add(slot);
                }
            }
            for (String value : distinct) {
                counts.merge(value, 1L, Long::sum);
            }
        }
        return counts;
    }

    private CountingTarget collectAll(String[][] docs) throws IOException {
        return collect(docs, null, true);
    }

    /**
     * Collects the documents. As a range when {@code asRange} and every document is wanted, the way a dense match
     * arrives; otherwise each one on its own.
     */
    private CountingTarget collect(String[][] docs, Set<Integer> only, boolean asRange) throws IOException {
        final CountingTarget target = new CountingTarget();
        withValues(docs, values -> {
            final PerDocumentCollector perDocument = new PerDocumentCollector(values, target);
            final ColumnarPageTermsCollector collector = pageCollector(values, target, perDocument);
            lastCollector = collector;
            lastPerDocument = perDocument;
            if (asRange && only == null) {
                collector.collectRange(0, docs.length);
            } else {
                for (int doc = 0; doc < docs.length; doc++) {
                    if (only == null || only.contains(doc)) {
                        collector.collect(doc, 0);
                    }
                }
            }
        });
        return target;
    }

    /**
     * Writes the documents, deletes some, and counts what a match-all search collects, through the collector
     * under test. {@code perDocument[0]} is left holding the ordinary collector it wrapped.
     */
    private void searchAll(String[][] docs, Set<Integer> deleted, CountingTarget target, PerDocumentCollector[] perDocument)
        throws IOException {
        search(docs, deleted, null, new MatchAllDocsQuery(), target, perDocument);
    }

    /**
     * As {@link #searchAll}, for any query. {@code tagged} marks documents with a field a query can match, or is null
     * for none.
     */
    private void search(
        String[][] docs,
        Set<Integer> deleted,
        Set<Integer> tagged,
        Query query,
        CountingTarget target,
        PerDocumentCollector[] perDocument
    ) throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec()))) {
                for (int i = 0; i < docs.length; i++) {
                    final Document doc = new Document();
                    doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                    if (tagged != null) {
                        doc.add(new StringField("tag", tagged.contains(i) ? "yes" : "no", Field.Store.NO));
                    }
                    if (docs[i] != null) {
                        doc.add(new Field(FIELD, encode(docs[i]), binaryDocValuesType()));
                    }
                    writer.addDocument(doc);
                }
                writer.commit();
                for (int doc : deleted) {
                    writer.deleteDocuments(new Term("id", Integer.toString(doc)));
                }
                if (deleted.isEmpty()) {
                    writer.forceMerge(1);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertThat("one segment", reader.leaves().size(), equalTo(1));
                assertThat(reader.numDeletedDocs(), equalTo(deleted.size()));
                final IndexSearcher searcher = new IndexSearcher(reader);
                searcher.search(query, new CollectorManager<Collector, Void>() {
                    @Override
                    public Collector newCollector() {
                        return new Collector() {
                            @Override
                            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                                final ColumnarPayloadSortableBinaryDocValues values = ColumnarPayloadSortableBinaryDocValues.from(
                                    context.reader(),
                                    FIELD
                                );
                                perDocument[0] = new PerDocumentCollector(values, target);
                                lastCollector = pageCollector(values, target, perDocument[0]);
                                return lastCollector;
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
    }

    private interface Check {
        void check(ColumnarPayloadSortableBinaryDocValues values) throws IOException;
    }

    /** {@code null} in place of a document's slots writes no field for it at all. */
    private void withValues(String[][] docs, Check check) throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec()))) {
                for (String[] slots : docs) {
                    final Document doc = new Document();
                    if (slots != null) {
                        doc.add(new Field(FIELD, encode(slots), binaryDocValuesType()));
                    }
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final ColumnarPayloadSortableBinaryDocValues values = ColumnarPayloadSortableBinaryDocValues.from(leaf, FIELD);
                assertNotNull("the field is a column", values.columnSource());
                check.check(values);
            }
        }
    }

    private static BytesRef encode(String[] slots) {
        final List<BytesRef> refs = new ArrayList<>(slots.length);
        for (String slot : slots) {
            refs.add(slot == null ? null : new BytesRef(slot));
        }
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(refs));
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
