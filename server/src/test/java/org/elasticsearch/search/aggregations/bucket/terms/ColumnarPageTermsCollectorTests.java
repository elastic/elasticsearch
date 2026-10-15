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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.FilterLeafCollector;
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
import org.elasticsearch.index.fielddata.ColumnarPayloadSortableBinaryDocValues;
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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Counting a columnar field with one value a document a page at a time has to give the counts of reading it a document at a
 * time. Each test searches a real columnar segment, so Lucene decides how the matches are delivered, and compares the
 * collector with a count made from the documents. A document is its slot: a value, a null, or no field.
 */
public class ColumnarPageTermsCollectorTests extends ESTestCase {

    private static final String FIELD = "kw";

    /** What a search counted, and how the documents arrived. */
    private record Outcome(Map<String, Long> counts, int perDocumentDocs, int ordinalPages, int flatPages) {}

    /** Counts into a map. The real target is the aggregator, which needs a whole search context. */
    private static final class CountingTarget implements ColumnarPageTermsCollector.Target {
        final Map<String, Long> counts = new TreeMap<>();
        private final ColumnarPageTermsCollector.Scratch scratch = new ColumnarPageTermsCollector.Scratch(PageBudget.UNLIMITED);

        @Override
        public void count(BytesRef term, long docCount) {
            counts.merge(term.utf8ToString(), docCount, Long::sum);
        }

        @Override
        public PageBudget budget() {
            return PageBudget.UNLIMITED;
        }

        @Override
        public ColumnarPageTermsCollector.Scratch scratch() {
            return scratch;
        }
    }

    /** The aggregator's ordinary per-document collector. */
    private static final class PerDocumentCollector extends LeafBucketCollector {
        private final ColumnarPayloadSortableBinaryDocValues values;
        private final ColumnarPageTermsCollector.Target target;
        int docs;

        PerDocumentCollector(ColumnarPayloadSortableBinaryDocValues values, ColumnarPageTermsCollector.Target target) {
            this.values = values;
            this.target = target;
        }

        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
            docs++;
            if (values.advanceExact(doc)) {
                target.count(values.nextValue(), 1);
            }
        }
    }

    /** A column that repeats a lot is read as dictionary pages. */
    public void testFewTermsOverSeveralPages() throws IOException {
        final String[] vocabulary = { "7.17.0", "8.4.1", "8.11.0", "8.12.2", "9.0.0" };
        final String[][] docs = new String[ColumnarPageTermsCollector.PAGE_DOCS * 2 + randomIntBetween(1, 1000)][];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = new String[] { vocabulary[randomIntBetween(0, vocabulary.length - 1)] };
        }
        final Outcome outcome = search(docs, Set.of(), null, new MatchAllDocsQuery(), false);
        assertThat(outcome.counts, equalTo(expected(docs, null)));
        assertThat("no document went the ordinary way", outcome.perDocumentDocs, equalTo(0));
        assertThat("every page is a dictionary and ordinals", outcome.ordinalPages, equalTo(3));
        assertThat(outcome.flatPages, equalTo(0));
    }

    /** A column that does not repeat is read as flat pages. */
    public void testNoRepeatsAreCountedAsFlatPages() throws IOException {
        final String[][] docs = new String[3000][];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = new String[] { "unique-" + i };
        }
        final Outcome outcome = search(docs, Set.of(), null, new MatchAllDocsQuery(), false);
        assertThat(outcome.counts, equalTo(expected(docs, null)));
        assertThat("a page of distinct values is handed over as values", outcome.flatPages, equalTo(1));
        assertThat(outcome.ordinalPages, equalTo(0));
    }

    /** With deleted documents Lucene delivers bit-set windows, which are streams too. */
    public void testDeletedDocumentsAreLeftOutOfPages() throws IOException {
        final String[][] docs = randomDocs(5000, 6);
        final Set<Integer> deleted = new HashSet<>();
        for (int doc = 0; doc < docs.length; doc += randomIntBetween(1, 30)) {
            deleted.add(doc);
        }
        final Outcome outcome = search(docs, deleted, null, new MatchAllDocsQuery(), false);
        assertThat(outcome.counts, equalTo(expected(docs, live(docs.length, deleted, null))));
        assertThat("the live documents arrived as streams", outcome.perDocumentDocs, equalTo(0));
    }

    /** Documents delivered one at a time go the ordinary way. */
    public void testDocumentsOneAtATimeAreNotCountedAsPages() throws IOException {
        final String[][] docs = randomDocs(5000, 6);
        final Outcome outcome = search(docs, Set.of(), null, new MatchAllDocsQuery(), true);
        assertThat(outcome.counts, equalTo(expected(docs, null)));
        assertThat(outcome.perDocumentDocs, equalTo(docs.length));
        assertThat(outcome.ordinalPages + outcome.flatPages, equalTo(0));
    }

    /** Random vocabulary, missing, null, deleted and filtered documents, and delivery. */
    public void testRandomLayouts() throws IOException {
        for (int round = 0; round < 10; round++) {
            final String[][] docs = randomDocs(
                randomIntBetween(1, ColumnarPageTermsCollector.PAGE_DOCS * 2 + 500),
                randomFrom(2, 10, 200, 20000)
            );
            final Set<Integer> deleted = new HashSet<>();
            if (randomBoolean()) {
                // From the second document: a segment with no live documents is dropped.
                for (int doc = 1; doc < docs.length; doc += randomIntBetween(1, 30)) {
                    deleted.add(doc);
                }
            }
            Set<Integer> tagged = null;
            if (randomBoolean()) {
                tagged = new HashSet<>();
                final double share = randomFrom(0.02, 0.3, 0.9);
                for (int doc = 0; doc < docs.length; doc++) {
                    if (random().nextDouble() < share) {
                        tagged.add(doc);
                    }
                }
            }
            final Query query = tagged == null ? new MatchAllDocsQuery() : new TermQuery(new Term("tag", "yes"));
            final Outcome outcome = search(docs, deleted, tagged, query, randomBoolean());
            assertThat(outcome.counts, equalTo(expected(docs, live(docs.length, deleted, tagged))));
        }
    }

    /** The scratch arrays are charged once, and the counts for their growth only. */
    public void testScratchIsChargedForItsGrowth() {
        final long[] charged = new long[1];
        final ColumnarPageTermsCollector.Scratch scratch = new ColumnarPageTermsCollector.Scratch(bytes -> charged[0] += bytes);

        assertThat(scratch.docs().length, equalTo(ColumnarPageTermsCollector.PAGE_DOCS));
        final long docsCharge = charged[0];
        assertThat(docsCharge, equalTo((long) ColumnarPageTermsCollector.PAGE_DOCS * Integer.BYTES));
        scratch.docs();
        assertThat("charged once", charged[0], equalTo(docsCharge));

        scratch.counts(100);
        final long first = charged[0] - docsCharge;
        assertThat("an int a slot at least", first, greaterThanOrEqualTo(100L * Integer.BYTES));
        scratch.counts(50);
        assertThat("big enough already", charged[0] - docsCharge, equalTo(first));
        scratch.counts(10_000);
        assertThat(charged[0] - docsCharge, greaterThanOrEqualTo(10_000L * Integer.BYTES));
        assertThat("the growth, not the whole size again", charged[0] - docsCharge, lessThan(2 * 10_000L * Integer.BYTES));
    }

    /** A leaf's reader charge is returned when the next leaf starts, so leaves do not add up. */
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

    /** A document is {@code null} for no field, {@code {null}} for a null slot, or {@code {value}}. */
    private static String[][] randomDocs(int numDocs, int vocabularySize) {
        final String[][] docs = new String[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            final int shape = randomIntBetween(0, 19);
            if (shape != 0) { // zero leaves the field absent
                docs[i] = new String[] { shape == 1 ? null : "term-" + randomIntBetween(0, vocabularySize - 1) };
            }
        }
        return docs;
    }

    /** The documents a search sees: not deleted, and tagged when tags are used. */
    private static Set<Integer> live(int numDocs, Set<Integer> deleted, Set<Integer> tagged) {
        final Set<Integer> live = new HashSet<>();
        for (int doc = 0; doc < numDocs; doc++) {
            if (deleted.contains(doc) == false && (tagged == null || tagged.contains(doc))) {
                live.add(doc);
            }
        }
        return live;
    }

    /** The counts of reading a document at a time. */
    private static Map<String, Long> expected(String[][] docs, Set<Integer> only) {
        final Map<String, Long> counts = new TreeMap<>();
        for (int doc = 0; doc < docs.length; doc++) {
            if (docs[doc] != null && docs[doc][0] != null && (only == null || only.contains(doc))) {
                counts.merge(docs[doc][0], 1L, Long::sum);
            }
        }
        return counts;
    }

    /**
     * Writes the documents to one segment, deletes some, searches it, and counts through the collector under test.
     * {@code tagged} marks documents for a query to match, or is null. With {@code oneAtATime} streams are passed on a
     * document at a time.
     */
    private Outcome search(String[][] docs, Set<Integer> deleted, Set<Integer> tagged, Query query, boolean oneAtATime) throws IOException {
        final CountingTarget target = new CountingTarget();
        final PerDocumentCollector[] perDocument = new PerDocumentCollector[1];
        final ColumnarPageTermsCollector[] collector = new ColumnarPageTermsCollector[1];
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
                                perDocument[0] = new PerDocumentCollector(values, target);
                                collector[0] = new ColumnarPageTermsCollector(values.columnSource().reader(), target, perDocument[0]);
                                return oneAtATime ? new FilterLeafCollector(collector[0]) {
                                    @Override
                                    public void collect(DocIdStream stream) throws IOException {
                                        stream.forEach(this::collect);
                                    }
                                } : collector[0];
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
        if (collector[0] == null) {
            return new Outcome(target.counts, 0, 0, 0); // a segment the search had no use for
        }
        return new Outcome(target.counts, perDocument[0].docs, collector[0].ordinalPages, collector[0].flatPages);
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
