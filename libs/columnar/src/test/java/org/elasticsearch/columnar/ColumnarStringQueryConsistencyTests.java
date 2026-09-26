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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.SummaryPolicy;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;

/**
 * The string queries over a real index — several segments, deleted documents, every shape a document can take — run
 * through Lucene's asserting searcher, which splits segments into ranges searched on several threads, and checked
 * against the values themselves.
 */
public class ColumnarStringQueryConsistencyTests extends ESTestCase {

    private static final String FIELD = "kw";
    private static final String ID = "id";
    private static final String[] VOCABULARY = { "", "a", "ab", "abc", "abd", "xyz", "google", "a.google.b", "zz-a-longer-value" };

    /** One query and the values it should select. */
    private record Case(String label, Query query, Predicate<String[]> expected) {}

    public void testQueriesAgreeWithTheValues() throws Exception {
        withIndex((searcher, docs) -> {
            for (Case c : cases()) {
                final FixedBitSet expected = expected(docs, c.expected());
                assertEquals(c.label(), expected, collect(searcher, c.query(), docs.length));
                assertEquals(c.label() + " count", expected.cardinality(), searcher.count(c.query()));
                // Its skip checks compare an iterator, which still sees deleted documents, with what was collected.
                if (searcher.getIndexReader().hasDeletions() == false) {
                    try {
                        QueryUtils.check(random(), c.query(), searcher, false);
                    } catch (AssertionError e) {
                        throw new AssertionError(c.label() + ": " + e.getMessage(), e);
                    }
                }
            }
        });
    }

    /** The same queries from several threads at once, over one reader. */
    public void testConcurrentSearches() throws Exception {
        withIndex((searcher, docs) -> {
            final List<Case> cases = cases();
            final List<FixedBitSet> expected = new ArrayList<>();
            for (Case c : cases) {
                expected.add(expected(docs, c.expected()));
            }
            final ExecutorService executor = Executors.newFixedThreadPool(between(2, 6));
            try {
                final List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < 8; t++) {
                    final List<Integer> order = new ArrayList<>();
                    for (int i = 0; i < cases.size(); i++) {
                        order.add(i);
                    }
                    Collections.shuffle(order, random());
                    futures.add(executor.submit(() -> {
                        for (int i : order) {
                            assertEquals(cases.get(i).label(), expected.get(i), collect(searcher, cases.get(i).query(), docs.length));
                        }
                        return null;
                    }));
                }
                for (Future<?> future : futures) {
                    future.get();
                }
            } finally {
                executor.shutdown();
                assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
            }
        });
    }

    private interface IndexCheck {
        void check(IndexSearcher searcher, String[][] docs) throws Exception;
    }

    /**
     * A random column: documents without the field, empty arrays, documents of nulls, several values, runs of equal
     * values, at a random density, with or without a dictionary, in several segments, some documents deleted.
     */
    private void withIndex(IndexCheck check) throws Exception {
        final String[][] docs = new String[between(2_000, 20_000)][];
        final double present = randomFrom(1.0, 0.9, 0.3, 0.02);
        final boolean multiValued = randomBoolean();
        String current = randomFrom(VOCABULARY);
        for (int d = 0; d < docs.length; d++) {
            if (random().nextInt(12) == 0) {
                current = random().nextInt(8) == 0 ? "rare-" + d : randomFrom(VOCABULARY);
            }
            if (random().nextDouble() >= present) {
                continue;
            }
            final int slots = multiValued && random().nextInt(4) == 0 ? between(0, 3) : 1;
            docs[d] = new String[slots];
            for (int s = 0; s < slots; s++) {
                docs[d][s] = multiValued && random().nextInt(6) == 0 ? null : s == 0 ? current : randomFrom(VOCABULARY);
            }
        }
        final DictionaryPolicy policy = randomFrom(DictionaryPolicy.NONE, new DictionaryPolicy(512 * 1024, 0.5, 0.2));
        final ColumNARDocValuesFormat format = new ColumNARDocValuesFormat(
            (field, type) -> NumericPipeline::defaultPipeline,
            field -> ColumnarFieldType.STRING,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            policy,
            SummaryPolicy.NONE
        );
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(format))
            .setMaxBufferedDocs(between(500, 8000))
            .setMergePolicy(randomBoolean() ? NoMergePolicy.INSTANCE : new LogDocMergePolicy());
        try (Directory dir = newDirectory()) {
            final FixedBitSet deleted = new FixedBitSet(docs.length);
            final boolean deletes = randomBoolean();
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int d = 0; d < docs.length; d++) {
                    final Document doc = new Document();
                    doc.add(new StringField(ID, Integer.toString(d), Field.Store.YES));
                    if (docs[d] != null) {
                        doc.add(new Field(FIELD, encode(docs[d]), columnarBinaryFieldType()));
                    }
                    writer.addDocument(doc);
                    if (deletes && d > 0 && random().nextInt(20) == 0) {
                        final int victim = between(0, d);
                        writer.deleteDocuments(new Term(ID, Integer.toString(victim)));
                        deleted.set(victim);
                    }
                }
                if (randomBoolean()) {
                    writer.forceMerge(between(1, 3));
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                // The index's own document ids, each mapped back to the values it was written with.
                final String[][] byDoc = new String[reader.maxDoc()][];
                final boolean[] live = new boolean[reader.maxDoc()];
                for (LeafReaderContext leaf : reader.leaves()) {
                    final StoredFields stored = leaf.reader().storedFields();
                    for (int d = 0; d < leaf.reader().maxDoc(); d++) {
                        if (leaf.reader().getLiveDocs() != null && leaf.reader().getLiveDocs().get(d) == false) {
                            continue;
                        }
                        final int id = Integer.parseInt(stored.document(d).get(ID));
                        assertFalse("a deleted document is still live", deleted.get(id));
                        byDoc[leaf.docBase + d] = docs[id];
                        live[leaf.docBase + d] = true;
                    }
                }
                check.check(newSearcher(reader, false, true), withLiveness(byDoc, live));
            }
        }
    }

    /** Values by index document id, a deleted document marked as matching nothing. */
    private static String[][] withLiveness(String[][] byDoc, boolean[] live) {
        final String[][] out = new String[byDoc.length][];
        for (int d = 0; d < byDoc.length; d++) {
            out[d] = live[d] ? (byDoc[d] == null ? ABSENT : byDoc[d]) : DELETED;
        }
        return out;
    }

    private static final String[] ABSENT = new String[0];
    private static final String[] DELETED = new String[] { "\u0000deleted" };

    private static List<Case> cases() {
        final List<Case> cases = new ArrayList<>();
        final String[] probes = { "", "a", "ab", "abc", "google", "absent", "zz-a-longer-value" };
        for (String probe : probes) {
            final BytesRef term = new BytesRef(probe);
            cases.add(
                new Case("term [" + probe + "]", ColumnarStringTermQuery.term(FIELD, term, s -> {}), v -> any(v, x -> x.equals(probe)))
            );
            cases.add(
                new Case(
                    "prefix [" + probe + "]",
                    ColumnarStringTermQuery.prefix(FIELD, term, s -> {}),
                    v -> any(v, x -> x.startsWith(probe))
                )
            );
            cases.add(
                new Case(
                    "contains [" + probe + "]",
                    ColumnarStringTermQuery.contains(FIELD, term, s -> {}),
                    v -> any(v, x -> x.contains(probe))
                )
            );
            cases.add(
                new Case(
                    "not [" + probe + "]",
                    new BooleanQuery.Builder().add(new MatchAllDocsQuery(), Occur.FILTER)
                        .add(ColumnarStringTermQuery.term(FIELD, term, s -> {}), Occur.MUST_NOT)
                        .build(),
                    v -> v != DELETED && any(v, x -> x.equals(probe)) == false
                )
            );
        }
        for (String pattern : new String[] { "a.*", ".*google.*", "ab[cd]", ".*" }) {
            final String regex = pattern;
            cases.add(
                new Case(
                    "automaton [" + pattern + "]",
                    new ColumnarStringAutomatonQuery(
                        FIELD,
                        Operations.determinize(new RegExp(pattern).toAutomaton(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT),
                        pattern,
                        s -> {}
                    ),
                    v -> any(v, x -> x.matches(regex))
                )
            );
        }
        cases.add(
            new Case(
                "prefix [a] and contains [b]",
                new BooleanQuery.Builder().add(ColumnarStringTermQuery.prefix(FIELD, new BytesRef("a"), s -> {}), Occur.FILTER)
                    .add(ColumnarStringTermQuery.contains(FIELD, new BytesRef("b"), s -> {}), Occur.FILTER)
                    .build(),
                v -> any(v, x -> x.startsWith("a")) && any(v, x -> x.contains("b"))
            )
        );
        cases.add(
            new Case(
                "contains [google] and not contains [.google.]",
                new BooleanQuery.Builder().add(ColumnarStringTermQuery.contains(FIELD, new BytesRef("google"), s -> {}), Occur.FILTER)
                    .add(ColumnarStringTermQuery.contains(FIELD, new BytesRef(".google."), s -> {}), Occur.MUST_NOT)
                    .build(),
                v -> any(v, x -> x.contains("google")) && any(v, x -> x.contains(".google.")) == false
            )
        );
        return cases;
    }

    /** Whether a live document holds a non-null value {@code test} accepts. */
    private static boolean any(String[] values, Predicate<String> test) {
        if (values == DELETED) {
            return false;
        }
        for (String v : values) {
            if (v != null && test.test(v)) {
                return true;
            }
        }
        return false;
    }

    private static FixedBitSet expected(String[][] docs, Predicate<String[]> test) {
        final FixedBitSet bits = new FixedBitSet(docs.length);
        for (int d = 0; d < docs.length; d++) {
            if (docs[d] != DELETED && test.test(docs[d])) {
                bits.set(d);
            }
        }
        return bits;
    }

    private static FixedBitSet collect(IndexSearcher searcher, Query query, int maxDoc) throws IOException {
        final FixedBitSet hits = new FixedBitSet(maxDoc);
        searcher.search(query, new org.apache.lucene.search.CollectorManager<SimpleCollector, Void>() {
            @Override
            public SimpleCollector newCollector() {
                return new SimpleCollector() {
                    private int docBase;

                    @Override
                    protected void doSetNextReader(LeafReaderContext context) {
                        docBase = context.docBase;
                    }

                    @Override
                    public void collect(int doc) {
                        synchronized (hits) {
                            hits.set(docBase + doc);
                        }
                    }

                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }
                };
            }

            @Override
            public Void reduce(java.util.Collection<SimpleCollector> collectors) {
                return null;
            }
        });
        return hits;
    }

    private static BytesRef encode(String[] slots) {
        final List<BytesRef> refs = new ArrayList<>(slots.length);
        for (String slot : slots) {
            refs.add(slot == null ? null : new BytesRef(slot));
        }
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(refs));
    }
}
