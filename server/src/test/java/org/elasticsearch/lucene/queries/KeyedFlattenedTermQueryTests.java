/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.flattened.ColumnarKeyedBinaryDocValues;
import org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class KeyedFlattenedTermQueryTests extends ESTestCase {

    private static final String FIELD = "field._keyed";

    // ---------------------------------------------------------------------------------
    // Writer helpers
    // ---------------------------------------------------------------------------------

    /**
     * Writer that routes {@code FIELD} through {@link FlattenedDocValuesFormat}; all other fields
     * (including the {@code .counts} companion numeric DV) stay on the default codec.
     */
    private static RandomIndexWriter newColumnarWriter(Directory dir) throws IOException {
        FlattenedDocValuesFormat fmt = new FlattenedDocValuesFormat();
        IndexWriterConfig iwc = newIndexWriterConfig().setCodec(new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return FIELD.equals(field) ? fmt : super.getDocValuesFormatForField(field);
            }
        });
        return new RandomIndexWriter(random(), dir, iwc);
    }

    /** Writer that uses ES819 (row-format) binary DV for {@code FIELD}. */
    private static RandomIndexWriter newRowWriter(Directory dir) throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig().setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()));
        return new RandomIndexWriter(random(), dir, iwc);
    }

    /**
     * Adds a document with the given non-null keyed values under {@code key}. Each value is written
     * as a {@code key\0value} slot.
     */
    private static void addColumnarDoc(RandomIndexWriter writer, String key, String... values) throws IOException {
        LuceneDocument doc = new LuceneDocument();
        for (String v : values) {
            KeyedArrayOrderInlineNull.recordValue(doc, FIELD, new BytesRef(key + "\0" + v));
        }
        writer.addDocument(doc);
    }

    /** Adds a document whose only slot for {@code key} is a null slot. */
    private static void addColumnarDocNullOnly(RandomIndexWriter writer, String key) throws IOException {
        LuceneDocument doc = new LuceneDocument();
        KeyedArrayOrderInlineNull.recordNull(doc, FIELD, new BytesRef(key + "\0"));
        writer.addDocument(doc);
    }

    /** Adds a document with mixed non-null and null slots for {@code key}. */
    private static void addColumnarDocWithNulls(RandomIndexWriter writer, String key, String... valuesOrNulls) throws IOException {
        LuceneDocument doc = new LuceneDocument();
        for (String v : valuesOrNulls) {
            if (v == null) {
                KeyedArrayOrderInlineNull.recordNull(doc, FIELD, new BytesRef(key + "\0"));
            } else {
                KeyedArrayOrderInlineNull.recordValue(doc, FIELD, new BytesRef(key + "\0" + v));
            }
        }
        writer.addDocument(doc);
    }

    /** Adds an empty document (no field at all). */
    private static void addEmptyDoc(RandomIndexWriter writer) throws IOException {
        writer.addDocument(new LuceneDocument());
    }

    private static KeyedFlattenedTermQuery query(String key, String value) {
        return new KeyedFlattenedTermQuery(FIELD, key, new BytesRef(key + "\0" + value));
    }

    // ---------------------------------------------------------------------------------
    // Correctness tests
    // ---------------------------------------------------------------------------------

    /** Basic matching on a columnar segment. */
    public void testColumnarBasicMatching() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarWriter(dir)) {
            addColumnarDoc(writer, "k", "apple");
            addColumnarDoc(writer, "k", "banana");
            addColumnarDoc(writer, "k", "apple", "cherry");
            addEmptyDoc(writer);
            writer.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(writer.w)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                assertTrue(
                    "expected ColumnarKeyedBinaryDocValues from FlattenedDocValuesFormat",
                    leaf.reader().getBinaryDocValues(FIELD) instanceof ColumnarKeyedBinaryDocValues
                );
                IndexSearcher searcher = newSearcher(reader);
                assertEquals(2, searcher.count(query("k", "apple")));
                assertEquals(1, searcher.count(query("k", "banana")));
                assertEquals(1, searcher.count(query("k", "cherry")));
                assertEquals(0, searcher.count(query("k", "missing")));
            }
        }
    }

    /**
     * When the key is absent from the segment {@link ColumnarKeyedBinaryDocValues#lookupKeyOrdinal}
     * returns -1 and the query must skip the whole leaf (return 0 hits without a per-doc scan).
     */
    public void testKeyAbsentFromSegment() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarWriter(dir)) {
            addColumnarDoc(writer, "other", "x");
            addColumnarDoc(writer, "other", "y");
            writer.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(writer.w)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                assertTrue(leaf.reader().getBinaryDocValues(FIELD) instanceof ColumnarKeyedBinaryDocValues);
                IndexSearcher searcher = newSearcher(reader);
                assertEquals(0, searcher.count(query("missing_key", "x")));
            }
        }
    }

    /**
     * Key present in only one of two segments; the per-leaf skip must not skip the segment that has it.
     */
    public void testKeyPresentInOnlyOneSegment() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarWriter(dir)) {
            // Segment 1: key "a" only.
            addColumnarDoc(writer, "a", "v1");
            writer.commit();
            // Segment 2: key "b" only.
            addColumnarDoc(writer, "b", "v2");
            // Do NOT forceMerge — keep two segments.
            try (IndexReader reader = DirectoryReader.open(writer.w)) {
                assertEquals(2, reader.leaves().size());
                IndexSearcher searcher = newSearcher(reader);
                // Query for key "a" — must find 1 hit despite the second segment having no column for it.
                assertEquals(1, searcher.count(query("a", "v1")));
                // Query for key "b" — same reasoning.
                assertEquals(1, searcher.count(query("b", "v2")));
                // Neither key in segment that doesn't have it.
                assertEquals(0, searcher.count(query("a", "v2")));
            }
        }
    }

    /**
     * Null slots must never match a term. A doc with only a null slot returns false.
     * A doc with [non-null, null] matches the non-null slot but not a term for the null position.
     */
    public void testNullSlots() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarWriter(dir)) {
            addColumnarDocNullOnly(writer, "k");               // doc 0: [null]
            addColumnarDocWithNulls(writer, "k", "a", null, "b"); // doc 1: [a, null, b]
            writer.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(writer.w)) {
                IndexSearcher searcher = newSearcher(reader);
                // Neither doc has an empty-string value; doc 0 is null-only; neither matches "".
                assertEquals(0, searcher.count(query("k", "")));
                // Doc 1 has "a" and "b"; only doc 1 matches those terms. Doc 0's null slot must not match.
                assertEquals(1, searcher.count(query("k", "a")));
                assertEquals(1, searcher.count(query("k", "b")));
            }
        }
    }

    /** An empty-string value (prefix 1, zero-length payload) is a real value and must match. */
    public void testEmptyStringValue() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarWriter(dir)) {
            addColumnarDoc(writer, "k", "");   // doc 0: [""]
            addColumnarDocNullOnly(writer, "k"); // doc 1: [null] — must not match ""
            writer.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(writer.w)) {
                IndexSearcher searcher = newSearcher(reader);
                assertEquals(1, searcher.count(query("k", "")));
            }
        }
    }

    /**
     * When the same field is queried for two different keys in a {@link BooleanQuery}, each clause must
     * use an independent {@link ColumnarKeyedBinaryDocValues} reader — two clauses must never share a
     * cursor and corrupt each other's slot scan.
     */
    public void testTwoKeysInBooleanQuery() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarWriter(dir)) {
            addColumnarDoc(writer, "a", "va"); // matches a:va, not b
            addColumnarDoc(writer, "b", "vb"); // matches b:vb, not a
            addColumnarDoc(writer, "a", "va"); // additionaly doc matching a:va
            writer.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(writer.w)) {
                IndexSearcher searcher = newSearcher(reader);
                Query aQuery = query("a", "va");
                Query bQuery = query("b", "vb");

                // SHOULD — both docs match.
                Query shouldQuery = new BooleanQuery.Builder().add(aQuery, BooleanClause.Occur.SHOULD)
                    .add(bQuery, BooleanClause.Occur.SHOULD)
                    .build();
                assertEquals(3, searcher.count(shouldQuery));

                // MUST + MUST — only docs with both keys match (none in this case).
                Query mustQuery = new BooleanQuery.Builder().add(aQuery, BooleanClause.Occur.MUST)
                    .add(bQuery, BooleanClause.Occur.MUST)
                    .build();
                assertEquals(0, searcher.count(mustQuery));
            }
        }
    }

    /**
     * Sparse segment: documents that have no value for {@code FIELD} at all must not be matched.
     * This exercises {@link org.elasticsearch.index.codec.flattened.FlattenedDocValuesProducer}'s
     * IndexedDISI path.
     */
    public void testSparseSegment() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarWriter(dir)) {
            addEmptyDoc(writer);   // doc 0: no field
            addColumnarDoc(writer, "k", "v"); // doc 1
            addEmptyDoc(writer);   // doc 2: no field
            addColumnarDoc(writer, "k", "v"); // doc 3
            writer.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(writer.w)) {
                IndexSearcher searcher = newSearcher(reader);
                assertEquals(2, searcher.count(query("k", "v")));
            }
        }
    }

    /**
     * Mixed columnar/row segments: one segment written with {@link FlattenedDocValuesFormat}, one with
     * the default (row) codec. Both must contribute hits correctly without a merge.
     */
    public void testMixedColumnarAndRowSegments() throws IOException {
        try (Directory dir = newDirectory()) {
            // Segment 1: columnar.
            try (RandomIndexWriter writer = newColumnarWriter(dir)) {
                addColumnarDoc(writer, "k", "v1");
                writer.commit();
            }
            // Segment 2: row (ES819).
            try (RandomIndexWriter writer = newRowWriter(dir)) {
                LuceneDocument doc = new LuceneDocument();
                KeyedArrayOrderInlineNull.recordValue(doc, FIELD, new BytesRef("k\0v2"));
                writer.addDocument(doc);
                writer.commit();
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                assertEquals(2, reader.leaves().size());
                // Verify one leaf is columnar and one is not, so the test actually exercises both paths.
                boolean hasColumnar = reader.leaves().stream().anyMatch(leaf -> {
                    try {
                        return leaf.reader().getBinaryDocValues(FIELD) instanceof ColumnarKeyedBinaryDocValues;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                assertTrue("expected at least one columnar leaf", hasColumnar);

                IndexSearcher searcher = newSearcher(reader);
                // Query spanning both segments.
                assertEquals(1, searcher.count(query("k", "v1")));
                assertEquals(1, searcher.count(query("k", "v2")));
                assertEquals(0, searcher.count(query("k", "missing")));
            }
        }
    }

    /**
     * Columnar/row duel: the same documents written with both codecs must produce identical hit sets
     * for a randomised term. This is the primary regression guard — it catches the
     * {@link ColumnarKeyedBinaryDocValues#lookupKeyOrdinal} key-without-separator contract violation,
     * which would make the columnar path silently match nothing.
     *
     * <p>Document data is a list of per-doc records; each record is a list of per-key slot lists
     * (one entry per key). An empty slot list means the key is absent from that document.
     */
    public void testColumnarRowDuel() throws IOException {
        String[] keys = { "a", "b", "c" };
        String[] values = { "v1", "v2", "v3", "", "x" };

        int numDocs = randomIntBetween(5, 40);
        // docData: outer = docs, middle = per-key slot lists, inner = slot values (null = null slot)
        List<List<List<String>>> docData = new ArrayList<>(numDocs);
        for (int d = 0; d < numDocs; d++) {
            List<List<String>> keySlots = new ArrayList<>(keys.length);
            for (int k = 0; k < keys.length; k++) {
                if (rarely()) {
                    keySlots.add(Collections.emptyList()); // key absent from this doc
                } else {
                    int arity = randomIntBetween(1, 3);
                    List<String> slots = new ArrayList<>();
                    for (int s = 0; s < arity; s++) {
                        slots.add(randomBoolean() ? null : randomFrom(values));
                    }
                    keySlots.add(slots);
                }
            }
            docData.add(keySlots);
        }

        for (String queryKey : keys) {
            for (String queryValue : values) {
                int columnarHits = countHits(queryKey, queryValue, keys, docData, true);
                int rowHits = countHits(queryKey, queryValue, keys, docData, false);
                assertEquals(
                    "columnar and row hit counts disagree for key=[" + queryKey + "] value=[" + queryValue + "]",
                    rowHits,
                    columnarHits
                );
            }
        }
    }

    private int countHits(String queryKey, String queryValue, String[] keys, List<List<List<String>>> docData, boolean columnar)
        throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = columnar ? newColumnarWriter(dir) : newRowWriter(dir)) {
                for (List<List<String>> keySlots : docData) {
                    LuceneDocument doc = new LuceneDocument();
                    boolean hasAnySlot = false;
                    for (int k = 0; k < keys.length; k++) {
                        List<String> slots = keySlots.get(k);
                        if (slots.isEmpty()) {
                            continue;
                        }
                        for (String v : slots) {
                            if (v == null) {
                                KeyedArrayOrderInlineNull.recordNull(doc, FIELD, new BytesRef(keys[k] + "\0"));
                            } else {
                                KeyedArrayOrderInlineNull.recordValue(doc, FIELD, new BytesRef(keys[k] + "\0" + v));
                            }
                            hasAnySlot = true;
                        }
                    }
                    if (hasAnySlot == false) {
                        writer.addDocument(new LuceneDocument());
                    } else {
                        writer.addDocument(doc);
                    }
                }
                writer.forceMerge(1);
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                return searcher.count(query(queryKey, queryValue));
            }
        }
    }

    /**
     * Slot-drain regression: after a match is found, all remaining slots for the current doc must still
     * be consumed. Leaving slots unread desynchronises the column cursor's payload pointer and silently
     * corrupts reads for all later docs in the same decompressed block.
     *
     * <p>Strategy: doc 3 has ["target", TRAILER] where TRAILER is a multi-byte value that encodes to
     * the same byte count as "target" is long. Doc 4 has ["target"]. If the query breaks early after
     * matching "target" in doc 3 slot 0, {@code payloadCursor} is left pointing at TRAILER's encoding.
     * When doc 4's slot is then read, the cursor reads from TRAILER's bytes and does not see "target",
     * so doc 4 is incorrectly missed and the count is 4 instead of 5.
     */
    public void testSlotDrainRegression() throws IOException {
        // "trailer" is a value that will NEVER be searched for, chosen so its byte encoding at the
        // start position is guaranteed to differ from "target"'s encoding. Length must differ from
        // "target" (6 bytes) so the prefix vint is distinguishably different.
        final String TRAILER = "aaaaaaaaaa"; // 10 bytes — prefix vint differs from "target" (prefix 7)
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarWriter(dir)) {
            // All five docs in one segment (and one block, since 5 << MAX_DOCS_PER_BLOCK_DEFAULT).
            addColumnarDoc(writer, "k", "x", "target");      // doc 0
            addColumnarDoc(writer, "k", "target");            // doc 1
            addColumnarDoc(writer, "k", "y", "target");      // doc 2
            addColumnarDoc(writer, "k", "target", TRAILER);  // doc 3 — canary with long trailer
            addColumnarDoc(writer, "k", "target");            // doc 4 — must still be found after doc 3
            writer.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(writer.w)) {
                IndexSearcher searcher = newSearcher(reader);
                // All 5 docs have "target". If the slot loop breaks early on doc 3, doc 4's cursor reads
                // from TRAILER's position and produces the wrong slot value, so doc 4 is not counted.
                assertEquals("all 5 docs must match target", 5, searcher.count(query("k", "target")));
                // Sanity: TRAILER exists in exactly one doc (doc 3).
                assertEquals(1, searcher.count(query("k", TRAILER)));
            }
        }
    }

    // ---------------------------------------------------------------------------------
    // equals / hashCode / toString
    // ---------------------------------------------------------------------------------

    public void testEqualsAndHashCode() {
        KeyedFlattenedTermQuery q1 = query("key", "val");
        KeyedFlattenedTermQuery q2 = query("key", "val");
        KeyedFlattenedTermQuery qDiffKey = query("other", "val");
        KeyedFlattenedTermQuery qDiffVal = query("key", "other");
        KeyedFlattenedTermQuery qDiffField = new KeyedFlattenedTermQuery("other._keyed", "key", new BytesRef("key\0val"));

        assertEquals(q1, q2);
        assertEquals(q1.hashCode(), q2.hashCode());
        assertNotEquals(q1, qDiffKey);
        assertNotEquals(q1, qDiffVal);
        assertNotEquals(q1, qDiffField);
        // Must not equal a ScanningBinaryDocValuesTermQuery with the same combined term.
        assertNotEquals(q1, new ScanningBinaryDocValuesTermQuery(FIELD, new BytesRef("key\0val"), false));
    }

    public void testToString() {
        KeyedFlattenedTermQuery q = query("mykey", "myval");
        String s = q.toString(FIELD);
        assertTrue("toString must mention fieldName", s.contains(FIELD));
    }

    // ---------------------------------------------------------------------------------
    // Planning / circuit breaker
    // ---------------------------------------------------------------------------------

    /**
     * {@link BinaryDocValues} must not be opened during {@code scorerSupplier()} (the planning phase).
     * Deferring it to {@code iterator()} is what makes per-leaf isolation and thread safety work:
     * {@code FlattenedDocValuesProducer.getBinary} returns a fresh clone each time and two query clauses
     * on different keys must each get their own instance.
     */
    public void testNoBinaryDocValuesOpenedDuringPlanning() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newColumnarWriter(dir)) {
                addColumnarDoc(writer, "k", "v");
                try (DirectoryReader reader = forbidBinaryDvOpenReader(DirectoryReader.open(writer.w))) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    Weight weight = query("k", "v").createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
                    for (LeafReaderContext ctx : reader.leaves()) {
                        weight.scorerSupplier(ctx);
                    }
                }
            }
        }
    }

    /** The circuit-breaker checkpoint must fire with 0 bytes when the reader is opened, but not when the field is absent. */
    public void testChecksCircuitBreakerWhenReaderOpened() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newColumnarWriter(dir)) {
                addColumnarDoc(writer, "k", "v");
                try (IndexReader reader = DirectoryReader.open(writer.w)) {
                    AtomicLong checkpointedBytes = new AtomicLong(-1);
                    CircuitBreaker breaker = new NoopCircuitBreaker("test") {
                        @Override
                        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                            checkpointedBytes.set(bytes);
                            throw new CircuitBreakingException("test trip", Durability.TRANSIENT);
                        }
                    };
                    ContextIndexSearcher searcher = new ContextIndexSearcher(
                        reader,
                        IndexSearcher.getDefaultSimilarity(),
                        IndexSearcher.getDefaultQueryCache(),
                        IndexSearcher.getDefaultQueryCachingPolicy(),
                        true
                    );
                    searcher.setCircuitBreaker(breaker);

                    expectThrows(CircuitBreakingException.class, () -> searcher.count(query("k", "v")));
                    assertEquals(0L, checkpointedBytes.get());

                    // A key absent from the segment returns null from getDocIdSetIterator without ever opening the reader —
                    // the circuit-breaker checkpoint should still fire because the field itself is present.
                    checkpointedBytes.set(-1);
                    expectThrows(CircuitBreakingException.class, () -> searcher.count(query("missing_key", "v")));
                    assertEquals(0L, checkpointedBytes.get());
                }
            }
        }
    }

    // ---------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------

    private static DirectoryReader forbidBinaryDvOpenReader(DirectoryReader reader) throws IOException {
        return new FilterDirectoryReader(reader, new FilterDirectoryReader.SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader leaf) {
                return new FilterLeafReader(leaf) {
                    @Override
                    public BinaryDocValues getBinaryDocValues(String field) {
                        throw new AssertionError(
                            "getBinaryDocValues() must not be called during scorerSupplier() (planning phase);"
                                + " defer reader construction to ScorerSupplier#get(). field=["
                                + field
                                + "]"
                        );
                    }

                    @Override
                    public IndexReader.CacheHelper getCoreCacheHelper() {
                        return null;
                    }

                    @Override
                    public IndexReader.CacheHelper getReaderCacheHelper() {
                        return null;
                    }
                };
            }
        }) {
            @Override
            protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
                return in;
            }

            @Override
            public IndexReader.CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }
}
