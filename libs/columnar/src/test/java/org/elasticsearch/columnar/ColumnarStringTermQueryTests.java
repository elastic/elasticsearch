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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;

/**
 * The term and prefix queries driven through a real {@link IndexSearcher} over a ColumNAR-coded index, so
 * the column answers them the way Lucene will ask.
 */
public class ColumnarStringTermQueryTests extends ESTestCase {

    private static final String FIELD = "kw";
    private static final String[] TERMS = { "alpha", "alpine", "bravo", "charlie", "delta" };

    /** Few distinct values, so the column carries a dictionary and terms match over ordinals. */
    public void testLowCardinality() throws IOException {
        assertQueries(values(between(300, 1500), d -> TERMS[d % TERMS.length]));
    }

    /** Every value distinct, so the column is plain and terms are compared against the values. */
    public void testHighCardinality() throws IOException {
        assertQueries(values(between(300, 1500), d -> "id-" + d));
    }

    /** Values in term order, which is the shape a bisection over a sorted column exploits. */
    public void testSorted() throws IOException {
        final List<String> sorted = new ArrayList<>(values(between(300, 1500), d -> TERMS[d % TERMS.length]));
        sorted.sort(String::compareTo);
        assertQueries(sorted);
    }

    /** Hot values over a long tail, so a term is answered by the dictionary or by the exceptions. */
    public void testHotValuesWithTail() throws IOException {
        assertQueries(values(between(600, 2000), d -> d % 40 == 7 ? "rare-" + d : TERMS[d % TERMS.length]));
    }

    /** Documents without a value, so matches have to be named in document ids and not in ranks. */
    public void testSparse() throws IOException {
        assertQueries(values(between(300, 1500), d -> d % 3 == 0 ? null : TERMS[d % TERMS.length]));
    }

    /**
     * Several segments merged into one, which carries ordinals over rather than resolving values. The order
     * has to be read from the carried ordinal too: a merged column wrongly called sorted would be bisected,
     * and a bisection over unsorted values finds almost nothing.
     */
    /**
     * The other way round: segments whose values are in order, merged in that order, are still in order, and
     * the merged column has to say so. A merge that gave up on the claim would leave every column after it
     * comparing a value a document rather than bisecting, which nothing else here would notice.
     */
    public void testMergedSortedSegmentsAreStillSorted() throws IOException {
        final List<String> ordered = new ArrayList<>(values(between(600, 2000), d -> TERMS[d % TERMS.length]));
        java.util.Collections.sort(ordered);
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec()).setMergePolicy(new LogDocMergePolicy());
            final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                int written = 0;
                for (String value : ordered) {
                    // Flushed in order, so each segment is sorted and so is the one they merge into.
                    if (++written % 200 == 0) {
                        writer.flush();
                    }
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, new BytesRef(value), type));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("merged into one segment", 1, reader.leaves().size());
                final org.apache.lucene.index.BinaryDocValues binary = reader.leaves().get(0).reader().getBinaryDocValues(FIELD);
                final org.elasticsearch.columnar.string.StringColumnReader column =
                    ((org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues) binary).reader();
                assertTrue("a merge of sorted segments is still sorted", column.valuesSorted());

                final IndexSearcher searcher = new IndexSearcher(reader);
                for (String probe : Arrays.asList("alpha", "alpine", "delta", "absent")) {
                    assertEquals(
                        "term [" + probe + "]",
                        expected(ordered, probe, true),
                        found(searcher, ColumnarStringTermQuery.term(FIELD, new BytesRef(probe)))
                    );
                }
            }
        }
    }

    public void testMergedSegmentsAreNotCalledSorted() throws IOException {
        assertQueries(values(between(600, 2000), d -> TERMS[d % TERMS.length]), true);
    }

    /**
     * A field whose values are reached through something other than the column itself, which is what a
     * doc values update produces: the values arrive layered behind a wrapper and the column is no longer
     * the instance the reader hands back. The query has to answer from the values it is given rather than
     * report that nothing matches.
     */
    public void testMatchesThroughAWrappedColumn() throws IOException {
        final List<String> values = values(between(600, 2000), d -> TERMS[d % TERMS.length]);
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec()).setMergePolicy(new LogDocMergePolicy());
            final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (String value : values) {
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, new BytesRef(value), type));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(hideTheColumn(reader));
                for (String probe : Arrays.asList("alpha", "alpine", "delta", "absent")) {
                    assertEquals(
                        "term [" + probe + "] through a wrapper",
                        expected(values, probe, true),
                        found(searcher, ColumnarStringTermQuery.term(FIELD, new BytesRef(probe)))
                    );
                    assertEquals(
                        "prefix [" + probe + "] through a wrapper",
                        expected(values, probe, false),
                        found(searcher, ColumnarStringTermQuery.prefix(FIELD, new BytesRef(probe)))
                    );
                }
            }
        }
    }

    /**
     * Hides the columnar instance behind a plain {@link BinaryDocValues}, leaving only the surface every
     * binary doc values has.
     */
    private static DirectoryReader hideTheColumn(DirectoryReader in) throws IOException {
        return new FilterDirectoryReader(in, new FilterDirectoryReader.SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader leaf) {
                return new FilterLeafReader(leaf) {
                    @Override
                    public BinaryDocValues getBinaryDocValues(String name) throws IOException {
                        final BinaryDocValues values = in.leaves().get(0).reader().getBinaryDocValues(name);
                        return values == null ? null : new BinaryDocValues() {
                            @Override
                            public BytesRef binaryValue() throws IOException {
                                return values.binaryValue();
                            }

                            @Override
                            public boolean advanceExact(int target) throws IOException {
                                return values.advanceExact(target);
                            }

                            @Override
                            public int docID() {
                                return values.docID();
                            }

                            @Override
                            public int nextDoc() throws IOException {
                                return values.nextDoc();
                            }

                            @Override
                            public int advance(int target) throws IOException {
                                return values.advance(target);
                            }

                            @Override
                            public long cost() {
                                return values.cost();
                            }
                        };
                    }

                    @Override
                    public CacheHelper getCoreCacheHelper() {
                        return leaf.getCoreCacheHelper();
                    }

                    @Override
                    public CacheHelper getReaderCacheHelper() {
                        return leaf.getReaderCacheHelper();
                    }
                };
            }
        }) {
            @Override
            protected DirectoryReader doWrapDirectoryReader(DirectoryReader reader) {
                return reader;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return in.getReaderCacheHelper();
            }
        };
    }

    private interface Value {
        String at(int doc);
    }

    private static List<String> values(int count, Value value) {
        final List<String> values = new ArrayList<>(count);
        for (int d = 0; d < count; d++) {
            values.add(value.at(d));
        }
        return values;
    }

    private void assertQueries(List<String> values) throws IOException {
        assertQueries(values, false);
    }

    private void assertQueries(List<String> values, boolean severalSegments) throws IOException {
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec()).setMergePolicy(new LogDocMergePolicy());
            final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                int written = 0;
                for (String value : values) {
                    if (severalSegments && ++written % 200 == 0) {
                        writer.flush();
                    }
                    final Document doc = new Document();
                    if (value != null) {
                        doc.add(new Field(FIELD, new BytesRef(value), type));
                    }
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                // Isolate the reader from the query: ask the column directly, the way the query will.
                final org.apache.lucene.index.LeafReader leaf = reader.leaves().get(0).reader();
                final org.apache.lucene.index.BinaryDocValues bdv = leaf.getBinaryDocValues(FIELD);
                final org.elasticsearch.columnar.string.StringColumnReader column =
                    ((org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues) bdv).reader();
                final List<Integer> direct = new ArrayList<>();
                final org.apache.lucene.search.DocIdSetIterator it = column.matchTerm(new BytesRef("alpha"));
                for (int d = it.nextDoc(); d != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; d = it.nextDoc()) {
                    direct.add(d);
                }
                assertEquals("reader-direct term [alpha]", expected(values, "alpha", true), direct);

                final IndexSearcher searcher = new IndexSearcher(reader);
                for (String probe : Arrays.asList("alpha", "alpine", "delta", "id-0", "absent")) {
                    assertEquals(
                        "term [" + probe + "]",
                        expected(values, probe, true),
                        found(searcher, ColumnarStringTermQuery.term(FIELD, new BytesRef(probe)))
                    );
                }
                for (String probe : Arrays.asList("al", "alp", "b", "id-", "zzz")) {
                    assertEquals(
                        "prefix [" + probe + "]",
                        expected(values, probe, false),
                        found(searcher, ColumnarStringTermQuery.prefix(FIELD, new BytesRef(probe)))
                    );
                }
            }
        }
    }

    private static List<Integer> expected(List<String> values, String probe, boolean exact) {
        final List<Integer> docs = new ArrayList<>();
        for (int d = 0; d < values.size(); d++) {
            final String value = values.get(d);
            if (value != null && (exact ? value.equals(probe) : value.startsWith(probe))) {
                docs.add(d);
            }
        }
        return docs;
    }

    private static List<Integer> found(IndexSearcher searcher, Query query) throws IOException {
        final TopDocs hits = searcher.search(query, Integer.MAX_VALUE);
        final List<Integer> docs = new ArrayList<>();
        for (ScoreDoc hit : hits.scoreDocs) {
            docs.add(hit.doc);
        }
        docs.sort(Integer::compareTo);
        return docs;
    }
}
