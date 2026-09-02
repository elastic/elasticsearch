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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;

/**
 * The term and prefix queries driven through a real {@link IndexSearcher} over a ColumNAR-coded index, so
 * the column answers them the way Lucene will ask.
 */
public class ColumnarStringTermQueryTests extends ESTestCase {

    private static final String FIELD = "kw";
    private static final DictionaryPolicy ROOMY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);
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
                final BinaryDocValues binary = reader.leaves().get(0).reader().getBinaryDocValues(FIELD);
                final StringColumnReader column = ((ColumnarStringBinaryDocValues) binary).reader();
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

    /**
     * Segments where the field is absent altogether merged with segments where it is not. A merge reads the
     * field from the readers that have it and has to leave the rest without a value rather than a wrong one,
     * so what survives is checked document by document and not only in the aggregate.
     */
    public void testMergeWithSegmentsMissingTheField() throws IOException {
        final List<String> values = new ArrayList<>();
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(ColumnarTestUtils.columnarCodecForField(FIELD))
                .setMergePolicy(new LogDocMergePolicy());
            final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int segment = 0; segment < 6; segment++) {
                    // Every other segment holds the field; the rest hold only the companion.
                    final boolean holdsTheField = segment % 2 == 0;
                    for (int d = 0; d < 300; d++) {
                        final Document doc = new Document();
                        doc.add(new NumericDocValuesField("other", d));
                        if (holdsTheField) {
                            final String value = TERMS[(segment + d) % TERMS.length];
                            doc.add(new Field(FIELD, new BytesRef(value), type));
                            values.add(value);
                        } else {
                            values.add(null);
                        }
                        writer.addDocument(doc);
                    }
                    writer.flush();
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("merged into one segment", 1, reader.leaves().size());
                final LeafReader leaf = reader.leaves().get(0).reader();
                final StringColumnReader column = ((ColumnarStringBinaryDocValues) leaf.getBinaryDocValues(FIELD)).reader();

                // Exactly the documents that were given a value have one, and it is the one they were given.
                final ColumnIterator presence = column.iterator();
                final List<Integer> withAValue = new ArrayList<>();
                for (int doc = presence.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = presence.nextDoc()) {
                    withAValue.add(doc);
                    assertEquals(
                        "value at doc " + doc,
                        values.get(doc),
                        column.valueAt(column.firstValueAddress(presence.rank())).utf8ToString()
                    );
                }
                final List<Integer> expectedDocs = new ArrayList<>();
                for (int d = 0; d < values.size(); d++) {
                    if (values.get(d) != null) {
                        expectedDocs.add(d);
                    }
                }
                assertEquals("documents with a value", expectedDocs, withAValue);

                final IndexSearcher searcher = new IndexSearcher(reader);
                for (String probe : TERMS) {
                    assertEquals(
                        "term [" + probe + "] after merging past segments without the field",
                        expected(values, probe, true),
                        found(searcher, ColumnarStringTermQuery.term(FIELD, new BytesRef(probe)))
                    );
                }
            }
        }
    }

    /**
     * The three ways a merge can know the terms of the column it is writing: taken from the segments'
     * dictionaries, summed from what they recorded surveying when their dictionaries do not cover it, and
     * surveyed afresh when neither is available. Which one runs is what the merge costs; what it produces
     * has to be the same column either way, so each is driven and read back value by value.
     */
    public void testMergedColumnIsTheSameWhicheverVocabularyIsUsed() throws IOException {
        record Shape(String name, DictionaryPolicy policy, IntFunction<String> value) {}
        final List<Shape> shapes = List.of(
            // Few terms and nothing escaping, so every segment has a dictionary and their union covers it.
            new Shape("dictionary union", ROOMY, d -> TERMS[d % TERMS.length]),
            // A long tail beside them, so a segment lets values escape and the union cannot stand for it.
            new Shape("combined summaries", ROOMY, d -> d % 9 == 4 ? "tail-" + d : TERMS[d % TERMS.length]),
            // No dictionary to take or sum, so the merged values are surveyed as a flush surveys them.
            new Shape("survey", DictionaryPolicy.NONE, d -> TERMS[d % TERMS.length])
        );
        for (Shape shape : shapes) {
            final List<String> values = values(between(900, 2000), shape.value()::apply);
            try (Directory dir = newDirectory()) {
                final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(
                    columnarCodec(
                        new ColumNARDocValuesFormat(
                            (fieldName, type) -> NumericPipeline::defaultPipeline,
                            ColumnarFieldType::fromField,
                            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
                            shape.policy()
                        )
                    )
                ).setMergePolicy(new LogDocMergePolicy());
                final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
                try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                    int written = 0;
                    for (String value : values) {
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
                    assertEquals(shape.name() + " merged into one segment", 1, reader.leaves().size());
                    final LeafReader leaf = reader.leaves().get(0).reader();
                    final StringColumnReader column = ((ColumnarStringBinaryDocValues) leaf.getBinaryDocValues(FIELD)).reader();
                    for (int d = 0; d < values.size(); d++) {
                        assertEquals(
                            shape.name() + " value at doc " + d,
                            values.get(d),
                            column.valueAt(column.firstValueAddress(d)).utf8ToString()
                        );
                    }
                    final IndexSearcher searcher = new IndexSearcher(reader);
                    for (String probe : TERMS) {
                        assertEquals(
                            shape.name() + " term [" + probe + "]",
                            expected(values, probe, true),
                            found(searcher, ColumnarStringTermQuery.term(FIELD, new BytesRef(probe)))
                        );
                    }
                }
            }
        }
    }

    public void testMergedSegmentsAreNotCalledSorted() throws IOException {
        assertQueries(values(between(600, 2000), d -> TERMS[d % TERMS.length]), true);
    }

    /**
     * Documents put in term order by a Lucene index sort rather than by the order they were added, so the
     * column is written from documents the merge reordered.
     *
     * <p>The sort is on a companion numeric field: an index sort on the keyword field itself would need
     * sorted doc values, and the column is written as binary ones. The companion carries each value's place
     * in term order, which puts the column in term order all the same.
     */
    public void testIndexSortedColumnBisects() throws IOException {
        final List<String> terms = Arrays.asList(TERMS);
        final List<String> values = values(between(600, 2000), d -> TERMS[(d * 7 + 3) % TERMS.length]);
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(ColumnarTestUtils.columnarCodecForField(FIELD))
                .setMergePolicy(new LogDocMergePolicy())
                .setIndexSort(new Sort(new SortField("order", SortField.Type.LONG)));
            final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                int written = 0;
                for (String value : values) {
                    if (++written % 200 == 0) {
                        writer.flush();
                    }
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, new BytesRef(value), type));
                    doc.add(new NumericDocValuesField("order", terms.indexOf(value)));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final StringColumnReader column = ((ColumnarStringBinaryDocValues) leaf.getBinaryDocValues(FIELD)).reader();
                assertTrue("expected a column in term order", column.valuesSorted());

                // The sort reordered the documents, so what each one holds is read back rather than assumed.
                final List<String> inDocOrder = new ArrayList<>();
                for (int d = 0; d < leaf.maxDoc(); d++) {
                    inDocOrder.add(column.valueAt(column.firstValueAddress(d)).utf8ToString());
                }
                final IndexSearcher searcher = new IndexSearcher(reader);
                for (String probe : TERMS) {
                    assertEquals(
                        "term [" + probe + "] on an index-sorted column",
                        expected(inDocOrder, probe, true),
                        found(searcher, ColumnarStringTermQuery.term(FIELD, new BytesRef(probe)))
                    );
                }
                for (String probe : Arrays.asList("al", "alp", "b", "d", "az", "zzz", "")) {
                    assertEquals(
                        "prefix [" + probe + "] on an index-sorted column",
                        expected(inDocOrder, probe, false),
                        found(searcher, ColumnarStringTermQuery.prefix(FIELD, new BytesRef(probe)))
                    );
                }
            }
        }
    }

    /**
     * An updated field, read as an overlay of its layers rather than as the column. The query has to answer
     * from the values it is given rather than report that nothing matches.
     */
    public void testMatchesThroughAnOverlaidColumn() throws IOException {
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
                final IndexSearcher searcher = new IndexSearcher(ColumnarTestUtils.hideTheColumn(reader));
                for (String probe : Arrays.asList("alpha", "alpine", "delta", "absent")) {
                    assertEquals(
                        "term [" + probe + "] through an overlay",
                        expected(values, probe, true),
                        found(searcher, ColumnarStringTermQuery.term(FIELD, new BytesRef(probe)))
                    );
                    assertEquals(
                        "prefix [" + probe + "] through an overlay",
                        expected(values, probe, false),
                        found(searcher, ColumnarStringTermQuery.prefix(FIELD, new BytesRef(probe)))
                    );
                }
                for (String probe : Arrays.asList("lph", "alpha", "a", "", "zzz")) {
                    assertEquals(
                        "contains [" + probe + "] through an overlay",
                        containing(values, probe),
                        found(searcher, ColumnarStringTermQuery.contains(FIELD, new BytesRef(probe)))
                    );
                }
            }
        }
    }

    /** A field this segment holds no value for matches nothing, which is not the same as having no column. */
    public void testFieldAbsentFromTheSegment() throws IOException {
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec()).setMergePolicy(new LogDocMergePolicy());
            final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int d = 0; d < 200; d++) {
                    final Document doc = new Document();
                    doc.add(new Field("other", new BytesRef("v" + d), type));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                assertEquals(List.of(), found(searcher, ColumnarStringTermQuery.term(FIELD, new BytesRef("alpha"))));
                assertEquals(List.of(), found(searcher, ColumnarStringAutomatonQuery.forWildcard(FIELD, "al*a")));
            }
        }
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
                final LeafReader leaf = reader.leaves().get(0).reader();
                final BinaryDocValues bdv = leaf.getBinaryDocValues(FIELD);
                final StringColumnReader column = ((ColumnarStringBinaryDocValues) bdv).reader();
                final List<Integer> direct = new ArrayList<>();
                final DocIdSetIterator it = column.matchTerm(new BytesRef("alpha"));
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

    /** The documents a search of every value would find. */
    private static List<Integer> containing(List<String> values, String probe) {
        final List<Integer> docs = new ArrayList<>();
        for (int d = 0; d < values.size(); d++) {
            if (values.get(d) != null && values.get(d).contains(probe)) {
                docs.add(d);
            }
        }
        return docs;
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
