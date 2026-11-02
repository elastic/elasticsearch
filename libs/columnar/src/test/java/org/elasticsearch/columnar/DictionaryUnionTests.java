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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesConsumer.MergedVocabulary;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.DictionaryStringColumnReader;
import org.elasticsearch.columnar.string.StringColumnMetadata;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnOptionsSelector;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.Vocabulary;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.elasticsearch.columnar.ColumnarTestUtils.stringPayload;

/**
 * A merge of dictionary columns that let nothing escape takes the union of their dictionaries as its vocabulary, and
 * what each segment's ordinals become in it from building that union. The union is every term in term order, and each
 * segment's ordinal lands on the term it named, whether the dictionaries are all the same, overlap, nest or share
 * nothing.
 */
public class DictionaryUnionTests extends ESTestCase {

    private static final String FIELD = "keyword";
    /** Every term a segment holds earns its place in the dictionary. */
    private static final DictionaryPolicy ROOMY = new DictionaryPolicy(512 * 1024, 0.0, Double.MAX_VALUE);
    private static final StringColumnOptions OPTIONS = StringColumnOptions.DEFAULT.withPolicies(ROOMY, StringColumnOptions.DEFAULT_SUMMARY);

    /** How each segment's terms are drawn from one pool. */
    private enum Shape {
        /** Every segment holds the whole pool. */
        IDENTICAL,
        /** Each segment holds a random part of the pool. */
        OVERLAPPING,
        /** Each segment holds a part of the pool no other holds. */
        DISJOINT,
        /** Each segment holds a longer prefix of the pool, in term order, than the one before. */
        NESTED
    }

    public void testUnionOfIdenticalDictionaries() throws IOException {
        assertUnion(Shape.IDENTICAL);
    }

    public void testUnionOfOverlappingDictionaries() throws IOException {
        assertUnion(Shape.OVERLAPPING);
    }

    public void testUnionOfDisjointDictionaries() throws IOException {
        assertUnion(Shape.DISJOINT);
    }

    public void testUnionOfNestedDictionaries() throws IOException {
        assertUnion(Shape.NESTED);
    }

    /** A union whose terms take more bytes than the policy allows is not taken, however its inputs are shaped. */
    public void testUnionOverThePolicyIsNotTaken() throws IOException {
        final Shape shape = randomFrom(Shape.values());
        try (Directory dir = newDirectory()) {
            final TreeSet<String> expected = writeSegments(dir, shape);
            long unionBytes = 0;
            for (String term : expected) {
                unionBytes += new BytesRef(term).length;
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final List<StringColumnReader> columns = columns(reader);
                final DictionaryPolicy fits = new DictionaryPolicy((int) unionBytes, 0.0, Double.MAX_VALUE);
                assertEquals(MergedVocabulary.Source.DICTIONARY_UNION, union(columns, fits).source());
                final DictionaryPolicy tight = new DictionaryPolicy((int) unionBytes - 1, 0.0, Double.MAX_VALUE);
                assertNotEquals(MergedVocabulary.Source.DICTIONARY_UNION, union(columns, tight).source());
            }
        }
    }

    private void assertUnion(Shape shape) throws IOException {
        try (Directory dir = newDirectory()) {
            final List<String> expected = new ArrayList<>(writeSegments(dir, shape));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final List<StringColumnReader> columns = columns(reader);
                final MergedVocabulary merged = union(columns, ROOMY);
                assertEquals(MergedVocabulary.Source.DICTIONARY_UNION, merged.source());

                final Vocabulary.Terms terms = merged.terms();
                final List<String> union = new ArrayList<>(terms.dictionarySize());
                final BytesRef scratch = new BytesRef();
                for (int ordinal = 0; ordinal < terms.dictionarySize(); ordinal++) {
                    terms.terms().get(terms.dictionaryIds()[ordinal], scratch);
                    union.add(scratch.utf8ToString());
                }
                assertEquals("every term, in term order", expected, union);

                assertEquals("a map for every input", columns.size(), merged.ordinalMaps().length);
                for (int i = 0; i < columns.size(); i++) {
                    final DictionaryStringColumnReader dictionary = (DictionaryStringColumnReader) columns.get(i);
                    final int[] map = merged.ordinalMaps()[i];
                    assertEquals(dictionary.dictionarySize() + StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL, map.length);
                    assertEquals(StringColumnMetadata.Dictionary.NULL_ORDINAL, map[StringColumnMetadata.Dictionary.NULL_ORDINAL]);
                    for (int t = 0; t < dictionary.dictionarySize(); t++) {
                        final int ordinal = StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL + t;
                        final String term = dictionary.termAt(ordinal, new BytesRef()).utf8ToString();
                        assertEquals(
                            "input " + i + " ordinal " + ordinal,
                            term,
                            union.get(map[ordinal] - StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL)
                        );
                    }
                }
            }
        }
    }

    private static MergedVocabulary union(List<StringColumnReader> columns, DictionaryPolicy policy) throws IOException {
        return ColumNARDocValuesConsumer.vocabularyFrom(columns, false, policy, StringColumnOptions.DEFAULT_SUMMARY);
    }

    /** Every segment's column, each a dictionary that let nothing escape, so the merge can take their union. */
    private static List<StringColumnReader> columns(DirectoryReader reader) throws IOException {
        final List<StringColumnReader> columns = new ArrayList<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            final BinaryDocValues values = leaf.reader().getBinaryDocValues(FIELD);
            assertTrue("expected a columnar column, got " + values, values instanceof ColumnarStringBinaryDocValues);
            final StringColumnReader column = ((ColumnarStringBinaryDocValues) values).reader();
            assertTrue("expected a dictionary column", column instanceof DictionaryStringColumnReader);
            assertEquals("expected nothing to escape", 0, column.escapeCount());
            columns.add(column);
        }
        return columns;
    }

    /**
     * Flushes a segment per part of the pool {@code shape} deals out, every term in it a few times over so it earns a
     * place in the dictionary, and answers every term written.
     */
    private static TreeSet<String> writeSegments(Directory dir, Shape shape) throws IOException {
        final List<String> pool = new ArrayList<>(new TreeSet<>(randomList(2, 200, () -> randomAlphaOfLengthBetween(1, 12))));
        final int numSegments = between(2, 8);
        final List<List<String>> parts = new ArrayList<>(numSegments);
        for (int s = 0; s < numSegments; s++) {
            parts.add(new ArrayList<>());
        }
        for (int t = 0; t < pool.size(); t++) {
            final String term = pool.get(t);
            switch (shape) {
                case IDENTICAL -> parts.forEach(part -> part.add(term));
                case OVERLAPPING -> {
                    parts.get(t % numSegments).add(term);
                    for (List<String> part : parts) {
                        if (randomBoolean()) {
                            part.add(term);
                        }
                    }
                }
                case DISJOINT -> parts.get(t % numSegments).add(term);
                case NESTED -> {
                    for (int s = t * numSegments / pool.size(); s < numSegments; s++) {
                        parts.get(s).add(term);
                    }
                }
            }
        }
        final TreeSet<String> written = new TreeSet<>();
        final ColumNARDocValuesFormat format = new ColumNARDocValuesFormat(
            (field, type) -> NumericPipeline::defaultPipeline,
            field -> ColumnarFieldType.STRING,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            StringColumnOptionsSelector.always(OPTIONS)
        );
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(format)).setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (List<String> part : parts) {
                if (part.isEmpty()) {
                    continue;
                }
                final List<String> values = new ArrayList<>();
                for (String term : part) {
                    for (int i = between(2, 4); i > 0; i--) {
                        values.add(term);
                    }
                }
                Collections.shuffle(values, random());
                for (String value : values) {
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, stringPayload(value), columnarBinaryFieldType()));
                    writer.addDocument(doc);
                }
                writer.commit();
                written.addAll(part);
            }
        }
        return written;
    }
}
