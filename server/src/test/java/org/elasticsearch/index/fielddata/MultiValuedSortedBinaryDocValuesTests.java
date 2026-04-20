/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class MultiValuedSortedBinaryDocValuesTests extends ESTestCase {

    private static List<BytesRef> randomSortedUniqueBytesRefs(int count) {
        TreeSet<BytesRef> sorted = new TreeSet<>();
        while (sorted.size() < count) {
            sorted.add(new BytesRef(randomAlphanumericOfLength(10)));
        }
        return new ArrayList<>(sorted);
    }

    /**
     * Verifies that {@link MultiValuedSortedBinaryDocValues} correctly reads multi-valued binary doc values written by
     * {@link MultiValuedBinaryDocValuesField.SeparateCount}.
     */
    public void testReadValuesFromSeparateCount() throws IOException {
        // given
        List<BytesRef> expected = randomSortedUniqueBytesRefs(3);

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                LuceneDocument doc = new LuceneDocument();
                for (BytesRef value : expected) {
                    MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "field", value);
                }
                iw.addDocument(doc);
            }

            // when
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                MultiValuedSortedBinaryDocValues values = MultiValuedSortedBinaryDocValues.fromMultiValued(leafReader, "field");

                // then
                assertTrue(values.advanceExact(0));
                assertEquals(expected.size(), values.docValueCount());
                List<BytesRef> actual = new ArrayList<>();
                for (int i = 0; i < values.docValueCount(); i++) {
                    actual.add(BytesRef.deepCopyOf(values.nextValue()));
                }
                assertEquals(expected, actual);
            }
        }
    }

    /**
     * Verifies that {@link MultiValuedSortedBinaryDocValues} correctly reads multi-valued binary doc values written by
     * {@link MultiValuedBinaryDocValuesField.IntegratedCount}.
     */
    public void testReadValuesFromSeparateCountWithPreviousIndexVersion() throws IOException {
        // given
        List<BytesRef> expected = randomSortedUniqueBytesRefs(3);

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                LuceneDocument doc = new LuceneDocument();
                IndexVersion previousVersion = IndexVersionUtils.getPreviousVersion(
                    IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES
                );
                for (BytesRef value : expected) {
                    MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(
                        doc,
                        "field",
                        value,
                        MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE,
                        previousVersion
                    );
                }
                iw.addDocument(doc);
            }

            // when
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                MultiValuedSortedBinaryDocValues values = MultiValuedSortedBinaryDocValues.fromMultiValued(leafReader, "field");

                // then
                assertTrue(values.advanceExact(0));
                assertEquals(expected.size(), values.docValueCount());
                List<BytesRef> actual = new ArrayList<>();
                for (int i = 0; i < values.docValueCount(); i++) {
                    actual.add(BytesRef.deepCopyOf(values.nextValue()));
                }
                assertEquals(expected, actual);
            }
        }
    }

    /**
     * Verifies that {@link MultiValuedSortedBinaryDocValues} correctly reads a single value written by
     * {@link MultiValuedBinaryDocValuesField.SeparateCount}.
     */
    public void testReadSingleValueFromSeparateCount() throws IOException {
        // given
        BytesRef expected = new BytesRef(randomAlphanumericOfLength(10));

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                LuceneDocument doc = new LuceneDocument();
                MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "field", expected);
                iw.addDocument(doc);
            }

            // when
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                MultiValuedSortedBinaryDocValues values = MultiValuedSortedBinaryDocValues.fromMultiValued(leafReader, "field");

                // then
                assertTrue(values.advanceExact(0));
                assertEquals(1, values.docValueCount());
                assertEquals(expected, values.nextValue());
            }
        }
    }

    /**
     * Verifies that {@link MultiValuedSortedBinaryDocValues} correctly reads a single value written by
     * {@link MultiValuedBinaryDocValuesField.IntegratedCount}.
     */
    public void testReadSingleValueFromSeparateCountWithPreviousIndexVersion() throws IOException {
        // given
        BytesRef expected = new BytesRef(randomAlphanumericOfLength(10));

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                LuceneDocument doc = new LuceneDocument();
                IndexVersion previousVersion = IndexVersionUtils.getPreviousVersion(
                    IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES
                );
                MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(
                    doc,
                    "field",
                    expected,
                    MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE,
                    previousVersion
                );
                iw.addDocument(doc);
            }

            // when
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                MultiValuedSortedBinaryDocValues values = MultiValuedSortedBinaryDocValues.fromMultiValued(leafReader, "field");

                // then
                assertTrue(values.advanceExact(0));
                assertEquals(1, values.docValueCount());
                assertEquals(expected, values.nextValue());
            }
        }
    }

    /**
     * Verifies that the PlainBinary reader correctly reads single-valued binary doc values
     * written as a plain {@link BinaryDocValuesField} via auto-detection.
     */
    public void testReadSingleValuedBinaryDocValues() throws IOException {
        // given
        BytesRef expected = new BytesRef(randomAlphanumericOfLength(10));

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                LuceneDocument doc = new LuceneDocument();
                doc.addWithKey("field", new BinaryDocValuesField("field", expected));
                iw.addDocument(doc);
            }

            // when
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                SortedBinaryDocValues values = MultiValuedSortedBinaryDocValues.from(leafReader, "field");

                // then
                assertNotNull(values);
                assertTrue(values.advanceExact(0));
                assertEquals(1, values.docValueCount());
                assertEquals(expected, values.nextValue());
                assertEquals(SortedBinaryDocValues.ValueMode.SINGLE_VALUED, values.getValueMode());
            }
        }
    }

    /**
     * Verifies that fromMultiValued() falls back to IntegratedCounts when .counts is absent.
     */
    public void testFromMultiValuedFallsBackToIntegratedCounts() throws IOException {
        IndexVersion oldVersion = IndexVersionUtils.getPreviousVersion(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES);

        // Write an IntegratedCount value using the old index version
        BytesRef expected = new BytesRef(randomAlphanumericOfLength(10));
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                LuceneDocument doc = new LuceneDocument();
                MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(
                    doc,
                    "field",
                    expected,
                    MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE,
                    oldVersion
                );
                iw.addDocument(doc);
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                SortedBinaryDocValues values = MultiValuedSortedBinaryDocValues.fromMultiValued(leafReader, "field");

                // IntegratedCounts reader should decode correctly
                assertTrue(values.advanceExact(0));
                assertEquals(1, values.docValueCount());
                assertEquals(expected, values.nextValue());
            }
        }
    }
}
