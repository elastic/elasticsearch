/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class MultiValuedBinaryDocValuesSortFieldTests extends ESTestCase {

    // =========================================================================
    // getSortKeyDocValues — plain BinaryDocValues (no companion .counts field)
    // =========================================================================

    /** When there is no companion {@code .counts} field, the raw BinaryDocValues are returned as-is. */
    public void testNoCounts_passthroughRawBytes() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            Document doc = new Document();
            doc.add(new BinaryDocValuesField("name", new BytesRef("hello")));
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues dvs = new MultiValuedBinaryDocValuesSortField("name", false, SortField.STRING_LAST, false)
                    .getSortKeyDocValues(leaf);
                assertTrue(dvs.advanceExact(0));
                assertEquals(new BytesRef("hello"), dvs.binaryValue());
            }
        }
    }

    // =========================================================================
    // getSortKeyDocValues — SeparateCount format (companion .counts field present)
    // =========================================================================

    /** count=1: binary payload is the raw term bytes; returned as-is regardless of mode. */
    public void testSingleValue_returnsRawBytes() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("alice"));
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                for (boolean maxMode : new boolean[] { false, true }) {
                    BinaryDocValues dvs = new MultiValuedBinaryDocValuesSortField("name", false, SortField.STRING_LAST, maxMode)
                        .getSortKeyDocValues(leaf);
                    assertTrue(dvs.advanceExact(0));
                    assertEquals("maxMode=" + maxMode, new BytesRef("alice"), dvs.binaryValue());
                }
            }
        }
    }

    /**
     * count=2, MIN mode: values stored sorted as ["bob","zebra"]; first value "bob" is the sort key.
     */
    public void testTwoValues_minMode_returnsSmallest() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("zebra"));
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("bob"));
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues dvs = new MultiValuedBinaryDocValuesSortField("name", false, SortField.STRING_LAST, false)
                    .getSortKeyDocValues(leaf);
                assertTrue(dvs.advanceExact(0));
                assertEquals(new BytesRef("bob"), dvs.binaryValue());
            }
        }
    }

    /**
     * count=2, MAX mode: values stored sorted as ["bob","zebra"]; last value "zebra" is the sort key.
     */
    public void testTwoValues_maxMode_returnsLargest() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("zebra"));
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("bob"));
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues dvs = new MultiValuedBinaryDocValuesSortField("name", false, SortField.STRING_LAST, true)
                    .getSortKeyDocValues(leaf);
                assertTrue(dvs.advanceExact(0));
                assertEquals(new BytesRef("zebra"), dvs.binaryValue());
            }
        }
    }

    /**
     * count=3, MIN mode: values stored sorted as ["apple","mango","orange"]; "apple" is the sort key.
     */
    public void testThreeValues_minMode_returnsSmallest() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("mango"));
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("apple"));
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("orange"));
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues dvs = new MultiValuedBinaryDocValuesSortField("name", false, SortField.STRING_LAST, false)
                    .getSortKeyDocValues(leaf);
                assertTrue(dvs.advanceExact(0));
                assertEquals(new BytesRef("apple"), dvs.binaryValue());
            }
        }
    }

    /**
     * count=3, MAX mode: values stored sorted as ["apple","mango","orange"]; "orange" is the sort key.
     */
    public void testThreeValues_maxMode_returnsLargest() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            LuceneDocument doc = new LuceneDocument();
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("mango"));
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("apple"));
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "name", new BytesRef("orange"));
            w.addDocument(doc);
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues dvs = new MultiValuedBinaryDocValuesSortField("name", false, SortField.STRING_LAST, true)
                    .getSortKeyDocValues(leaf);
                assertTrue(dvs.advanceExact(0));
                assertEquals(new BytesRef("orange"), dvs.binaryValue());
            }
        }
    }

    // =========================================================================
    // Provider round-trip serialization
    // =========================================================================

    public void testProviderRoundTrip_minMode_stringFirst() throws IOException {
        assertRoundTrip(new MultiValuedBinaryDocValuesSortField("host.name", false, SortField.STRING_FIRST, false));
    }

    public void testProviderRoundTrip_maxMode_stringLast() throws IOException {
        assertRoundTrip(new MultiValuedBinaryDocValuesSortField("host.name", true, SortField.STRING_LAST, true));
    }

    public void testProviderRoundTrip_minMode_nullMissingValue() throws IOException {
        assertRoundTrip(new MultiValuedBinaryDocValuesSortField("host.name", false, null, false));
    }

    public void testProviderRoundTrip_maxMode_stringFirst() throws IOException {
        assertRoundTrip(new MultiValuedBinaryDocValuesSortField("host.name", true, SortField.STRING_FIRST, true));
    }

    public void testProviderRoundTrip_minMode_reverseTrue() throws IOException {
        assertRoundTrip(new MultiValuedBinaryDocValuesSortField("host.name", true, SortField.STRING_LAST, false));
    }

    private static void assertRoundTrip(MultiValuedBinaryDocValuesSortField original) throws IOException {
        var provider = new MultiValuedBinaryDocValuesSortField.Provider();
        var buf = new ByteBuffersDataOutput();
        provider.writeSortField(original, buf);
        MultiValuedBinaryDocValuesSortField restored = (MultiValuedBinaryDocValuesSortField) provider.readSortField(buf.toDataInput());
        assertEquals(original.getField(), restored.getField());
        assertEquals(original.getReverse(), restored.getReverse());
        assertEquals(original.getMissingValue(), restored.getMissingValue());
        assertEquals(original.isMaxMode(), restored.isMaxMode());
    }
}
