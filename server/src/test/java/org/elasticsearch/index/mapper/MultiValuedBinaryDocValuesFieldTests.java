/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.IntegratedCount;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ValueOrdering;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.util.List;

public class MultiValuedBinaryDocValuesFieldTests extends ESTestCase {

    // =====================================================================================================================================
    // IntegratedCount tests
    // =====================================================================================================================================

    public void testIntegratedCountSingleValue() throws IOException {
        // given
        var field = new IntegratedCount("field", ValueOrdering.SORTED_UNIQUE);
        field.add(new BytesRef("potato"));

        // when
        BytesRef binary = field.binaryValue();

        // then
        try (var in = new BytesStreamOutput()) {
            in.writeVInt(1);  // value count
            in.writeVInt(6);  // length of "potato"
            in.writeBytes(new byte[] { 'p', 'o', 't', 'a', 't', 'o' }, 0, 6);
            assertEquals(in.bytes().toBytesRef(), binary);
        }
    }

    public void testIntegratedCountMultipleValues() throws IOException {
        // given
        var field = new IntegratedCount("field", ValueOrdering.SORTED_UNIQUE);
        field.add(new BytesRef("bbb"));
        field.add(new BytesRef("aaa"));

        // when
        BytesRef binary = field.binaryValue();

        // then — TreeSet sorts, so aaa comes first
        try (var in = new BytesStreamOutput()) {
            in.writeVInt(2);  // value count
            in.writeVInt(3);
            in.writeBytes(new byte[] { 'a', 'a', 'a' }, 0, 3);
            in.writeVInt(3);
            in.writeBytes(new byte[] { 'b', 'b', 'b' }, 0, 3);
            assertEquals(in.bytes().toBytesRef(), binary);
        }
    }

    public void testIntegratedCountDeduplicates() {
        // given
        var field = new IntegratedCount("field", ValueOrdering.SORTED_UNIQUE);

        // when
        field.add(new BytesRef("aaa"));
        field.add(new BytesRef("aaa"));

        // then
        assertEquals(1, field.count());
    }

    public void testIntegratedCountEncode() throws IOException {
        // given
        List<BytesRef> values = List.of(new BytesRef("aaa"), new BytesRef("bbb"));

        // when
        BytesRef encoded = IntegratedCount.encode(values);

        // then
        try (var expected = new BytesStreamOutput()) {
            expected.writeVInt(2);  // value count
            expected.writeVInt(3);
            expected.writeBytes(new byte[] { 'a', 'a', 'a' }, 0, 3);
            expected.writeVInt(3);
            expected.writeBytes(new byte[] { 'b', 'b', 'b' }, 0, 3);
            assertEquals(expected.bytes().toBytesRef(), encoded);
        }
    }

    // =====================================================================================================================================
    // SeparateCount tests
    // =====================================================================================================================================

    public void testSeparateCountSingleValue() {
        // given
        var field = new SeparateCount("field", ValueOrdering.SORTED_UNIQUE);
        field.add(new BytesRef("hello"));

        // when
        BytesRef binary = field.binaryValue();

        // then — single value is stored raw, no length prefix
        assertEquals(new BytesRef("hello"), binary);
    }

    public void testSeparateCountMultipleValues() throws IOException {
        // given
        var field = new SeparateCount("field", ValueOrdering.SORTED_UNIQUE);
        field.add(new BytesRef("bbb"));
        field.add(new BytesRef("aaa"));

        // when
        BytesRef binary = field.binaryValue();

        // then — TreeSet sorts, so aaa comes first; no count prefix
        try (var expected = new BytesStreamOutput()) {
            expected.writeVInt(3);
            expected.writeBytes(new byte[] { 'a', 'a', 'a' }, 0, 3);
            expected.writeVInt(3);
            expected.writeBytes(new byte[] { 'b', 'b', 'b' }, 0, 3);
            assertEquals(expected.bytes().toBytesRef(), binary);
        }
    }

    public void testSeparateCountDeduplicates() {
        // given
        var field = new SeparateCount("field", ValueOrdering.SORTED_UNIQUE);

        // when
        field.add(new BytesRef("aaa"));
        field.add(new BytesRef("aaa"));

        // then
        assertEquals(1, field.count());
    }

    public void testSeparateCountFieldName() {
        // given
        var field = new SeparateCount("my_field", ValueOrdering.SORTED_UNIQUE);

        // then
        assertEquals("my_field.counts", field.countFieldName());
    }

    // =====================================================================================================================================
    // ValueOrdering tests
    // =====================================================================================================================================

    public void testSortedUniqueOrderingDeduplicatesAndSorts() throws IOException {
        // given
        var field = new SeparateCount("field", ValueOrdering.SORTED_UNIQUE);
        field.add(new BytesRef("ccc"));
        field.add(new BytesRef("aaa"));
        field.add(new BytesRef("aaa"));

        // when
        BytesRef binary = field.binaryValue();

        // then — duplicates removed, sorted
        assertEquals(2, field.count());
        try (var expected = new BytesStreamOutput()) {
            expected.writeVInt(3);
            expected.writeBytes(new byte[] { 'a', 'a', 'a' }, 0, 3);
            expected.writeVInt(3);
            expected.writeBytes(new byte[] { 'c', 'c', 'c' }, 0, 3);
            assertEquals(expected.bytes().toBytesRef(), binary);
        }
    }

    public void testSortedOrderingKeepsDuplicatesAndSorts() throws IOException {
        // given
        var field = new SeparateCount("field", ValueOrdering.SORTED);
        field.add(new BytesRef("ccc"));
        field.add(new BytesRef("aaa"));
        field.add(new BytesRef("aaa"));

        // when
        BytesRef binary = field.binaryValue();

        // then — duplicates kept, sorted at encode time
        assertEquals(3, field.count());
        try (var expected = new BytesStreamOutput()) {
            expected.writeVInt(3);
            expected.writeBytes(new byte[] { 'a', 'a', 'a' }, 0, 3);
            expected.writeVInt(3);
            expected.writeBytes(new byte[] { 'a', 'a', 'a' }, 0, 3);
            expected.writeVInt(3);
            expected.writeBytes(new byte[] { 'c', 'c', 'c' }, 0, 3);
            assertEquals(expected.bytes().toBytesRef(), binary);
        }
    }

    public void testUnsortedOrderingKeepsDuplicatesAndInsertionOrder() throws IOException {
        // given
        var field = new SeparateCount("field", ValueOrdering.UNSORTED);
        field.add(new BytesRef("ccc"));
        field.add(new BytesRef("aaa"));
        field.add(new BytesRef("aaa"));

        // when
        BytesRef binary = field.binaryValue();

        // then — duplicates kept, insertion order preserved
        assertEquals(3, field.count());
        try (var expected = new BytesStreamOutput()) {
            expected.writeVInt(3);
            expected.writeBytes(new byte[] { 'c', 'c', 'c' }, 0, 3);
            expected.writeVInt(3);
            expected.writeBytes(new byte[] { 'a', 'a', 'a' }, 0, 3);
            expected.writeVInt(3);
            expected.writeBytes(new byte[] { 'a', 'a', 'a' }, 0, 3);
            assertEquals(expected.bytes().toBytesRef(), binary);
        }
    }

    // =====================================================================================================================================
    // addToBinaryFieldInDoc version dispatch tests
    // =====================================================================================================================================

    public void testAddToBinaryFieldInDocUsesSeparateCountForCurrentVersion() {
        // given
        LuceneDocument doc = new LuceneDocument();

        // when
        MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "field", new BytesRef("val"));

        // then
        assertNotNull(doc.getByKey("field"));
        assertTrue(doc.getByKey("field") instanceof SeparateCount);
        assertNotNull(doc.getByKey("field.counts"));
    }

    public void testAddToBinaryFieldInDocUsesIntegratedCountForPreviousVersion() {
        // given
        LuceneDocument doc = new LuceneDocument();
        IndexVersion previousVersion = IndexVersionUtils.getPreviousVersion(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES);

        // when
        MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "field", new BytesRef("val"), previousVersion);

        // then
        assertNotNull(doc.getByKey("field"));
        assertTrue(doc.getByKey("field") instanceof IntegratedCount);
        assertNull(doc.getByKey("field.counts"));
    }

    public void testAddToBinaryFieldInDocAccumulatesValues() {
        // given
        LuceneDocument doc = new LuceneDocument();

        // when
        MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "field", new BytesRef("aaa"));
        MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, "field", new BytesRef("bbb"));

        // then
        var field = (SeparateCount) doc.getByKey("field");
        assertEquals(2, field.count());
    }

}
