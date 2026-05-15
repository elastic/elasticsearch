/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexableField;
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
        MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(
            doc,
            "field",
            new BytesRef("val"),
            MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE,
            previousVersion
        );

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

    // =====================================================================================================================================
    // addAllIgnoredValues tests
    // =====================================================================================================================================

    public void testAddIgnoredSourceValuesUsesSeparateCountForCurrentVersion() {
        // given
        LuceneDocument doc = new LuceneDocument();
        var nameValue = new IgnoredSourceFieldMapper.NameValue("field", 0, new BytesRef("val"), doc);

        // when
        MultiValuedBinaryDocValuesField.addIgnoredSourceValues(
            List.of(nameValue),
            "field",
            ValueOrdering.SORTED_UNIQUE,
            IndexVersion.current(),
            false
        );

        // then — SeparateCount field and a companion count field are added
        var fields = doc.getFields("field");
        assertEquals(1, fields.size());
        assertTrue(fields.getFirst() instanceof SeparateCount);

        var countFields = doc.getFields("field.counts");
        assertEquals(1, countFields.size());
        assertTrue(countFields.getFirst() instanceof NumericDocValuesField);
        assertEquals(1L, ((NumericDocValuesField) countFields.getFirst()).numericValue().longValue());
    }

    public void testAddIgnoredSourceValuesUsesIntegratedCountForOldVersion() {
        // given
        LuceneDocument doc = new LuceneDocument();
        IndexVersion oldVersion = IndexVersionUtils.getPreviousVersion(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES);
        var nameValue = new IgnoredSourceFieldMapper.NameValue("field", 0, new BytesRef("val"), doc);

        // when
        MultiValuedBinaryDocValuesField.addIgnoredSourceValues(List.of(nameValue), "field", ValueOrdering.SORTED_UNIQUE, oldVersion, false);

        // then — IntegratedCount field added, no companion count field
        var fields = doc.getFields("field");
        assertEquals(1, fields.size());
        assertTrue(fields.getFirst() instanceof IntegratedCount);
        assertTrue(doc.getFields("field.counts").isEmpty());
    }

    public void testAddIgnoredValuesGroupsMultipleSourceValuesPerDoc() {
        // given
        LuceneDocument doc = new LuceneDocument();
        var nameValue1 = new IgnoredSourceFieldMapper.NameValue("field", 0, new BytesRef("aaa"), doc);
        var nameValue2 = new IgnoredSourceFieldMapper.NameValue("field", 0, new BytesRef("bbb"), doc);

        // when
        MultiValuedBinaryDocValuesField.addIgnoredSourceValues(
            List.of(nameValue1, nameValue2),
            "field",
            ValueOrdering.SORTED_UNIQUE,
            IndexVersion.current(),
            true
        );

        // then — both values go into a single SeparateCount field on the document
        var fields = doc.getFields("field");
        assertEquals(1, fields.size());
        var field = (SeparateCount) fields.getFirst();
        assertEquals(2, field.count());

        var countFields = doc.getFields("field.counts");
        assertEquals(1, countFields.size());
        assertEquals(2L, countFields.getFirst().numericValue().longValue());
    }

    public void testAddIgnoredSourceValuesSeparateFieldsPerDoc() {
        // given
        LuceneDocument doc1 = new LuceneDocument();
        LuceneDocument doc2 = new LuceneDocument();
        var nameValue1 = new IgnoredSourceFieldMapper.NameValue("field", 0, new BytesRef("aaa"), doc1);
        var nameValue2 = new IgnoredSourceFieldMapper.NameValue("field", 0, new BytesRef("bbb"), doc2);

        // when
        MultiValuedBinaryDocValuesField.addIgnoredSourceValues(
            List.of(nameValue1, nameValue2),
            "field",
            ValueOrdering.SORTED_UNIQUE,
            IndexVersion.current(),
            true
        );

        // then — each document gets its own field
        assertEquals(1, doc1.getFields("field").size());
        assertEquals(1, ((SeparateCount) doc1.getFields("field").getFirst()).count());
        assertEquals(1, doc2.getFields("field").size());
        assertEquals(1, ((SeparateCount) doc2.getFields("field").getFirst()).count());
    }

    // =====================================================================================================================================
    // multi_value=false tests
    // =====================================================================================================================================

    public void testMultiValueFalseUsesBinaryDocValuesFieldWithRawBytes() {
        // given
        LuceneDocument doc = new LuceneDocument();
        BytesRef value = new BytesRef(randomAlphanumericOfLength(10));

        // when — use DocValuesFieldFactory which handles multi_value=false branching
        DocValuesFieldFactory factory = new DocValuesFieldFactory(false, false, IndexVersion.current());
        factory.addBinaryField(doc, "field", value, ValueOrdering.SORTED_UNIQUE);

        // then — field is stored as a plain BinaryDocValuesField with the raw value
        IndexableField storedField = doc.getField("field");
        assertNotNull(storedField);
        assertTrue(storedField instanceof BinaryDocValuesField);
        assertEquals(value, storedField.binaryValue());
    }

    /**
     * This test verifies that we're not double storing field names in keyedFields ({@link LuceneDocument}) and singleValuedFields
     * ({@link DocumentParserContext}). This ensures that we're not double storing.
     */
    public void testMultiValueFalseDoesNotStoreInKeyedFields() {
        // given
        LuceneDocument doc = new LuceneDocument();

        // when
        DocValuesFieldFactory factory = new DocValuesFieldFactory(false, false, IndexVersion.current());
        factory.addBinaryField(doc, "field", new BytesRef(randomAlphanumericOfLength(10)), ValueOrdering.SORTED_UNIQUE);

        // then — field is NOT registered in keyedFields; only in the Lucene fields list. Single-value enforcement is handled at the
        // DocumentParserContext level, not by keyed dedup.
        assertNull(doc.getByKey("field"));
        assertNull(doc.getByKey("field.counts"));
        assertNull(doc.getField("field.counts"));
    }

}
