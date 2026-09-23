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
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.IntegratedCount;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ValueOrdering;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
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
        // count field is added to the fields list un-keyed (no second keyedFields put needed)
        assertNull(doc.getByKey("field.counts"));
        assertNotNull(doc.getField("field.counts"));
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

    public void testEncodeTuplesSingleSlot() throws IOException {
        // given
        BytesRef keyPrefix = new BytesRef("key1\0");
        BytesRef value = new BytesRef("v1");
        BytesRef[] tuples = { keyPrefix, value };

        // when
        BytesRef actual = encodeTuples(tuples, 1);

        // then — VInt prefix is valueLen+1 (no single-slot raw passthrough)
        try (var expected = new BytesStreamOutput()) {
            expected.writeVInt(3);                                               // valueLen+1 = 2+1
            expected.writeBytes(new byte[] { 'k', 'e', 'y', '1', 0 }, 0, 5);   // key\0
            expected.writeBytes(new byte[] { 'v', '1' }, 0, 2);                 // value
            assertEquals(expected.bytes().toBytesRef(), actual);
        }
    }

    public void testEncodeTuplesMultipleSlots() throws IOException {
        // given - two slots; slot order must be preserved in the output
        BytesRef[] tuples = { new BytesRef("k1\0"), new BytesRef("a"), new BytesRef("k2\0"), new BytesRef("b"), };

        // when
        BytesRef actual = encodeTuples(tuples, 2);

        // then
        try (var expected = new BytesStreamOutput()) {
            expected.writeVInt(2);                              // "a".length + 1
            expected.writeBytes(new byte[] { 'k', '1', 0 }, 0, 3);
            expected.writeBytes(new byte[] { 'a' }, 0, 1);
            expected.writeVInt(2);                              // "b".length + 1
            expected.writeBytes(new byte[] { 'k', '2', 0 }, 0, 3);
            expected.writeBytes(new byte[] { 'b' }, 0, 1);
            assertEquals(expected.bytes().toBytesRef(), actual);
        }
    }

    public void testEncodeTuplesNullSlot() throws IOException {
        // given - a JSON null: key prefix written with VInt 0, no value bytes
        BytesRef keyPrefix = new BytesRef("k\0");
        BytesRef[] tuples = { keyPrefix, null };

        // when
        BytesRef actual = encodeTuples(tuples, 1);

        // then — the separator byte is always written even for null slots; decoders skip keyLen+1
        try (var expected = new BytesStreamOutput()) {
            expected.writeVInt(0);                              // null sentinel
            expected.writeBytes(new byte[] { 'k', 0 }, 0, 2); // key\0
            assertEquals(expected.bytes().toBytesRef(), actual);
        }
    }

    /**
     * An empty-string value and a null share the same payload bytes ({@code key\0}) and differ only in the VInt prefix:
     * {@code 1} for empty-string (valueLen=0, bias+1=1) versus {@code 0} for null. Getting the prefix wrong here is the
     * one case where the mistake is invisible in the payload bytes.
     */
    public void testEncodeTuplesEmptyStringValueDistinctFromNull() throws IOException {
        // given
        BytesRef keyPrefix = new BytesRef("k\0");
        BytesRef[] tuples = {
            keyPrefix,
            new BytesRef(""),  // slot 0: empty string — same payload bytes as null, different prefix
            keyPrefix,
            null,              // slot 1: null
        };

        // when
        BytesRef actual = encodeTuples(tuples, 2);

        // then
        try (var expected = new BytesStreamOutput()) {
            expected.writeVInt(1);                              // empty string: valueLen+1 = 0+1 = 1
            expected.writeBytes(new byte[] { 'k', 0 }, 0, 2);
            expected.writeVInt(0);                              // null
            expected.writeBytes(new byte[] { 'k', 0 }, 0, 2);
            assertEquals(expected.bytes().toBytesRef(), actual);
        }
    }

    /** A {@code \0} byte inside the value is legal — only the key's trailing separator delimits. */
    public void testEncodeTuplesValueContainingSeparatorByte() throws IOException {
        // given
        BytesRef keyPrefix = new BytesRef("k\0");
        BytesRef value = new BytesRef(new byte[] { 'v', 0, 'x' });
        BytesRef[] tuples = { keyPrefix, value };

        // when
        BytesRef actual = encodeTuples(tuples, 1);

        // then — the inner \0 in the value is not confused with the key separator
        try (var expected = new BytesStreamOutput()) {
            expected.writeVInt(4);                                       // valueLen+1 = 3+1
            expected.writeBytes(new byte[] { 'k', 0 }, 0, 2);           // key\0
            expected.writeBytes(new byte[] { 'v', 0, 'x' }, 0, 3);     // value containing \0
            assertEquals(expected.bytes().toBytesRef(), actual);
        }
    }

    /** VInt prefixes are byte lengths, not character counts — multi-byte UTF-8 sequences count as multiple bytes. */
    public void testEncodeTuplesNonAsciiKeyAndValue() throws IOException {
        // given — "café" is 5 UTF-8 bytes (é encodes to 2 bytes), so keyPrefix is 6 bytes including \0
        BytesRef keyPrefix = new BytesRef("café\0");
        BytesRef value = new BytesRef("lait");
        BytesRef[] tuples = { keyPrefix, value };

        // when
        BytesRef actual = encodeTuples(tuples, 1);

        // then
        try (var expected = new BytesStreamOutput()) {
            expected.writeVInt(value.length + 1);
            expected.writeBytes(keyPrefix.bytes, keyPrefix.offset, keyPrefix.length);
            expected.writeBytes(value.bytes, value.offset, value.length);
            assertEquals(expected.bytes().toBytesRef(), actual);
        }
    }

    /**
     * Callers reuse an oversized tuples buffer across documents; entries beyond {@code slotCount}
     * must be silently ignored.
     */
    public void testEncodeTuplesIgnoresEntriesBeyondSlotCount() throws IOException {
        // given - three logical slots in the array, but only the first two should be encoded
        BytesRef[] tuples = {
            new BytesRef("k1\0"),
            new BytesRef("v1"),
            new BytesRef("k2\0"),
            new BytesRef("v2"),
            new BytesRef("STALE\0"),
            new BytesRef("STALE"),  // must be ignored
        };

        // when
        BytesRef actual = encodeTuples(tuples, 2);

        // then - result equals encoding only the two valid slots
        BytesRef[] fresh = { new BytesRef("k1\0"), new BytesRef("v1"), new BytesRef("k2\0"), new BytesRef("v2") };
        // A separate scratch buffer: `actual` is a view over the first one and would be overwritten by a second encode into it.
        assertEquals(encodeTuples(fresh, 2), actual);
    }

    /**
     * Checks that the tuple encoder produces exactly the same bytes as the row-path list encoder for identical logical slots,
     * ensuring the two paths cannot silently diverge.
     */
    public void testEncodeTuplesMatchesLegacyEncode() {
        // given - three slots covering all cases: non-null, null, empty-string value
        ArrayList<BytesRef> legacySlots = new ArrayList<>();
        BitSet legacyNulls = new BitSet();
        legacySlots.add(new BytesRef("host\0server1"));  // slot 0: non-null
        legacySlots.add(new BytesRef("port\0"));          // slot 1: null (same bytes as an empty-string slot, differs by bit below)
        legacyNulls.set(1);
        legacySlots.add(new BytesRef("tag\0"));           // slot 2: empty-string value (null marker clear)

        BytesRef[] tuples = {
            new BytesRef("host\0"),
            new BytesRef("server1"),  // slot 0: non-null
            new BytesRef("port\0"),
            null,                      // slot 1: null
            new BytesRef("tag\0"),
            new BytesRef(""),           // slot 2: empty string
        };

        // when
        BytesRef fromTuples = encodeTuples(tuples, 3);
        BytesRef fromLegacy = KeyedArrayOrderInlineNull.encode(legacySlots, legacyNulls);

        // then
        assertEquals(fromLegacy, fromTuples);
    }

    /**
     * The blob buffer is only ever grown and is rewritten from position 0 per document, so a short document appended after a long one
     * leaves the previous document's tail in the array. The emitted view must be bounded by the current document's length.
     */
    public void testAppendSlotBufferReuseDoesNotLeakPreviousDocument() {
        // given - a long document followed by a much shorter one, sharing one buffer
        BytesRefBuilder blob = new BytesRefBuilder();
        BytesRef[] longDoc = {
            new BytesRef("averyveryverylongkeyname\0"),
            new BytesRef("averyveryverylongvaluepayload"),
            new BytesRef("secondlongkeyname\0"),
            new BytesRef("secondlongvaluepayload"), };
        BytesRef[] shortDoc = { new BytesRef("k\0"), new BytesRef("v") };

        // when - both documents are appended into the same buffer, each starting from position 0
        appendTuplesInto(blob, longDoc, 2);
        BytesRef second = appendTuplesInto(blob, shortDoc, 1);

        // then
        assertEquals(encodeTuples(shortDoc, 1), second);
    }

    /** Once grown to the largest blob in a batch, the buffer must be reused rather than reallocated per document. */
    public void testAppendSlotReusesBuffer() {
        // given - the first document sizes the buffer
        BytesRefBuilder blob = new BytesRefBuilder();
        BytesRef[] tuples = { new BytesRef("key\0"), new BytesRef("value") };
        appendTuplesInto(blob, tuples, 1);
        byte[] afterFirst = blob.bytes();

        // when - a subsequent document of the same shape is appended
        appendTuplesInto(blob, tuples, 1);

        // then - no reallocation
        assertSame(afterFirst, blob.bytes());
    }

    /**
     * Checks that appending slots one at a time produces exactly the same bytes as the row-path collection encoder, ensuring the
     * columnar and row paths cannot silently diverge.
     */
    public void testArrayOrderAppendSlotMatchesCollectionEncode() {
        // given - a null slot between two values
        BytesRef[] slots = { new BytesRef("a"), null, new BytesRef("ccc") };

        // when
        BytesRef appended = appendSlotsInto(new BytesRefBuilder(), slots, 3);
        BytesRef fromCollection = MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.encode(Arrays.asList(slots));

        // then
        assertEquals(fromCollection, appended);
    }

    public void testArrayOrderAppendSlotBufferReuseDoesNotLeakPreviousDocument() {
        // given
        BytesRefBuilder blob = new BytesRefBuilder();
        BytesRef[] longDoc = { new BytesRef("averyverylongvalue"), new BytesRef("anotherveryverylongvalue") };
        BytesRef[] shortDoc = { new BytesRef("a"), new BytesRef("b") };

        // when
        appendSlotsInto(blob, longDoc, 2);
        BytesRef second = appendSlotsInto(blob, shortDoc, 2);

        // then
        assertEquals(appendSlotsInto(new BytesRefBuilder(), shortDoc, 2), second);
    }

    public void testArrayOrderAppendSlotValueEndsAtReturnedPosition() {
        for (BytesRef only : List.of(new BytesRef("solo"), new BytesRef(randomAlphanumericOfLength(500)))) {
            BytesRefBuilder blob = new BytesRefBuilder();

            // when
            int pos = MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.appendSlot(blob, 0, only);

            // then - the raw value occupies [pos - length, pos), matching the single-slot passthrough encoding
            BytesRef raw = new BytesRef(blob.bytes(), pos - only.length, only.length);
            assertEquals(only, raw);
            assertEquals(MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.encode(List.of(only)), raw);
        }
    }

    /** Builds a keyed blob one slot at a time, mirroring what {@code mapColumnGroupBatch} does per document. */
    private static BytesRef encodeTuples(BytesRef[] tuples, int slotCount) {
        return appendTuplesInto(new BytesRefBuilder(), tuples, slotCount);
    }

    /** As {@link #encodeTuples}, but appending into a caller-supplied buffer so reuse across documents can be asserted. */
    private static BytesRef appendTuplesInto(BytesRefBuilder blob, BytesRef[] tuples, int slotCount) {
        int pos = 0;
        for (int i = 0; i < slotCount; i++) {
            pos = KeyedArrayOrderInlineNull.appendSlot(blob, pos, tuples[2 * i], tuples[2 * i + 1]);
        }
        blob.setLength(pos);
        return blob.get();
    }

    /** Builds an unkeyed array-order blob one slot at a time, mirroring what {@code mapColumnBatchArrayOrder} does per document. */
    private static BytesRef appendSlotsInto(BytesRefBuilder blob, BytesRef[] slots, int count) {
        int pos = 0;
        for (int i = 0; i < count; i++) {
            pos = MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.appendSlot(blob, pos, slots[i]);
        }
        blob.setLength(pos);
        return blob.get();
    }
}
