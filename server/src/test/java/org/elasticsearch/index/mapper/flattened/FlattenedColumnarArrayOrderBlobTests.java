/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class FlattenedColumnarArrayOrderBlobTests extends AbstractFlattenedColumnarArrayOrderTestCase {

    private static final String KEYED_FIELD = FIELD + FlattenedFieldMapper.KEYED_FIELD_SUFFIX;
    private static final String COUNTS_FIELD = KEYED_FIELD + MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

    private void withIndexedDocumentInEveryMode(
        CheckedConsumer<XContentBuilder, IOException> doc,
        CheckedConsumer<LeafReader, IOException> check
    ) throws Exception {
        forEachStrictColumnarMode(
            mode -> withIndexedDocument(flattenedMapperService(mode), doc, indexed -> check.accept(indexed.leaf().reader()))
        );
    }

    private static BytesRef rawBlob(LeafReader leaf) throws IOException {
        final BinaryDocValues binary = DocValues.getBinary(leaf, KEYED_FIELD);
        assertTrue(binary.advanceExact(0));
        return BytesRef.deepCopyOf(binary.binaryValue());
    }

    private static byte[] keyedValue(String key, String value) {
        return (key + "\0" + value).getBytes(StandardCharsets.UTF_8);
    }

    // NOTE: built by hand as [vint(valueLen + 1)][key\0value] so the expected bytes do not depend on the production encoder.
    private static byte[] slot(String key, String value) {
        final byte[] keyedValue = keyedValue(key, value);
        final int prefix = value.getBytes(StandardCharsets.UTF_8).length + 1;
        assertTrue("test supports prefixes of at most two bytes", prefix < (1 << 14));
        final byte[] prefixBytes = prefix < 128
            ? new byte[] { (byte) prefix }
            : new byte[] { (byte) ((prefix & 0x7F) | 0x80), (byte) (prefix >>> 7) };
        final byte[] slot = Arrays.copyOf(prefixBytes, prefixBytes.length + keyedValue.length);
        System.arraycopy(keyedValue, 0, slot, prefixBytes.length, keyedValue.length);
        return slot;
    }

    private static byte[] concat(byte[] first, byte[] second) {
        final byte[] joined = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, joined, first.length, second.length);
        return joined;
    }

    public void testBlobHoldsOneLengthPrefixedKeyedSlotPerValueInDocumentOrder() throws Exception {
        final String pilot = randomAlphaOfLengthBetween(1, 300);
        final String copilot = randomAlphaOfLengthBetween(1, 300);
        withIndexedDocumentInEveryMode(b -> b.startObject("crew").field("pilot", pilot).field("copilot", copilot).endObject(), leaf -> {
            final NumericDocValues counts = leaf.getNumericDocValues(COUNTS_FIELD);
            assertTrue(counts.advanceExact(0));
            assertEquals(2L, counts.longValue());
            final byte[] expected = concat(slot("pilot", pilot), slot("copilot", copilot));
            assertEquals(new BytesRef(expected), rawBlob(leaf));
        });
    }

    public void testSeparateCountReaderReturnsSingleSlotBlobUnchanged() throws Exception {
        final String pilot = randomAlphaOfLengthBetween(1, 300);
        withIndexedDocumentInEveryMode(b -> b.startObject("crew").field("pilot", pilot).endObject(), leaf -> {
            final BytesRef blob = rawBlob(leaf);
            assertEquals(new BytesRef(slot("pilot", pilot)), blob);

            final SortedBinaryDocValues reader = MultiValuedSortedBinaryDocValues.from(leaf, KEYED_FIELD);
            assertTrue(reader.advanceExact(0));
            assertEquals(1, reader.docValueCount());
            final BytesRef value = reader.nextValue();
            assertEquals("the length prefix is still in the value", blob, value);
            assertEquals(
                "key stripping removes the length prefix together with the key",
                new BytesRef(pilot),
                FlattenedFieldParser.extractValue(value)
            );
        });
    }

    public void testSeparateCountReaderTruncatesFirstSlotOfTwoSlotBlob() throws Exception {
        final String pilot = randomAlphaOfLengthBetween(1, 300);
        final String copilot = randomAlphaOfLengthBetween(1, 300);
        withIndexedDocumentInEveryMode(b -> b.startObject("crew").field("pilot", pilot).field("copilot", copilot).endObject(), leaf -> {
            final SortedBinaryDocValues reader = MultiValuedSortedBinaryDocValues.from(leaf, KEYED_FIELD);
            assertTrue(reader.advanceExact(0));
            assertEquals(2, reader.docValueCount());

            final byte[] firstSlot = keyedValue("pilot", pilot);
            final int valueLengthPlusOne = pilot.getBytes(StandardCharsets.UTF_8).length + 1;
            final BytesRef decoded = reader.nextValue();
            assertEquals(
                "the prefix counts the value only, so the reader takes valueLen + 1 bytes of key\\0value",
                new BytesRef(Arrays.copyOf(firstSlot, valueLengthPlusOne)),
                decoded
            );
            assertNotEquals(new BytesRef(firstSlot), decoded);
        });
    }
}
