/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.FormatVersion;

import java.io.IOException;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;

/** What a string column records about itself, written and read back on its own. */
public class StringColumnMetadataTests extends ColumnarStringTestCase {

    /** Everything a column records survives the round trip, over a column that was really written. */
    public void testRoundTrip() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(1, 2000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = randomBoolean() ? null : new BytesRef(randomAlphaOfLengthBetween(1, 40));
        }
        withColumn(docValues, (metadata, reader) -> assertRoundTrips(metadata, docValues.length));
    }

    /**
     * Two trailing tables, each written only when a count already on the wire says so, so all four
     * combinations of present and absent have to parse back to the same record.
     */
    public void testRoundTripAcrossBothTables() throws IOException {
        for (boolean multiValued : new boolean[] { false, true }) {
            for (boolean nulls : new boolean[] { false, true }) {
                final BytesRef[][] docSlots = randomDocSlots(between(20, 400), multiValued ? 6 : 1, randomBoolean(), nulls);
                withColumn(docSlots, (metadata, reader) -> {
                    // A single-slot document always holds a value, so asking for nulls does not always get any.
                    assertEquals("null table present", numNullSlots(docSlots) > 0, metadata.hasNullSlots());
                    assertRoundTrips(metadata, docSlots.length);
                });
            }
        }
    }

    /** The framing is recorded, not inferred, so a column re-encodes into the one its field was written with. */
    public void testFramingSurvivesTheRoundTrip() throws IOException {
        for (StringBinaryPayload.Framing framing : StringBinaryPayload.Framing.values()) {
            final BytesRef[][] docSlots = randomDocSlots(between(20, 200), 4, false, false);
            withColumn(docSlots, framing, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), (metadata, reader) -> {
                assertEquals("recorded framing", framing, metadata.framing());
                assertEquals("framing survives", framing, roundTrip(metadata, docSlots.length).framing());
                assertEquals("reader agrees", framing, reader.framing());
            });
        }
    }

    /**
     * A column no document has a value in stops after the document count, so nothing else it might have
     * recorded is written or read.
     */
    public void testEmptyColumnShortCircuits() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(1, 200)];
        withColumn(docValues, (metadata, reader) -> {
            assertEquals("no documents have a value", 0, metadata.numDocsWithField());
            final StringColumnMetadata read = roundTrip(metadata, docValues.length);
            assertEquals("numDocsWithField", 0, read.numDocsWithField());
            assertEquals("numValues", 0L, read.numValues());
            assertEquals("numNullSlots", 0L, read.numNullSlots());
            assertFalse("single-valued", read.multiValued());
            assertFalse("no null slots", read.hasNullSlots());
        });
    }

    /** A column holds more slots than it has documents exactly when a document holds more than one. */
    public void testMultiValuedFollowsFromTheCounts() throws IOException {
        final BytesRef[][] docSlots = randomDocSlots(between(2, 50), 1, false, false);
        withColumn(docSlots, (metadata, reader) -> assertFalse("as many slots as documents", metadata.multiValued()));

        final BytesRef[][] several = randomDocSlots(between(2, 50), 1, false, false);
        several[between(0, several.length - 1)] = new BytesRef[] { new BytesRef("a"), new BytesRef("b") };
        withColumn(several, (metadata, reader) -> {
            assertTrue("more slots than documents", metadata.multiValued());
            assertEquals("numValues counts slots", numValues(several), metadata.numValues());
        });
    }

    private static void assertRoundTrips(StringColumnMetadata metadata, int maxDoc) throws IOException {
        final StringColumnMetadata read = roundTrip(metadata, maxDoc);
        assertEquals("numDocsWithField", metadata.numDocsWithField(), read.numDocsWithField());
        assertEquals("numValues", metadata.numValues(), read.numValues());
        assertEquals("numNullSlots", metadata.numNullSlots(), read.numNullSlots());
        assertEquals("layout", metadata.layout(), read.layout());
        assertEquals("framing", metadata.framing(), read.framing());
        assertEquals("stream values", metadata.values().numValues(), read.values().numValues());
        assertEquals("values per block", metadata.values().valuesPerBlock(), read.values().valuesPerBlock());
        assertEquals("multi-valued", metadata.multiValued(), read.multiValued());
        assertEquals("has null slots", metadata.hasNullSlots(), read.hasNullSlots());
        assertEquals("value addresses", metadata.valueAddressesDataLength(), read.valueAddressesDataLength());
        assertArrayEquals("value address meta", metadata.valueAddressesMeta(), read.valueAddressesMeta());
        assertEquals("null slots", metadata.nullSlotsDataLength(), read.nullSlotsDataLength());
        assertArrayEquals("null slot meta", metadata.nullSlotsMeta(), read.nullSlotsMeta());
    }

    private static StringColumnMetadata roundTrip(StringColumnMetadata metadata, int maxDoc) throws IOException {
        final byte[] buffer = new byte[1 << 16];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        metadata.writeTo(out);
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        return StringColumnMetadata.readFrom(in, Math.max(maxDoc, 1), FormatVersion.CURRENT);
    }
}
