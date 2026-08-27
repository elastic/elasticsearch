/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The payload format on its own, away from any column: the self-describing shape the mapper writes for a
 * columnar field, and the re-encode back into the framing readers expect.
 *
 * <p>Both directions are checked against encoders written out longhand here rather than against the ones under
 * test, since the whole point of the re-encode is that it produces bytes some other code already reads.
 */
public class StringBinaryPayloadTests extends ESTestCase {

    public void testRoundTripsOneSlot() {
        assertRoundTrip(slots(new BytesRef("only")));
    }

    public void testRoundTripsManySlots() {
        final List<BytesRef> slots = new ArrayList<>();
        for (int i = 0; i < between(2, 200); i++) {
            slots.add(new BytesRef(randomAlphaOfLengthBetween(0, 40)));
        }
        assertRoundTrip(slots);
    }

    /** An empty value is a real value; it must not read back as a null slot, which is what the bias is for. */
    public void testRoundTripsEmptyValuesBesideNulls() {
        assertRoundTrip(slots(new BytesRef(""), null, new BytesRef(""), new BytesRef("x"), null));
        assertRoundTrip(slots(new BytesRef("")));
        assertRoundTrip(slots(null, new BytesRef("a")));
        assertRoundTrip(slots(new BytesRef("a"), null));
    }

    /** Lengths past 127, where a slot's prefix stops fitting in one byte. */
    public void testRoundTripsMultiByteLengths() {
        assertRoundTrip(slots(new BytesRef("x".repeat(126)), new BytesRef("y".repeat(127)), new BytesRef("z".repeat(128))));
        final List<BytesRef> wide = new ArrayList<>();
        for (int i = 0; i < between(2, 20); i++) {
            wide.add(new BytesRef(randomAlphaOfLengthBetween(16_000, 20_000)));
        }
        assertRoundTrip(wide);
    }

    public void testRoundTripsRandomSlots() {
        for (int iter = 0; iter < 200; iter++) {
            final List<BytesRef> slots = new ArrayList<>();
            for (int i = 0; i < between(1, 30); i++) {
                slots.add(randomBoolean() ? null : new BytesRef(randomAlphaOfLengthBetween(0, 300)));
            }
            assertRoundTrip(slots);
        }
    }

    /** The slot-at-a-time path a caller uses when the count is only known once the document closes. */
    public void testAppendSlotAgreesWithEncode() {
        for (int iter = 0; iter < 200; iter++) {
            final List<BytesRef> slots = new ArrayList<>();
            for (int i = 0; i < between(1, 30); i++) {
                slots.add(randomBoolean() ? null : new BytesRef(randomAlphaOfLengthBetween(0, 100)));
            }
            final BytesRefBuilder blob = new BytesRefBuilder();
            int pos = StringBinaryPayload.COUNT_RESERVE;
            for (BytesRef slot : slots) {
                pos = StringBinaryPayload.appendSlot(blob, pos, slot);
            }
            final int start = StringBinaryPayload.writeCountBefore(blob, slots.size());
            final BytesRef appended = new BytesRef(blob.bytes(), start, pos - start);
            assertEquals("appended slot by slot", StringBinaryPayload.encode(slots), appended);
        }
    }

    public void testNullSlotCount() {
        for (int iter = 0; iter < 200; iter++) {
            final List<BytesRef> slots = new ArrayList<>();
            int nulls = 0;
            for (int i = 0; i < between(1, 30); i++) {
                final boolean isNull = randomBoolean();
                slots.add(isNull ? null : new BytesRef(randomAlphaOfLengthBetween(0, 60)));
                nulls += isNull ? 1 : 0;
            }
            final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
            decoder.reset(StringBinaryPayload.encode(slots));
            assertEquals("null slots", nulls, decoder.nullSlotCount());
            // Asking again mid-iteration must not disturb the cursor.
            assertEquals("first slot", slots.get(0), decoder.next());
            assertEquals("null slots, mid-walk", nulls, decoder.nullSlotCount());
        }
    }

    /** A decoder is reused across documents, so the previous payload must leave nothing behind. */
    public void testDecoderIsReusable() {
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        for (int iter = 0; iter < 200; iter++) {
            final List<BytesRef> slots = new ArrayList<>();
            for (int i = 0; i < between(1, 10); i++) {
                slots.add(randomBoolean() ? null : new BytesRef(randomAlphaOfLengthBetween(0, 30)));
            }
            final int count = decoder.reset(StringBinaryPayload.encode(slots));
            assertEquals("slot count", slots.size(), count);
            for (BytesRef expected : slots) {
                assertEquals(expected, decoder.next());
            }
        }
    }

    /** The re-encode, against the framing spelled out longhand — what a reader consulting {@code .counts} sees. */
    public void testLegacyEncoderMatchesTheMappersFraming() throws IOException {
        for (StringBinaryPayload.Framing framing : new StringBinaryPayload.Framing[] {
            StringBinaryPayload.Framing.SEPARATE_COUNT,
            StringBinaryPayload.Framing.ARRAY_ORDER }) {
            final boolean nulls = framing == StringBinaryPayload.Framing.ARRAY_ORDER;
            final StringBinaryPayload.LegacyEncoder encoder = new StringBinaryPayload.LegacyEncoder();
            for (int iter = 0; iter < 200; iter++) {
                final List<BytesRef> slots = new ArrayList<>();
                for (int i = 0; i < between(1, 30); i++) {
                    slots.add(nulls && randomBoolean() ? null : new BytesRef(randomAlphaOfLengthBetween(0, 200)));
                }
                if (slots.size() == 1 && slots.get(0) == null) {
                    slots.set(0, new BytesRef("kept"));
                }
                encoder.begin(framing, slots.size());
                for (BytesRef slot : slots) {
                    encoder.append(slot);
                }
                assertEquals("under " + framing, legacyEncode(slots, framing), encoder.get());
            }
        }
    }

    /** A lone slot is handed back as its own bytes under both framings, with no prefix at all. */
    public void testLoneSlotIsRaw() {
        for (StringBinaryPayload.Framing framing : StringBinaryPayload.Framing.values()) {
            final BytesRef only = new BytesRef(randomAlphaOfLengthBetween(0, 200));
            final StringBinaryPayload.LegacyEncoder encoder = new StringBinaryPayload.LegacyEncoder();
            encoder.begin(framing, 1);
            encoder.append(only);
            assertEquals("raw under " + framing, only, encoder.get());
        }
    }

    public void testFramingIdsAreFrozen() {
        assertEquals(0, StringBinaryPayload.Framing.SEPARATE_COUNT.id());
        assertEquals(1, StringBinaryPayload.Framing.ARRAY_ORDER.id());
        assertEquals(2, StringBinaryPayload.Framing.PLAIN.id());
        assertFalse("a plain field carries no count", StringBinaryPayload.Framing.PLAIN.isSelfDescribing());
        for (StringBinaryPayload.Framing framing : StringBinaryPayload.Framing.values()) {
            assertEquals(framing, StringBinaryPayload.Framing.forId(framing.id()));
        }
        expectThrows(IllegalArgumentException.class, () -> StringBinaryPayload.Framing.forId((byte) 7));
    }

    private static void assertRoundTrip(List<BytesRef> slots) {
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        final BytesRef payload = StringBinaryPayload.encode(slots);
        assertEquals("slot count", slots.size(), decoder.reset(payload));
        for (int i = 0; i < slots.size(); i++) {
            assertEquals("slot " + i, slots.get(i), decoder.next());
        }
    }

    private static List<BytesRef> slots(BytesRef... slots) {
        return Arrays.asList(slots);
    }

    /**
     * The framing the mapper writes: a lone slot raw, otherwise a length per slot, biased by one where the
     * framing can carry a null.
     */
    private static BytesRef legacyEncode(List<BytesRef> slots, StringBinaryPayload.Framing framing) throws IOException {
        if (slots.size() == 1) {
            return slots.get(0);
        }
        final int bias = framing == StringBinaryPayload.Framing.ARRAY_ORDER ? 1 : 0;
        int upperBound = 0;
        for (BytesRef slot : slots) {
            upperBound += StringBinaryPayload.VINT_MAX_BYTES + (slot == null ? 0 : slot.length);
        }
        final byte[] buffer = new byte[upperBound];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        for (BytesRef slot : slots) {
            if (slot == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(slot.length + bias);
                out.writeBytes(slot.bytes, slot.offset, slot.length);
            }
        }
        return new BytesRef(buffer, 0, out.getPosition());
    }
}
