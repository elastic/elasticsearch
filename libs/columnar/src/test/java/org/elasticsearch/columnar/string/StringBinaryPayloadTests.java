/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The payload format on its own, away from any column: the shape the mapper writes for a columnar field and the
 * codec rebuilds on read.
 *
 * <p>The format is the contract between the mapper and the codec, so what these pin is that whatever goes in comes
 * back out — slot for slot, nulls included, in order.
 */
public class StringBinaryPayloadTests extends ESTestCase {

    public void testRoundTripsOneSlot() throws IOException {
        assertRoundTrip(slots(new BytesRef("only")));
    }

    public void testRoundTripsManySlots() throws IOException {
        final List<BytesRef> slots = new ArrayList<>();
        for (int i = 0; i < between(2, 200); i++) {
            slots.add(new BytesRef(randomAlphaOfLengthBetween(0, 40)));
        }
        assertRoundTrip(slots);
    }

    /** An empty value is a real value; it must not read back as a null slot, which is what the bias is for. */
    public void testRoundTripsEmptyValuesBesideNulls() throws IOException {
        assertRoundTrip(slots(new BytesRef(""), null, new BytesRef(""), new BytesRef("x"), null));
        assertRoundTrip(slots(new BytesRef("")));
        assertRoundTrip(slots(null, new BytesRef("a")));
        assertRoundTrip(slots(new BytesRef("a"), null));
    }

    /** Lengths past 127, where a slot's prefix stops fitting in one byte. */
    public void testRoundTripsMultiByteLengths() throws IOException {
        assertRoundTrip(slots(new BytesRef("x".repeat(126)), new BytesRef("y".repeat(127)), new BytesRef("z".repeat(128))));
        final List<BytesRef> wide = new ArrayList<>();
        for (int i = 0; i < between(2, 20); i++) {
            wide.add(new BytesRef(randomAlphaOfLengthBetween(16_000, 20_000)));
        }
        assertRoundTrip(wide);
    }

    public void testRoundTripsRandomSlots() throws IOException {
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

    public void testNullSlotCount() throws IOException {
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
    public void testDecoderIsReusable() throws IOException {
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

    /** An empty array is a count of zero and nothing after it, which no other shape produces. */
    public void testRoundTripsNoSlots() throws IOException {
        assertRoundTrip(List.of());
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        assertEquals("the empty payload", 0, decoder.reset(StringBinaryPayload.EMPTY));
        assertEquals(StringBinaryPayload.EMPTY, StringBinaryPayload.encode(List.of()));
    }

    private static void assertRoundTrip(List<BytesRef> slots) throws IOException {
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

}
