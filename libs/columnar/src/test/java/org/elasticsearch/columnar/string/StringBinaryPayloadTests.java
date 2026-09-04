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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

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

    /**
     * The slot-at-a-time path, where the count is only known once the document closes. One builder carried across documents has to
     * produce, for each of them, exactly what a builder that had seen nothing else would — which is what makes reuse safe on both the
     * mapper's write path and the batch one.
     */
    public void testReusedBuilderAgreesWithAFreshOne() {
        final StringBinaryPayload.Builder builder = new StringBinaryPayload.Builder();
        for (int iter = 0; iter < 200; iter++) {
            final List<BytesRef> slots = new ArrayList<>();
            for (int i = 0; i < between(0, 30); i++) {
                slots.add(randomBoolean() ? null : new BytesRef(randomAlphaOfLengthBetween(0, 100)));
            }
            builder.reset();
            for (BytesRef slot : slots) {
                builder.appendSlot(slot);
            }
            assertEquals("built slot by slot", encode(slots), builder.build());
        }
    }

    /** Building twice without appending anything in between must not carry the first document's slots over. */
    public void testBuilderResets() {
        final StringBinaryPayload.Builder builder = new StringBinaryPayload.Builder();
        builder.appendSlot(new BytesRef("a"));
        builder.appendSlot(null);
        assertEquals(encode(Arrays.asList(new BytesRef("a"), null)), builder.build());
        builder.reset();
        assertEquals("nothing carried over", StringBinaryPayload.EMPTY, builder.build());
        builder.appendSlot(new BytesRef("b"));
        assertEquals(encode(List.of(new BytesRef("b"))), builder.build());
    }

    /** {@link StringBinaryPayload.Builder#build} may be called more than once for the same document. */
    public void testBuildIsRepeatable() {
        final StringBinaryPayload.Builder builder = new StringBinaryPayload.Builder();
        builder.appendSlot(new BytesRef("a"));
        builder.appendSlot(new BytesRef("bb"));
        final BytesRef first = BytesRef.deepCopyOf(builder.build());
        assertEquals(first, builder.build());
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
            decoder.reset(encode(slots));
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
            final int count = decoder.reset(encode(slots));
            assertEquals("slot count", slots.size(), count);
            for (BytesRef expected : slots) {
                assertEquals(expected, decoder.next());
            }
        }
    }

    /** The extreme is whichever non-null slot sorts first or last, wherever in the payload it sits. */
    public void testExtremeFindsTheEndsOfThePayload() throws IOException {
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        // The winner first, last, and in the middle, so a scan that dropped either end of the payload would be caught.
        assertExtreme(decoder, slots(new BytesRef("a"), new BytesRef("m"), new BytesRef("z")), "a", "z");
        assertExtreme(decoder, slots(new BytesRef("z"), new BytesRef("a"), new BytesRef("m")), "a", "z");
        assertExtreme(decoder, slots(new BytesRef("m"), new BytesRef("z"), new BytesRef("a")), "a", "z");
        // A lone slot is both ends of the payload.
        assertExtreme(decoder, slots(new BytesRef("only")), "only", "only");
        // A value held by several slots wins once, and ties do not shift which one it is.
        assertExtreme(decoder, slots(new BytesRef("b"), new BytesRef("b"), new BytesRef("a"), new BytesRef("b")), "a", "b");
        // An empty value is a value, and sorts ahead of every other one.
        assertExtreme(decoder, slots(new BytesRef("b"), new BytesRef(""), new BytesRef("a")), "", "b");
    }

    /** Nulls are not values, so they are stepped over whichever end of the payload they sit at. */
    public void testExtremeSkipsNullSlots() throws IOException {
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        assertExtreme(decoder, slots(null, new BytesRef("b"), null, new BytesRef("a"), null), "a", "b");
        assertExtreme(decoder, slots(null, new BytesRef("only"), null), "only", "only");
    }

    /**
     * A document whose slots are all null. The other encodings write no blob at all for one, so the payload is the only shape that can
     * be asked and has to answer that there is no key — at either end, however many nulls it frames.
     */
    public void testExtremeOfAnAllNullPayload() throws IOException {
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        assertNoExtreme(decoder, "a lone null slot", encode(slots((BytesRef) null)));
        for (int nulls = 2; nulls <= 30; nulls++) {
            final List<BytesRef> allNull = new ArrayList<>();
            for (int i = 0; i < nulls; i++) {
                allNull.add(null);
            }
            assertNoExtreme(decoder, nulls + " null slots", encode(allNull));
        }
        // A decoder that has just found a value must not carry it into the next document, which has none.
        assertExtreme(decoder, slots(new BytesRef("a"), null), "a", "a");
        assertNoExtreme(decoder, "all nulls after a value", encode(slots(null, null)));
    }

    /**
     * A document holding no slot at all, the other shape with nothing to sort on. Handing back its bytes would sort the document on
     * its own framing.
     */
    public void testExtremeOfAPayloadWithNoSlots() throws IOException {
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        assertNoExtreme(decoder, "no slots at all", encode(List.of()));
        assertNoExtreme(decoder, "the empty payload", StringBinaryPayload.EMPTY);
    }

    /** Against the slots themselves over random payloads, on one decoder reused across them as a caller reuses it. */
    public void testExtremeAgreesWithTheSlots() throws IOException {
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        for (int iter = 0; iter < 200; iter++) {
            final List<BytesRef> slots = new ArrayList<>();
            for (int i = 0; i < between(1, 30); i++) {
                slots.add(randomBoolean() ? null : new BytesRef(randomAlphaOfLengthBetween(0, 60)));
            }
            final BytesRef payload = encode(slots);
            final List<BytesRef> values = slots.stream().filter(Objects::nonNull).sorted().toList();
            if (values.isEmpty()) {
                assertNoExtreme(decoder, "nothing but nulls", payload);
                continue;
            }
            assertEquals("minimum", values.get(0), decoder.extreme(payload, false));
            assertEquals("maximum", values.get(values.size() - 1), decoder.extreme(payload, true));
        }
    }

    /** An empty array is a count of zero and nothing after it, which no other shape produces. */
    public void testRoundTripsNoSlots() throws IOException {
        assertRoundTrip(List.of());
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        assertEquals("the empty payload", 0, decoder.reset(StringBinaryPayload.EMPTY));
        assertEquals(StringBinaryPayload.EMPTY, encode(List.of()));
    }

    private static void assertRoundTrip(List<BytesRef> slots) throws IOException {
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        final BytesRef payload = encode(slots);
        assertEquals("slot count", slots.size(), decoder.reset(payload));
        for (int i = 0; i < slots.size(); i++) {
            assertEquals("slot " + i, slots.get(i), decoder.next());
        }
    }

    /** Both ends of one payload, so a decoder that leaked state between the two calls would not agree with either. */
    private static void assertExtreme(StringBinaryPayload.Decoder decoder, List<BytesRef> slots, String min, String max)
        throws IOException {
        final BytesRef payload = encode(slots);
        assertEquals("minimum of " + slots, new BytesRef(min), decoder.extreme(payload, false));
        assertEquals("maximum of " + slots, new BytesRef(max), decoder.extreme(payload, true));
    }

    /** Neither end of a payload with no value exists, so both modes have to report it rather than one of them by luck. */
    private static void assertNoExtreme(StringBinaryPayload.Decoder decoder, String label, BytesRef payload) throws IOException {
        assertNull("minimum of " + label, decoder.extreme(payload, false));
        assertNull("maximum of " + label, decoder.extreme(payload, true));
    }

    private static List<BytesRef> slots(BytesRef... slots) {
        return Arrays.asList(slots);
    }

    /**
     * Encodes {@code slots} through a builder of their own, so the payload owns its bytes and outlives any other builder these tests
     * drive. The reference every assertion here compares against.
     */
    private static BytesRef encode(List<BytesRef> slots) {
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(slots));
    }

}
