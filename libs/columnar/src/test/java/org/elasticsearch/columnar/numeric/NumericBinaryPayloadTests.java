/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NumericBinaryPayloadTests extends ESTestCase {

    /** Slots built one at a time come back in order, nulls in place, and {@link NumericBinaryPayload#decode} keeps only the values. */
    public void testBuilderRoundTrip() throws IOException {
        final NumericBinaryPayload.Builder builder = new NumericBinaryPayload.Builder();
        final NumericBinaryPayload.Decoder decoder = new NumericBinaryPayload.Decoder();
        for (int iter = 0; iter < 100; iter++) {
            final List<Long> slots = randomSlots();
            builder.reset();
            for (Long slot : slots) {
                if (slot == null) {
                    builder.addNull();
                } else {
                    builder.addValue(slot);
                }
            }
            assertEquals(slots.size(), builder.slotCount());
            final BytesRef payload = BytesRef.deepCopyOf(builder.build());

            assertEquals(slots, readSlots(decoder, payload));
            assertEquals(slots.stream().filter(s -> s == null).count(), decoder.nullSlotCount());

            final long[][] values = { new long[1] };
            final int count = NumericBinaryPayload.decode(payload, values);
            assertEquals(slots.stream().filter(s -> s != null).toList(), toList(values[0], count));
        }
    }

    /**
     * {@link NumericBinaryPayload.Builder#encode} writes the same bytes as adding the values a slot at a time, and replaces
     * whatever the builder held before, nulls included.
     */
    public void testEncodeMatchesAddingValues() throws IOException {
        final long[] values = new long[between(0, 40)];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomLong();
        }
        final NumericBinaryPayload.Builder slotAtATime = new NumericBinaryPayload.Builder();
        for (long v : values) {
            slotAtATime.addValue(v);
        }
        final NumericBinaryPayload.Builder encoder = new NumericBinaryPayload.Builder();
        for (int i = 0; i < between(0, 20); i++) {
            encoder.addNull();
            encoder.addValue(randomLong());
        }
        final BytesRef encoded = encoder.encode(values, values.length);
        assertEquals(slotAtATime.build(), encoded);
        final NumericBinaryPayload.Decoder decoder = new NumericBinaryPayload.Decoder();
        assertEquals(values.length, decoder.reset(encoded));
        assertEquals(0, decoder.nullSlotCount());
    }

    /** An empty array is a payload of zero slots, not an absent one. */
    public void testEmptyArray() throws IOException {
        final BytesRef payload = new NumericBinaryPayload.Builder().build();
        assertTrue(payload.length > 0);
        assertEquals(0, new NumericBinaryPayload.Decoder().reset(payload));
        assertEquals(0, NumericBinaryPayload.decode(payload, new long[][] { new long[0] }));
    }

    /** Resetting clears the null bitmap, so a reused builder doesn't carry nulls into the next document. */
    public void testResetClearsNulls() throws IOException {
        final NumericBinaryPayload.Builder builder = new NumericBinaryPayload.Builder();
        for (int i = 0; i < 20; i++) {
            builder.addNull();
        }
        builder.build();
        builder.reset();
        for (int i = 0; i < 20; i++) {
            builder.addValue(i);
        }
        final NumericBinaryPayload.Decoder decoder = new NumericBinaryPayload.Decoder();
        decoder.reset(builder.build());
        assertEquals(0, decoder.nullSlotCount());
    }

    private static List<Long> randomSlots() {
        final int count = between(0, 70);
        final List<Long> slots = new ArrayList<>(count);
        final boolean withNulls = randomBoolean();
        for (int i = 0; i < count; i++) {
            slots.add(withNulls && rarely() ? null : randomLong());
        }
        return slots;
    }

    static List<Long> readSlots(NumericBinaryPayload.Decoder decoder, BytesRef payload) throws IOException {
        final int count = decoder.reset(payload);
        final List<Long> slots = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            slots.add(decoder.nextIsNull() ? null : decoder.nextValue());
        }
        return slots;
    }

    private static List<Long> toList(long[] values, int count) {
        return Arrays.stream(values, 0, count).boxed().toList();
    }
}
