/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

public class TimeBasedUUIDGeneratorTests extends ESTestCase {

    private void assertUUIDUniqueness(final UUIDGenerator generator, int count) {
        assertEquals(count, generateUUIDs(generator, count).size());
    }

    private Set<String> generateUUIDs(final UUIDGenerator generator, int count) {
        final Set<String> uuids = new HashSet<>();
        for (int i = 0; i < count; i++) {
            uuids.add(generator.getBase64UUID());
        }
        return uuids;
    }

    private void assertUUIDFormat(UUIDGenerator generator, int count) {
        for (int i = 0; i < count; i++) {
            final String uuid = generator.getBase64UUID();
            assertNotNull(uuid);
            assertEquals(20, uuid.length());
            assertFalse(uuid.contains("+"));
            assertFalse(uuid.contains("/"));
            assertFalse(uuid.contains("="));
        }
    }

    private UUIDGenerator createGenerator(
        final Supplier<Long> timestampSupplier,
        final Supplier<Integer> sequenceIdSupplier,
        final Supplier<byte[]> macAddressSupplier
    ) {
        return new TimeBasedUUIDGenerator(timestampSupplier, sequenceIdSupplier, macAddressSupplier);
    }

    private UUIDGenerator createKOrderedGenerator(
        final Supplier<Long> timestampSupplier,
        final Supplier<Integer> sequenceIdSupplier,
        final Supplier<byte[]> macAddressSupplier
    ) {
        return new TimeBasedKOrderedUUIDGenerator(timestampSupplier, sequenceIdSupplier, macAddressSupplier);
    }

    public void testTimeBasedUUIDGeneration() {
        assertUUIDFormat(createGenerator(() -> Instant.now().toEpochMilli(), () -> 0, new TestRandomMacAddressSupplier()), 100_000);
    }

    public void testTimeBasedUUIDUniqueness() {
        assertUUIDUniqueness(createGenerator(() -> Instant.now().toEpochMilli(), () -> 0, new TestRandomMacAddressSupplier()), 100_000);
    }

    public void testTimeBasedUUIDSequenceOverflow() {
        // The assumption here is that our system will not generate more than 1000 UUIDs within the same millisecond.
        // The sequence ID is set close to its max value (0x00FF_FFFF) to quickly trigger an overflow.
        // However, since we are generating only 1000 UUIDs, the timestamp is expected to change at least once,
        // ensuring uniqueness even if the sequence ID wraps around.
        assertEquals(
            1000,
            generateUUIDs(
                createGenerator(() -> Instant.now().toEpochMilli(), () -> 0x00FF_FFFF - 10, new TestRandomMacAddressSupplier()),
                1000
            ).size()
        );
    }

    public void testTimeBasedUUIDClockReset() {
        final Supplier<Long> unreliableClock = new TestClockResetTimestampSupplier(
            Instant.now(),
            1,
            ChronoUnit.MILLIS,
            Instant.now().plus(100, ChronoUnit.MILLIS),
            50
        );
        final UUIDGenerator generator = createGenerator(unreliableClock, () -> 0, new TestRandomMacAddressSupplier());

        final Set<String> beforeReset = generateUUIDs(generator, 5_000);
        final Set<String> afterReset = generateUUIDs(generator, 5_000);

        assertEquals(5_000, beforeReset.size());
        assertEquals(5_000, afterReset.size());
        beforeReset.addAll(afterReset);
        assertEquals(10_000, beforeReset.size());
    }

    public void testKOrderedUUIDGeneration() {
        assertUUIDFormat(createKOrderedGenerator(() -> Instant.now().toEpochMilli(), () -> 0, new TestRandomMacAddressSupplier()), 100_000);
    }

    public void testKOrderedUUIDUniqueness() {
        assertUUIDUniqueness(
            createKOrderedGenerator(() -> Instant.now().toEpochMilli(), () -> 0, new TestRandomMacAddressSupplier()),
            100_000
        );
    }

    public void testKOrderedUUIDSequenceOverflow() {
        UUIDGenerator generator = createKOrderedGenerator(
            () -> Instant.now().toEpochMilli(),
            () -> 0x00FF_FFFF - 10,
            new TestRandomMacAddressSupplier()
        );
        Set<String> uuids = generateUUIDs(generator, 1000);

        // The assumption here is that our system will not generate more than 1000 UUIDs within the same millisecond.
        // The sequence ID is set close to its max value (0x00FF_FFFF) to quickly trigger an overflow.
        // However, since we are generating only 1000 UUIDs, the timestamp is expected to change at least once,
        // ensuring uniqueness even if the sequence ID wraps around.
        assertEquals(1000, uuids.size());
    }

    public void testKOrderedUUIDClockReset() {
        final Supplier<Long> unreliableClock = new TestClockResetTimestampSupplier(
            Instant.now(),
            1,
            ChronoUnit.MILLIS,
            Instant.now().plus(100, ChronoUnit.MILLIS),
            50
        );
        UUIDGenerator generator = createKOrderedGenerator(unreliableClock, () -> 0, new TestRandomMacAddressSupplier());

        final Set<String> beforeReset = generateUUIDs(generator, 5_000);
        final Set<String> afterReset = generateUUIDs(generator, 5_000);

        assertEquals(5_000, beforeReset.size());
        assertEquals(5_000, afterReset.size());
        beforeReset.addAll(afterReset);
        assertEquals(10_000, beforeReset.size());
    }

    private static class TestRandomMacAddressSupplier implements Supplier<byte[]> {
        private final byte[] macAddress = new byte[] { randomByte(), randomByte(), randomByte(), randomByte(), randomByte(), randomByte() };

        @Override
        public byte[] get() {
            return macAddress;
        }
    }

    private static class TestClockResetTimestampSupplier implements Supplier<Long> {
        private long currentMillis;
        private final long delta;
        private final Instant resetThreshold;
        private final long resetDelta;

        TestClockResetTimestampSupplier(Instant startTime, long delta, ChronoUnit unit, Instant resetThreshold, long resetDelta) {
            this.currentMillis = startTime.toEpochMilli();
            this.delta = delta;
            this.resetThreshold = resetThreshold;
            this.resetDelta = resetDelta;
        }

        @Override
        public Long get() {
            if (currentMillis >= resetThreshold.toEpochMilli()) {
                currentMillis -= resetDelta;
            } else {
                currentMillis += delta;
            }
            return currentMillis;
        }
    }
}
