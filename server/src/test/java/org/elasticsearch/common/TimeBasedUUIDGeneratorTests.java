/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.HashSet;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class TimeBasedUUIDGeneratorTests extends ESTestCase {

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
        // Simulate a clock that resets itself after reaching a threshold.
        final Supplier<Long> unreliableClock = new TestClockResetTimestampSupplier(
            Instant.now(),
            1,
            50,
            ChronoUnit.MILLIS,
            Instant.now().plus(100, ChronoUnit.MILLIS)
        );
        final UUIDGenerator generator = createGenerator(unreliableClock, () -> 0, new TestRandomMacAddressSupplier());

        final Set<String> beforeReset = generateUUIDs(generator, 5_000);
        final Set<String> afterReset = generateUUIDs(generator, 5_000);

        // Ensure all UUIDs are unique, even after the clock resets.
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
        final UUIDGenerator generator = createKOrderedGenerator(
            () -> Instant.now().toEpochMilli(),
            () -> 0x00FF_FFFF - 10,
            new TestRandomMacAddressSupplier()
        );
        final Set<String> uuids = generateUUIDs(generator, 1000);

        // The assumption here is that our system will not generate more than 1000 UUIDs within the same millisecond.
        // The sequence ID is set close to its max value (0x00FF_FFFF) to quickly trigger an overflow.
        // However, since we are generating only 1000 UUIDs, the timestamp is expected to change at least once,
        // ensuring uniqueness even if the sequence ID wraps around.
        assertEquals(1000, uuids.size());
    }

    public void testUUIDEncodingDecoding() {
        testUUIDEncodingDecodingHelper(
            Instant.parse("2024-11-13T10:12:43Z").toEpochMilli(),
            12345,
            new TestRandomMacAddressSupplier().get()
        );
    }

    public void testUUIDEncodingDecodingWithRandomValues() {
        testUUIDEncodingDecodingHelper(
            randomInstantBetween(Instant.now().minus(1, ChronoUnit.DAYS), Instant.now()).toEpochMilli(),
            randomIntBetween(0, 0x00FF_FFFF),
            new TestRandomMacAddressSupplier().get()
        );
    }

    public void testUUIDEncodingDecodingWithHash() {
        int hash = randomInt();
        byte[] decoded = Base64.getUrlDecoder().decode(UUIDs.base64TimeBasedKOrderedUUIDWithHash(OptionalInt.of(hash)));
        assertEquals(hash, ByteUtils.readIntLE(decoded, decoded.length - 9));
    }

    private void testUUIDEncodingDecodingHelper(final long timestamp, final int sequenceId, final byte[] macAddress) {
        final TestTimeBasedKOrderedUUIDDecoder decoder = new TestTimeBasedKOrderedUUIDDecoder(
            createKOrderedGenerator(() -> timestamp, () -> sequenceId, () -> macAddress).getBase64UUID()
        );

        // The sequence ID is incremented by 1 when generating the UUID.
        assertEquals("Sequence ID does not match", sequenceId + 1, decoder.decodeSequenceId());
        // Truncate the timestamp to milliseconds to match the UUID generation granularity.
        assertEquals(
            "Timestamp does not match",
            Instant.ofEpochMilli(timestamp).truncatedTo(ChronoUnit.MILLIS),
            Instant.ofEpochMilli(decoder.decodeTimestamp()).truncatedTo(ChronoUnit.MILLIS)
        );
        assertArrayEquals("MAC address does not match", macAddress, decoder.decodeMacAddress());
    }

    private void assertUUIDUniqueness(final UUIDGenerator generator, final int count) {
        assertEquals(count, generateUUIDs(generator, count).size());
    }

    private Set<String> generateUUIDs(final UUIDGenerator generator, final int count) {
        return IntStream.range(0, count).mapToObj(i -> generator.getBase64UUID()).collect(HashSet::new, Set::add, Set::addAll);
    }

    private void assertUUIDFormat(final UUIDGenerator generator, final int count) {
        IntStream.range(0, count).forEach(i -> {
            final String uuid = generator.getBase64UUID();
            assertNotNull(uuid);
            assertEquals(20, uuid.length());
            assertFalse(uuid.contains("+"));
            assertFalse(uuid.contains("/"));
            assertFalse(uuid.contains("="));
        });
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

    private static class TestRandomMacAddressSupplier implements Supplier<byte[]> {
        private final byte[] macAddress = new byte[] { randomByte(), randomByte(), randomByte(), randomByte(), randomByte(), randomByte() };

        @Override
        public byte[] get() {
            return macAddress;
        }
    }

    /**
     * A {@link Supplier} implementation that simulates a clock that can move forward or backward in time.
     * This supplier provides timestamps in milliseconds since the epoch, adjusting based on a given delta
     * until a reset threshold is reached. After crossing the threshold, the timestamp moves backwards by a reset delta.
     */
    private static class TestClockResetTimestampSupplier implements Supplier<Long> {
        private Instant currentTime;
        private final long delta;
        private final long resetDelta;
        private final ChronoUnit unit;
        private final Instant resetThreshold;

        /**
         * Constructs a new {@link TestClockResetTimestampSupplier}.
         *
         * @param startTime       The initial starting time.
         * @param delta           The amount of time to add to the current time in each forward step.
         * @param resetDelta      The amount of time to subtract once the reset threshold is reached.
         * @param unit            The unit of time for both delta and resetDelta.
         * @param resetThreshold  The threshold after which the time is reset backwards.
         */
        TestClockResetTimestampSupplier(
            final Instant startTime,
            final long delta,
            final long resetDelta,
            final ChronoUnit unit,
            final Instant resetThreshold
        ) {
            this.currentTime = startTime;
            this.delta = delta;
            this.resetDelta = resetDelta;
            this.unit = unit;
            this.resetThreshold = resetThreshold;
        }

        /**
         * Provides the next timestamp in milliseconds since the epoch.
         * If the current time is before the reset threshold, it advances the time by the delta.
         * Otherwise, it subtracts the reset delta.
         *
         * @return The current time in milliseconds since the epoch.
         */
        @Override
        public Long get() {
            if (currentTime.isBefore(resetThreshold)) {
                currentTime = currentTime.plus(delta, unit);
            } else {
                currentTime = currentTime.minus(resetDelta, unit);
            }
            return currentTime.toEpochMilli();
        }
    }

    /**
     * A utility class to decode the K-ordered UUID extracting the original timestamp, MAC address and sequence ID.
     */
    private static class TestTimeBasedKOrderedUUIDDecoder {

        private final byte[] decodedBytes;

        /**
         * Constructs a new {@link TestTimeBasedKOrderedUUIDDecoder} using a base64-encoded UUID string.
         *
         * @param base64UUID The base64-encoded UUID string to decode.
         */
        TestTimeBasedKOrderedUUIDDecoder(final String base64UUID) {
            this.decodedBytes = Base64.getUrlDecoder().decode(base64UUID);
        }

        /**
         * Decodes the timestamp from the UUID using the following bytes:
         * 0 (most significant), 1, 2, 3, 11, 13 (least significant).
         *
         * @return The decoded timestamp in milliseconds.
         */
        public long decodeTimestamp() {
            return ((long) (decodedBytes[0] & 0xFF) << 40) | ((long) (decodedBytes[1] & 0xFF) << 32) | ((long) (decodedBytes[2] & 0xFF)
                << 24) | ((long) (decodedBytes[3] & 0xFF) << 16) | ((long) (decodedBytes[11] & 0xFF) << 8) | (decodedBytes[13] & 0xFF);
        }

        /**
         * Decodes the MAC address from the UUID using bytes 4 to 9.
         *
         * @return The decoded MAC address as a byte array.
         */
        public byte[] decodeMacAddress() {
            byte[] macAddress = new byte[6];
            System.arraycopy(decodedBytes, 4, macAddress, 0, 6);
            return macAddress;
        }

        /**
         * Decodes the sequence ID from the UUID using bytes:
         * 10 (most significant), 12 (middle), 14 (least significant).
         *
         * @return The decoded sequence ID.
         */
        public int decodeSequenceId() {
            return ((decodedBytes[10] & 0xFF) << 16) | ((decodedBytes[12] & 0xFF) << 8) | (decodedBytes[14] & 0xFF);
        }
    }
}
