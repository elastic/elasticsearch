/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.snapshots.blobstore;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;

public class SlicedInputStreamTests extends ESTestCase {

    public void testReadRandom() throws IOException {
        int parts = randomIntBetween(1, 20);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        int numWriteOps = scaledRandomIntBetween(1000, 10000);
        final long seed = randomLong();
        Random random = new Random(seed);
        for (int i = 0; i < numWriteOps; i++) {
            switch (random.nextInt(5)) {
                case 1 -> stream.write(random.nextInt(Byte.MAX_VALUE));
                default -> stream.write(randomBytes(random));
            }
        }

        final CheckClosedInputStream[] streams = new CheckClosedInputStream[parts];
        byte[] bytes = stream.toByteArray();
        int slice = bytes.length / parts;
        int offset = 0;
        int length;
        for (int i = 0; i < parts; i++) {
            length = i == parts - 1 ? bytes.length - offset : slice;
            streams[i] = new CheckClosedInputStream(new ByteArrayInputStream(bytes, offset, length));
            offset += length;
        }

        SlicedInputStream input = new SlicedInputStream(parts) {
            @Override
            protected InputStream openSlice(int slice) throws IOException {
                return streams[slice];
            }
        };
        random = new Random(seed);
        assertThat(input.available(), equalTo(streams[0].available()));
        for (int i = 0; i < numWriteOps; i++) {
            switch (random.nextInt(5)) {
                case 1 -> assertThat(random.nextInt(Byte.MAX_VALUE), equalTo(input.read()));
                default -> {
                    byte[] b = randomBytes(random);
                    byte[] buffer = new byte[b.length];
                    int read = readFully(input, buffer);
                    assertThat(b.length, equalTo(read));
                    assertArrayEquals(b, buffer);
                }
            }
        }

        assertThat(input.available(), equalTo(0));
        for (int i = 0; i < streams.length - 1; i++) {
            assertTrue(streams[i].closed);
        }
        input.close();

        for (int i = 0; i < streams.length; i++) {
            assertTrue(streams[i].closed);
        }
    }

    public void testSkip() throws IOException {
        final int slices = randomIntBetween(1, 20);
        final var bytes = randomByteArrayOfLength(randomIntBetween(1000, 10000));
        final int sliceSize = bytes.length / slices;

        final var streamsOpened = new ArrayList<CheckClosedInputStream>();
        SlicedInputStream input = new SlicedInputStream(slices) {
            @Override
            protected InputStream openSlice(int slice) throws IOException {
                final int sliceOffset = slice * sliceSize;
                final int length = slice == slices - 1 ? bytes.length - sliceOffset : sliceSize;
                final var stream = new CheckClosedInputStream(new ByteArrayInputStream(bytes, sliceOffset, length));
                streamsOpened.add(stream);
                return stream;
            }
        };

        // Skip up to a random point
        final int skip = randomIntBetween(0, bytes.length);
        input.skipNBytes(skip);

        // Read all remaining bytes, which should be the bytes from skip up to the end
        final int remainingBytes = bytes.length - skip;
        if (remainingBytes > 0) {
            final var remainingBytesRead = new byte[remainingBytes];
            input.readNBytes(remainingBytesRead, 0, remainingBytes);
            final var expectedRemainingBytes = Arrays.copyOfRange(bytes, skip, bytes.length);
            assertArrayEquals(expectedRemainingBytes, remainingBytesRead);
        }

        // Confirm we reached the end and close the stream
        assertThat(input.read(), equalTo(-1));
        input.close();
        streamsOpened.forEach(stream -> assertTrue(stream.closed));
    }

    public void testRandomMarkReset() throws IOException {
        final int slices = randomIntBetween(1, 20);
        final var bytes = randomByteArrayOfLength(randomIntBetween(1000, 10000));
        final int sliceSize = bytes.length / slices;

        final var streamsOpened = new ArrayList<CheckClosedInputStream>();
        SlicedInputStream input = new SlicedInputStream(slices) {
            @Override
            protected InputStream openSlice(int slice) throws IOException {
                final int sliceOffset = slice * sliceSize;
                final int length = slice == slices - 1 ? bytes.length - sliceOffset : sliceSize;
                final var stream = new CheckClosedInputStream(new ByteArrayInputStream(bytes, sliceOffset, length));
                streamsOpened.add(stream);
                return stream;
            }
        };

        // Read or skip up to a random point
        final int mark = randomIntBetween(0, bytes.length);
        if (mark > 0) {
            if (randomBoolean()) {
                final var bytesReadUntilMark = new byte[mark];
                input.readNBytes(bytesReadUntilMark, 0, mark);
                final var expectedBytesUntilMark = Arrays.copyOfRange(bytes, 0, mark);
                assertArrayEquals(expectedBytesUntilMark, bytesReadUntilMark);
            } else {
                input.skipNBytes(mark);
            }
        }

        // Reset should throw since there is no mark
        expectThrows(IOException.class, input::reset);

        // Mark
        input.mark(randomNonNegativeInt());
        int slicesOpenedAtMark = streamsOpened.size();

        // Read or skip up to another random point
        int moreBytes = randomIntBetween(0, bytes.length - mark);
        if (moreBytes > 0) {
            if (randomBoolean()) {
                final var moreBytesRead = new byte[moreBytes];
                input.readNBytes(moreBytesRead, 0, moreBytes);
                final var expectedMoreBytes = Arrays.copyOfRange(bytes, mark, mark + moreBytes);
                assertArrayEquals(expectedMoreBytes, moreBytesRead);
            } else {
                input.skipNBytes(moreBytes);
            }
        }

        // Randomly read to EOF
        if (randomBoolean()) {
            moreBytes += input.readAllBytes().length;
        }

        // Reset
        input.reset();
        int slicesOpenedAfterReset = streamsOpened.size();
        assert moreBytes > 0 || mark == 0 || mark == bytes.length || slicesOpenedAfterReset == slicesOpenedAtMark
            : "Reset at mark should not re-open slices";

        // Read all remaining bytes, which should be the bytes from mark up to the end
        final int remainingBytes = bytes.length - mark;
        if (remainingBytes > 0) {
            final var remainingBytesRead = new byte[remainingBytes];
            input.readNBytes(remainingBytesRead, 0, remainingBytes);
            final var expectedRemainingBytes = Arrays.copyOfRange(bytes, mark, bytes.length);
            assertArrayEquals(expectedRemainingBytes, remainingBytesRead);
        }

        // Confirm we reached the end and close the stream
        assertThat(input.read(), equalTo(-1));
        input.close();
        streamsOpened.forEach(stream -> assertTrue(stream.closed));
    }

    public void testMarkSkipResetInBigSlice() throws IOException {
        SlicedInputStream input = new SlicedInputStream(1) {
            @Override
            protected InputStream openSlice(int slice) throws IOException {
                assertThat(slice, equalTo(0));
                return new IncreasingBytesUnlimitedInputStream();
            }
        };

        // Buffer to use for reading a few KiB from a start byte of IncreasingBytesUnlimitedInputStream, to verify expected bytes.
        final byte[] buffer = new byte[Math.toIntExact(ByteSizeValue.ofKb(randomIntBetween(1, 8)).getBytes())];
        Consumer<Long> readAndAssert = (start) -> {
            try {
                final int read = input.read(buffer);
                assertThat("Unexpected number of bytes read", read, equalTo(buffer.length));
                for (int i = 0; i < read; i++) {
                    assertThat("Unexpected value for startByte=" + start + " and i=" + i, buffer[i], equalTo((byte) ((start + i) % 255)));
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };

        // Skip up to a random point that is larger than 2GiB so that the marked offset is larger than an int (ES-9639).
        final long mark = randomLongBetween(Integer.MAX_VALUE, Long.MAX_VALUE - buffer.length);
        input.skipNBytes(mark);

        // Mark
        input.mark(randomNonNegativeInt());

        // Skip a large amount of bytes
        final long skipTo = randomLongBetween(mark, Long.MAX_VALUE - buffer.length);
        input.skipNBytes(skipTo - mark);

        // Read a few KiB, asserting the bytes are what they are expected
        readAndAssert.accept(skipTo);

        // Reset
        input.reset();

        // Read a few KiB, asserting the bytes are what they are expected
        readAndAssert.accept(mark);
    }

    public void testMarkBeyondEOF() throws IOException {
        final int slices = randomIntBetween(1, 20);
        SlicedInputStream input = new SlicedInputStream(slices) {
            @Override
            protected InputStream openSlice(int slice) throws IOException {
                return new ByteArrayInputStream(new byte[] { 0 }, 0, 1);
            }
        };

        input.readAllBytes();
        assertThat(input.read(), equalTo(-1));
        input.mark(randomNonNegativeInt());
        assertThat(input.read(), equalTo(-1));
        input.reset();
        assertThat(input.read(), equalTo(-1));
    }

    public void testMarkResetClosedStream() throws IOException {
        final int slices = randomIntBetween(1, 20);
        SlicedInputStream input = new SlicedInputStream(slices) {
            @Override
            protected InputStream openSlice(int slice) throws IOException {
                return new ByteArrayInputStream(new byte[] { 0 }, 0, 1);
            }
        };

        input.skipNBytes(randomIntBetween(1, slices));
        input.mark(randomNonNegativeInt());
        input.close();
        // SlicedInputStream supports reading -1 after close without throwing
        assertThat(input.read(), equalTo(-1));
        expectThrows(IOException.class, input::reset);
        assertThat(input.read(), equalTo(-1));
        input.mark(randomNonNegativeInt());
        assertThat(input.read(), equalTo(-1));
    }

    public void testMarkResetUnsupportedStream() throws IOException {
        final int slices = randomIntBetween(1, 20);
        SlicedInputStream input = new SlicedInputStream(slices) {
            @Override
            protected InputStream openSlice(int slice) throws IOException {
                return new ByteArrayInputStream(new byte[] { 0 }, 0, 1);
            }

            @Override
            public boolean markSupported() {
                return false;
            }
        };

        input.mark(randomNonNegativeInt());
        expectThrows(IOException.class, input::reset);
        input.close();
    }

    public void testMarkResetZeroSlices() throws IOException {
        SlicedInputStream input = new SlicedInputStream(0) {
            @Override
            protected InputStream openSlice(int slice) throws IOException {
                throw new AssertionError("should not be called");
            }
        };

        if (randomBoolean()) {
            // randomly initialize the stream
            assertThat(input.read(), equalTo(-1));
        }

        input.mark(randomNonNegativeInt());
        input.reset();
        assertThat(input.read(), equalTo(-1));
        input.close();
    }

    private int readFully(InputStream stream, byte[] buffer) throws IOException {
        for (int i = 0; i < buffer.length;) {
            int read = stream.read(buffer, i, buffer.length - i);
            if (read == -1) {
                if (i == 0) {
                    return -1;
                } else {
                    return i;
                }
            }
            i += read;
        }
        return buffer.length;
    }

    private byte[] randomBytes(Random random) {
        int length = RandomNumbers.randomIntBetween(random, 1, 10);
        byte[] data = new byte[length];
        random.nextBytes(data);
        return data;
    }

    private static final class CheckClosedInputStream extends FilterInputStream {

        public boolean closed = false;

        CheckClosedInputStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }

    private static final class IncreasingBytesUnlimitedInputStream extends InputStream {
        long currentByte = 0;

        @Override
        public int read() throws IOException {
            return (int) (currentByte++ % 255);
        }

        @Override
        public long skip(long n) throws IOException {
            currentByte += n;
            return n;
        }
    }
}
