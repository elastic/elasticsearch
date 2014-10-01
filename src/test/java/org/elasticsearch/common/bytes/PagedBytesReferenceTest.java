/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class PagedBytesReferenceTest extends ElasticsearchTestCase {

    private static final int PAGE_SIZE = BigArrays.BYTE_PAGE_SIZE;

    private BigArrays bigarrays;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        bigarrays = new BigArrays(ImmutableSettings.EMPTY, null, new NoneCircuitBreakerService());
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testGet() {
        int length = randomIntBetween(1, PAGE_SIZE * 3);
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        int sliceOffset = randomIntBetween(0, length / 2);
        int sliceLength = Math.max(1, length - sliceOffset - 1);
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        assertEquals(pbr.get(sliceOffset), slice.get(0));
        assertEquals(pbr.get(sliceOffset + sliceLength - 1), slice.get(sliceLength - 1));
    }

    public void testLength() {
        int[] sizes = {0, randomInt(PAGE_SIZE), PAGE_SIZE, randomInt(PAGE_SIZE * 3)};

        for (int i = 0; i < sizes.length; i++) {
            BytesReference pbr = getRandomizedPagedBytesReference(sizes[i]);
            assertEquals(sizes[i], pbr.length());
        }
    }

    public void testSlice() {
        int length = randomInt(PAGE_SIZE * 3);
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        int sliceOffset = randomIntBetween(0, length / 2);
        int sliceLength = Math.max(0, length - sliceOffset - 1);
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        assertEquals(sliceLength, slice.length());

        if (slice.hasArray()) {
            assertEquals(sliceOffset, slice.arrayOffset());
        } else {
            try {
                slice.arrayOffset();
                fail("expected IllegalStateException");
            } catch (IllegalStateException ise) {
                // expected
            }
        }
    }

    public void testStreamInput() throws IOException {
        int length = randomIntBetween(10, scaledRandomIntBetween(PAGE_SIZE * 2, PAGE_SIZE * 20));
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        StreamInput si = pbr.streamInput();
        assertNotNull(si);

        // read single bytes one by one
        assertEquals(pbr.get(0), si.readByte());
        assertEquals(pbr.get(1), si.readByte());
        assertEquals(pbr.get(2), si.readByte());

        // reset the stream for bulk reading
        si.reset();

        // buffer for bulk reads
        byte[] origBuf = new byte[length];
        getRandom().nextBytes(origBuf);
        byte[] targetBuf = Arrays.copyOf(origBuf, origBuf.length);

        // bulk-read 0 bytes: must not modify buffer
        si.readBytes(targetBuf, 0, 0);
        assertEquals(origBuf[0], targetBuf[0]);
        si.reset();

        // read a few few bytes as ints
        int bytesToRead = randomIntBetween(1, length / 2);
        for (int i = 0; i < bytesToRead; i++) {
            int b = si.read();
            assertEquals(pbr.get(i), b);
        }
        si.reset();

        // bulk-read all
        si.readFully(targetBuf);
        assertArrayEquals(pbr.toBytes(), targetBuf);

        // continuing to read should now fail with EOFException
        try {
            si.readByte();
            fail("expected EOF");
        } catch (EOFException eof) {
            // yay
        }

        // try to read more than the stream contains
        si.reset();
        try {
            si.readBytes(targetBuf, 0, length * 2);
            fail("expected IndexOutOfBoundsException: le > stream.length");
        } catch (IndexOutOfBoundsException ioob) {
            // expected
        }
    }

    public void testStreamInputBulkReadWithOffset() throws IOException {
        int length = randomIntBetween(10, scaledRandomIntBetween(PAGE_SIZE * 2, PAGE_SIZE * 20));
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        StreamInput si = pbr.streamInput();
        assertNotNull(si);

        // read a bunch of single bytes one by one
        int offset = randomIntBetween(1, length / 2);
        for (int i = 0; i < offset; i++) {
            assertEquals(pbr.get(i), si.readByte());
        }

        // now do NOT reset the stream - keep the stream's offset!

        // buffer to compare remaining bytes against bulk read
        byte[] pbrBytesWithOffset = Arrays.copyOfRange(pbr.toBytes(), offset, length);
        // randomized target buffer to ensure no stale slots
        byte[] targetBytes = new byte[pbrBytesWithOffset.length];
        getRandom().nextBytes(targetBytes);

        // bulk-read all
        si.readFully(targetBytes);
        assertArrayEquals(pbrBytesWithOffset, targetBytes);
    }

    public void testRandomReads() throws IOException {
        int length = randomIntBetween(10, scaledRandomIntBetween(PAGE_SIZE * 2, PAGE_SIZE * 20));
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        StreamInput streamInput = pbr.streamInput();
        BytesRefBuilder target = new BytesRefBuilder();
        while (target.length() < pbr.length()) {
            switch (randomIntBetween(0, 10)) {
                case 6:
                case 5:
                    target.append(new BytesRef(new byte[]{streamInput.readByte()}));
                    break;
                case 4:
                case 3:
                    BytesRef bytesRef = streamInput.readBytesRef(scaledRandomIntBetween(1, pbr.length() - target.length()));
                    target.append(bytesRef);
                    break;
                default:
                    byte[] buffer = new byte[scaledRandomIntBetween(1, pbr.length() - target.length())];
                    int offset = scaledRandomIntBetween(0, buffer.length - 1);
                    int read = streamInput.read(buffer, offset, buffer.length - offset);
                    target.append(new BytesRef(buffer, offset, read));
                    break;
            }
        }
        assertEquals(pbr.length(), target.length());
        BytesRef targetBytes = target.get();
        assertArrayEquals(pbr.toBytes(), Arrays.copyOfRange(targetBytes.bytes, targetBytes.offset, targetBytes.length));
    }

    public void testSliceStreamInput() throws IOException {
        int length = randomIntBetween(10, scaledRandomIntBetween(PAGE_SIZE * 2, PAGE_SIZE * 20));
        BytesReference pbr = getRandomizedPagedBytesReference(length);

        // test stream input over slice (upper half of original)
        int sliceOffset = randomIntBetween(1, length / 2);
        int sliceLength = length - sliceOffset;
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        StreamInput sliceInput = slice.streamInput();

        // single reads
        assertEquals(slice.get(0), sliceInput.readByte());
        assertEquals(slice.get(1), sliceInput.readByte());
        assertEquals(slice.get(2), sliceInput.readByte());

        // reset the slice stream for bulk reading
        sliceInput.reset();

        // bulk read
        byte[] sliceBytes = new byte[sliceLength];
        sliceInput.readFully(sliceBytes);

        // compare slice content with upper half of original
        byte[] pbrSliceBytes = Arrays.copyOfRange(pbr.toBytes(), sliceOffset, length);
        assertArrayEquals(pbrSliceBytes, sliceBytes);

        // compare slice bytes with bytes read from slice via streamInput :D
        byte[] sliceToBytes = slice.toBytes();
        assertEquals(sliceBytes.length, sliceToBytes.length);
        assertArrayEquals(sliceBytes, sliceToBytes);

        sliceInput.reset();
        byte[] buffer = new byte[sliceLength + scaledRandomIntBetween(1, 100)];
        int offset = scaledRandomIntBetween(0, Math.max(1, buffer.length - sliceLength - 1));
        int read = sliceInput.read(buffer, offset, sliceLength / 2);
        sliceInput.read(buffer, offset + read, sliceLength);
        assertArrayEquals(sliceBytes, Arrays.copyOfRange(buffer, offset, offset + sliceLength));
    }

    public void testWriteToOutputStream() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * 4);
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        BytesStreamOutput out = new BytesStreamOutput();
        pbr.writeTo(out);
        assertEquals(pbr.length(), out.size());
        assertArrayEquals(pbr.toBytes(), out.bytes().toBytes());
        out.close();
    }

    public void testWriteToChannel() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * 4);
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        File tFile = newTempFile();
        try (RandomAccessFile file = new RandomAccessFile(tFile, "rw")) {
            pbr.writeTo(file.getChannel());
            assertEquals(pbr.length(), file.length());
            assertArrayEquals(pbr.toBytes(), Streams.copyToByteArray(tFile));
        }
    }

    public void testSliceWriteToOutputStream() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 5));
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        int sliceOffset = randomIntBetween(1, length / 2);
        int sliceLength = length - sliceOffset;
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        BytesStreamOutput sliceOut = new BytesStreamOutput(sliceLength);
        slice.writeTo(sliceOut);
        assertEquals(slice.length(), sliceOut.size());
        assertArrayEquals(slice.toBytes(), sliceOut.bytes().toBytes());
        sliceOut.close();
    }

    public void testSliceWriteToChannel() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 5));
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        int sliceOffset = randomIntBetween(1, length / 2);
        int sliceLength = length - sliceOffset;
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        File tFile = newTempFile();
        try (RandomAccessFile file = new RandomAccessFile(tFile, "rw")) {
            slice.writeTo(file.getChannel());
            assertEquals(slice.length(), file.length());
            assertArrayEquals(slice.toBytes(), Streams.copyToByteArray(tFile));
        }
    }

    public void testToBytes() {
        int[] sizes = {0, randomInt(PAGE_SIZE), PAGE_SIZE, randomIntBetween(2, PAGE_SIZE * randomIntBetween(2, 5))};

        for (int i = 0; i < sizes.length; i++) {
            BytesReference pbr = getRandomizedPagedBytesReference(sizes[i]);
            byte[] bytes = pbr.toBytes();
            assertEquals(sizes[i], bytes.length);
            // verify that toBytes() is cheap for small payloads
            if (sizes[i] <= PAGE_SIZE) {
                assertSame(bytes, pbr.toBytes());
            } else {
                assertNotSame(bytes, pbr.toBytes());
            }
        }
    }

    public void testToBytesArraySharedPage() {
        int length = randomIntBetween(10, PAGE_SIZE);
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        BytesArray ba = pbr.toBytesArray();
        BytesArray ba2 = pbr.toBytesArray();
        assertNotNull(ba);
        assertNotNull(ba2);
        assertEquals(pbr.length(), ba.length());
        assertEquals(ba.length(), ba2.length());
        // single-page optimization
        assertSame(ba.array(), ba2.array());
    }

    public void testToBytesArrayMaterializedPages() {
        // we need a length != (n * pagesize) to avoid page sharing at boundaries
        int length = 0;
        while ((length % PAGE_SIZE) == 0) {
            length = randomIntBetween(PAGE_SIZE, PAGE_SIZE * randomIntBetween(2, 5));
        }
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        BytesArray ba = pbr.toBytesArray();
        BytesArray ba2 = pbr.toBytesArray();
        assertNotNull(ba);
        assertNotNull(ba2);
        assertEquals(pbr.length(), ba.length());
        assertEquals(ba.length(), ba2.length());
        // ensure no single-page optimization
        assertNotSame(ba.array(), ba2.array());
    }

    public void testCopyBytesArray() {
        // small PBR which would normally share the first page
        int length = randomIntBetween(10, PAGE_SIZE);
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        BytesArray ba = pbr.copyBytesArray();
        BytesArray ba2 = pbr.copyBytesArray();
        assertNotNull(ba);
        assertNotSame(ba, ba2);
        assertNotSame(ba.array(), ba2.array());
    }

    public void testSliceCopyBytesArray() {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 8));
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        int sliceOffset = randomIntBetween(0, pbr.length());
        int sliceLength = randomIntBetween(pbr.length() - sliceOffset, pbr.length() - sliceOffset);
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);

        BytesArray ba1 = slice.copyBytesArray();
        BytesArray ba2 = slice.copyBytesArray();
        assertNotNull(ba1);
        assertNotNull(ba2);
        assertNotSame(ba1.array(), ba2.array());
        assertArrayEquals(slice.toBytes(), ba1.array());
        assertArrayEquals(slice.toBytes(), ba2.array());
        assertArrayEquals(ba1.array(), ba2.array());
    }

    public void testToChannelBuffer() {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 8));
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        ChannelBuffer cb = pbr.toChannelBuffer();
        assertNotNull(cb);
        byte[] bufferBytes = new byte[length];
        cb.getBytes(0, bufferBytes);
        assertArrayEquals(pbr.toBytes(), bufferBytes);
    }

    public void testEmptyToChannelBuffer() {
        BytesReference pbr = getRandomizedPagedBytesReference(0);
        ChannelBuffer cb = pbr.toChannelBuffer();
        assertNotNull(cb);
        assertEquals(0, pbr.length());
        assertEquals(0, cb.capacity());
    }

    public void testSliceToChannelBuffer() {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 8));
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        int sliceOffset = randomIntBetween(0, pbr.length());
        int sliceLength = randomIntBetween(pbr.length() - sliceOffset, pbr.length() - sliceOffset);
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        ChannelBuffer cbSlice = slice.toChannelBuffer();
        assertNotNull(cbSlice);
        byte[] sliceBufferBytes = new byte[sliceLength];
        cbSlice.getBytes(0, sliceBufferBytes);
        assertArrayEquals(slice.toBytes(), sliceBufferBytes);
    }

    public void testHasArray() {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(1, 3));
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        // must return true for <= pagesize
        assertEquals(length <= PAGE_SIZE, pbr.hasArray());
    }

    public void testArray() {
        int[] sizes = {0, randomInt(PAGE_SIZE), PAGE_SIZE, randomIntBetween(2, PAGE_SIZE * randomIntBetween(2, 5))};

        for (int i = 0; i < sizes.length; i++) {
            BytesReference pbr = getRandomizedPagedBytesReference(sizes[i]);
            // verify that array() is cheap for small payloads
            if (sizes[i] <= PAGE_SIZE) {
                byte[] array = pbr.array();
                assertNotNull(array);
                assertEquals(sizes[i], array.length);
                assertSame(array, pbr.array());
            } else {
                try {
                    pbr.array();
                    fail("expected IllegalStateException");
                } catch (IllegalStateException isx) {
                    // expected
                }
            }
        }
    }

    public void testArrayOffset() {
        int length = randomInt(PAGE_SIZE * randomIntBetween(2, 5));
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        if (pbr.hasArray()) {
            assertEquals(0, pbr.arrayOffset());
        } else {
            try {
                pbr.arrayOffset();
                fail("expected IllegalStateException");
            } catch (IllegalStateException ise) {
                // expected
            }
        }
    }

    public void testSliceArrayOffset() {
        int length = randomInt(PAGE_SIZE * randomIntBetween(2, 5));
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        int sliceOffset = randomIntBetween(0, pbr.length());
        int sliceLength = randomIntBetween(pbr.length() - sliceOffset, pbr.length() - sliceOffset);
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        if (slice.hasArray()) {
            assertEquals(sliceOffset, slice.arrayOffset());
        } else {
            try {
                slice.arrayOffset();
                fail("expected IllegalStateException");
            } catch (IllegalStateException ise) {
                // expected
            }
        }
    }

    public void testToUtf8() throws IOException {
        // test empty
        BytesReference pbr = getRandomizedPagedBytesReference(0);
        assertEquals("", pbr.toUtf8());
        // TODO: good way to test?
    }

    public void testToBytesRef() {
        int length = randomIntBetween(0, PAGE_SIZE);
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        BytesRef ref = pbr.toBytesRef();
        assertNotNull(ref);
        assertEquals(pbr.arrayOffset(), ref.offset);
        assertEquals(pbr.length(), ref.length);
    }

    public void testSliceToBytesRef() {
        int length = randomIntBetween(0, PAGE_SIZE);
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        // get a BytesRef from a slice
        int sliceOffset = randomIntBetween(0, pbr.length());
        int sliceLength = randomIntBetween(pbr.length() - sliceOffset, pbr.length() - sliceOffset);
        BytesRef sliceRef = pbr.slice(sliceOffset, sliceLength).toBytesRef();
        // note that these are only true if we have <= than a page, otherwise offset/length are shifted
        assertEquals(sliceOffset, sliceRef.offset);
        assertEquals(sliceLength, sliceRef.length);
    }

    public void testCopyBytesRef() {
        int length = randomIntBetween(0, PAGE_SIZE * randomIntBetween(2, 5));
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        BytesRef ref = pbr.copyBytesRef();
        assertNotNull(ref);
        assertEquals(pbr.length(), ref.length);
    }

    public void testHashCode() {
        // empty content must have hash 1 (JDK compat)
        BytesReference pbr = getRandomizedPagedBytesReference(0);
        assertEquals(Arrays.hashCode(BytesRef.EMPTY_BYTES), pbr.hashCode());

        // test with content
        pbr = getRandomizedPagedBytesReference(randomIntBetween(0, PAGE_SIZE * randomIntBetween(2, 5)));
        int jdkHash = Arrays.hashCode(pbr.toBytes());
        int pbrHash = pbr.hashCode();
        assertEquals(jdkHash, pbrHash);

        // test hashes of slices
        int sliceFrom = randomIntBetween(0, pbr.length());
        int sliceLength = randomIntBetween(pbr.length() - sliceFrom, pbr.length() - sliceFrom);
        BytesReference slice = pbr.slice(sliceFrom, sliceLength);
        int sliceJdkHash = Arrays.hashCode(slice.toBytes());
        int sliceHash = slice.hashCode();
        assertEquals(sliceJdkHash, sliceHash);
    }

    public void testEquals() {
        int length = randomIntBetween(100, PAGE_SIZE * randomIntBetween(2, 5));
        ByteArray ba1 = bigarrays.newByteArray(length, false);
        ByteArray ba2 = bigarrays.newByteArray(length, false);

        // copy contents
        for (long i = 0; i < length; i++) {
            ba2.set(i, ba1.get(i));
        }

        // get refs & compare
        BytesReference pbr = new PagedBytesReference(bigarrays, ba1, length);
        BytesReference pbr2 = new PagedBytesReference(bigarrays, ba2, length);
        assertEquals(pbr, pbr2);
    }

    public void testEqualsPeerClass() {
        int length = randomIntBetween(100, PAGE_SIZE * randomIntBetween(2, 5));
        BytesReference pbr = getRandomizedPagedBytesReference(length);
        BytesReference ba = new BytesArray(pbr.toBytes());
        assertEquals(pbr, ba);
    }

    public void testSliceEquals() {
        int length = randomIntBetween(100, PAGE_SIZE * randomIntBetween(2, 5));
        ByteArray ba1 = bigarrays.newByteArray(length, false);
        BytesReference pbr = new PagedBytesReference(bigarrays, ba1, length);

        // test equality of slices
        int sliceFrom = randomIntBetween(0, pbr.length());
        int sliceLength = randomIntBetween(pbr.length() - sliceFrom, pbr.length() - sliceFrom);
        BytesReference slice1 = pbr.slice(sliceFrom, sliceLength);
        BytesReference slice2 = pbr.slice(sliceFrom, sliceLength);
        assertArrayEquals(slice1.toBytes(), slice2.toBytes());

        // test a slice with same offset but different length,
        // unless randomized testing gave us a 0-length slice.
        if (sliceLength > 0) {
            BytesReference slice3 = pbr.slice(sliceFrom, sliceLength / 2);
            assertFalse(Arrays.equals(slice1.toBytes(), slice3.toBytes()));
        }
    }

    private BytesReference getRandomizedPagedBytesReference(int length) {
        // we know bytes stream output always creates a paged bytes reference, we use it to create randomized content
        ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(length, bigarrays);
        try {
            for (int i = 0; i < length; i++) {
                out.writeByte((byte) getRandom().nextInt(1 << 8));
            }
        } catch (IOException e) {
            fail("should not happen " + e.getMessage());
        }
        assertThat(out.size(), Matchers.equalTo(length));
        BytesReference ref = out.bytes();
        assertThat(ref.length(), Matchers.equalTo(length));
        assertThat(ref, Matchers.instanceOf(PagedBytesReference.class));
        return ref;
    }

}
