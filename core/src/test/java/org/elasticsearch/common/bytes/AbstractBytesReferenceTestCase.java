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
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

public abstract class AbstractBytesReferenceTestCase extends ESTestCase {

    protected static final int PAGE_SIZE = BigArrays.BYTE_PAGE_SIZE;
    protected final BigArrays bigarrays = new BigArrays(null, new NoneCircuitBreakerService(), false);

    public void testGet() throws IOException {
        int length = randomIntBetween(1, PAGE_SIZE * 3);
        BytesReference pbr = newBytesReference(length);
        int sliceOffset = randomIntBetween(0, length / 2);
        int sliceLength = Math.max(1, length - sliceOffset - 1);
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        assertEquals(pbr.get(sliceOffset), slice.get(0));
        assertEquals(pbr.get(sliceOffset + sliceLength - 1), slice.get(sliceLength - 1));
    }

    public void testLength() throws IOException {
        int[] sizes = {0, randomInt(PAGE_SIZE), PAGE_SIZE, randomInt(PAGE_SIZE * 3)};

        for (int i = 0; i < sizes.length; i++) {
            BytesReference pbr = newBytesReference(sizes[i]);
            assertEquals(sizes[i], pbr.length());
        }
    }

    public void testSlice() throws IOException {
        int length = randomInt(PAGE_SIZE * 3);
        BytesReference pbr = newBytesReference(length);
        int sliceOffset = randomIntBetween(0, length / 2);
        int sliceLength = Math.max(0, length - sliceOffset - 1);
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        assertEquals(sliceLength, slice.length());

        if (slice.hasArray()) {
            assertEquals(sliceOffset, slice.arrayOffset());
        } else {
           expectThrows(IllegalStateException.class, () ->
                slice.arrayOffset());
        }
    }

    public void testStreamInput() throws IOException {
        int length = randomIntBetween(10, scaledRandomIntBetween(PAGE_SIZE * 2, PAGE_SIZE * 20));
        BytesReference pbr = newBytesReference(length);
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
        random().nextBytes(origBuf);
        byte[] targetBuf = Arrays.copyOf(origBuf, origBuf.length);

        // bulk-read 0 bytes: must not modify buffer
        si.readBytes(targetBuf, 0, 0);
        assertEquals(origBuf[0], targetBuf[0]);
        si.reset();

        // read a few few bytes as ints
        int bytesToRead = randomIntBetween(1, length / 2);
        for (int i = 0; i < bytesToRead; i++) {
            int b = si.read();
            assertEquals(pbr.get(i) & 0xff, b);
        }
        si.reset();

        // bulk-read all
        si.readFully(targetBuf);
        assertArrayEquals(pbr.toBytes(), targetBuf);

        // continuing to read should now fail with EOFException
        try {
            si.readByte();
            fail("expected EOF");
        } catch (EOFException | IndexOutOfBoundsException eof) {
            // yay
        }

        // try to read more than the stream contains
        si.reset();
        expectThrows(IndexOutOfBoundsException.class, () ->
            si.readBytes(targetBuf, 0, length * 2));
    }

    public void testStreamInputBulkReadWithOffset() throws IOException {
        final int length = randomIntBetween(10, scaledRandomIntBetween(PAGE_SIZE * 2, PAGE_SIZE * 20));
        BytesReference pbr = newBytesReference(length);
        StreamInput si = pbr.streamInput();
        assertNotNull(si);

        // read a bunch of single bytes one by one
        int offset = randomIntBetween(1, length / 2);
        for (int i = 0; i < offset; i++) {
            assertEquals(si.available(), length - i);
            assertEquals(pbr.get(i), si.readByte());
        }

        // now do NOT reset the stream - keep the stream's offset!

        // buffer to compare remaining bytes against bulk read
        byte[] pbrBytesWithOffset = Arrays.copyOfRange(pbr.toBytes(), offset, length);
        // randomized target buffer to ensure no stale slots
        byte[] targetBytes = new byte[pbrBytesWithOffset.length];
        random().nextBytes(targetBytes);

        // bulk-read all
        si.readFully(targetBytes);
        assertArrayEquals(pbrBytesWithOffset, targetBytes);
        assertEquals(si.available(), 0);
    }

    public void testRandomReads() throws IOException {
        int length = randomIntBetween(10, scaledRandomIntBetween(PAGE_SIZE * 2, PAGE_SIZE * 20));
        BytesReference pbr = newBytesReference(length);
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
        BytesReference pbr = newBytesReference(length);

        // test stream input over slice (upper half of original)
        int sliceOffset = randomIntBetween(1, length / 2);
        int sliceLength = length - sliceOffset;
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        StreamInput sliceInput = slice.streamInput();
        assertEquals(sliceInput.available(), sliceLength);

        // single reads
        assertEquals(slice.get(0), sliceInput.readByte());
        assertEquals(slice.get(1), sliceInput.readByte());
        assertEquals(slice.get(2), sliceInput.readByte());
        assertEquals(sliceInput.available(), sliceLength - 3);

        // reset the slice stream for bulk reading
        sliceInput.reset();
        assertEquals(sliceInput.available(), sliceLength);

        // bulk read
        byte[] sliceBytes = new byte[sliceLength];
        sliceInput.readFully(sliceBytes);
        assertEquals(sliceInput.available(), 0);

        // compare slice content with upper half of original
        byte[] pbrSliceBytes = Arrays.copyOfRange(pbr.toBytes(), sliceOffset, length);
        assertArrayEquals(pbrSliceBytes, sliceBytes);

        // compare slice bytes with bytes read from slice via streamInput :D
        byte[] sliceToBytes = slice.toBytes();
        assertEquals(sliceBytes.length, sliceToBytes.length);
        assertArrayEquals(sliceBytes, sliceToBytes);

        sliceInput.reset();
        assertEquals(sliceInput.available(), sliceLength);
        byte[] buffer = new byte[sliceLength + scaledRandomIntBetween(1, 100)];
        int offset = scaledRandomIntBetween(0, Math.max(1, buffer.length - sliceLength - 1));
        int read = sliceInput.read(buffer, offset, sliceLength / 2);
        assertEquals(sliceInput.available(), sliceLength - read);
        sliceInput.read(buffer, offset + read, sliceLength - read);
        assertArrayEquals(sliceBytes, Arrays.copyOfRange(buffer, offset, offset + sliceLength));
        assertEquals(sliceInput.available(), 0);
    }

    public void testWriteToOutputStream() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * 4);
        BytesReference pbr = newBytesReference(length);
        BytesStreamOutput out = new BytesStreamOutput();
        pbr.writeTo(out);
        assertEquals(pbr.length(), out.size());
        assertArrayEquals(pbr.toBytes(), out.bytes().toBytes());
        out.close();
    }

    public void testSliceWriteToOutputStream() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 5));
        BytesReference pbr = newBytesReference(length);
        int sliceOffset = randomIntBetween(1, length / 2);
        int sliceLength = length - sliceOffset;
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        BytesStreamOutput sliceOut = new BytesStreamOutput(sliceLength);
        slice.writeTo(sliceOut);
        assertEquals(slice.length(), sliceOut.size());
        assertArrayEquals(slice.toBytes(), sliceOut.bytes().toBytes());
        sliceOut.close();
    }

    public void testToBytes() throws IOException {
        int[] sizes = {0, randomInt(PAGE_SIZE), PAGE_SIZE, randomIntBetween(2, PAGE_SIZE * randomIntBetween(2, 5))};
        for (int i = 0; i < sizes.length; i++) {
            BytesReference pbr = newBytesReference(sizes[i]);
            byte[] bytes = pbr.toBytes();
            assertEquals(sizes[i], bytes.length);
        }
    }

    public void testToBytesArraySharedPage() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE);
        BytesReference pbr = newBytesReference(length);
        BytesArray ba = pbr.toBytesArray();
        BytesArray ba2 = pbr.toBytesArray();
        assertNotNull(ba);
        assertNotNull(ba2);
        assertEquals(pbr.length(), ba.length());
        assertEquals(ba.length(), ba2.length());
        // single-page optimization
        assertSame(ba.array(), ba2.array());
    }

    public void testToBytesArrayMaterializedPages() throws IOException {
        // we need a length != (n * pagesize) to avoid page sharing at boundaries
        int length = 0;
        while ((length % PAGE_SIZE) == 0) {
            length = randomIntBetween(PAGE_SIZE, PAGE_SIZE * randomIntBetween(2, 5));
        }
        BytesReference pbr = newBytesReference(length);
        BytesArray ba = pbr.toBytesArray();
        BytesArray ba2 = pbr.toBytesArray();
        assertNotNull(ba);
        assertNotNull(ba2);
        assertEquals(pbr.length(), ba.length());
        assertEquals(ba.length(), ba2.length());
    }

    public void testCopyBytesArray() throws IOException {
        // small PBR which would normally share the first page
        int length = randomIntBetween(10, PAGE_SIZE);
        BytesReference pbr = newBytesReference(length);
        BytesArray ba = pbr.copyBytesArray();
        BytesArray ba2 = pbr.copyBytesArray();
        assertNotNull(ba);
        assertNotSame(ba, ba2);
        assertNotSame(ba.array(), ba2.array());
    }

    public void testSliceCopyBytesArray() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 8));
        BytesReference pbr = newBytesReference(length);
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

    public void testEmptyToBytesRefIterator() throws IOException {
        BytesReference pbr = newBytesReference(0);
        assertNull(pbr.iterator().next());
    }

    public void testIterator() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 8));
        BytesReference pbr = newBytesReference(length);
        BytesRefIterator iterator = pbr.iterator();
        BytesRef ref;
        BytesRefBuilder builder = new BytesRefBuilder();
        while((ref = iterator.next()) != null) {
            builder.append(ref);
        }
        assertArrayEquals(pbr.toBytes(), BytesRef.deepCopyOf(builder.toBytesRef()).bytes);
    }

    public void testSliceIterator() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 8));
        BytesReference pbr = newBytesReference(length);
        int sliceOffset = randomIntBetween(0, pbr.length());
        int sliceLength = randomIntBetween(pbr.length() - sliceOffset, pbr.length() - sliceOffset);
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        BytesRefIterator iterator = slice.iterator();
        BytesRef ref = null;
        BytesRefBuilder builder = new BytesRefBuilder();
        while((ref = iterator.next()) != null) {
            builder.append(ref);
        }
        assertArrayEquals(slice.toBytes(), BytesRef.deepCopyOf(builder.toBytesRef()).bytes);
    }

    public void testIteratorRandom() throws IOException {
        int length = randomIntBetween(10, PAGE_SIZE * randomIntBetween(2, 8));
        BytesReference pbr = newBytesReference(length);
        if (randomBoolean()) {
            int sliceOffset = randomIntBetween(0, pbr.length());
            int sliceLength = randomIntBetween(pbr.length() - sliceOffset, pbr.length() - sliceOffset);
            pbr = pbr.slice(sliceOffset, sliceLength);
        }

        if (randomBoolean()) {
            pbr = pbr.toBytesArray();
        }
        BytesRefIterator iterator = pbr.iterator();
        BytesRef ref = null;
        BytesRefBuilder builder = new BytesRefBuilder();
        while((ref = iterator.next()) != null) {
            builder.append(ref);
        }
        assertArrayEquals(pbr.toBytes(), BytesRef.deepCopyOf(builder.toBytesRef()).bytes);
    }

    public void testArray() throws IOException {
        int[] sizes = {0, randomInt(PAGE_SIZE), PAGE_SIZE, randomIntBetween(2, PAGE_SIZE * randomIntBetween(2, 5))};

        for (int i = 0; i < sizes.length; i++) {
            BytesReference pbr = newBytesReference(sizes[i]);
            byte[] array = pbr.array();
            assertNotNull(array);
            assertEquals(sizes[i], array.length);
            assertSame(array, pbr.array());
        }
    }

    public void testArrayOffset() throws IOException {
        int length = randomInt(PAGE_SIZE * randomIntBetween(2, 5));
        BytesReference pbr = newBytesReference(length);
        if (pbr.hasArray()) {
            assertEquals(0, pbr.arrayOffset());
        } else {
            expectThrows(IllegalStateException.class, () ->
                pbr.arrayOffset());
        }
    }

    public void testSliceArrayOffset() throws IOException {
        int length = randomInt(PAGE_SIZE * randomIntBetween(2, 5));
        BytesReference pbr = newBytesReference(length);
        int sliceOffset = randomIntBetween(0, pbr.length());
        int sliceLength = randomIntBetween(pbr.length() - sliceOffset, pbr.length() - sliceOffset);
        BytesReference slice = pbr.slice(sliceOffset, sliceLength);
        if (slice.hasArray()) {
            assertEquals(sliceOffset, slice.arrayOffset());
        } else {
            expectThrows(IllegalStateException.class, () ->
                slice.arrayOffset());
        }
    }

    public void testToUtf8() throws IOException {
        // test empty
        BytesReference pbr = newBytesReference(0);
        assertEquals("", pbr.toUtf8());
        // TODO: good way to test?
    }

    public void testToBytesRef() throws IOException {
        int length = randomIntBetween(0, PAGE_SIZE);
        BytesReference pbr = newBytesReference(length);
        BytesRef ref = pbr.toBytesRef();
        assertNotNull(ref);
        assertEquals(pbr.arrayOffset(), ref.offset);
        assertEquals(pbr.length(), ref.length);
    }

    public void testSliceToBytesRef() throws IOException {
        int length = randomIntBetween(0, PAGE_SIZE);
        BytesReference pbr = newBytesReference(length);
        // get a BytesRef from a slice
        int sliceOffset = randomIntBetween(0, pbr.length());
        int sliceLength = randomIntBetween(pbr.length() - sliceOffset, pbr.length() - sliceOffset);
        BytesRef sliceRef = pbr.slice(sliceOffset, sliceLength).toBytesRef();
        // note that these are only true if we have <= than a page, otherwise offset/length are shifted
        assertEquals(sliceOffset, sliceRef.offset);
        assertEquals(sliceLength, sliceRef.length);
    }

    public void testCopyBytesRef() throws IOException {
        int length = randomIntBetween(0, PAGE_SIZE * randomIntBetween(2, 5));
        BytesReference pbr = newBytesReference(length);
        BytesRef ref = pbr.copyBytesRef();
        assertNotNull(ref);
        assertEquals(pbr.length(), ref.length);
    }

    public void testHashCode() throws IOException {
        // empty content must have hash 1 (JDK compat)
        BytesReference pbr = newBytesReference(0);
        assertEquals(Arrays.hashCode(BytesRef.EMPTY_BYTES), pbr.hashCode());

        // test with content
        pbr = newBytesReference(randomIntBetween(0, PAGE_SIZE * randomIntBetween(2, 5)));
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

    public void testEqualsPeerClass() throws IOException {
        int length = randomIntBetween(100, PAGE_SIZE * randomIntBetween(2, 5));
        BytesReference pbr = newBytesReference(length);
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

    protected abstract BytesReference newBytesReference(int length) throws IOException;

}
