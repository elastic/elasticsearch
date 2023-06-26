/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.util.ByteUtils;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class CompositeBytesReferenceTests extends AbstractBytesReferenceTestCase {

    @Override
    protected BytesReference newBytesReference(int length) {
        return newBytesReferenceWithOffsetOfZero(length);
    }

    @Override
    protected BytesReference newBytesReferenceWithOffsetOfZero(int length) {
        // we know bytes stream output always creates a paged bytes reference, we use it to create randomized content
        List<BytesReference> referenceList = newRefList(length);
        BytesReference ref = CompositeBytesReference.of(referenceList.toArray(new BytesReference[0]));
        assertEquals(length, ref.length());
        return ref;
    }

    private List<BytesReference> newRefList(int length) {
        int emptySlices = between(0, 10);
        List<BytesReference> referenceList = new ArrayList<>();
        for (int i = 0; i < length;) {
            int remaining = length - i;
            int sliceLength = randomIntBetween(emptySlices > 0 ? 0 : 1, remaining);
            if (sliceLength == 0) {
                emptySlices -= 1;
                referenceList.add(BytesArray.EMPTY);
                continue;
            }
            ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(sliceLength, bigarrays);
            for (int j = 0; j < sliceLength; j++) {
                out.writeByte((byte) random().nextInt(1 << 8));
            }
            assertEquals(sliceLength, out.size());
            referenceList.add(out.bytes());
            i += sliceLength;
        }
        while (emptySlices > 0) {
            emptySlices -= 1;
            referenceList.add(BytesArray.EMPTY);
        }
        return referenceList;
    }

    public void testCompositeBuffer() throws IOException {
        List<BytesReference> referenceList = newRefList(randomIntBetween(0, PAGE_SIZE * 2));
        BytesReference ref = CompositeBytesReference.of(referenceList.toArray(new BytesReference[0]));
        BytesRefIterator iterator = ref.iterator();
        BytesRefBuilder builder = new BytesRefBuilder();

        for (BytesReference reference : referenceList) {
            BytesRefIterator innerIter = reference.iterator(); // sometimes we have a paged ref - pull an iter and walk all pages!
            BytesRef scratch;
            while ((scratch = innerIter.next()) != null) {
                BytesRef next = iterator.next();
                assertNotNull(next);
                assertEquals(next, scratch);
                builder.append(next);
            }

        }
        assertNull(iterator.next());

        int offset = 0;
        for (BytesReference reference : referenceList) {
            assertEquals(reference, ref.slice(offset, reference.length()));
            int probes = randomIntBetween(Math.min(10, reference.length()), reference.length());
            for (int i = 0; i < probes; i++) {
                int index = randomIntBetween(0, reference.length() - 1);
                assertEquals(ref.get(offset + index), reference.get(index));
            }
            offset += reference.length();
        }

        BytesArray array = new BytesArray(builder.toBytesRef());
        assertEquals(array, ref);
        assertEquals(array.hashCode(), ref.hashCode());

        BytesStreamOutput output = new BytesStreamOutput();
        ref.writeTo(output);
        assertEquals(array, output.bytes());
    }

    @Override
    public void testToBytesRefSharedPage() throws IOException {
        // CompositeBytesReference doesn't share pages
    }

    @Override
    public void testSliceArrayOffset() throws IOException {
        // the assertions in this test only work on no-composite buffers
    }

    @Override
    public void testSliceToBytesRef() throws IOException {
        // CompositeBytesReference shifts offsets
    }

    public void testSliceIsNotCompositeIfMatchesSingleSubSlice() {
        BytesReference bytesRef = CompositeBytesReference.of(
            new BytesArray(new byte[12]),
            new BytesArray(new byte[15]),
            new BytesArray(new byte[13])
        );

        // Slices that cross boundaries are composite too
        assertThat(bytesRef.slice(5, 8), Matchers.instanceOf(CompositeBytesReference.class));

        // But not slices that cover a single sub reference
        assertThat(bytesRef.slice(13, 10), Matchers.not(Matchers.instanceOf(CompositeBytesReference.class))); // strictly within sub
        assertThat(bytesRef.slice(12, 15), Matchers.not(Matchers.instanceOf(CompositeBytesReference.class))); // equal to sub
    }

    public void testForbidsLengthOverflow() {
        final List<BytesReference> referenceList = newRefList(randomIntBetween(0, PAGE_SIZE * 2));
        final int length = referenceList.stream().mapToInt(BytesReference::length).sum();
        if (length == 0) {
            final int firstExtraBufferLength = between(1, Integer.MAX_VALUE);
            referenceList.add(new ZeroBytesReference(firstExtraBufferLength));
            referenceList.add(new ZeroBytesReference(Integer.MAX_VALUE - firstExtraBufferLength + 1));
        } else {
            referenceList.add(new ZeroBytesReference(Integer.MAX_VALUE - length + 1));
        }
        Randomness.shuffle(referenceList);
        final BytesReference[] referenceArray = referenceList.toArray(new BytesReference[0]);
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> CompositeBytesReference.of(referenceArray)).getMessage(),
            equalTo("CompositeBytesReference cannot hold more than 2GB")
        );
    }

    public void testGetIntLE() {
        BytesReference[] refs = new BytesReference[] {
            new BytesArray(new byte[] { 0x12, 0x10, 0x12, 0x00 }),
            new BytesArray(new byte[] { 0x01, 0x02, 0x03, 0x04 }) };
        BytesReference comp = CompositeBytesReference.of(refs);
        assertThat(comp.getIntLE(0), equalTo(0x00121012));
        assertThat(comp.getIntLE(1), equalTo(0x01001210));
        assertThat(comp.getIntLE(2), equalTo(0x02010012));
        assertThat(comp.getIntLE(3), equalTo(0x03020100));
        assertThat(comp.getIntLE(4), equalTo(0x04030201));
        // The jvm can optimize throwing ArrayIndexOutOfBoundsException by reusing the same exception,
        // but these reused exceptions have no message or stack trace. This sometimes happens when running this test case.
        // We can assert the exception message if -XX:-OmitStackTraceInFastThrow is set in gradle test task.
        expectThrows(ArrayIndexOutOfBoundsException.class, () -> comp.getIntLE(5));
    }

    public void testGetDoubleLE() {
        // first double = 1.2, second double = 1.4, third double = 1.6
        // tag::noformat
        byte[] data = new byte[] {
            0x33, 0x33, 0x33, 0x33, 0x33, 0x33, -0xD, 0x3F,
            0x66, 0x66, 0x66, 0x66, 0x66, 0x66, -0xA, 0x3F,
            -0x66, -0x67, -0x67, -0x67, -0x67, -0x67, -0x7, 0x3F};
        // end::noformat

        List<BytesReference> refs = new ArrayList<>();
        int bytesPerChunk = randomFrom(4, 16);
        for (int offset = 0; offset < data.length; offset += bytesPerChunk) {
            int length = Math.min(bytesPerChunk, data.length - offset);
            refs.add(new BytesArray(data, offset, length));
        }
        BytesReference comp = CompositeBytesReference.of(refs.toArray(BytesReference[]::new));
        assertThat(comp.getDoubleLE(0), equalTo(1.2));
        assertThat(comp.getDoubleLE(8), equalTo(1.4));
        assertThat(comp.getDoubleLE(16), equalTo(1.6));
        // The jvm can optimize throwing ArrayIndexOutOfBoundsException by reusing the same exception,
        // but these reused exceptions have no message or stack trace. This sometimes happens when running this test case.
        // We can assert the exception message if -XX:-OmitStackTraceInFastThrow is set in gradle test task.
        expectThrows(IndexOutOfBoundsException.class, () -> comp.getDoubleLE(17));
    }

    public void testGetLongLE() {
        // first long = 2, second long = 44, third long = 512
        // tag::noformat
        byte[] data = new byte[] {
            0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
            0x2C, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
            0x0, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0};
        // end::noformat

        byte[] d = new byte[8];
        ByteUtils.writeLongLE(2, d, 0);

        List<BytesReference> refs = new ArrayList<>();
        int bytesPerChunk = randomFrom(4, 16);
        for (int offset = 0; offset < data.length; offset += bytesPerChunk) {
            int length = Math.min(bytesPerChunk, data.length - offset);
            refs.add(new BytesArray(data, offset, length));
        }
        BytesReference comp = CompositeBytesReference.of(refs.toArray(BytesReference[]::new));
        assertThat(comp.getLongLE(0), equalTo(2L));
        assertThat(comp.getLongLE(8), equalTo(44L));
        assertThat(comp.getLongLE(16), equalTo(512L));
        expectThrows(IndexOutOfBoundsException.class, () -> comp.getLongLE(17));
    }
}
