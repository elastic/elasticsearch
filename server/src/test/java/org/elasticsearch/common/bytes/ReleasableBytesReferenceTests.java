/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.core.Releasable;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.hamcrest.CoreMatchers.equalTo;

public class ReleasableBytesReferenceTests extends AbstractBytesReferenceTestCase {

    @Override
    protected BytesReference newBytesReference(int length) throws IOException {
        return newBytesReferenceWithOffsetOfZero(length);
    }

    @Override
    protected BytesReference newBytesReferenceWithOffsetOfZero(int length) throws IOException {
        return newBytesReference(randomByteArrayOfLength(length));
    }

    @Override
    protected BytesReference newBytesReference(byte[] content) throws IOException {
        BytesReference delegate;
        String composite = "composite";
        String paged = "paged";
        String array = "array";
        String type = randomFrom(composite, paged, array);
        if (array.equals(type)) {
            final BytesStreamOutput out = new BytesStreamOutput(content.length);
            out.writeBytes(content, 0, content.length);
            assertThat(content.length, equalTo(out.size()));
            BytesArray ref = new BytesArray(out.bytes().toBytesRef().bytes, 0, content.length);
            assertThat(content.length, equalTo(ref.length()));
            assertThat(ref.length(), Matchers.equalTo(content.length));
            delegate = ref;
        } else if (paged.equals(type)) {
            ByteArray byteArray = bigarrays.newByteArray(content.length);
            byteArray.set(0, content, 0, content.length);
            assertThat(byteArray.size(), Matchers.equalTo((long) content.length));
            BytesReference ref = BytesReference.fromByteArray(byteArray, content.length);
            assertThat(ref.length(), Matchers.equalTo(content.length));
            delegate = ref;
        } else {
            assert composite.equals(type);
            List<BytesReference> referenceList = new ArrayList<>();
            for (int i = 0; i < content.length;) {
                int remaining = content.length - i;
                int sliceLength = randomIntBetween(1, remaining);
                ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(sliceLength, bigarrays);
                out.writeBytes(content, content.length - remaining, sliceLength);
                assertThat(sliceLength, equalTo(out.size()));
                referenceList.add(out.bytes());
                i += sliceLength;
            }
            BytesReference ref = CompositeBytesReference.of(referenceList.toArray(new BytesReference[0]));
            assertThat(content.length, equalTo(ref.length()));
            delegate = ref;
        }
        return ReleasableBytesReference.wrap(delegate);
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

    public void testAdoptPlainBytesArray() {
        byte[] payload = randomByteArrayOfLength(randomIntBetween(1, 100));
        BytesArray raw = new BytesArray(payload);
        ReleasableBytesReference adopted = ReleasableBytesReference.adopt(raw);
        assertEquals(payload.length, adopted.length());
        assertThat(adopted, equalBytes(raw));
        assertTrue(adopted.hasReferences());
        adopted.decRef();
    }

    public void testAdoptEmptyReturnsEmpty() {
        assertSame(ReleasableBytesReference.empty(), ReleasableBytesReference.adopt(BytesArray.EMPTY));
    }

    /**
     * {@link org.elasticsearch.common.io.stream.BytesStreamOutput#bytes()} returns {@link BytesArray} or {@link PagedBytesReference}, not
     * {@link ReleasableBytesReference}; exercise {@code adopt} → {@link ReleasableBytesReference#retain()} with a real {@link Releasable}.
     */
    public void testAdoptReleasableSingletonIsRetainOnSameInstance() {
        AtomicBoolean released = new AtomicBoolean(false);
        ReleasableBytesReference wrapped = new ReleasableBytesReference(
            new BytesArray(randomByteArrayOfLength(8)),
            (Releasable) () -> released.set(true)
        );
        ReleasableBytesReference adopted = ReleasableBytesReference.adopt(wrapped);
        assertSame(wrapped, adopted);
        assertFalse(released.get());
        adopted.decRef();
        assertTrue(wrapped.hasReferences());
        wrapped.decRef();
        assertTrue(released.get());
        assertFalse(wrapped.hasReferences());
    }

    /**
     * {@link ReleasableBytesReference#adopt} on a {@link CompositeBytesReference} retains each {@link ReleasableBytesReference} leaf and
     * bundles one {@link Releasable#close} for those retains. Closing {@code adopted} undoes only {@code adopt}'s {@code retain()} — the
     * original ref count from construction remains until explicitly {@link ReleasableBytesReference#decRef}'d.
     */
    public void testAdoptCompositeReleasesRetainedComponents() {
        byte[] b1 = randomByteArrayOfLength(randomIntBetween(1, 100));
        byte[] b2 = randomByteArrayOfLength(randomIntBetween(1, 100));
        AtomicInteger releaseCount = new AtomicInteger(0);
        ReleasableBytesReference p1 = new ReleasableBytesReference(new BytesArray(b1), () -> releaseCount.incrementAndGet());
        ReleasableBytesReference p2 = new ReleasableBytesReference(new BytesArray(b2), () -> releaseCount.incrementAndGet());
        BytesReference composite = CompositeBytesReference.of(p1, p2);
        ReleasableBytesReference adopted = ReleasableBytesReference.adopt(composite);
        assertEquals(0, releaseCount.get());
        assertEquals(composite.length(), adopted.length());
        assertArrayEquals(BytesReference.toBytes(composite), BytesReference.toBytes(adopted));
        adopted.decRef();
        assertEquals(0, releaseCount.get());
        assertTrue(p1.hasReferences());
        assertTrue(p2.hasReferences());
        p1.decRef();
        p2.decRef();
        assertEquals(2, releaseCount.get());
    }
}
