/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

public class BaseVectorizationTests extends ESTestCase {

    @Before
    public void sanity() {
        assert Runtime.version().feature() < 21 || ModuleLayer.boot().findModule("jdk.incubator.vector").isPresent();
    }

    public static ESVectorizationProvider defaultProvider() {
        return new DefaultESVectorizationProvider();
    }

    public static ESVectorizationProvider maybePanamaProvider() {
        return ESVectorizationProvider.lookup(true);
    }

    /**
     * Opens {@code name} from {@code dir} and returns it routed through
     * {@link #wrapForAssertion}. Use in place of
     * {@code dir.openInput(name, IOContext.DEFAULT)} in tests that want
     * to assert per-slice byte lengths.
     */
    static IndexInput openTestInput(Directory dir, String name, int expectedSliceLength) throws IOException {
        return wrapForAssertion(dir.openInput(name, IOContext.DEFAULT), expectedSliceLength);
    }

    /**
     * Wraps {@code in} with an asserting filter if it exposes one of the
     * optional fast-path interfaces consumed by
     * {@code IndexInputUtils.withSlice} / {@code withSliceAddresses}. The
     * filter verifies that every per-slice byte length the requested by
     * the caller equals {@code expectedSliceLength}, then delegates to the
     * wrapped implementation.
     *
     * <p>If {@code in} does not expose a fast-path interface this method
     * returns it unchanged.
     */
    static IndexInput wrapForAssertion(IndexInput in, int expectedSliceLength) {
        IndexInput unwrapped = FilterIndexInput.unwrapOnlyTest(in);
        if (unwrapped instanceof DirectAccessInput) {
            return new AssertingDirectAccessInput("asserting(" + unwrapped + ")", unwrapped, expectedSliceLength);
        } else if (unwrapped instanceof MemorySegmentAccessInput) {
            return new AssertingMemorySegmentAccessInput("asserting(" + unwrapped + ")", unwrapped, expectedSliceLength);
        }
        return unwrapped;
    }

    /**
     * A {@link FilterIndexInput} that also implements
     * {@link DirectAccessInput}: each direct-access call asserts that the
     * requested slice length matches the configured expected length, then
     * delegates to the wrapped input's own DAI implementation. The wrapped
     * input must itself be a {@link DirectAccessInput}.
     */
    static final class AssertingDirectAccessInput extends FilterIndexInput implements DirectAccessInput {
        private final DirectAccessInput delegate;
        private final int expectedSliceLength;

        private AssertingDirectAccessInput(String resourceDescription, IndexInput delegate, int expectedSliceLength) {
            super(resourceDescription, delegate);
            if (delegate instanceof DirectAccessInput == false) {
                throw new IllegalArgumentException("delegate must implement DirectAccessInput; got " + delegate.getClass().getName());
            }
            this.delegate = (DirectAccessInput) delegate;
            this.expectedSliceLength = expectedSliceLength;
        }

        @Override
        public boolean withByteBufferSlice(long offset, long length, CheckedConsumer<ByteBuffer, IOException> action) throws IOException {
            assertEquals("unexpected slice length", expectedSliceLength, (int) length);
            return delegate.withByteBufferSlice(offset, length, action);
        }

        @Override
        public boolean withByteBufferSlices(long[] offsets, int length, int count, CheckedConsumer<ByteBuffer[], IOException> action)
            throws IOException {
            assertEquals("unexpected slice length", expectedSliceLength, length);
            return delegate.withByteBufferSlices(offsets, length, count, action);
        }

        @Override
        public AssertingDirectAccessInput clone() {
            return new AssertingDirectAccessInput(toString(), ((IndexInput) delegate).clone(), expectedSliceLength);
        }
    }

    /**
     * A {@link FilterIndexInput} that also implements {@link MemorySegmentAccessInput}: each segmentSliceOrNull call asserts the requested
     * slice length matches the configured expected length, then delegates.
     * The wrapped input must itself be a {@link MemorySegmentAccessInput}.
     */
    static final class AssertingMemorySegmentAccessInput extends FilterIndexInput implements MemorySegmentAccessInput {
        private final MemorySegmentAccessInput delegate;
        private final int expectedSliceLength;

        private AssertingMemorySegmentAccessInput(String resourceDescription, IndexInput delegate, int expectedSliceLength) {
            super(resourceDescription, delegate);
            if (delegate instanceof MemorySegmentAccessInput == false) {
                throw new IllegalArgumentException(
                    "delegate must implement MemorySegmentAccessInput; got " + delegate.getClass().getName()
                );
            }
            this.delegate = (MemorySegmentAccessInput) delegate;
            this.expectedSliceLength = expectedSliceLength;
        }

        @Override
        public MemorySegment segmentSliceOrNull(long pos, long len) throws IOException {
            assertEquals("unexpected slice length", expectedSliceLength, (int) len);
            return delegate.segmentSliceOrNull(pos, len);
        }

        @Override
        public byte readByte(long pos) throws IOException {
            return delegate.readByte(pos);
        }

        @Override
        public short readShort(long pos) throws IOException {
            return delegate.readShort(pos);
        }

        @Override
        public int readInt(long pos) throws IOException {
            return delegate.readInt(pos);
        }

        @Override
        public long readLong(long pos) throws IOException {
            return delegate.readLong(pos);
        }

        @Override
        public AssertingMemorySegmentAccessInput clone() {
            return new AssertingMemorySegmentAccessInput(toString(), ((IndexInput) delegate).clone(), expectedSliceLength);
        }
    }
}
