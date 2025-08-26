/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An extension to {@link BytesReference} that requires releasing its content. This
 * class exists to make it explicit when a bytes reference needs to be released, and when not.
 */
public final class ReleasableBytesReference implements RefCounted, Releasable, BytesReference {

    private static final ReleasableBytesReference EMPTY = new ReleasableBytesReference(BytesArray.EMPTY, RefCounted.ALWAYS_REFERENCED);

    private BytesReference delegate;
    private final RefCounted refCounted;

    public static ReleasableBytesReference empty() {
        return EMPTY;
    }

    public ReleasableBytesReference(BytesReference delegate, Releasable releasable) {
        this(delegate, new RefCountedReleasable(releasable));
    }

    public ReleasableBytesReference(BytesReference delegate, RefCounted refCounted) {
        this.delegate = delegate;
        this.refCounted = refCounted;
        assert refCounted.hasReferences();
    }

    public static ReleasableBytesReference wrap(BytesReference reference) {
        assert reference instanceof ReleasableBytesReference == false : "use #retain() instead of #wrap() on a " + reference.getClass();
        return reference.length() == 0 ? empty() : new ReleasableBytesReference(reference, ALWAYS_REFERENCED);
    }

    @Override
    public void incRef() {
        refCounted.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return refCounted.tryIncRef();
    }

    @Override
    public boolean decRef() {
        boolean res = refCounted.decRef();
        if (res) {
            delegate = null;
        }
        return res;
    }

    @Override
    public boolean hasReferences() {
        boolean hasRef = refCounted.hasReferences();
        // delegate is nulled out when the ref-count reaches zero but only via a plain store, and also we could be racing with a concurrent
        // decRef so need to check #refCounted again in case we run into a non-null delegate but saw a reference before
        assert delegate != null || refCounted.hasReferences() == false;
        return hasRef;
    }

    public ReleasableBytesReference retain() {
        refCounted.mustIncRef();
        return this;
    }

    public ReleasableBytesReference retainedSlice(int from, int length) {
        assert hasReferences();
        if (from == 0 && length() == length) {
            return retain();
        }
        final BytesReference slice = delegate.slice(from, length);
        refCounted.incRef();
        return new ReleasableBytesReference(slice, refCounted);
    }

    @Override
    public void close() {
        refCounted.decRef();
    }

    @Override
    public byte get(int index) {
        assert hasReferences();
        return delegate.get(index);
    }

    @Override
    public int getInt(int index) {
        assert hasReferences();
        return delegate.getInt(index);
    }

    @Override
    public int getIntLE(int index) {
        assert hasReferences();
        return delegate.getIntLE(index);
    }

    @Override
    public long getLongLE(int index) {
        assert hasReferences();
        return delegate.getLongLE(index);
    }

    @Override
    public double getDoubleLE(int index) {
        assert hasReferences();
        return delegate.getDoubleLE(index);
    }

    @Override
    public int indexOf(byte marker, int from) {
        assert hasReferences();
        return delegate.indexOf(marker, from);
    }

    @Override
    public int length() {
        assert hasReferences();
        return delegate.length();
    }

    @Override
    public BytesReference slice(int from, int length) {
        assert hasReferences();
        return delegate.slice(from, length);
    }

    @Override
    public long ramBytesUsed() {
        assert hasReferences();
        return delegate.ramBytesUsed();
    }

    @Override
    public StreamInput streamInput() throws IOException {
        assert hasReferences();
        return new BytesReferenceStreamInput(delegate) {
            private ReleasableBytesReference retainAndSkip(int len) throws IOException {
                if (len == 0) {
                    return ReleasableBytesReference.empty();
                }
                // instead of reading the bytes from a stream we just create a slice of the underlying bytes
                final ReleasableBytesReference result = retainedSlice(offset(), len);
                // move the stream manually since creating the slice didn't move it
                skip(len);
                return result;
            }

            @Override
            public ReleasableBytesReference readReleasableBytesReference() throws IOException {
                final int len = readVInt();
                return retainAndSkip(len);
            }

            @Override
            public ReleasableBytesReference readAllToReleasableBytesReference() throws IOException {
                return retainAndSkip(length() - offset());
            }

            @Override
            public boolean supportReadAllToReleasableBytesReference() {
                return true;
            }
        };
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        assert hasReferences();
        delegate.writeTo(os);
    }

    @Override
    public String utf8ToString() {
        assert hasReferences();
        return delegate.utf8ToString();
    }

    @Override
    public BytesRef toBytesRef() {
        assert hasReferences();
        return delegate.toBytesRef();
    }

    @Override
    public BytesRefIterator iterator() {
        assert hasReferences();
        return delegate.iterator();
    }

    @Override
    public int compareTo(BytesReference o) {
        assert hasReferences();
        return delegate.compareTo(o);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        assert hasReferences();
        return delegate.toXContent(builder, params);
    }

    @Override
    public boolean isFragment() {
        assert hasReferences();
        return delegate.isFragment();
    }

    @Override
    public boolean equals(Object obj) {
        assert hasReferences();
        return delegate.equals(obj);
    }

    @Override
    public int hashCode() {
        assert hasReferences();
        return delegate.hashCode();
    }

    @Override
    public boolean hasArray() {
        assert hasReferences();
        return delegate.hasArray();
    }

    @Override
    public byte[] array() {
        assert hasReferences();
        return delegate.array();
    }

    @Override
    public int arrayOffset() {
        assert hasReferences();
        return delegate.arrayOffset();
    }

    private static final class RefCountedReleasable extends AbstractRefCounted {

        private final Releasable releasable;

        RefCountedReleasable(Releasable releasable) {
            this.releasable = releasable;
        }

        @Override
        protected void closeInternal() {
            Releasables.closeExpectNoException(releasable);
        }
    }
}
