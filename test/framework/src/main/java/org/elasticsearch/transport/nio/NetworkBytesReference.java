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

package org.elasticsearch.transport.nio;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class NetworkBytesReference extends BytesReference implements NetworkBytes {

    final RefCountedReleasable refCountedReleasable;

    final int length;
    int writeIndex;
    int readIndex;

    private AtomicBoolean isClosed = new AtomicBoolean(false);

    NetworkBytesReference(Releasable releasable, int length) {
        this.refCountedReleasable = new RefCountedReleasable(releasable);
        this.length = length;
    }

    NetworkBytesReference(RefCountedReleasable refCountedReleasable, int length) {
        this.refCountedReleasable = refCountedReleasable;
        this.length = length;
    }

    public int getWriteIndex() {
        return writeIndex;
    }

    public void incrementWrite(int delta) {
        int newWriteIndex = writeIndex + delta;
        if (newWriteIndex > length) {
            throw new IndexOutOfBoundsException("New write index [" + newWriteIndex + "] would be greater than length" +
                " [" + length + "]");
        }

        writeIndex = newWriteIndex;
    }

    public int getWriteRemaining() {
        return length - writeIndex;
    }

    public boolean hasWriteRemaining() {
        return getWriteRemaining() > 0;
    }

    public int getReadIndex() {
        return readIndex;
    }

    public void incrementRead(int delta) {
        int newReadIndex = readIndex + delta;
        if (newReadIndex > writeIndex) {
            throw new IndexOutOfBoundsException("New read index [" + newReadIndex + "] would be greater than write" +
                " index [" + writeIndex + "]");
        }
        readIndex = newReadIndex;
    }

    public int getReadRemaining() {
        return writeIndex - readIndex;
    }

    public boolean hasReadRemaining() {
        return getReadRemaining() > 0;
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            refCountedReleasable.decRef();
        } else {
            throw new IllegalStateException("Attempting to close NetworkBytesReference that is already closed.");
        }
    }

    public abstract NetworkBytesReference sliceAndRetain(int from, int length);

    public abstract ByteBuffer getWriteByteBuffer();

    public abstract ByteBuffer getReadByteBuffer();

    protected static class RefCountedReleasable extends AbstractRefCounted {

        private static final String REF_COUNTED_ARRAY_NAME = "network bytes";

        private final Releasable releasable;

        private RefCountedReleasable(Releasable releasable) {
            super(REF_COUNTED_ARRAY_NAME);
            this.releasable = releasable;
        }

        @Override
        protected void closeInternal() {
            if (releasable != null) {
                releasable.close();
            }
        }
    }
}
