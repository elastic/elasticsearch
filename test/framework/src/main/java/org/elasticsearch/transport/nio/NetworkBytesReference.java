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

    final Releasable releasable;

    final int length;
    int index;

    private AtomicBoolean isClosed = new AtomicBoolean(false);

    NetworkBytesReference(Releasable releasable, int length) {
        this.releasable = releasable;
        this.length = length;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public void incrementIndex(int delta) {
        int newIndex = index + delta;
        NetworkBytes.validateIndex(newIndex, length);
        index = newIndex;
    }

    @Override
    public int getRemaining() {
        return length - index;
    }

    @Override
    public boolean hasRemaining() {
        return getRemaining() != 0;
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            if (releasable != null) {
                releasable.close();
            }
        } else {
            throw new IllegalStateException("Attempting to close NetworkBytesReference that is already closed.");
        }
    }

    public abstract NetworkBytesReference sliceAndRetain(int from, int length);

    public abstract ByteBuffer postIndexByteBuffer();

    public abstract ByteBuffer preIndexByteBuffer();

}
