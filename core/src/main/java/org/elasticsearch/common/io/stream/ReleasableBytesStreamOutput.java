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

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.io.ReleasableBytesStream;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;

import java.util.concurrent.atomic.AtomicReference;

/**
 * An bytes stream output that allows providing a {@link BigArrays} instance
 * expecting it to require releasing its content ({@link #bytes()}) once done.
 * <p>
 * Please note, its is the responsibility of the caller to make sure the bytes
 * reference do not "escape" and are released. A utility method {@link #releaseIfNecessary()}
 * is available that to close all unreleased bytes. This is accomplished by tracking the
 * releasable objects created from this stream output and closing them if they
 * have not been released already.
 */
public class ReleasableBytesStreamOutput extends BytesStreamOutput implements ReleasableBytesStream {

    private final AtomicReference<Releasable> releasable = new AtomicReference<>(null);

    public ReleasableBytesStreamOutput(BigArrays bigarrays) {
        super(BigArrays.PAGE_SIZE_IN_BYTES, bigarrays);
    }

    public ReleasableBytesStreamOutput(int expectedSize, BigArrays bigArrays) {
        super(expectedSize, bigArrays);
    }

    /**
     * Returns a {@link Releasable} implementation of a {@link org.elasticsearch.common.bytes.BytesReference}
     * that represents the current state of the bytes in the stream. Once this method has been called, it must
     * not be called again unless {@link #reset()} is called first.
     */
    @Override
    public ReleasablePagedBytesReference bytes() {
        ReleasablePagedBytesReference bytesRef = new ReleasablePagedBytesReference(bigArrays, bytes, count);
        // keep track of the releasable bytes we create
        if (releasable.compareAndSet(null, Releasables.releaseOnce(bytesRef)) == false) {
            throw new IllegalStateException("bytes has been called once already and the stream has not been reset before calling again!");
        }
        return bytesRef;
    }

    @Override
    public void reset() {
        super.reset();
        releasable.set(null);
    }

    /**
     * Releases the {@link Releasable} byte array backing this stream output
     * and the {@link ReleasablePagedBytesReference} instances returned by the
     * {@link #bytes()} method if the {@link Releasable} has not already been released.
     */
    public void releaseIfNecessary() {
        Releasables.close(releasable.get() == null ? bytes : releasable.get());
    }
}
