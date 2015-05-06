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
package org.elasticsearch.index.translog;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * an implementation of {@link org.elasticsearch.index.translog.Translog.Snapshot}, wrapping
 * a {@link ChannelReader}. This class is NOT thread-safe.
 */
public class ChannelSnapshot implements Closeable {

    protected final ChannelReader reader;
    protected final AtomicBoolean closed = new AtomicBoolean(false);

    // we use an atomic long to allow passing it by reference :(
    protected long position;

    public ChannelSnapshot(ChannelReader reader) {
        this.reader = reader;
        this.position = reader.firstPosition();
    }

    public long translogId() {
        return reader.translogId();
    }

    public int estimatedTotalOperations() {
        return reader.totalOperations();
    }

    public Translog.Operation next(ByteBuffer reusableBuffer) throws IOException {
        if (position >= reader.sizeInBytes()) {
            return null;
        }
        final int opSize = reader.readSize(reusableBuffer, position);
        Translog.Operation op = reader.read(reusableBuffer, position, opSize);
        position += opSize;
        return op;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                IOUtils.close(reader);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to close translogs", e);
            }
        }
    }
}
