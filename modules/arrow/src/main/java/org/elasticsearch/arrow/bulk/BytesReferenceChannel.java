/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * A {@code ReadableByteChannel} that reads from {@code ByteReference} data. That data
 * can be updated, allowing incremental parsing from a single channel.
 */
class BytesReferenceChannel implements ReadableByteChannel {

    private BytesRefIterator iterator;
    private BytesRef current;
    private int currentOffset;
    private int endOffset;
    private boolean lastData = false;

    BytesReferenceChannel() {
        // Keep zero/null values
    }

    BytesReferenceChannel(BytesReference data) throws IOException {
        setData(data, true);
    }

    void setData(BytesReference data, boolean lastData) throws IOException {
        this.lastData = lastData;
        this.iterator = data.iterator();
        nextBytesRef();
    }

    private void nextBytesRef() throws IOException {
        this.current = iterator.next();
        if (this.current == null) {
            this.currentOffset = 0;
            this.endOffset = 0;
        } else {
            this.currentOffset = this.current.offset;
            this.endOffset = this.currentOffset + this.current.length;
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int written = 0;
        int remaining;
        while ((remaining = dst.remaining()) > 0 && this.current != null) {
            int len = Math.min(remaining, this.endOffset - this.currentOffset);
            dst.put(this.current.bytes, this.currentOffset, len);
            this.currentOffset += len;
            written += len;

            if (this.currentOffset == this.endOffset) {
                nextBytesRef();
            }
        }

        return written == 0 && lastData ? -1 : written;
    }

    @Override
    public boolean isOpen() {
        return iterator != null;
    }

    @Override
    public void close() {
        iterator = null;
        current = null;
    }
}
