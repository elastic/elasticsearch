/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Prevents writes when the max size is breached
 */
public class BoundedOutputStream extends FilterOutputStream {
    private final int maxSize;
    private int size;

    // As soon as a write request exceeds maxSize, permit no more writes, even if there is capacity for them
    private boolean closed = false;

    public BoundedOutputStream(OutputStream out, int maxSize) {
        super(out);
        this.maxSize = maxSize;
        this.size = 0;
    }

    private boolean hasCapacity(int bytes) {
        return size + bytes <= maxSize;
    }

    @Override
    public void write(int b) throws IOException {
        if (closed == false && hasCapacity(1)) {
            super.write(b);

            /*
                We only need to increment size here as both super.write(byte[] b) and
                super.write(byte[] b, int off, int len) write each byte individually via this
                method, and we have already checked in each respective method whether we have
                sufficient capacity for that entire write
             */
            size++;
        } else {
            closed = true;
            throw new BoundedOutputStreamFailedWriteException();
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (closed == false && hasCapacity(b.length)) {
            super.write(b);
        } else {
            closed = true;
            throw new BoundedOutputStreamFailedWriteException();
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (closed == false && hasCapacity(len)) {
            super.write(b, off, len);
        } else {
            closed = true;
            throw new BoundedOutputStreamFailedWriteException();
        }
    }
}
