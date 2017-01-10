/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.status;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Simple wrapper around an inputstream instance that counts
 * all the bytes passing through it reporting that number to
 * the {@link StatusReporter}
 * <p>
 * Overrides the read methods counting the number of bytes read.
 */
public class CountingInputStream extends FilterInputStream {
    private StatusReporter statusReporter;

    /**
     * @param in
     *            input stream
     * @param statusReporter
     *            Write number of records, bytes etc.
     */
    public CountingInputStream(InputStream in, StatusReporter statusReporter) {
        super(in);
        this.statusReporter = statusReporter;
    }

    /**
     * Report 1 byte read
     */
    @Override
    public int read() throws IOException {
        int read = in.read();
        statusReporter.reportBytesRead(read < 0 ? 0 : 1);

        return read;
    }

    @Override
    public int read(byte[] b) throws IOException {
        int read = in.read(b);

        statusReporter.reportBytesRead(read < 0 ? 0 : read);

        return read;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read = in.read(b, off, len);

        statusReporter.reportBytesRead(read < 0 ? 0 : read);
        return read;
    }
}
