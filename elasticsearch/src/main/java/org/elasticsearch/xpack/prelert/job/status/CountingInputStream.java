/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.status;

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
     * We don't care if the count is one byte out
     * because we don't check for the case where read
     * returns -1.
     * <p>
     * One of the buffered read(..) methods is more likely to
     * be called anyway.
     */
    @Override
    public int read() throws IOException {
        statusReporter.reportBytesRead(1);

        return in.read();
    }

    /**
     * Don't bother checking for the special case where
     * the stream is closed/finished and read returns -1.
     * Our count will be 1 byte out.
     */
    @Override
    public int read(byte[] b) throws IOException {
        int read = in.read(b);

        statusReporter.reportBytesRead(read);

        return read;
    }

    /**
     * Don't bother checking for the special case where
     * the stream is closed/finished and read returns -1.
     * Our count will be 1 byte out.
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read = in.read(b, off, len);

        statusReporter.reportBytesRead(read);
        return read;
    }

}
