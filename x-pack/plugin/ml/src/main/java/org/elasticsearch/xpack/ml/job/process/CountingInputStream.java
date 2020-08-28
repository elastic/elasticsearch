/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Simple wrapper around an inputstream instance that counts
 * all the bytes passing through it reporting that number to
 * the {@link DataCountsReporter}
 * <p>
 * Overrides the read methods counting the number of bytes read.
 */
public class CountingInputStream extends FilterInputStream {
    private DataCountsReporter dataCountsReporter;
    private int markPosition = 0;
    private int currentPosition = 0;

    /**
     * @param in
     *            input stream
     * @param dataCountsReporter
     *            Write number of records, bytes etc.
     */
    public CountingInputStream(InputStream in, DataCountsReporter dataCountsReporter) {
        super(in);
        this.dataCountsReporter = dataCountsReporter;
    }

    /**
     * Report 1 byte read
     */
    @Override
    public int read() throws IOException {
        int read = in.read();
        dataCountsReporter.reportBytesRead(read < 0 ? 0 : 1);
        currentPosition += read < 0 ? 0 : 1;

        return read;
    }

    @Override
    public int read(byte[] b) throws IOException {
        int read = in.read(b);
        dataCountsReporter.reportBytesRead(read < 0 ? 0 : read);
        currentPosition += read < 0 ? 0 : read;

        return read;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read = in.read(b, off, len);
        dataCountsReporter.reportBytesRead(read < 0 ? 0 : read);
        currentPosition += read < 0 ? 0 : read;

        return read;
    }

    @Override
    public synchronized void mark(int readlimit) {
        super.mark(readlimit);
        markPosition = currentPosition;
    }

    @Override
    public synchronized void reset() throws IOException {
        super.reset();
        dataCountsReporter.reportBytesRead(-(currentPosition - markPosition));
        currentPosition = markPosition;
    }
}
