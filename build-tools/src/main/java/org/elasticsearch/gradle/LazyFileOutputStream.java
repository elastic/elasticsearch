/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Server Side Public License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * An outputstream to a File that is lazily opened on the first write.
 */
class LazyFileOutputStream extends OutputStream {
    private final File file;
    private volatile OutputStream delegate;
    private volatile boolean initialized = false;
    private final Object lock = new Object();

    LazyFileOutputStream(File file) {
        this.file = file;
    }

    private void ensureInitialized() throws IOException {
        if (initialized == false) {
            synchronized (lock) {
                if (initialized == false) {
                    file.getParentFile().mkdirs();
                    delegate = new FileOutputStream(file);
                    initialized = true;
                }
            }
        }
    }

    @Override
    public void write(int b) throws IOException {
        ensureInitialized();
        delegate.write(b);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        ensureInitialized();
        delegate.write(b, off, len);
    }

    @Override
    public void write(byte b[]) throws IOException {
        ensureInitialized();
        delegate.write(b);
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            if (initialized && delegate != null) {
                delegate.close();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        if (initialized && delegate != null) {
            delegate.flush();
        }
    }
}
