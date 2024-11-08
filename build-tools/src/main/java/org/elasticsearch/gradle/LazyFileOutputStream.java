/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
    private OutputStream delegate;

    LazyFileOutputStream(File file) {
        // use an initial dummy delegate to avoid doing a conditional on every write
        this.delegate = new OutputStream() {
            private void bootstrap() throws IOException {
                file.getParentFile().mkdirs();
                delegate = new FileOutputStream(file);
            }

            @Override
            public void write(int b) throws IOException {
                bootstrap();
                delegate.write(b);
            }

            @Override
            public void write(byte b[], int off, int len) throws IOException {
                bootstrap();
                delegate.write(b, off, len);
            }

            @Override
            public void write(byte b[]) throws IOException {
                bootstrap();
                delegate.write(b);
            }
        };
    }

    @Override
    public void write(int b) throws IOException {
        delegate.write(b);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        delegate.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
