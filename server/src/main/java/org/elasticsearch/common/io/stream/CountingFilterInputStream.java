/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class CountingFilterInputStream extends FilterInputStream {

    private int bytesRead = 0;

    public CountingFilterInputStream(InputStream in) {
        super(in);
    }

    @Override
    public int read() throws IOException {
        assert assertInvariant();
        final int result = super.read();
        if (result != -1) {
            bytesRead += 1;
        }
        return result;
    }

    // Not overriding read(byte[]) because FilterInputStream delegates to read(byte[], int, int)

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        assert assertInvariant();
        final int n = super.read(b, off, len);
        if (n != -1) {
            bytesRead += n;
        }
        return n;
    }

    @Override
    public long skip(long n) throws IOException {
        assert assertInvariant();
        final long skipped = super.skip(n);
        bytesRead += Math.toIntExact(skipped);
        return skipped;
    }

    public int getBytesRead() {
        return bytesRead;
    }

    protected boolean assertInvariant() {
        return true;
    }
}
