/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * {@link InputStream} with very basic support for tar format, just enough to parse archives provided by GeoIP database service from Infra.
 * This class is not suitable for general purpose tar processing!
 */
class TarInputStream extends FilterInputStream {

    private TarEntry currentEntry;
    private long remaining;
    private final byte[] buf = new byte[512];

    TarInputStream(InputStream in) {
        super(in);
    }

    public TarEntry getNextEntry() throws IOException {
        if (currentEntry != null) {
            //go to the end of the current entry
            skipN(remaining);
            long reminder = currentEntry.size % 512;
            if (reminder != 0) {
                skipN(512 - reminder);
            }
        }
        int read = in.readNBytes(buf, 0, 512);
        if (read == 0) {
            return null;
        }
        if (read != 512) {
            throw new EOFException();
        }
        if (Arrays.compare(buf, new byte[512]) == 0) {
            return null;
        }
        String name = getString(345, 155) + getString(0, 100);

        String sizeString = getString(124, 12);
        remaining = sizeString.isEmpty() ? 0 : Long.parseLong(sizeString, 8);
        boolean notFile = (buf[156] != 0 && buf[156] != '0') || name.endsWith("/");
        currentEntry = new TarEntry(name, remaining, notFile);
        if (notFile) {
            remaining = 0;
        }
        return currentEntry;
    }

    @Override
    public int read() throws IOException {
        if (remaining == 0) {
            return -1;
        }
        remaining--;
        return in.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (remaining > 0) {
            int read = in.read(b, off, remaining > Integer.MAX_VALUE ? len : (int) Math.min(len, remaining));
            remaining -= read;
            return read;
        }
        return -1;
    }

    private String getString(int offset, int maxLen) {
        return new String(buf, offset, maxLen, StandardCharsets.UTF_8).trim();
    }

    private void skipN(long n) throws IOException {
        while (n > 0) {
            long skip = in.skip(n);
            if (skip < n) {
                int read = in.read();
                if (read == -1) {
                    throw new EOFException();
                }
                n--;
            }
            n -= skip;
        }
    }

    static class TarEntry {
        private final String name;
        private final long size;
        private final boolean notFile;

        TarEntry(String name, long size, boolean notFile) {
            this.name = name;
            this.size = size;
            this.notFile = notFile;
        }

        public String getName() {
            return name;
        }

        public boolean isNotFile() {
            return notFile;
        }
    }
}

