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
    private long reminder;
    private final byte[] buf = new byte[512];

    TarInputStream(InputStream in) {
        super(in);
    }

    public TarEntry getNextEntry() throws IOException {
        if (currentEntry != null) {
            //go to the end of the current entry
            skipN(remaining);
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

        String name = getString(0, 100);

        boolean notFile = (buf[156] != 0 && buf[156] != '0') || name.endsWith("/");

        if(notFile){
            remaining = 0;
            reminder = 0;
        } else {
            String sizeString = getString(124, 12);
            remaining = sizeString.isEmpty() ? 0 : Long.parseLong(sizeString, 8);
            reminder = remaining % 512;
        }

        currentEntry = new TarEntry(name, notFile);
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
        if (remaining <= 0) {
            return -1;
        }
        int read = in.read(b, off, remaining > Integer.MAX_VALUE ? len : (int) Math.min(len, remaining));
        remaining -= read;
        return read;
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
        private final boolean notFile;

        TarEntry(String name, boolean notFile) {
            this.name = name;
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

