/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class TarInputStream extends FilterInputStream {

    private TarEntry currentEntry;
    private long remaining;
    private final byte[] buf = new byte[512];

    public TarInputStream(InputStream is) {
        super(is);
    }

    public TarEntry getNextEntry() throws IOException {
        if (currentEntry != null) {
            //go to the end of the current entry
            skipN(remaining);
            long reminder = currentEntry.getSize() % 512;
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
        if (bufAllZeroes()) {
            return null;
        }
        String name = getString(0, 100);
        int mode = getInt(100);
        int uid = getInt(108);
        int gid = getInt(116);
        remaining = getLong(124);
        long mTime = getLong(136);
        int checksum = getInt(148);
        TarEntry.Type type = TarEntry.getType(getString(156, 1));
        String linkName = getString(157, 100);
        boolean ustar = getString(257, 6).equals("ustar");
        String uname = getString(265, 32);
        String gname = getString(297, 32);
        int devMajor = getInt(329);
        int devMinor = getInt(337);
        name = getString(345, 155) + name;
        if (name.endsWith("/")) {
            type = TarEntry.Type.DIRECTORY;
            name = name.substring(0, name.length() - 1);
        }
        ensureConsistency(checksum);
        currentEntry = new TarEntry(name, mode, uid, gid, remaining, mTime, type, linkName, uname, gname, devMajor, devMinor, ustar);
        if (type == TarEntry.Type.DIRECTORY) {
            remaining = 0;
        }
        return currentEntry;
    }

    private void ensureConsistency(int checksum) throws IOException {
        for (int i = 0; i < 148; i++) {
            checksum -= (buf[i] & 0xff);
        }
        for (int i = 156; i < 512; i++) {
            checksum -= (buf[i] & 0xff);
        }
        if (checksum != 256) {
            throw new IOException("entry header corrupted - checksum mismatch");
        }
    }

    @Override
    public int available() throws IOException {
        return remaining > 0 ? 1 : 0;
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
        return new String(buf, offset, maxLen).trim();
    }

    private int getInt(int offset) {
        String s = getString(offset, 8);
        return s.isEmpty() ? 0 : Integer.parseInt(s, 8);
    }

    private long getLong(int offset) {
        String s = getString(offset, 12);
        return s.isEmpty() ? 0 : Long.parseLong(s, 8);
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

    private boolean bufAllZeroes() {
        for (byte b : buf) {
            if (b != 0) {
                return false;
            }
        }
        return true;
    }
}

