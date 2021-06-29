/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An input stream that allows to add a listener to monitor progress
 * The listener is triggered whenever a full percent is increased
 * The listener is never triggered twice on the same percentage
 * The listener will always return 99 percent, if the expectedTotalSize is exceeded, until it is finished
 *
 * Only used by the InstallPluginCommand, thus package private here
 */
abstract class ProgressInputStream extends FilterInputStream {

    private final int expectedTotalSize;
    private int currentPercent;
    private int count = 0;

    ProgressInputStream(InputStream is, int expectedTotalSize) {
        super(is);
        this.expectedTotalSize = expectedTotalSize;
        this.currentPercent = 0;
    }

    @Override
    public int read() throws IOException {
        int read = in.read();
        checkProgress(read == -1 ? -1 : 1);
        return read;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int byteCount = super.read(b, off, len);
        checkProgress(byteCount);
        return byteCount;
    }

    @Override
    public int read(byte b[]) throws IOException {
        return read(b, 0, b.length);
    }

    void checkProgress(int byteCount) {
        // are we done?
        if (byteCount == -1) {
            currentPercent = 100;
            onProgress(currentPercent);
        } else {
            count += byteCount;
            // rounding up to 100% would mean we say we are done, before we are...
            // this also catches issues, when expectedTotalSize was guessed wrong
            int percent = Math.min(99, (int) Math.floor(100.0 * count / expectedTotalSize));
            if (percent > currentPercent) {
                currentPercent = percent;
                onProgress(percent);
            }
        }
    }

    public void onProgress(int percent) {}
}
