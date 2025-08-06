/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.compress.fsst;

import org.apache.lucene.store.DataOutput;

import java.io.Closeable;
import java.io.IOException;

public class BulkCompressBufferer implements Closeable {
    private static final int MAX_LINES = 512;
    private static final int MAX_INPUT_DATA = 128 << 10;
    private static final int MAX_OUTPUT_DATA = MAX_INPUT_DATA * 2;

    final byte[] inData = new byte[MAX_INPUT_DATA + 8];
    final int[] inOffsets = new int[MAX_LINES + 1]; // 1 additional space for offset where next item would have been
    byte[] outBuf = new byte[MAX_OUTPUT_DATA + 8];
    int[] outOffsets = new int[MAX_LINES + 1]; // 1 additional space for offset where next item would have been
    private final DataOutput finalOutput;
    private final FSST.SymbolTable st;
    private final FSST.OffsetWriter offsetWriter;
    private int numLines = 0;
    private int inOff = 0;

    public BulkCompressBufferer(DataOutput finalOutput, FSST.SymbolTable st, FSST.OffsetWriter offsetWriter) {
        this.finalOutput = finalOutput;
        this.st = st;
        this.offsetWriter = offsetWriter;
    }

    private void addToBuffer(byte[] bytes, int offset, int length) {
        System.arraycopy(bytes, offset, inData, inOff, length);
        int lineIdx = numLines;
        inOffsets[lineIdx] = inOff;
        inOff += length;
        numLines++;
    }

    public void addLine(byte[] bytes, int offset, int length) throws IOException {
        if (inOff + length > MAX_INPUT_DATA || numLines == MAX_LINES) {
            // can't fit another
            compressAndWriteBuffer();

            if (length > MAX_INPUT_DATA) {
                // new item doesn't fit by itself, so deal with it by itself
                compressAndWriteSingle(bytes, offset, length);
            } else {
                // does fit
                addToBuffer(bytes, offset, length);
            }
        } else {
            // does fit
            addToBuffer(bytes, offset, length);
        }
    }

    private void compressAndWriteSingle(byte[] bytes, int offset, int length) throws IOException {
        assert numLines == 0 && inOff == 0;

        int off = offset;
        int lenToWrite = length;
        int totalOutLen = 0;

        while (lenToWrite > 0) {
            int len = Math.min(lenToWrite, MAX_INPUT_DATA);

            // copy data into buffer
            numLines = 1;
            inOffsets[0] = off;
            inOffsets[1] = off + len;

            long outLine = st.compressBulk(numLines, bytes, inOffsets, outBuf, outOffsets);
            assert outLine == numLines;
            long outLen = outOffsets[(int) outLine];
            totalOutLen += (int) outLen;
            finalOutput.writeBytes(outBuf, 0, (int) outLen);

            off += len;
            lenToWrite -= len;

        }
        offsetWriter.addLen(totalOutLen);

        clear();
    }

    private void compressAndWriteBuffer() throws IOException {
        assert numLines < MAX_LINES + 1;
        assert inOff <= MAX_INPUT_DATA;

        // add a pseudo-offset to provide last line's length
        inOffsets[numLines] = inOff;

        long outLines = st.compressBulk(numLines, inData, inOffsets, outBuf, outOffsets);
        assert outLines == numLines;
        long fullOutLen = outOffsets[(int) outLines];

        finalOutput.writeBytes(outBuf, 0, (int) fullOutLen);
        for (int i = 0; i < numLines; ++i) {
            int len = outOffsets[i + 1] - outOffsets[i];
            offsetWriter.addLen(len);
        }

        clear();
    }

    void clear() {
        numLines = inOff = 0;
    }

    @Override
    public void close() throws IOException {
        if (numLines > 0) {
            compressAndWriteBuffer();
        }
        clear();
    }
}
