/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

final class TestRecordSplitters {

    private TestRecordSplitters() {}

    static RecordSplitter newlineSplitter(int maxRecordBytes) {
        return new RecordSplitter() {
            @Override
            public long findNextRecordBoundary(InputStream stream) throws IOException {
                InputStream input = stream.markSupported() ? stream : new BufferedInputStream(stream);
                long consumed = 0;
                int b;
                while ((b = input.read()) != -1) {
                    consumed++;
                    if (consumed > maxRecordBytes) {
                        return RECORD_TOO_LARGE;
                    }
                    if (b == '\n') {
                        return consumed;
                    }
                    if (b == '\r') {
                        input.mark(1);
                        int next = input.read();
                        if (next == '\n') {
                            consumed++;
                            return consumed > maxRecordBytes ? RECORD_TOO_LARGE : consumed;
                        }
                        if (next != -1) {
                            input.reset();
                        }
                        return consumed;
                    }
                }
                return -1;
            }

            @Override
            public int findLastRecordBoundary(byte[] buf, int offset, int length) {
                int end = offset + length;
                int recordStart = offset;
                int lastBoundary = -1;
                for (int i = offset; i < end; i++) {
                    if (buf[i] == '\n' || buf[i] == '\r') {
                        int boundary = i;
                        if (buf[i] == '\r' && i + 1 < end && buf[i + 1] == '\n') {
                            boundary = ++i;
                        }
                        if (boundary - recordStart + 1 > maxRecordBytes) {
                            return lastBoundary >= 0 ? lastBoundary : (int) RECORD_TOO_LARGE;
                        }
                        lastBoundary = boundary;
                        recordStart = boundary + 1;
                    }
                }
                return end - recordStart > maxRecordBytes && lastBoundary < 0 ? (int) RECORD_TOO_LARGE : lastBoundary;
            }

            @Override
            public int maxRecordBytes() {
                return maxRecordBytes;
            }
        };
    }
}
