/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ColumnarFormatVersionTests extends ESTestCase {

    private static final byte[] SEGMENT_ID = new byte[16];
    private static final String SUFFIX = "";

    public void testTooNewVersionRejected() throws IOException {
        final int futureVersion = FormatVersion.CURRENT.version() + 1;
        try (ByteBuffersDirectory dir = new ByteBuffersDirectory()) {
            writeHeaderOnly(dir, "test.cnm", ColumNARDocValuesFormat.META_CODEC, futureVersion);
            try (ChecksumIndexInput in = dir.openChecksumInput("test.cnm")) {
                expectThrows(
                    IndexFormatTooNewException.class,
                    () -> ColumnarCodecUtil.checkHeader(in, ColumNARDocValuesFormat.META_CODEC, SEGMENT_ID, SUFFIX)
                );
            }
        }
    }

    public void testTooOldVersionRejected() throws IOException {
        final int pastVersion = FormatVersion.BASELINE.version() - 1;
        try (ByteBuffersDirectory dir = new ByteBuffersDirectory()) {
            writeHeaderOnly(dir, "test.cnm", ColumNARDocValuesFormat.META_CODEC, pastVersion);
            try (ChecksumIndexInput in = dir.openChecksumInput("test.cnm")) {
                expectThrows(
                    IndexFormatTooOldException.class,
                    () -> ColumnarCodecUtil.checkHeader(in, ColumNARDocValuesFormat.META_CODEC, SEGMENT_ID, SUFFIX)
                );
            }
        }
    }

    public void testWriteProfileBoundsEnforced() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new ColumnarWriteProfile(new FormatVersion(FormatVersion.CURRENT.version() + 1))
        );
        final ColumnarWriteProfile ok = new ColumnarWriteProfile(FormatVersion.CURRENT);
        assertEquals(FormatVersion.CURRENT, ok.version());
    }

    public void testCurrentProfileMatchesVersionCurrent() {
        assertEquals(FormatVersion.CURRENT, ColumnarWriteProfile.current().version());
    }

    public void testBuilderNullArgsRejected() {
        expectThrows(NullPointerException.class, () -> new ColumNARDocValuesFormat.Builder().pipelineSelector(null));
        expectThrows(NullPointerException.class, () -> new ColumNARDocValuesFormat.Builder().writeProfile(null));
    }

    public void testBuilderInvalidBlockSizeRejected() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new ColumNARDocValuesFormat.Builder().blockSize(ColumNARDocValuesFormat.MIN_BLOCK_SIZE - 1).build()
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new ColumNARDocValuesFormat.Builder().blockSize(ColumNARDocValuesFormat.MAX_BLOCK_SIZE + 1).build()
        );
        expectThrows(IllegalArgumentException.class, () -> new ColumNARDocValuesFormat.Builder().blockSize(300).build());
    }

    private static void writeHeaderOnly(final ByteBuffersDirectory dir, final String name, final String codec, int version)
        throws IOException {
        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
            CodecUtil.writeIndexHeader(out, codec, version, SEGMENT_ID, SUFFIX);
            CodecUtil.writeFooter(out);
        }
    }
}
