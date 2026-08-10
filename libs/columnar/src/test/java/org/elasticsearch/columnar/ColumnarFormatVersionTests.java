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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Checks that format-version boundary conditions produce the right exceptions.
 *
 * <p>A header stamped at {@code CURRENT.version() + 1} must throw {@link IndexFormatTooNewException}
 * so a not-yet-upgraded node fails loudly at segment open rather than deep inside a block decode.
 * A header stamped below {@code MIN_SUPPORTED} must throw {@link IndexFormatTooOldException}.
 * The meta/data version-mismatch rejection is documented for the first post-BASELINE bump; while
 * {@code CURRENT == BASELINE} the test is skipped and does not exercise {@link CorruptIndexException}.
 */
public class ColumnarFormatVersionTests extends ESTestCase {

    private static final byte[] SEGMENT_ID = new byte[16];
    private static final String SUFFIX = "";

    public void testEnsureReadableAcceptsCurrentVersion() {
        FormatVersion.CURRENT.ensureReadable();
    }

    public void testEnsureReadableRejectsFutureVersion() {
        final FormatVersion future = new FormatVersion(FormatVersion.CURRENT.version() + 1);
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, future::ensureReadable);
        assertTrue(ex.getMessage().contains(String.valueOf(future.version())));
    }

    public void testEnsureReadableRejectsPastVersion() {
        // NOTE: at BASELINE, MIN_SUPPORTED.version() - 1 == -1, which the FormatVersion constructor
        // rejects before ensureReadable() can. The test is meaningful only once MIN_SUPPORTED > 0.
        assumeTrue("past-version test requires MIN_SUPPORTED > 0", FormatVersion.MIN_SUPPORTED.version() > 0);
        final FormatVersion past = new FormatVersion(FormatVersion.MIN_SUPPORTED.version() - 1);
        expectThrows(IllegalArgumentException.class, past::ensureReadable);
    }

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
        final int pastVersion = FormatVersion.MIN_SUPPORTED.version() - 1;
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

    public void testMetadataVersionMismatchContractForFirstBump() throws IOException {
        // NOTE: when V_1 lands, rewrite this to open a ColumNARDocValuesProducer on the two
        // mismatched files and assert CorruptIndexException instead of just checking the versions differ.
        assumeTrue("mismatch test requires CURRENT >= 1", FormatVersion.CURRENT.version() >= 1);
        final int metaVersion = FormatVersion.CURRENT.version() - 1;
        final int dataVersion = FormatVersion.CURRENT.version();
        try (ByteBuffersDirectory dir = new ByteBuffersDirectory()) {
            writeHeaderOnly(dir, "seg.cnm", ColumNARDocValuesFormat.META_CODEC, metaVersion);
            writeHeaderOnly(dir, "seg.cnd", ColumNARDocValuesFormat.DATA_CODEC, dataVersion);
            try (ChecksumIndexInput meta = dir.openChecksumInput("seg.cnm")) {
                final FormatVersion readMeta = ColumnarCodecUtil.checkHeader(meta, ColumNARDocValuesFormat.META_CODEC, SEGMENT_ID, SUFFIX);
                try (ChecksumIndexInput data = dir.openChecksumInput("seg.cnd")) {
                    final FormatVersion readData = ColumnarCodecUtil.checkHeader(
                        data,
                        ColumNARDocValuesFormat.DATA_CODEC,
                        SEGMENT_ID,
                        SUFFIX
                    );
                    assertFalse(readMeta.matches(readData));
                }
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
