/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.FormatVersion;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;

import java.io.IOException;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;
import static org.hamcrest.Matchers.greaterThan;

/**
 * A column stages its ordinals and its escapes in temporary files. Writing one can fail partway through,
 * and whatever it opened before the failure is still a file to delete.
 */
public class StringColumnTempFileTests extends ColumnarStringTestCase {

    private static final DictionaryPolicy ROOMY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);
    private static final String DATA_FILE = "column.cnd";

    /** Writes {@code docValues} as a dictionary column into {@code dir}. */
    private void write(Directory dir, BytesRef[] docValues) throws IOException {
        final byte[] segmentId = new byte[16];
        random().nextBytes(segmentId);
        try (IndexOutput out = dir.createOutput(DATA_FILE, IOContext.DEFAULT)) {
            ColumnarCodecUtil.writeHeader(out, "ColumNARStringData", FormatVersion.CURRENT, segmentId, "");
            StringColumnWriter.write(
                docValues.length,
                numDocsWithField(docValues),
                numDocsWithField(docValues),
                () -> cursor(docValues),
                randomValidBlockSize(),
                randomChunkCodec(),
                randomTargetChunkBytes(),
                ROOMY,
                null,
                dir,
                IOContext.DEFAULT,
                out
            );
        }
    }

    /** Fails the nth temporary file a column asks for, and counts what it was asked for. */
    private static final class FailsNthTempOutput extends FilterDirectory {
        private final int failAt;
        int opened;

        FailsNthTempOutput(Directory in, int failAt) {
            super(in);
            this.failAt = failAt;
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            if (++opened == failAt) {
                throw new IOException("no more temporary files for [" + prefix + "." + suffix + "]");
            }
            return super.createTempOutput(prefix, suffix, context);
        }
    }

    /**
     * Whichever temporary file a column fails to open, the ones it opened before are gone by the time the
     * failure reaches the caller.
     */
    public void testTempFilesAreDeletedWhenWritingFails() throws IOException {
        final String[] terms = { "alpha", "bravo", "charlie" };
        final BytesRef[] docValues = new BytesRef[between(400, 1200)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = d % 9 == 4 ? new BytesRef("escaped-" + d) : new BytesRef(terms[d % terms.length]);
        }
        // How many temporary files a write that succeeds asks for, so every one of them can be failed.
        final int total;
        try (Directory real = newDirectory()) {
            final FailsNthTempOutput counting = new FailsNthTempOutput(real, -1);
            write(counting, docValues);
            total = counting.opened;
        }
        assertThat("a dictionary column stages more than one temporary file", total, greaterThan(1));

        // Every temporary file the write asks for, failed in turn.
        for (int failAt = 1; failAt <= total; failAt++) {
            final Directory real = newDirectory();
            try {
                final Directory dir = new FailsNthTempOutput(real, failAt);
                boolean failed = false;
                try {
                    write(dir, docValues);
                } catch (IOException e) {
                    failed = true;
                }
                assertTrue("failing temporary file " + failAt + " of " + total + " did not fail the write", failed);
                for (String name : real.listAll()) {
                    assertFalse(
                        "temporary file [" + name + "] left behind when the write failed at temporary file " + failAt,
                        name.contains("columnar-ordinals") || name.contains("columnar-escapes")
                    );
                }
            } finally {
                try {
                    real.close();
                } catch (RuntimeException e) {
                    throw new AssertionError("failAt=" + failAt + ": " + e.getMessage(), e);
                }
            }
        }
    }
}
