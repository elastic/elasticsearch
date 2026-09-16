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
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;
import static org.hamcrest.Matchers.greaterThan;

/**
 * A column stages its ordinals, its escapes and its slot counts in temporary files. Writing one can fail
 * partway through, and whatever it opened before the failure is still a file to delete.
 */
public class StringColumnTempFileTests extends ColumnarStringTestCase {

    private static final DictionaryPolicy ROOMY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);
    private static final String DATA_FILE = "column.cnd";

    /** Writes {@code docSlots} as a dictionary column into {@code dir}. */
    private void write(Directory dir, BytesRef[][] docSlots) throws IOException {
        final byte[] segmentId = new byte[16];
        random().nextBytes(segmentId);
        try (IndexOutput out = dir.createOutput(DATA_FILE, IOContext.DEFAULT)) {
            ColumnarCodecUtil.writeHeader(out, "ColumNARStringData", FormatVersion.CURRENT, segmentId, "");
            StringColumnWriter.write(
                docSlots.length,
                numDocsWithField(docSlots),
                numValues(docSlots),
                numNullSlots(docSlots),
                () -> cursor(docSlots),
                randomValidBlockSize(),
                randomChunkCodec(),
                randomTargetChunkBytes(),
                randomTargetChunkBytes(),
                StringColumnOptions.DEFAULT_COMPRESSED_ORDINAL_BLOCK_SIZE,
                StringColumnOptions.DEFAULT_SLOT_COUNTS_BLOCK_SIZE,
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
        final Set<String> suffixes = new HashSet<>();

        FailsNthTempOutput(Directory in, int failAt) {
            super(in);
            this.failAt = failAt;
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            if (++opened == failAt) {
                throw new IOException("no more temporary files for [" + prefix + "." + suffix + "]");
            }
            suffixes.add(suffix);
            return super.createTempOutput(prefix, suffix, context);
        }
    }

    /**
     * Whichever temporary file a column fails to open, the ones it opened before are gone by the time the
     * failure reaches the caller. A column whose documents hold several slots stages its counts as well, so
     * both shapes are put through it.
     */
    public void testTempFilesAreDeletedWhenWritingFails() throws IOException {
        for (boolean multiValued : new boolean[] { false, true }) {
            final BytesRef[][] docSlots = column(multiValued);
            // How many temporary files a write that succeeds asks for, so every one of them can be failed.
            final int total;
            try (Directory real = newDirectory()) {
                final FailsNthTempOutput counting = new FailsNthTempOutput(real, -1);
                write(counting, docSlots);
                total = counting.opened;
                // Only a column whose slots are out of step with its documents stages counts, so the
                // multi-valued shape is what puts that file through the failure paths below.
                assertEquals(
                    "counts staged for " + (multiValued ? "a multi-valued" : "a single-valued") + " column",
                    multiValued,
                    counting.suffixes.contains("columnar-counts")
                );
            }
            assertThat("a dictionary column stages more than one temporary file", total, greaterThan(1));

            // Every temporary file the write asks for, failed in turn.
            for (int failAt = 1; failAt <= total; failAt++) {
                final Directory real = newDirectory();
                try {
                    final Directory dir = new FailsNthTempOutput(real, failAt);
                    boolean failed = false;
                    try {
                        write(dir, docSlots);
                    } catch (IOException e) {
                        failed = true;
                    }
                    assertTrue("failing temporary file " + failAt + " of " + total + " did not fail the write", failed);
                    for (String name : real.listAll()) {
                        assertFalse(
                            "temporary file ["
                                + name
                                + "] left behind when the write failed at temporary file "
                                + failAt
                                + (multiValued ? " (multi-valued)" : " (single-valued)"),
                            name.contains("columnar-ordinals") || name.contains("columnar-escapes") || name.contains("columnar-counts")
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

    /**
     * A dictionary column with escapes among its values. When {@code multiValued}, its documents hold
     * differing numbers of slots, which is what puts the counts in a temporary file of their own.
     */
    private BytesRef[][] column(boolean multiValued) {
        final String[] terms = { "alpha", "bravo", "charlie" };
        final int docs = between(400, 1200);
        final BytesRef[][] docSlots = new BytesRef[docs][];
        for (int d = 0; d < docs; d++) {
            final int slots = multiValued ? d % 4 : 1;
            final BytesRef[] values = new BytesRef[slots];
            for (int i = 0; i < slots; i++) {
                values[i] = (d + i) % 9 == 4 ? new BytesRef("escaped-" + d + "-" + i) : new BytesRef(terms[(d + i) % terms.length]);
            }
            docSlots[d] = values;
        }
        return docSlots;
    }
}
