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
import org.elasticsearch.columnar.substrate.ColumnTestFiles;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class PlainStringColumnTempFileTests extends ColumnarStringTestCase {

    private static final String DIRECTORY_SUFFIX = "columnar-plain-bounds-directory";
    private static final String PAYLOAD_SUFFIX = "columnar-plain-bounds-payload";

    private void write(Directory dir, BytesRef[][] docSlots, boolean eligible) throws IOException {
        final byte[] segmentId = new byte[16];
        random().nextBytes(segmentId);
        try (ColumnTestFiles.Outputs out = ColumnTestFiles.create(dir, "column", segmentId)) {
            StringColumnWriter.write(
                docSlots.length,
                totals(docSlots),
                () -> cursor(docSlots),
                StringColumnOptions.DEFAULT.withPolicies(DictionaryPolicy.NONE, SummaryPolicy.NONE).withPlainSkipIndex(eligible),
                null,
                dir,
                IOContext.DEFAULT,
                out.outputs(),
                out.skipIndex()
            );
        }
    }

    private static BytesRef[][] descendingColumn() {
        final BytesRef[][] docSlots = new BytesRef[3000][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[] { new BytesRef(String.format(java.util.Locale.ROOT, "value-%06d", docSlots.length - doc)) };
        }
        return docSlots;
    }

    public void testAColumnWithoutBoundsStagesNothingForThem() throws IOException {
        try (Directory real = newDirectory()) {
            final RecordingDirectory dir = new RecordingDirectory(real, -1);
            write(dir, descendingColumn(), false);
            assertThat(dir.suffixes, not(hasItem(DIRECTORY_SUFFIX)));
            assertThat(dir.suffixes, not(hasItem(PAYLOAD_SUFFIX)));
        }
    }

    public void testAColumnWithBoundsStagesTwoFilesAndLeavesNoneWhateverFails() throws IOException {
        final BytesRef[][] docSlots = descendingColumn();
        final int total;
        try (Directory real = newDirectory()) {
            final RecordingDirectory dir = new RecordingDirectory(real, -1);
            write(dir, docSlots, true);
            total = dir.opened;
            assertThat(dir.suffixes, hasItem(DIRECTORY_SUFFIX));
            assertThat(dir.suffixes, hasItem(PAYLOAD_SUFFIX));
            assertNoStagedBounds(real, "after a successful write");
        }
        for (int failAt = 1; failAt <= total; failAt++) {
            try (Directory real = newDirectory()) {
                boolean failed = false;
                try {
                    write(new RecordingDirectory(real, failAt), docSlots, true);
                } catch (IOException e) {
                    failed = true;
                }
                assertTrue("failing temporary file " + failAt + " of " + total + " did not fail the write", failed);
                assertNoStagedBounds(real, "when the write failed at temporary file " + failAt);
            }
        }
    }

    private static void assertNoStagedBounds(Directory real, String when) throws IOException {
        for (String name : real.listAll()) {
            assertFalse("temporary file [" + name + "] left behind " + when, name.contains("columnar-plain-bounds"));
        }
    }

    private static final class RecordingDirectory extends FilterDirectory {
        private final int failAt;
        int opened;
        final Set<String> suffixes = new HashSet<>();

        RecordingDirectory(Directory in, int failAt) {
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
}
