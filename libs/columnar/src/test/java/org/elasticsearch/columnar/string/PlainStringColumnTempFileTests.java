/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.columnar.substrate.BytesSink;
import org.elasticsearch.columnar.substrate.ColumnTestFiles;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class PlainStringColumnTempFileTests extends ColumnarStringTestCase {

    private static final String DIRECTORY_SUFFIX = "columnar-plain-bounds-directory";
    private static final String PAYLOAD_SUFFIX = "columnar-plain-bounds-payload";

    private void write(Directory dir, BytesRef[][] docSlots, boolean eligible) throws IOException {
        write(dir, docSlots, eligible, () -> cursor(docSlots));
    }

    private void write(Directory dir, BytesRef[][] docSlots, boolean eligible, IOSupplier<StringColumnValues> cursors) throws IOException {
        final byte[] segmentId = new byte[16];
        random().nextBytes(segmentId);
        try (ColumnTestFiles.Outputs out = ColumnTestFiles.create(dir, "column", segmentId)) {
            StringColumnWriter.write(
                docSlots.length,
                totals(docSlots),
                cursors,
                StringColumnOptions.DEFAULT.withPolicies(DictionaryPolicy.NONE, SummaryPolicy.NONE)
                    .withSizes(PlainStringColumnSkipperTests.sizes(128, 256))
                    .withPlainSkipIndex(eligible),
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

    public void testAFailureOpeningTheCursorLeavesNoStagedBounds() throws IOException {
        final BytesRef[][] docSlots = descendingColumn();
        final int total = cursorsOpenedByASuccessfulWrite(docSlots);
        for (int attempt = 1; attempt <= total; attempt++) {
            final int failAt = attempt;
            final AtomicInteger opened = new AtomicInteger();
            assertWriteFailsAndLeavesNothing(docSlots, directory -> directory, () -> {
                if (opened.incrementAndGet() == failAt) {
                    throw new IOException("no cursor " + failAt);
                }
                return cursor(docSlots);
            });
        }
    }

    public void testAFailureWhileIteratingTheValuesLeavesNoStagedBounds() throws IOException {
        final BytesRef[][] docSlots = descendingColumn();
        final int total = cursorsOpenedByASuccessfulWrite(docSlots);
        for (int failAtDoc : new int[] { 1, 2, 1500, 3000 }) {
            for (boolean mergeAbort : new boolean[] { false, true }) {
                final AtomicInteger opened = new AtomicInteger();
                assertWriteFailsAndLeavesNothing(docSlots, directory -> directory, () -> {
                    final StringColumnValues values = cursor(docSlots);
                    if (opened.incrementAndGet() != total) {
                        return values;
                    }
                    final AtomicInteger advanced = new AtomicInteger();
                    return new HookedValues(values, () -> {
                        if (advanced.incrementAndGet() == failAtDoc) {
                            throw mergeAbort ? new MergePolicy.MergeAbortedException("aborted") : new IOException("unreadable value");
                        }
                    });
                });
            }
        }
    }

    public void testAFailureWritingOrClosingTheStagedFilesLeavesNone() throws IOException {
        final BytesRef[][] docSlots = descendingColumn();
        for (long failAfterBytes : new long[] { 0, 5, 200 }) {
            assertWriteFailsAndLeavesNothing(docSlots, directory -> new FailingStagedOutputs(directory, failAfterBytes, false), null);
        }
        assertWriteFailsAndLeavesNothing(docSlots, directory -> new FailingStagedOutputs(directory, Long.MAX_VALUE, true), null);
    }

    public void testAFailureCopyingTheStagedFilesLeavesNone() throws IOException {
        assertWriteFailsAndLeavesNothing(descendingColumn(), FailingStagedReads::new, null);
    }

    private int cursorsOpenedByASuccessfulWrite(BytesRef[][] docSlots) throws IOException {
        final AtomicInteger opened = new AtomicInteger();
        try (Directory real = newDirectory()) {
            write(real, docSlots, true, () -> {
                opened.incrementAndGet();
                return cursor(docSlots);
            });
        }
        return opened.get();
    }

    private void assertWriteFailsAndLeavesNothing(
        BytesRef[][] docSlots,
        UnaryOperator<Directory> wrap,
        IOSupplier<StringColumnValues> cursors
    ) throws IOException {
        try (Directory real = newDirectory()) {
            boolean failed = false;
            try {
                write(wrap.apply(real), docSlots, true, cursors == null ? () -> cursor(docSlots) : cursors);
            } catch (IOException e) {
                failed = true;
            }
            assertTrue("the injected failure did not fail the write", failed);
            assertNoStagedBounds(real, "after a failed write");
        }
    }

    private static void assertNoStagedBounds(Directory real, String when) throws IOException {
        for (String name : real.listAll()) {
            assertFalse("temporary file [" + name + "] left behind " + when, name.contains("columnar-plain-bounds"));
        }
    }

    private static final class HookedValues extends StringColumnValues {
        private final StringColumnValues delegate;
        private final IOAction onNextDoc;

        HookedValues(StringColumnValues delegate, IOAction onNextDoc) {
            this.delegate = delegate;
            this.onNextDoc = onNextDoc;
        }

        @Override
        public int valueCount() {
            return delegate.valueCount();
        }

        @Override
        public int nullCount() throws IOException {
            return delegate.nullCount();
        }

        @Override
        public void nextValue() throws IOException {
            delegate.nextValue();
        }

        @Override
        public BytesRef value() throws IOException {
            return delegate.value();
        }

        @Override
        public int valueLength() throws IOException {
            return delegate.valueLength();
        }

        @Override
        public void transferValue(BytesSink sink) throws IOException {
            delegate.transferValue(sink);
        }

        @Override
        public boolean streamsValues() {
            return delegate.streamsValues();
        }

        @Override
        public boolean mayBeSorted() {
            return delegate.mayBeSorted();
        }

        @Override
        public int docID() {
            return delegate.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            onNextDoc.run();
            return delegate.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return delegate.advance(target);
        }

        @Override
        public long cost() {
            return delegate.cost();
        }
    }

    private interface IOAction {
        void run() throws IOException;
    }

    private static final class FailingStagedOutputs extends FilterDirectory {
        private final long failAfterBytes;
        private final boolean failOnClose;

        FailingStagedOutputs(Directory in, long failAfterBytes, boolean failOnClose) {
            super(in);
            this.failAfterBytes = failAfterBytes;
            this.failOnClose = failOnClose;
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            final IndexOutput out = super.createTempOutput(prefix, suffix, context);
            return suffix.startsWith("columnar-plain-bounds") ? new FailingOutput(out, failAfterBytes, failOnClose) : out;
        }
    }

    private static final class FailingOutput extends IndexOutput {
        private final IndexOutput delegate;
        private final long failAfterBytes;
        private final boolean failOnClose;

        FailingOutput(IndexOutput delegate, long failAfterBytes, boolean failOnClose) {
            super(delegate.toString(), delegate.getName());
            this.delegate = delegate;
            this.failAfterBytes = failAfterBytes;
            this.failOnClose = failOnClose;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
            if (failOnClose) {
                throw new IOException("cannot close " + getName());
            }
        }

        @Override
        public long getFilePointer() {
            return delegate.getFilePointer();
        }

        @Override
        public long getChecksum() throws IOException {
            return delegate.getChecksum();
        }

        @Override
        public void writeByte(byte b) throws IOException {
            check(1);
            delegate.writeByte(b);
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            check(length);
            delegate.writeBytes(b, offset, length);
        }

        private void check(int length) throws IOException {
            if (delegate.getFilePointer() + length > failAfterBytes) {
                throw new IOException("cannot write " + getName());
            }
        }
    }

    private static final class FailingStagedReads extends FilterDirectory {
        FailingStagedReads(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            if (name.contains("columnar-plain-bounds")) {
                throw new IOException("cannot read " + name);
            }
            return super.openInput(name, context);
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
