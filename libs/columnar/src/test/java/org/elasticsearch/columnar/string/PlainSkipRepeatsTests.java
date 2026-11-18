/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.columnar.string;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.columnar.FormatVersion;
import org.elasticsearch.columnar.substrate.ColumnTestFiles;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

public class PlainSkipRepeatsTests extends ColumnarStringTestCase {

    private enum Order {
        ASCENDING,
        DESCENDING,
        SHUFFLED
    }

    public void testAColumnWrittenWithoutReadingItsRepeatsIsByteForByteTheSame() throws IOException {
        for (int iter = 0; iter < 40; iter++) {
            final Order order = randomFrom(Order.values());
            final boolean constantLength = randomBoolean();
            final BytesRef[][] docSlots = randomColumn(order, constantLength);
            final StringColumnOptions options = PlainStringColumnSkipperTests.plain(
                true,
                PlainStringColumnSkipperTests.sizes(randomFrom(128, 256), between(128, 1024))
            );
            final byte[] segmentId = new byte[16];
            random().nextBytes(segmentId);
            try (Directory everyValueRead = new ByteBuffersDirectory(); Directory repeatsSkipped = new ByteBuffersDirectory()) {
                write(everyValueRead, segmentId, docSlots, options, () -> cursor(docSlots));
                final Counts counts = new Counts();
                final boolean mayBeSorted = randomBoolean();
                write(repeatsSkipped, segmentId, docSlots, options, () -> new ReportsRepeats(cursor(docSlots), mayBeSorted, counts));
                assertSameFiles(everyValueRead, repeatsSkipped);
                assertEquals("only the slots not reported as repeats are read", numValues(docSlots) - counts.reported, counts.reads);
            }
        }
    }

    private static void write(
        Directory dir,
        byte[] segmentId,
        BytesRef[][] docSlots,
        StringColumnOptions options,
        IOSupplier<StringColumnValues> cursors
    ) throws IOException {
        final StringColumnMetadata written;
        try (ColumnTestFiles.Outputs out = ColumnTestFiles.create(dir, "column", segmentId)) {
            written = StringColumnWriter.write(
                docSlots.length,
                totals(docSlots),
                cursors,
                options,
                null,
                null,
                dir,
                IOContext.DEFAULT,
                out.outputs(),
                out.skipIndex()
            );
        }
        try (IndexOutput meta = dir.createOutput("metadata", IOContext.DEFAULT)) {
            written.writeTo(meta, FormatVersion.CURRENT);
        }
    }

    private static void assertSameFiles(Directory expected, Directory actual) throws IOException {
        final String[] names = expected.listAll();
        final String[] actualNames = actual.listAll();
        Arrays.sort(names);
        Arrays.sort(actualNames);
        assertArrayEquals(names, actualNames);
        for (String name : names) {
            assertEquals(name, expected.fileLength(name), actual.fileLength(name));
            try (
                IndexInput left = expected.openInput(name, IOContext.READONCE);
                IndexInput right = actual.openInput(name, IOContext.READONCE)
            ) {
                final byte[] leftBytes = new byte[(int) left.length()];
                final byte[] rightBytes = new byte[(int) right.length()];
                left.readBytes(leftBytes, 0, leftBytes.length);
                right.readBytes(rightBytes, 0, rightBytes.length);
                assertArrayEquals(name, leftBytes, rightBytes);
            }
        }
    }

    /** Runs of equal values in term order, so a region is written, or shuffled so it is not, with nulls and gaps unless of one length. */
    private static BytesRef[][] randomColumn(Order order, boolean constantLength) {
        final int distinct = between(5, 400);
        final int[] rank = new int[distinct];
        for (int i = 0; i < distinct; i++) {
            rank[i] = order == Order.DESCENDING ? distinct - 1 - i : i;
        }
        final BytesRef[][] docSlots = new BytesRef[between(50, 2000)][];
        int run = 0;
        int term = 0;
        for (int doc = 0; doc < docSlots.length; doc++) {
            if (constantLength == false && rarely()) {
                continue;
            }
            docSlots[doc] = new BytesRef[constantLength ? 1 : between(1, 3)];
            for (int slot = 0; slot < docSlots[doc].length; slot++) {
                if (constantLength == false && rarely()) {
                    docSlots[doc][slot] = null;
                    continue;
                }
                if (run == 0) {
                    run = between(1, 30);
                    term = order == Order.SHUFFLED ? randomIntBetween(0, distinct - 1) : Math.min(term + 1, distinct - 1);
                }
                run--;
                final String value = String.format(Locale.ROOT, "value-%06d", rank[term]);
                docSlots[doc][slot] = new BytesRef(constantLength ? value : value + "x".repeat(rank[term] % 3));
            }
        }
        if (numDocsWithField(docSlots) == 0) {
            docSlots[0] = new BytesRef[] { new BytesRef("value-000000") };
        }
        return docSlots;
    }

    private static final class Counts {
        int reads;
        int reported;
    }

    /**
     * A cursor that, like the one a merge of plain columns reads from, says which values repeat the one before and may
     * miss some, and counts the values it is asked for.
     */
    private static final class ReportsRepeats extends StringColumnValues {
        private final StringColumnValues in;
        private final boolean mayBeSorted;
        private final Counts counts;
        private BytesRef previous;
        private boolean repeats;

        ReportsRepeats(StringColumnValues in, boolean mayBeSorted, Counts counts) {
            this.in = in;
            this.mayBeSorted = mayBeSorted;
            this.counts = counts;
        }

        @Override
        public boolean streamsValues() {
            return true;
        }

        @Override
        public boolean mayBeSorted() {
            return mayBeSorted;
        }

        @Override
        public void nextValue() throws IOException {
            in.nextValue();
            final BytesRef value = in.value();
            repeats = previous != null && value != null && previous.bytesEquals(value) && rarely() == false;
            previous = value == null ? null : BytesRef.deepCopyOf(value);
            if (repeats) {
                counts.reported++;
            }
        }

        @Override
        public boolean repeatsPrevious() {
            return repeats;
        }

        @Override
        public BytesRef value() throws IOException {
            counts.reads++;
            return in.value();
        }

        @Override
        public int valueCount() throws IOException {
            return in.valueCount();
        }

        @Override
        public int nullCount() throws IOException {
            return in.nullCount();
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return in.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return in.advance(target);
        }

        @Override
        public long cost() {
            return in.cost();
        }
    }
}
