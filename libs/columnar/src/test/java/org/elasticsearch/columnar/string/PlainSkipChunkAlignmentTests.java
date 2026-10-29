/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.greaterThan;

public class PlainSkipChunkAlignmentTests extends ColumnarStringTestCase {

    private static final int NO_VALUE_CAP = Integer.MAX_VALUE;

    public void testEntriesStartWhereTheByteCapCutsTheChunks() throws IOException {
        final int length = 8;
        final int chunkBytes = 100;
        final int numDocs = 1000;
        final BytesRef[][] docSlots = descending(numDocs, length);
        final StringColumnOptions.Sizes sizes = PlainStringColumnSkipperTests.sizes(128, chunkBytes, NO_VALUE_CAP);
        withColumnAndSkipIndex(docSlots, PlainStringColumnSkipperTests.plain(true, sizes), (metadata, reader, skip) -> {
            final List<PlainSkipFixture.Parsed> parsed = PlainSkipFixture.parse(skip, ((StringColumnMetadata.Plain) metadata).skipper());
            final int totalBytes = numDocs * length;
            assertEquals((totalBytes + chunkBytes - 1) / chunkBytes, parsed.size());
            for (int chunk = 0; chunk < parsed.size(); chunk++) {
                final int first = (chunk * chunkBytes + length - 1) / length;
                final int end = chunk + 1 < parsed.size() ? ((chunk + 1) * chunkBytes + length - 1) / length : numDocs;
                final PlainSkipFixture.Parsed entry = parsed.get(chunk);
                assertEquals(first, entry.firstSlot());
                assertEquals(first, entry.firstDoc());
                assertEquals(docSlots[first][0], entry.max());
                assertEquals(docSlots[end - 1][0], entry.min());
            }
            StringMatchOracle.assertRange(reader, docSlots, docSlots[numDocs - 1][0], true, docSlots[numDocs - 5][0], true);
            StringMatchOracle.assertRange(reader, docSlots, docSlots[700][0], false, docSlots[300][0], false);
        });
    }

    public void testOnlyTheChunksOfTheKeptEntryAndTheEdgesOfItsBlocksAreDecompressed() throws IOException {
        final int length = 24;
        final int chunkBytes = 4096;
        final int valuesPerBlock = 128;
        final int numDocs = 60 * valuesPerBlock;
        final BytesRef[][] docSlots = descending(numDocs, length);
        final int termSlot = numDocs / 2;
        final BytesRef term = docSlots[termSlot][0];
        final int totalChunks = (numDocs * length + chunkBytes - 1) / chunkBytes;
        final int ownerChunk = termSlot * length / chunkBytes;
        final int firstOwnedSlot = (ownerChunk * chunkBytes + length - 1) / length;
        final int lastOwnedSlot = ((ownerChunk + 1) * chunkBytes - 1) / length;
        final int firstChunkRead = firstOwnedSlot / valuesPerBlock * valuesPerBlock * length / chunkBytes;
        final int lastChunkRead = ((lastOwnedSlot / valuesPerBlock + 1) * valuesPerBlock * length - 1) / chunkBytes;
        final StringColumnOptions.Sizes sizes = PlainStringColumnSkipperTests.sizes(valuesPerBlock, chunkBytes, NO_VALUE_CAP);
        final long[] chunksRead = new long[2];
        for (int arm = 0; arm < 2; arm++) {
            final Counter seeks = new Counter();
            final int index = arm;
            withColumnAndSkipIndex(
                docSlots,
                PlainStringColumnSkipperTests.plain(arm == 0, sizes),
                directory -> new SeekCountingDirectory(directory, seeks),
                (metadata, reader, skip) -> {
                    assertEquals(totalChunks, ((StringColumnMetadata.Plain) metadata).values().chunks().numChunks());
                    final long before = seeks.get();
                    assertEquals(StringMatchOracle.expected(docSlots, term::equals), StringMatchOracle.matched(reader.matchTerm(term)));
                    chunksRead[index] = seeks.get() - before;
                }
            );
        }
        assertEquals("with the bounds", lastChunkRead - firstChunkRead + 1, chunksRead[0]);
        assertEquals("without the bounds", totalChunks, chunksRead[1]);
    }

    public void testAValueSpanningSeveralChunksIsBoundedByTheChunkHoldingItsFirstByte() throws IOException {
        final int length = 250;
        final int numDocs = 20;
        final BytesRef[][] docSlots = descending(numDocs, length);
        final StringColumnOptions.Sizes sizes = PlainStringColumnSkipperTests.sizes(128, 100, NO_VALUE_CAP);
        withColumnAndSkipIndex(docSlots, PlainStringColumnSkipperTests.plain(true, sizes), (metadata, reader, skip) -> {
            final List<PlainSkipFixture.Parsed> parsed = PlainSkipFixture.parse(skip, ((StringColumnMetadata.Plain) metadata).skipper());
            assertEquals(numDocs, parsed.size());
            for (int slot = 0; slot < numDocs; slot++) {
                assertEquals(slot, parsed.get(slot).firstSlot());
                assertEquals(docSlots[slot][0], parsed.get(slot).min());
                assertEquals(docSlots[slot][0], parsed.get(slot).max());
                final BytesRef term = docSlots[slot][0];
                StringMatchOracle.assertMatches(docSlots, term::equals, () -> reader.matchTerm(term));
            }
        });
    }

    public void testSlotsAfterAChunkThatIsNeverEmittedStillGetAnEntry() throws IOException {
        final int length = 10;
        final BytesRef[][] docSlots = new BytesRef[15][];
        final BytesRef[][] values = descending(10, length);
        System.arraycopy(values, 0, docSlots, 0, values.length);
        for (int doc = 10; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[] { null };
        }
        final StringColumnOptions.Sizes sizes = PlainStringColumnSkipperTests.sizes(128, 100, NO_VALUE_CAP);
        withColumnAndSkipIndex(docSlots, PlainStringColumnSkipperTests.plain(true, sizes), (metadata, reader, skip) -> {
            final StringColumnMetadata.Plain.Skipper skipper = ((StringColumnMetadata.Plain) metadata).skipper();
            assertNotNull(skipper);
            assertEquals(1, ((StringColumnMetadata.Plain) metadata).values().chunks().numChunks());
            final List<PlainSkipFixture.Parsed> parsed = PlainSkipFixture.parse(skip, skipper);
            assertEquals(2, parsed.size());
            assertEquals(10, parsed.get(1).firstSlot());
            assertEquals(10, parsed.get(1).firstDoc());
            assertNull(parsed.get(1).min());
            assertNull(parsed.get(1).max());
            StringMatchOracle.assertRange(reader, docSlots, null, true, null, true);
            StringMatchOracle.assertRange(reader, docSlots, values[9][0], true, values[0][0], true);
        });
    }

    public void testARepeatAtTheStartOfAChunkIsBoundedByTheChunkThatOwnsIt() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[12][];
        docSlots[0] = new BytesRef[] { value(99, 20) };
        for (int doc = 1; doc <= 8; doc++) {
            docSlots[doc] = new BytesRef[] { value(99 - doc, 10) };
        }
        docSlots[9] = new BytesRef[] { docSlots[8][0] };
        docSlots[10] = new BytesRef[] { value(80, 10) };
        docSlots[11] = new BytesRef[] { value(79, 10) };
        final StringColumnOptions.Sizes sizes = PlainStringColumnSkipperTests.sizes(128, 100, NO_VALUE_CAP);
        withColumnAndSkipIndex(docSlots, PlainStringColumnSkipperTests.plain(true, sizes), (metadata, reader, skip) -> {
            final List<PlainSkipFixture.Parsed> parsed = PlainSkipFixture.parse(skip, ((StringColumnMetadata.Plain) metadata).skipper());
            assertThat("chunks", parsed.size(), greaterThan(1));
            assertEquals(9, parsed.get(1).firstSlot());
            assertEquals(docSlots[8][0], parsed.get(1).max());
            assertEquals(docSlots[11][0], parsed.get(1).min());
            final BytesRef repeated = docSlots[8][0];
            StringMatchOracle.assertMatches(docSlots, repeated::equals, () -> reader.matchTerm(repeated));
            StringMatchOracle.assertRange(reader, docSlots, repeated, true, repeated, true);
        });
    }

    private static BytesRef[][] descending(int numDocs, int length) {
        final BytesRef[][] docSlots = new BytesRef[numDocs][];
        for (int doc = 0; doc < numDocs; doc++) {
            docSlots[doc] = new BytesRef[] { value(numDocs - doc, length) };
        }
        return docSlots;
    }

    private static BytesRef value(int id, int length) {
        final StringBuilder builder = new StringBuilder(String.format(Locale.ROOT, "v-%06d", id));
        while (builder.length() < length) {
            builder.append('x');
        }
        return new BytesRef(builder.substring(0, length));
    }
}
