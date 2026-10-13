/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;

public class PlainSkipIndexCodecTests extends ESTestCase {

    private static final int INTERVAL = PlainSkipIndexCodec.DEFAULT_INTERVAL_SIZE;
    private static final int ROOT_INTERVALS = 1 << (PlainSkipIndexCodec.DEFAULT_LEVEL_SHIFT * (PlainSkipIndexCodec.DEFAULT_MAX_LEVEL - 1));

    private static final PlainSkipIndexCodec KEEP_ALL = new PlainSkipIndexCodec(
        PlainSkipIndexCodec.DEFAULT_INTERVAL_SIZE,
        PlainSkipIndexCodec.DEFAULT_MAX_LEVEL,
        PlainSkipIndexCodec.DEFAULT_LEVEL_SHIFT,
        PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES,
        2.0,
        PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS
    );

    private record Region(byte[] bytes, StringColumnMetadata.Plain.Skipper meta) {
        IndexInput input(byte[] contents) {
            return new ByteBuffersIndexInput(new ByteBuffersDataInput(List.of(ByteBuffer.wrap(contents))), "skip");
        }

        PlainStringColumnSkipper skipper(byte[] contents) throws IOException {
            return new PlainStringColumnSkipper(meta, input(contents));
        }
    }

    private interface Documents {
        BytesRef valueOf(int doc);
    }

    private static Region write(int docs, Documents documents) throws IOException {
        return write(KEEP_ALL, docs, documents);
    }

    private static Region write(PlainSkipIndexCodec codec, int docs, Documents documents) throws IOException {
        return write(codec, false, docs, documents);
    }

    private static Region write(PlainSkipIndexCodec codec, boolean bisected, int docs, Documents documents) throws IOException {
        final ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
        final PlainSkipIndexCodec.Writer writer;
        final StringColumnMetadata.Plain.Skipper meta;
        try (ByteBuffersIndexOutput out = new ByteBuffersIndexOutput(buffer, "skip", "skip")) {
            writer = codec.writer(out);
            for (int doc = 0; doc < docs; doc++) {
                writer.startDoc(doc);
                final BytesRef value = documents.valueOf(doc);
                if (value != null) {
                    writer.add(value);
                }
            }
            meta = writer.finish(bisected);
        }
        return new Region(buffer.toArrayCopy(), meta);
    }

    private static BytesRef ascending(int doc) {
        return new BytesRef(String.format(Locale.ROOT, "%08d", doc));
    }

    public void testRootGroupIsSteppedOverAsOne() throws IOException {
        final int docs = INTERVAL * (ROOT_INTERVALS + 1);
        final Region region = write(docs, PlainSkipIndexCodecTests::ascending);

        final PlainStringColumnSkipper first = region.skipper(region.bytes());
        first.advance(0);
        assertEquals(PlainSkipIndexCodec.DEFAULT_MAX_LEVEL, first.numLevels());
        assertEquals(INTERVAL * ROOT_INTERVALS - 1, first.maxDocID(PlainSkipIndexCodec.DEFAULT_MAX_LEVEL - 1));
        assertEquals(ascending(0), first.minTerm(PlainSkipIndexCodec.DEFAULT_MAX_LEVEL - 1));
        assertEquals(ascending(INTERVAL * ROOT_INTERVALS - 1), first.maxTerm(PlainSkipIndexCodec.DEFAULT_MAX_LEVEL - 1));

        final PlainStringColumnSkipper beyond = region.skipper(region.bytes());
        final int target = INTERVAL * ROOT_INTERVALS + between(0, INTERVAL - 1);
        beyond.advance(target);
        assertEquals(1, beyond.numLevels());
        assertEquals(INTERVAL * ROOT_INTERVALS, beyond.minDocID(0));
        assertEquals(docs - 1, beyond.maxDocID(0));
        assertEquals(ascending(INTERVAL * ROOT_INTERVALS), beyond.minTerm(0));
        assertEquals(ascending(docs - 1), beyond.maxTerm(0));
    }

    public void testEveryTargetFindsTheIntervalHoldingIt() throws IOException {
        final int docs = INTERVAL * between(3, 80) + between(0, INTERVAL - 1);
        final Region region = write(docs, PlainSkipIndexCodecTests::ascending);
        final PlainStringColumnSkipper skipper = region.skipper(region.bytes());
        int target = 0;
        while (target < docs) {
            skipper.advance(target);
            final int first = target / INTERVAL * INTERVAL;
            final int last = Math.min(first + INTERVAL, docs) - 1;
            assertEquals(first, skipper.minDocID(0));
            assertEquals(last, skipper.maxDocID(0));
            assertEquals(ascending(first), skipper.minTerm(0));
            assertEquals(ascending(last), skipper.maxTerm(0));
            assertEquals(last - first + 1, skipper.docCount(0));
            target = last + 1 + between(0, INTERVAL * 3);
        }
        skipper.advance(docs);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper.maxDocID(0));
    }

    public void testLongBoundsStayConservativeAndShort() throws IOException {
        final int docs = between(1, INTERVAL);
        final BytesRef[] values = new BytesRef[docs];
        final byte[] prefix = randomByteArrayOfLength(between(0, 2 * PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES));
        for (int doc = 0; doc < docs; doc++) {
            final byte[] tail = randomByteArrayOfLength(between(0, 3 * PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES));
            final byte[] value = new byte[prefix.length + tail.length];
            System.arraycopy(prefix, 0, value, 0, prefix.length);
            System.arraycopy(tail, 0, value, prefix.length, tail.length);
            values[doc] = new BytesRef(value);
        }
        final Region region = write(docs, doc -> values[doc]);
        final PlainStringColumnSkipper skipper = region.skipper(region.bytes());
        skipper.advance(0);
        assertTrue(skipper.minTerm(0).length <= PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES);
        assertTrue(skipper.maxTerm(0).length <= PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES);
        for (BytesRef value : values) {
            assertTrue(skipper.minTerm(0).compareTo(value) <= 0);
            assertTrue(skipper.maxTerm(0).compareTo(value) >= 0);
        }
    }

    public void testABoundOfRaisableBytesIsRaisedPastTheValue() throws IOException {
        final byte[] value = new byte[PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES + between(1, 100)];
        java.util.Arrays.fill(value, (byte) 0xFF);
        value[PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES - 1 - between(0, 5)] = (byte) between(0, 0xFE);
        final BytesRef bytes = new BytesRef(value);
        final Region region = write(1, doc -> bytes);
        final PlainStringColumnSkipper skipper = region.skipper(region.bytes());
        skipper.advance(0);
        assertTrue(skipper.maxTerm(0).compareTo(bytes) > 0);
        assertTrue(skipper.maxTerm(0).length <= PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES);
    }

    public void testAValueOfOnlyMaxBytesKeepsItsWholeBound() throws IOException {
        final byte[] value = new byte[PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES + 10];
        java.util.Arrays.fill(value, (byte) 0xFF);
        final BytesRef bytes = new BytesRef(value);
        final Region region = write(1, doc -> bytes);
        final PlainStringColumnSkipper skipper = region.skipper(region.bytes());
        skipper.advance(0);
        assertEquals(bytes, skipper.maxTerm(0));
    }

    public void testHugeValuesDoNotGrowTheRegionOrOverflowAGroupLength() throws IOException {
        final byte[] huge = new byte[1 << 16];
        java.util.Arrays.fill(huge, (byte) 'a');
        final BytesRef value = new BytesRef(huge);
        final int docs = INTERVAL * (ROOT_INTERVALS + 1);
        final Region region = write(docs, doc -> doc % INTERVAL == 0 ? value : null);
        final long maxEntryBytes = 1 + PlainSkipIndexCodec.DEFAULT_MAX_LEVEL * (4L * Integer.BYTES + 2 * (5
            + PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES));
        assertTrue("region of [" + region.bytes().length + "] bytes", region.bytes().length <= (ROOT_INTERVALS + 1) * maxEntryBytes);
        final PlainStringColumnSkipper skipper = region.skipper(region.bytes());
        skipper.advance(INTERVAL * ROOT_INTERVALS);
        assertEquals(INTERVAL * ROOT_INTERVALS, skipper.minDocID(0));
    }

    public void testAnyGeometryIsReadBackFromTheRegion() throws IOException {
        final int intervalSize = between(1, 40);
        final int levelShift = between(1, 3);
        final int maxLevel = between(1, 4);
        final PlainSkipIndexCodec codec = new PlainSkipIndexCodec(
            intervalSize,
            maxLevel,
            levelShift,
            PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES,
            2.0,
            PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS
        );
        final int rootIntervals = 1 << (levelShift * (maxLevel - 1));
        final int docs = intervalSize * between(1, 3 * rootIntervals) + between(0, intervalSize - 1);
        final Region region = write(codec, docs, PlainSkipIndexCodecTests::ascending);
        assertEquals(maxLevel, region.meta().maxLevel());
        assertEquals(docs - 1, region.meta().maxDocId());

        final PlainStringColumnSkipper skipper = region.skipper(region.bytes());
        int target = 0;
        while (target < docs) {
            skipper.advance(target);
            final int first = target / intervalSize * intervalSize;
            final int last = Math.min(first + intervalSize, docs) - 1;
            assertTrue(skipper.numLevels() <= maxLevel);
            assertEquals(first, skipper.minDocID(0));
            assertEquals(last, skipper.maxDocID(0));
            assertEquals(ascending(first), skipper.minTerm(0));
            assertEquals(ascending(last), skipper.maxTerm(0));
            assertEquals(last - first + 1, skipper.docCount(0));
            for (int level = 1; level < skipper.numLevels(); level++) {
                assertTrue(skipper.minDocID(level) <= first);
                assertTrue(skipper.maxDocID(level) >= last);
                assertTrue(skipper.minTerm(level).compareTo(ascending(first)) <= 0);
                assertTrue(skipper.maxTerm(level).compareTo(ascending(last)) >= 0);
            }
            target = last + 1 + between(0, intervalSize * 3);
        }
        skipper.advance(docs);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper.maxDocID(0));
    }

    public void testAnyBoundCapKeepsBoundsConservative() throws IOException {
        final int cap = between(1, 40);
        final PlainSkipIndexCodec codec = new PlainSkipIndexCodec(
            PlainSkipIndexCodec.DEFAULT_INTERVAL_SIZE,
            PlainSkipIndexCodec.DEFAULT_MAX_LEVEL,
            PlainSkipIndexCodec.DEFAULT_LEVEL_SHIFT,
            cap,
            2.0,
            PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS
        );
        final int docs = between(1, 200);
        final BytesRef[] values = new BytesRef[docs];
        for (int doc = 0; doc < docs; doc++) {
            final byte[] bytes = randomByteArrayOfLength(between(0, 3 * cap));
            for (int i = 0; i < bytes.length; i++) {
                if (bytes[i] == (byte) 0xFF) {
                    bytes[i] = (byte) 0xFE;
                }
            }
            values[doc] = new BytesRef(bytes);
        }
        final Region region = write(codec, docs, doc -> values[doc]);
        final PlainStringColumnSkipper skipper = region.skipper(region.bytes());
        skipper.advance(0);
        assertTrue(skipper.minTerm(0).length <= cap);
        assertTrue(skipper.maxTerm(0).length <= cap);
        for (BytesRef value : values) {
            assertTrue(skipper.minTerm(0).compareTo(value) <= 0);
            assertTrue(skipper.maxTerm(0).compareTo(value) >= 0);
        }
    }

    public void testBoundsAreTheExactSmallestAndLargestValues() throws IOException {
        final int intervalSize = between(1, 20);
        final PlainSkipIndexCodec codec = new PlainSkipIndexCodec(
            intervalSize,
            4,
            3,
            1024,
            2.0,
            PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS
        );
        final int docs = between(1, 400);
        final byte[] alphabet = { 0, 1, 0x7F, (byte) 0x80, (byte) 0xFF };
        final byte[] shared = new byte[between(6, 10)];
        for (int i = 0; i < shared.length; i++) {
            shared[i] = alphabet[between(0, alphabet.length - 1)];
        }
        final BytesRef[] values = new BytesRef[docs];
        for (int doc = 0; doc < docs; doc++) {
            if (rarely()) {
                continue;
            }
            final byte[] head = randomBoolean() ? shared : new byte[0];
            final byte[] tail = new byte[between(0, 6)];
            for (int i = 0; i < tail.length; i++) {
                tail[i] = alphabet[between(0, alphabet.length - 1)];
            }
            final byte[] value = new byte[head.length + tail.length];
            System.arraycopy(head, 0, value, 0, head.length);
            System.arraycopy(tail, 0, value, head.length, tail.length);
            values[doc] = new BytesRef(value);
        }
        final Region region = write(codec, docs, doc -> values[doc]);
        final PlainStringColumnSkipper skipper = region.skipper(region.bytes());
        for (int first = 0; first < docs; first += intervalSize) {
            final int last = Math.min(first + intervalSize, docs) - 1;
            BytesRef min = null;
            BytesRef max = null;
            for (int doc = first; doc <= last; doc++) {
                if (values[doc] == null) {
                    continue;
                }
                min = min == null || values[doc].compareTo(min) < 0 ? values[doc] : min;
                max = max == null || values[doc].compareTo(max) > 0 ? values[doc] : max;
            }
            skipper.advance(first);
            assertEquals(min == null ? new BytesRef() : min, skipper.minTerm(0));
            assertEquals(max == null ? new BytesRef() : max, skipper.maxTerm(0));
        }
    }

    public void testAnInvalidGeometryIsRejected() {
        final int size = PlainSkipIndexCodec.DEFAULT_INTERVAL_SIZE;
        final int shift = PlainSkipIndexCodec.DEFAULT_LEVEL_SHIFT;
        final int bound = PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES;
        final double overlap = PlainSkipIndexCodec.DEFAULT_MAX_OVERLAP_FRACTION;
        expectThrows(
            IllegalArgumentException.class,
            () -> new PlainSkipIndexCodec(0, 4, shift, bound, overlap, PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new PlainSkipIndexCodec(size, 0, shift, bound, overlap, PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new PlainSkipIndexCodec(size, 4, 0, bound, overlap, PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new PlainSkipIndexCodec(size, 4, shift, 0, overlap, PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new PlainSkipIndexCodec(size, 12, shift, bound, overlap, PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new PlainSkipIndexCodec(size, 4, shift, bound, 0, PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new PlainSkipIndexCodec(size, 4, shift, bound, -0.5, PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new PlainSkipIndexCodec(size, 4, shift, bound, Double.NaN, PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS)
        );
        for (int sampleIntervals : new int[] { Integer.MIN_VALUE, -1, 0, 1 }) {
            expectThrows(IllegalArgumentException.class, () -> new PlainSkipIndexCodec(size, 4, shift, bound, overlap, sampleIntervals));
        }
        assertNotNull(new PlainSkipIndexCodec(size, 4, shift, bound, overlap, 2));
        assertNotNull(new PlainSkipIndexCodec(size, 4, shift, bound, overlap, ROOT_INTERVALS * 4));
        assertNotNull(new PlainSkipIndexCodec(size, 11, shift, bound, overlap, PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS));
    }

    public void testAColumnWhoseIntervalsOverlapGetsNoRegion() throws IOException {
        final int intervals = between(2, 40);
        final Region region = write(PlainSkipIndexCodec.DEFAULT, intervals * INTERVAL, doc -> new BytesRef(randomAlphaOfLength(8)));
        assertNull(region.meta());
        assertEquals(0, region.bytes().length);
    }

    public void testAColumnWithLocalityKeepsItsRegion() throws IOException {
        final Region region = write(PlainSkipIndexCodec.DEFAULT, INTERVAL * between(2, 40), PlainSkipIndexCodecTests::ascending);
        assertNotNull(region.meta());
        assertTrue(region.bytes().length > 0);
    }

    public void testADisabledWriterKeepsAndWritesNothing() throws IOException {
        final PlainSkipIndexCodec.Writer writer = PlainSkipIndexCodec.DEFAULT.disabledWriter();
        for (int doc = 0; doc < INTERVAL * between(1, 20); doc++) {
            writer.startDoc(doc);
            writer.add(ascending(doc));
        }
        assertNull(writer.finish(randomBoolean()));
    }

    public void testAColumnTheReaderBisectsGetsNoRegion() throws IOException {
        final int sampleIntervals = PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS;
        for (int intervals : new int[] { 2, sampleIntervals, sampleIntervals + 4, 40 }) {
            final Region region = write(KEEP_ALL, true, INTERVAL * intervals, PlainSkipIndexCodecTests::ascending);
            assertNull("[" + intervals + "] intervals", region.meta());
            assertEquals("[" + intervals + "] intervals", 0, region.bytes().length);
        }
    }

    public void testTheSampleDecidesAndLaterDocumentsWriteNothing() throws IOException {
        final int intervalSize = between(4, 16);
        final int sampleIntervals = between(2, 30);
        final PlainSkipIndexCodec codec = codecSamplingIntervals(intervalSize, sampleIntervals);
        final int sampleDocs = intervalSize * sampleIntervals;
        final Region region = write(codec, sampleDocs * between(2, 4) + between(0, intervalSize), doc -> randomValue());
        assertNull(region.meta());
        assertEquals(0, region.bytes().length);
    }

    public void testAColumnThatTurnsRandomAfterTheSampleKeepsItsRegion() throws IOException {
        final int intervalSize = between(1, 8);
        final int sampleIntervals = between(2, 30);
        final PlainSkipIndexCodec codec = codecSamplingIntervals(intervalSize, sampleIntervals);
        final int sampleDocs = intervalSize * sampleIntervals;
        final Region region = write(codec, sampleDocs * 2, doc -> doc < sampleDocs ? ascending(doc) : randomValue());
        assertNotNull(region.meta());
        assertEquals(sampleDocs * 2 - 1, region.meta().maxDocId());
    }

    public void testTheDecisionIsTheSameWhetherTheSampleEndsTheColumnOrNot() throws IOException {
        final int intervalSize = between(4, 16);
        final int sampleIntervals = between(2, 30);
        final PlainSkipIndexCodec codec = codecSamplingIntervals(intervalSize, sampleIntervals);
        final int sampleDocs = intervalSize * sampleIntervals;
        for (int docs : new int[] { sampleDocs, sampleDocs + 1 }) {
            assertNull("random, " + docs + " docs", write(codec, docs, doc -> randomValue()).meta());
            final int half = (docs + 1) / 2;
            assertNotNull("two ascending runs, " + docs + " docs", write(codec, docs, doc -> ascending(doc % half)).meta());
        }
    }

    public void testAColumnOfOneIntervalGetsNoRegionAndTwoDisjointIntervalsKeepOne() throws IOException {
        final PlainSkipIndexCodec codec = PlainSkipIndexCodec.DEFAULT;
        assertNull(write(codec, between(1, INTERVAL), PlainSkipIndexCodecTests::ascending).meta());
        assertNotNull(write(codec, INTERVAL * 2, PlainSkipIndexCodecTests::ascending).meta());
    }

    public void testAnIntervalWithNoTermIsNeverCountedAsOverlapping() throws IOException {
        final Region oneWithTerms = write(PlainSkipIndexCodec.DEFAULT, INTERVAL * 2, doc -> doc < INTERVAL ? ascending(doc) : null);
        assertNotNull(oneWithTerms.meta());
        final Region none = write(PlainSkipIndexCodec.DEFAULT, INTERVAL * between(2, 6), doc -> null);
        assertNull("nothing to bound, so nothing to skip", none.meta());
        assertEquals(0, none.bytes().length);
    }

    public void testTheRegionBytesFollowTheDocumentedLayout() throws IOException {
        final PlainSkipIndexCodec codec = new PlainSkipIndexCodec(2, 2, 1, 4, 2.0, 2);
        final BytesRef[] values = { new BytesRef("a"), new BytesRef("b"), new BytesRef("c"), new BytesRef("d") };
        final Region region = write(codec, values.length, doc -> values[doc]);

        final ByteBuffersDataOutput expected = new ByteBuffersDataOutput();
        expected.writeByte((byte) 2);
        writeRecord(expected, 3, 62, 0, 4, "a", "d");
        writeRecord(expected, 1, 41, 0, 2, "a", "b");
        expected.writeByte((byte) 1);
        writeRecord(expected, 3, 21, 2, 2, "c", "d");
        assertArrayEquals(expected.toArrayCopy(), region.bytes());

        final ByteBuffersDataOutput metadata = new ByteBuffersDataOutput();
        region.meta().writeTo(metadata);
        assertArrayEquals(new byte[] { 0, 62, 3, 2, 4, 0 }, metadata.toArrayCopy());
    }

    private static void writeRecord(
        ByteBuffersDataOutput out,
        int maxDocId,
        int groupBytes,
        int minDocId,
        int docCount,
        String min,
        String max
    ) throws IOException {
        out.writeInt(maxDocId);
        out.writeInt(groupBytes);
        out.writeInt(minDocId);
        out.writeInt(docCount);
        out.writeVInt(min.length());
        out.writeBytes(min.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        out.writeVInt(max.length());
        out.writeBytes(max.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static PlainSkipIndexCodec codecSamplingIntervals(int intervalSize, int sampleIntervals) {
        return new PlainSkipIndexCodec(
            intervalSize,
            PlainSkipIndexCodec.DEFAULT_MAX_LEVEL,
            PlainSkipIndexCodec.DEFAULT_LEVEL_SHIFT,
            PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES,
            PlainSkipIndexCodec.DEFAULT_MAX_OVERLAP_FRACTION,
            sampleIntervals
        );
    }

    private BytesRef randomValue() {
        return new BytesRef(randomAlphaOfLength(8));
    }

    public void testTheOverlapLimitDecidesFromTheIntervalRanges() throws IOException {
        final int intervalSize = between(1, 6);
        final int intervals = between(1, 40);
        final int docs = intervalSize * intervals;
        final BytesRef[] values = new BytesRef[docs];
        final int spread = between(1, 30);
        for (int doc = 0; doc < docs; doc++) {
            final int base = doc / intervalSize * between(0, 3);
            values[doc] = new BytesRef(String.format(Locale.ROOT, "%04d", base + between(0, spread)));
        }
        final BytesRef[] mins = new BytesRef[intervals];
        final BytesRef[] maxs = new BytesRef[intervals];
        for (int doc = 0; doc < docs; doc++) {
            final int interval = doc / intervalSize;
            if (mins[interval] == null || values[doc].compareTo(mins[interval]) < 0) {
                mins[interval] = values[doc];
            }
            if (maxs[interval] == null || values[doc].compareTo(maxs[interval]) > 0) {
                maxs[interval] = values[doc];
            }
        }
        int overlapping = 0;
        for (int i = 0; i < intervals; i++) {
            for (int j = 0; j < intervals; j++) {
                if (mins[i].compareTo(maxs[j]) <= 0 && mins[j].compareTo(maxs[i]) <= 0) {
                    overlapping++;
                }
            }
        }
        final double fraction = overlapping / (double) (intervals * intervals);
        final double limit = fraction == 0 ? 1 : randomBoolean() ? fraction : Math.nextUp(fraction);
        final PlainSkipIndexCodec codec = new PlainSkipIndexCodec(
            intervalSize,
            PlainSkipIndexCodec.DEFAULT_MAX_LEVEL,
            PlainSkipIndexCodec.DEFAULT_LEVEL_SHIFT,
            PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES,
            limit,
            Math.max(2, intervals)
        );
        final Region region = write(codec, docs, doc -> values[doc]);
        if (fraction >= limit) {
            assertNull(region.meta());
        } else {
            assertNotNull(region.meta());
        }
    }

    public void testALevelCountOutsideWhatAnEntryCanHoldIsCorrupt() throws IOException {
        final Region region = write(PlainSkipIndexCodec.DEFAULT_INTERVAL_SIZE, PlainSkipIndexCodecTests::ascending);
        for (int maxLevel : new int[] { 0, -1, Byte.MAX_VALUE + 1 }) {
            final StringColumnMetadata.Plain.Skipper meta = new StringColumnMetadata.Plain.Skipper(
                region.meta().dataOffset(),
                region.meta().dataLength(),
                region.meta().maxDocId(),
                maxLevel,
                region.meta().maxBoundBytes(),
                region.meta().codecId()
            );
            expectThrows(CorruptIndexException.class, () -> new PlainStringColumnSkipper(meta, region.input(region.bytes())));
        }
    }

    public void testABoundCapBelowOneIsCorrupt() throws IOException {
        final Region region = write(PlainSkipIndexCodec.DEFAULT_INTERVAL_SIZE, PlainSkipIndexCodecTests::ascending);
        for (int maxBoundBytes : new int[] { 0, -1, Integer.MIN_VALUE }) {
            final StringColumnMetadata.Plain.Skipper meta = new StringColumnMetadata.Plain.Skipper(
                region.meta().dataOffset(),
                region.meta().dataLength(),
                region.meta().maxDocId(),
                region.meta().maxLevel(),
                maxBoundBytes,
                region.meta().codecId()
            );
            expectThrows(CorruptIndexException.class, () -> new PlainStringColumnSkipper(meta, region.input(region.bytes())));
        }
    }

    public void testTheRegionRecordsTheBoundCapAndItsMetadataRoundTrips() throws IOException {
        final int cap = between(1, 300);
        final PlainSkipIndexCodec codec = new PlainSkipIndexCodec(
            between(1, 40),
            PlainSkipIndexCodec.DEFAULT_MAX_LEVEL,
            PlainSkipIndexCodec.DEFAULT_LEVEL_SHIFT,
            cap,
            2.0,
            PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS
        );
        final Region region = write(codec, between(1, 300), PlainSkipIndexCodecTests::ascending);
        assertEquals(cap, region.meta().maxBoundBytes());

        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        region.meta().writeTo(out);
        assertEquals(region.meta(), StringColumnMetadata.Plain.Skipper.readFrom(out.toDataInput()));
    }

    public void testBoundsAreExactExactlyWhenNoValueIsLongerThanTheCap() {
        final int cap = between(1, 300);
        final StringColumnMetadata.Plain.Skipper meta = new StringColumnMetadata.Plain.Skipper(
            0,
            0,
            0,
            PlainSkipIndexCodec.DEFAULT_MAX_LEVEL,
            cap,
            PlainSkipIndexCodec.ID
        );
        assertTrue(meta.boundsAreExact(0));
        assertTrue(meta.boundsAreExact(cap - 1));
        assertTrue(meta.boundsAreExact(cap));
        assertFalse(meta.boundsAreExact(cap + 1));
        assertFalse(meta.boundsAreExact(Integer.MAX_VALUE));
    }

    public void testAValueOneByteOverTheCapIsCutAndTheBoundsAreNotExact() throws IOException {
        final int cap = between(2, 40);
        final PlainSkipIndexCodec codec = new PlainSkipIndexCodec(
            PlainSkipIndexCodec.DEFAULT_INTERVAL_SIZE,
            PlainSkipIndexCodec.DEFAULT_MAX_LEVEL,
            PlainSkipIndexCodec.DEFAULT_LEVEL_SHIFT,
            cap,
            2.0,
            PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS
        );
        final BytesRef fits = new BytesRef("a".repeat(cap));
        final BytesRef over = new BytesRef("a".repeat(cap) + "b");
        final BytesRef[] values = { fits, over };

        final Region region = write(codec, values.length, doc -> values[doc]);
        assertTrue(region.meta().boundsAreExact(cap));
        assertFalse(region.meta().boundsAreExact(over.length));

        final PlainStringColumnSkipper skipper = region.skipper(region.bytes());
        skipper.advance(0);
        assertEquals(fits, skipper.minTerm(0));
        assertNotEquals("the longer value is cut, so its bound is not the value", over, skipper.maxTerm(0));
        assertTrue(skipper.maxTerm(0).compareTo(over) > 0);
    }

    public void testBoundsAreTheTrueMinAndMaxWheneverTheColumnFitsTheCap() throws IOException {
        final int cap = between(1, 24);
        final PlainSkipIndexCodec codec = new PlainSkipIndexCodec(
            PlainSkipIndexCodec.DEFAULT_INTERVAL_SIZE,
            PlainSkipIndexCodec.DEFAULT_MAX_LEVEL,
            PlainSkipIndexCodec.DEFAULT_LEVEL_SHIFT,
            cap,
            2.0,
            PlainSkipIndexCodec.DEFAULT_LOCALITY_SAMPLE_INTERVALS
        );
        final int docs = between(1, 200);
        final BytesRef[] values = new BytesRef[docs];
        int columnMaxLength = 0;
        BytesRef trueMin = null;
        BytesRef trueMax = null;
        for (int doc = 0; doc < docs; doc++) {
            final byte[] bytes = randomByteArrayOfLength(between(0, cap + 2));
            values[doc] = new BytesRef(bytes);
            columnMaxLength = Math.max(columnMaxLength, bytes.length);
            trueMin = trueMin == null || values[doc].compareTo(trueMin) < 0 ? values[doc] : trueMin;
            trueMax = trueMax == null || values[doc].compareTo(trueMax) > 0 ? values[doc] : trueMax;
        }
        final Region region = write(codec, docs, doc -> values[doc]);
        final PlainStringColumnSkipper skipper = region.skipper(region.bytes());
        skipper.advance(0);
        if (region.meta().boundsAreExact(columnMaxLength)) {
            assertEquals(trueMin, skipper.minTerm(0));
            assertEquals(trueMax, skipper.maxTerm(0));
        } else {
            assertTrue(skipper.minTerm(0).compareTo(trueMin) <= 0);
            assertTrue(skipper.maxTerm(0).compareTo(trueMax) >= 0);
        }
    }

    public void testAJumpThatDoesNotMoveForwardIsCorrupt() throws IOException {
        final Region region = write(INTERVAL * 2, PlainSkipIndexCodecTests::ascending);
        final int firstGroupBytesAt = 1 + Integer.BYTES;
        for (int jump : new int[] { 0, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, region.bytes().length + 1 }) {
            final byte[] corrupt = region.bytes().clone();
            ByteBuffer.wrap(corrupt).putInt(firstGroupBytesAt, jump);
            final PlainStringColumnSkipper skipper = region.skipper(corrupt);
            expectThrows(CorruptIndexException.class, () -> skipper.advance(INTERVAL));
        }
    }

    public void testABoundLongerThanTheRegionIsCorrupt() throws IOException {
        final Region region = write(1, doc -> new BytesRef("abc"));
        final int minLengthAt = 1 + 4 * Integer.BYTES;
        final int maxLengthAt = minLengthAt + 1 + 3;
        assertEquals(3, region.bytes()[minLengthAt]);
        assertEquals(3, region.bytes()[maxLengthAt]);
        for (int at : new int[] { minLengthAt, maxLengthAt }) {
            final byte[] corrupt = region.bytes().clone();
            corrupt[at] = (byte) between(region.bytes().length, 127);
            final PlainStringColumnSkipper skipper = region.skipper(corrupt);
            expectThrows(CorruptIndexException.class, () -> skipper.advance(0));
        }
    }

    public void testANegativeBoundLengthIsCorrupt() throws IOException {
        final Region region = write(1, doc -> new BytesRef("abc"));
        final int minLengthAt = 1 + 4 * Integer.BYTES;
        final byte[] negativeVInt = { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x0F };
        final byte[] corrupt = new byte[region.bytes().length - 1 + negativeVInt.length];
        System.arraycopy(region.bytes(), 0, corrupt, 0, minLengthAt);
        System.arraycopy(negativeVInt, 0, corrupt, minLengthAt, negativeVInt.length);
        System.arraycopy(
            region.bytes(),
            minLengthAt + 1,
            corrupt,
            minLengthAt + negativeVInt.length,
            region.bytes().length - minLengthAt - 1
        );
        final PlainStringColumnSkipper skipper = region.skipper(corrupt);
        expectThrows(CorruptIndexException.class, () -> skipper.advance(0));
    }

    public void testAnEntryWithNoLevelsIsCorrupt() throws IOException {
        final Region region = write(INTERVAL, PlainSkipIndexCodecTests::ascending);
        for (int levels : new int[] { 0, PlainSkipIndexCodec.DEFAULT_MAX_LEVEL + 1, -1 }) {
            final byte[] corrupt = region.bytes().clone();
            corrupt[0] = (byte) levels;
            final PlainStringColumnSkipper skipper = region.skipper(corrupt);
            expectThrows(CorruptIndexException.class, () -> skipper.advance(0));
        }
    }
}
