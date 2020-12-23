package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.store.Directory;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdToSegmentAndSegmentOrd.Reader;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdToSegmentAndSegmentOrd.ReaderProvider;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdToSegmentAndSegmentOrd.Writer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.index.fielddata.ordinals.OrdinalSequenceTests.neverWrites;
import static org.elasticsearch.index.fielddata.ordinals.OrdinalSequenceTests.noFilesLeftBehindDir;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class GlobalOrdToSegmentAndSegmentOrdTests extends ESTestCase {
    public void testEmpty() throws IOException {
        try (OrdinalSequence.GroupWriter segmentOut = neverWrites(); OrdinalSequence.GroupWriter segmentOrdOut = neverWrites()) {
            Writer writer = new Writer(segmentOut, segmentOrdOut, 0, 0, 0);
            assertIdentity(segmentOut, segmentOrdOut, writer.readerProvider());
        }
    }

    public void testAllInFirstSegment() throws IOException {
        try (OrdinalSequence.GroupWriter segmentOut = neverWrites(); OrdinalSequence.GroupWriter segmentOrdOut = neverWrites()) {
            Writer writer = new Writer(segmentOut, segmentOrdOut, 0, 0, 0);
            int count = randomInt(1000);
            for (int i = 0; i < count; i++) {
                writer.write(i, 0, i);
            }
            assertIdentity(segmentOut, segmentOrdOut, writer.readerProvider());
        }
    }

    private void assertIdentity(OrdinalSequence.GroupWriter segmentOut, OrdinalSequence.GroupWriter segmentOrdOut, ReaderProvider provider)
        throws IOException {
        try (OrdinalSequence.InHelper segmentIn = segmentOut.finish(); OrdinalSequence.InHelper segmentOrdIn = segmentOrdOut.finish()) {
            assertThat(provider.ramBytesUsed(), lessThan(100L));
            Reader reader = provider.get();
            long l = randomLong();
            assertThat(reader.containingSegment(l), equalTo(0));
            assertThat(reader.containingSegmentOrd(l), equalTo(l));
        }
    }

    public void testAllInOneSegment() throws IOException {
        int count = randomInt(1000);
        try (
            Directory directory = noFilesLeftBehindDir();
            OrdinalSequence.GroupWriter segmentOut = OrdinalSequence.GroupWriter.tmpFile("tmp", "s2g", directory);
            OrdinalSequence.GroupWriter segmentOrdOut = OrdinalSequence.GroupWriter.tmpFile("tmp", "s2gord", directory);
        ) {
            Writer writer = new Writer(segmentOut, segmentOrdOut, count, count, count);
            int[] expectedSegment = new int[count];
            long[] expectedSegmentOrd = new long[count];
            for (int i = 0; i < count; i++) {
                writer.write(i, i, 0);
                expectedSegment[i] = i;
                expectedSegmentOrd[i] = 0;
            }
            assertExpected(segmentOut, segmentOrdOut, expectedSegment, expectedSegmentOrd, writer.readerProvider());
        }
    }

    public void testAllInFewSegments() throws IOException {
        for (int count = 10; count < 100_000_000; count *= 10) {
            for (int segmentCount : new int[] { 2, 10, 50, 100, 1000 }) {
                long[] segmentOrds = new long[segmentCount];

                int[] expectedSegment = new int[count];
                long[] expectedSegmentOrd = new long[count];
                long maxDelta = 0;
                for (int i = 0; i < count; i++) {
                    int segment = randomInt(segmentCount - 1);
                    expectedSegment[i] = segment;
                    expectedSegmentOrd[i] = segmentOrds[segment];
                    maxDelta = Math.max(maxDelta, i - segmentOrds[segment]);
                    segmentOrds[segment]++;
                }

                try (
                    Directory directory = noFilesLeftBehindDir();
                    OrdinalSequence.GroupWriter segmentOut = OrdinalSequence.GroupWriter.tmpFile("tmp", "s2g", directory);
                    OrdinalSequence.GroupWriter segmentOrdOut = OrdinalSequence.GroupWriter.tmpFile("tmp", "s2gord", directory);
                ) {
                    Writer writer = new Writer(segmentOut, segmentOrdOut, count, segmentCount, maxDelta);
                    long start = System.nanoTime();
                    for (int i = 0; i < count; i++) {
                        writer.write(i, expectedSegment[i], expectedSegmentOrd[i]);
                    }
                    long time = System.nanoTime() - start;
                    ReaderProvider provider = writer.readerProvider();
                    long diskUsed = assertExpected(segmentOut, segmentOrdOut, expectedSegment, expectedSegmentOrd, provider);
                    System.out.printf(
                        Locale.ROOT,
                        "adsfdsaf count: %09d segments: %04d disk: %09d ram: %03d took: %010d\n",
                        count,
                        segmentCount,
                        diskUsed,
                        provider.ramBytesUsed(),
                        time
                    );
                }
            }
        }
    }

    private long assertExpected(
        OrdinalSequence.GroupWriter segmentOut,
        OrdinalSequence.GroupWriter segmentOrdOut,
        int[] expectedSegment,
        long[] expectedSegmentOrd,
        ReaderProvider provider
    ) throws IOException {
        try (OrdinalSequence.InHelper segmentIn = segmentOut.finish(); OrdinalSequence.InHelper segmentOrdIn = segmentOrdOut.finish()) {
            assertThat(expectedSegmentOrd.length, equalTo(expectedSegment.length));
            assertThat(provider.ramBytesUsed(), greaterThan(100L));
            assertThat(provider.ramBytesUsed(), lessThan(1000L));
            Reader reader = provider.get();
            for (int i = 0; i < expectedSegment.length; i++) {
                assertThat(reader.containingSegment(i), equalTo(expectedSegment[i]));
                assertThat(reader.containingSegmentOrd(i), equalTo(expectedSegmentOrd[i]));
            }
            // And again with random access
            for (int n = 0; n < 1000; n++) {
                int i = between(0, expectedSegment.length - 1);
                assertThat(reader.containingSegment(i), equalTo(expectedSegment[i]));
                assertThat(reader.containingSegmentOrd(i), equalTo(expectedSegmentOrd[i]));
            }
            return segmentIn.diskBytesUsed() + segmentOrdIn.diskBytesUsed();
        }
    }
}
