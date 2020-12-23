package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.packed.DirectWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.fielddata.ordinals.OrdinalSequence.InHelper;
import org.elasticsearch.index.fielddata.ordinals.OrdinalSequence.OutHelper;
import org.elasticsearch.index.fielddata.ordinals.OrdinalSequence.ReaderProvider;
import org.elasticsearch.index.fielddata.ordinals.OrdinalSequence.Writer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.function.LongUnaryOperator;

import static org.elasticsearch.index.fielddata.ordinals.OrdinalSequence.directWriter;
import static org.elasticsearch.index.fielddata.ordinals.OrdinalSequence.negativeDeltaWriter;
import static org.elasticsearch.index.fielddata.ordinals.OrdinalSequence.positiveDeltaWriter;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class OrdinalSequenceTests extends ESTestCase {
    public void testEmptyDirect() throws IOException {
        try (OutHelper out = neverWrites()) {
            Writer writer = directWriter(out, 0, 0);
            assertAllZeros(out, writer);
        }
    }

    public void testEmptyPositiveDeltas() throws IOException {
        try (OutHelper out = neverWrites()) {
            Writer writer = positiveDeltaWriter(out, 0);
            assertDeltaIdentity(out, writer);
        }
    }

    public void testEmptyNegativeDeltas() throws IOException {
        try (OutHelper out = neverWrites()) {
            Writer writer = negativeDeltaWriter(out, 0, 0);
            assertDeltaIdentity(out, writer);
        }
    }

    public void testAllZeroDirect() throws IOException {
        try (OutHelper out = neverWrites()) {
            Writer writer = directWriter(out, 0, 0);
            int count = randomInt(10000);
            for (int i = 0; i < count; i++) {
                writer.add(i, 0);
            }
            assertAllZeros(out, writer);
        }
    }

    public void testIdentityPositiveDeltas() throws IOException {
        try (OutHelper out = neverWrites()) {
            Writer writer = positiveDeltaWriter(out, 0);
            writeIdentity(writer);
            assertDeltaIdentity(out, writer);
        }
    }

    public void testIdentityNegativeDeltas() throws IOException {
        try (OutHelper out = neverWrites()) {
            Writer writer = negativeDeltaWriter(out, 0, 0);
            writeIdentity(writer);
            assertDeltaIdentity(out, writer);
        }
    }

    private void assertAllZeros(OutHelper out, Writer writer) throws IOException {
        try (InHelper in = out.finish()) {
            ReaderProvider provider = writer.readerProvider();
            out.close();
            assertThat(provider.ramBytesUsed(), lessThan(100L));
            LongUnaryOperator reader = provider.get(in.input());
            long l = randomLong();
            assertThat(reader.applyAsLong(l), equalTo(0L));
        }
    }

    private void assertDeltaIdentity(OutHelper out, Writer writer) throws IOException {
        try (InHelper in = out.finish()) {
            ReaderProvider provider = writer.readerProvider();
            out.close();
            assertThat(provider.ramBytesUsed(), lessThan(100L));
            LongUnaryOperator reader = provider.get(in.input());
            long l = randomLong();
            assertThat(reader.applyAsLong(l), equalTo(l));
        }
    }

    private void writeIdentity(Writer writer) throws IOException {
        int count = randomInt(10000);
        for (int i = 0; i < count; i++) {
            writer.add(i, i);
        }
    }

    public void testRandomDirect() throws IOException {
        int count = between(1, 10000);
        long max = PackedInts.maxValue(between(1, 63));
        int bitsPerValue = DirectWriter.bitsRequired(max);
        long[] expected = new long[count];
        try (Directory directory = noFilesLeftBehindDir(); OutHelper out = OutHelper.tmpFile("test", "test", directory)) {
            Writer writer = directWriter(out, count, bitsPerValue);
            for (int i = 0; i < count; i++) {
                long v = randomLongBetween(0, max);
                expected[i] = v;
                writer.add(i, v);
            }
            assertExpected(out, expected, writer);
        }
    }

    public void testRandomDirectNegativeOk() throws IOException {
        int count = between(1, 10000);
        long[] expected = new long[count];
        try (Directory directory = noFilesLeftBehindDir(); OutHelper out = OutHelper.tmpFile("test", "test", directory)) {
            Writer writer = directWriter(out, count, 64);
            for (int i = 0; i < count; i++) {
                long v = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);
                expected[i] = v;
                writer.add(i, v);
            }
            assertExpected(out, expected, writer);
        }
    }

    public void testRandomPositiveDeltas() throws IOException {
        int count = between(1, 10000);
        long[] expected = new long[count];
        try (Directory directory = noFilesLeftBehindDir(); OutHelper out = OutHelper.tmpFile("test", "test", directory)) {
            Writer writer = positiveDeltaWriter(out, count);
            long v = 0;
            for (int i = 0; i < count; i++) {
                v += between(1, 100);
                expected[i] = v;
                writer.add(i, v);
            }
            assertExpected(out, expected, writer);
        }
    }

    public void testSharedPositiveDeltas() throws IOException {
        int count = between(1, 10000);
        long[][] expected = new long[10][count];
        try (Directory directory = noFilesLeftBehindDir()) {
            InHelper in;
            Writer[] writers = new Writer[10];
            try (OutHelper out = OutHelper.tmpFile("test", "test", directory)) {
                for (int w = 0; w < writers.length; w++) {
                    writers[w] = positiveDeltaWriter(out, count);
                }
                long[] v = new long[10];
                for (int i = 0; i < count; i++) {
                    for (int w = 0; w < writers.length; w++) {
                        v[w] += between(1, 100);
                        expected[w][i] = v[w];
                        writers[w].add(i, v[w]);
                    }
                }
                in = out.finish();
            }
            try {
                RandomAccessInput input = in.input();
                for (int w = 0; w < writers.length; w++) {
                    assertExpected(input, expected[w], writers[w].readerProvider());
                }
            } finally {
                in.close();
            }
        }
    }

    public void testRandomNegativeDeltas() throws IOException {
        int count = between(1, 10000);
        long[] expected = new long[count];
        try (Directory directory = noFilesLeftBehindDir(); OutHelper out = OutHelper.tmpFile("test", "test", directory)) {
            Writer writer = negativeDeltaWriter(out, count, count);
            for (int i = 0; i < count; i++) {
                int v = between(0, i);
                expected[i] = v;
                writer.add(i, v);
            }
            assertExpected(out, expected, writer);
        }
    }

    public void testRandomNegativeSmallDeltas() throws IOException {
        int count = between(1, 10000);
        int maxDelta = (int) PackedInts.maxValue(between(1, 5));
        long[] expected = new long[count];
        try (Directory directory = noFilesLeftBehindDir(); OutHelper out = OutHelper.tmpFile("test", "test", directory)) {
            Writer writer = negativeDeltaWriter(out, count, maxDelta);
            for (int i = 0; i < count; i++) {
                int v = between(Math.max(0, i - maxDelta), i);
                expected[i] = v;
                writer.add(i, v);
            }
            assertExpected(out, expected, writer);
        }
    }

    public void testLeadingIdentity() throws IOException {
        int leadingIdentity = between(1, 10000);
        int count = between(1, 10000);
        long[] expected = new long[leadingIdentity + count];
        try (Directory directory = noFilesLeftBehindDir(); OutHelper out = OutHelper.tmpFile("test", "test", directory)) {
            Writer writer = positiveDeltaWriter(out, leadingIdentity + count);
            long v = 0;
            for (int i = 0; i < leadingIdentity; i++) {
                expected[i] = v;
                writer.add(i, v);
                v++;
            }
            for (int i = 0; i < count; i++) {
                v += between(1, 100);
                expected[leadingIdentity + i] = v;
                writer.add(leadingIdentity + i, v);
            }
            assertExpected(out, expected, writer);
        }
    }

    public void testDirectFailure() throws IOException {
        int count = between(1, 10000);
        try (Directory directory = noFilesLeftBehindDir(); OutHelper out = OutHelper.tmpFile("test", "test", directory)) {
            Writer writer = directWriter(out, count, 64);
            for (int i = 0; i < count; i++) {
                writer.add(i, randomLong());
            }
            /*
             * Closing the OutHelper without calling finish should delete
             * all the files we've made. If it doesn't, then closing directory
             * will ail.
             */
        }
    }

    public void testPositiveDeltaFailure() throws IOException {
        int count = between(1, 10000);
        try (Directory directory = noFilesLeftBehindDir(); OutHelper out = OutHelper.tmpFile("test", "test", directory)) {
            Writer writer = positiveDeltaWriter(out, count);
            long v = 0;
            for (int i = 0; i < count; i++) {
                v += between(1, 100);
                writer.add(i, v);
            }
            /*
             * Closing the OutHelper without calling finish should delete
             * all the files we've made. If it doesn't, then closing directory
             * will ail.
             */
        }
    }

    public void testNegativeDeltaFailure() throws IOException {
        int count = between(1, 10000);
        try (Directory directory = noFilesLeftBehindDir(); OutHelper out = OutHelper.tmpFile("test", "test", directory)) {
            Writer writer = negativeDeltaWriter(out, count, count);
            for (int i = 0; i < count; i++) {
                int v = between(0, i);
                writer.add(i, v);
            }
            /*
             * Closing the OutHelper without calling finish should delete
             * all the files we've made. If it doesn't, then closing directory
             * will ail.
             */
        }
    }

    // NOCOMMIT gaps in direct and negative deltas?

    public void testGapsInPositiveDeltas() throws IOException {
        int count = between(1, 10000);
        long[] expected = new long[count];
        try (Directory directory = noFilesLeftBehindDir(); OutHelper out = OutHelper.tmpFile("test", "test", directory)) {
            Writer writer = positiveDeltaWriter(out, count);
            long v = 0;
            for (int i = 0; i < count; i++) {
                v += between(1, 100);
                if (randomBoolean()) {
                    expected[i] = v;
                    writer.add(i, v);
                } else {
                    // Gap
                    expected[i] = -1;
                }
            }
            assertExpected(out, expected, writer);
        }
    }

    private void assertExpected(OutHelper out, long[] expected, Writer writer) throws IOException {
        try (InHelper in = out.finish()) {
            ReaderProvider provider = writer.readerProvider();
            out.close();
            // NOCOMMIT in.ramBytesUsed assertion too
            assertThat(in.diskBytesUsed(), greaterThan(0L));
            assertExpected(in.input(), expected, provider);
        }
    }

    private void assertExpected(RandomAccessInput input, long[] expected, ReaderProvider provider) throws IOException {
        assertThat(provider.ramBytesUsed(), greaterThan(32L));
        LongUnaryOperator reader = provider.get(input);
        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != -1) {
                // -1 encodes a gap in the test data
                assertThat(reader.applyAsLong(i), equalTo(expected[i]));
            }
        }
        // And again with random access
        for (int n = 0; n < 1000; n++) {
            int i = between(0, expected.length - 1);
            if (expected[i] != -1) {
                // -1 encodes a gap in the test data
                assertThat(reader.applyAsLong(i), equalTo(expected[i]));
            }
        }
    }

    static OutHelper neverWrites() {
        return new OutHelper() {
            @Override
            protected IndexOutput buildOutput() throws IOException {
                throw new AssertionError();
            }

            @Override
            protected InHelper buildInput(String name) throws IOException {
                throw new AssertionError();
            }
        };
    }

    public static BaseDirectoryWrapper noFilesLeftBehindDir() {
        return new BaseDirectoryWrapper(newDirectory(random())) {
            @Override
            public void close() throws IOException {
                assertThat("deleted all files", listAll(), equalTo(new String[] {}));
                super.close();
            }
        };
    }
}
