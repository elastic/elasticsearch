package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class OrdinalSequenceTests extends ESTestCase {
    public void testEmptyDirect() throws IOException {
        try (OrdinalSequence.Writer writer = directWriterThatNeverWrites()) {
            assertAllZeros(writer.readerProvider());
        }
    }

    public void testEmptyPositiveDeltas() throws IOException {
        try (OrdinalSequence.Writer writer = positiveDeltaWriterThatNeverWrites()) {
            assertDeltaIdentity(writer.readerProvider());
        }
    }

    public void testEmptyNegativeDeltas() throws IOException {
        try (OrdinalSequence.Writer writer = negativeDeltaWriterThatNeverWrites()) {
            assertDeltaIdentity(writer.readerProvider());
        }
    }

    public void testAllZeroDirect() throws IOException {
        try (OrdinalSequence.Writer writer = directWriterThatNeverWrites()) {
            int count = randomInt(10000);
            for (int i = 0; i < count; i++) {
                writer.add(i, 0);
            }
            assertAllZeros(writer.readerProvider());
        }
    }

    public void testIdentityPositiveDeltas() throws IOException {
        try (OrdinalSequence.Writer writer = positiveDeltaWriterThatNeverWrites()) {
            writeIdentity(writer);
            assertDeltaIdentity(writer.readerProvider());
        }
    }

    public void testIdentityNegativeDeltas() throws IOException {
        try (OrdinalSequence.Writer writer = positiveDeltaWriterThatNeverWrites()) {
            writeIdentity(writer);
            assertDeltaIdentity(writer.readerProvider());
        }
    }

    private void assertAllZeros(OrdinalSequence.ReaderProvider provider) throws IOException {
        assertThat(provider.ramBytesUsed(), lessThan(100L));
        assertThat(provider.diskBytesUsed(), equalTo(0L));
        try (OrdinalSequence.Reader reader = provider.get()) {
            long l = randomLong();
            assertThat(reader.applyAsLong(l), equalTo(0L));
        }
    }

    private void assertDeltaIdentity(OrdinalSequence.ReaderProvider provider) throws IOException {
        assertThat(provider.ramBytesUsed(), lessThan(100L));
        assertThat(provider.diskBytesUsed(), equalTo(0L));
        try (OrdinalSequence.Reader reader = provider.get()) {
            long l = randomLong();
            assertThat(reader.applyAsLong(l), equalTo(l));
        }
    }

    private void writeIdentity(OrdinalSequence.Writer writer) throws IOException {
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
        try (
            Directory directory = newDirectory();
            OrdinalSequence.Writer writer = OrdinalSequence.directWriter(directoryIO(directory), count, bitsPerValue)
        ) {
            for (int i = 0; i < count; i++) {
                long v = randomLongBetween(0, max);
                expected[i] = v;
                writer.add(i, v);
            }
            assertExpected(expected, writer.readerProvider());
        }
    }

    public void testRandomDirectNegativeOk() throws IOException {
        int count = between(1, 10000);
        long[] expected = new long[count];
        try (
            Directory directory = newDirectory();
            OrdinalSequence.Writer writer = OrdinalSequence.directWriter(directoryIO(directory), count, 64)
        ) {
            for (int i = 0; i < count; i++) {
                long v = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);
                expected[i] = v;
                writer.add(i, v);
            }
            assertExpected(expected, writer.readerProvider());
        }
    }

    public void testRandomPositiveDeltas() throws IOException {
        int count = between(1, 10000);
        long[] expected = new long[count];
        try (
            Directory directory = newDirectory();
            OrdinalSequence.Writer writer = OrdinalSequence.positiveDeltaWriter(directoryIO(directory), count)
        ) {
            long v = 0;
            for (int i = 0; i < count; i++) {
                v += between(1, 100);
                expected[i] = v;
                writer.add(i, v);
            }
            assertExpected(expected, writer.readerProvider());
        }
    }

    public void testRandomNegativeDeltas() throws IOException {
        int count = between(1, 10000);
        long[] expected = new long[count];
        try (
            Directory directory = newDirectory();
            OrdinalSequence.Writer writer = OrdinalSequence.negativeDeltaWriter(directoryIO(directory), count, count)
        ) {
            for (int i = 0; i < count; i++) {
                int v = between(0, i);
                expected[i] = v;
                writer.add(i, v);
            }
            assertExpected(expected, writer.readerProvider());
        }
    }

    public void testRandomNegativeSmallDeltas() throws IOException {
        int count = between(1, 10000);
        int maxDelta = (int) PackedInts.maxValue(between(1, 5));
        long[] expected = new long[count];
        try (
            Directory directory = newDirectory();
            OrdinalSequence.Writer writer = OrdinalSequence.negativeDeltaWriter(directoryIO(directory), count, maxDelta)
        ) {
            for (int i = 0; i < count; i++) {
                int v = between(Math.max(0, i - maxDelta), i);
                expected[i] = v;
                writer.add(i, v);
            }
            assertExpected(expected, writer.readerProvider());
        }
    }

    public void testLeadingIdentity() throws IOException {
        int leadingIdentity = between(1, 10000);
        int conut = between(1, 10000);
        long[] expected = new long[leadingIdentity + conut];
        try (
            Directory directory = newDirectory();
            OrdinalSequence.Writer writer = OrdinalSequence.positiveDeltaWriter(directoryIO(directory), leadingIdentity + conut)
        ) {
            long v = 0;
            for (int i = 0; i < leadingIdentity; i++) {
                expected[i] = v;
                writer.add(i, v);
                v++;
            }
            for (int i = 0; i < conut; i++) {
                v += between(1, 100);
                expected[leadingIdentity + i] = v;
                writer.add(leadingIdentity + i, v);
            }
            assertExpected(expected, writer.readerProvider());
        }
    }

    // NOCOMMIT gaps in direct and negative deltas?

    public void testGapsInPositiveDeltas() throws IOException {
        int count = between(1, 10000);
        long[] expected = new long[count];
        try (
            Directory directory = newDirectory();
            OrdinalSequence.Writer writer = OrdinalSequence.positiveDeltaWriter(directoryIO(directory), count)
        ) {
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
            assertExpected(expected, writer.readerProvider());
        }
    }

    private void assertExpected(long[] expected, OrdinalSequence.ReaderProvider provider) throws IOException {
        assertThat(provider.ramBytesUsed(), greaterThan(100L));
        assertThat(provider.diskBytesUsed(), greaterThan(0L));
        try (OrdinalSequence.Reader reader = provider.get()) {
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
    }

    private OrdinalSequence.Writer directWriterThatNeverWrites() {
        return OrdinalSequence.directWriter(ioThatNeverWrites(), 0, 0);
    }

    private OrdinalSequence.Writer positiveDeltaWriterThatNeverWrites() {
        return OrdinalSequence.positiveDeltaWriter(ioThatNeverWrites(), 0);
    }

    private OrdinalSequence.Writer negativeDeltaWriterThatNeverWrites() {
        return OrdinalSequence.negativeDeltaWriter(ioThatNeverWrites(), 0, 0);
    }

    static OrdinalSequence.IO ioThatNeverWrites() {
        return new OrdinalSequence.IO() {
            @Override
            public IndexOutput createOutput() throws IOException {
                throw new AssertionError();
            }

            @Override
            public IndexInput openInput(String name) throws IOException {
                throw new AssertionError();
            }

            @Override
            public long diskBytesUsed(String name) throws IOException {
                throw new AssertionError();
            }

            @Override
            public void delete(String name) throws IOException {
                throw new AssertionError();
            }
        };
    }

    static OrdinalSequence.IO directoryIO(Directory directory) {
        return new OrdinalSequence.IO() {
            @Override
            public IndexOutput createOutput() throws IOException {
                return directory.createTempOutput("test", "test", IOContext.DEFAULT);
            }

            @Override
            public IndexInput openInput(String name) throws IOException {
                return directory.openInput(name, IOContext.READONCE);
            }

            @Override
            public long diskBytesUsed(String name) throws IOException {
                return directory.fileLength(name);
            }

            @Override
            public void delete(String name) throws IOException {
                directory.deleteFile(name);
            }
        };
    }
}
