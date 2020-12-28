/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.fielddata.ordinals.OrdinalSequence.GroupReader;
import org.elasticsearch.index.fielddata.ordinals.OrdinalSequence.GroupWriter;
import org.elasticsearch.index.fielddata.ordinals.OrdinalSequence.ReaderProvider;
import org.elasticsearch.index.fielddata.ordinals.OrdinalSequence.Writer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.LongUnaryOperator;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class OrdinalSequenceTests extends ESTestCase {
    private static final int MAX = 100000;

    public void testEmptyNonNegative() throws IOException {
        try (GroupWriter gWriter = neverWrites()) {
            Writer writer = gWriter.nonNegativeWriter(0, 0);
            assertAllZeros(gWriter, writer);
        }
    }

    public void testEmptyPositiveDeltas() throws IOException {
        try (GroupWriter gWriter = neverWrites()) {
            Writer writer = gWriter.positiveDeltaWriter(0);
            assertDeltaIdentity(gWriter, writer);
        }
    }

    public void testEmptyNegativeDeltas() throws IOException {
        try (GroupWriter gWriter = neverWrites()) {
            Writer writer = gWriter.negativeDeltaWriter(0, 0);
            assertDeltaIdentity(gWriter, writer);
        }
    }

    public void testAllZeroNonNegative() throws IOException {
        try (GroupWriter gWriter = neverWrites()) {
            Writer writer = gWriter.nonNegativeWriter(0, 0);
            int count = randomInt(MAX);
            for (int i = 0; i < count; i++) {
                writer.add(i, 0);
            }
            assertAllZeros(gWriter, writer);
        }
    }

    public void testIdentityPositiveDeltas() throws IOException {
        try (GroupWriter gWriter = neverWrites()) {
            Writer writer = gWriter.positiveDeltaWriter(0);
            writeIdentity(writer);
            assertDeltaIdentity(gWriter, writer);
        }
    }

    public void testIdentityNegativeDeltas() throws IOException {
        try (GroupWriter gWriter = neverWrites()) {
            Writer writer = gWriter.negativeDeltaWriter(0, 0);
            writeIdentity(writer);
            assertDeltaIdentity(gWriter, writer);
        }
    }

    private void assertAllZeros(GroupWriter gWriter, Writer writer) throws IOException {
        try (GroupReader gReader = gWriter.finish()) {
            gWriter.close();  // It's safe to close as soon as we've called `finish`
            ReaderProvider provider = writer.readerProvider();
            assertThat(provider.ramBytesUsed(), lessThan(100L));
            LongUnaryOperator reader = provider.get(gReader.input());
            long l = randomLong();
            assertThat(reader.applyAsLong(l), equalTo(0L));
            assertThat(gReader.diskBytesUsed(), equalTo(0L));
        }
    }

    private void assertDeltaIdentity(GroupWriter gWriter, Writer writer) throws IOException {
        try (GroupReader gReader = gWriter.finish()) {
            gWriter.close();  // It's safe to close as soon as we've called `finish`
            ReaderProvider provider = writer.readerProvider();
            assertThat(provider.ramBytesUsed(), lessThan(100L));
            LongUnaryOperator reader = provider.get(gReader.input());
            long l = randomLong();
            assertThat(reader.applyAsLong(l), equalTo(l));
            assertThat(gReader.diskBytesUsed(), equalTo(0L));
        }
    }

    private void writeIdentity(Writer writer) throws IOException {
        int count = randomInt(MAX);
        for (int i = 0; i < count; i++) {
            writer.add(i, i);
        }
    }

    public void testRandomNonNegative() throws IOException {
        int count = between(1, MAX);
        long[] expected = new long[count];
        try (Directory directory = noFilesLeftBehindDir(); GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
            Writer writer = nonNegativeWriter(gWriter, count);
            for (int i = 0; i < count; i++) {
                long v = randomLongBetween(0, Long.MAX_VALUE);
                expected[i] = v;
                writer.add(i, v);
            }
            assertExpected(gWriter, expected, writer);
        }
    }

    public void testSharedNonNegative() throws IOException {
        int count = between(1, MAX);
        long[][] expected = new long[10][count];
        try (Directory directory = noFilesLeftBehindDir()) {
            GroupReader gReader;
            Writer[] writers = new Writer[10];
            try (GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
                for (int w = 0; w < writers.length; w++) {
                    writers[w] = nonNegativeWriter(gWriter, count);
                }
                for (int i = 0; i < count; i++) {
                    for (int w = 0; w < writers.length; w++) {
                        long v = randomLongBetween(0, Long.MAX_VALUE);
                        expected[w][i] = v;
                        writers[w].add(i, v);
                    }
                }
                gReader = gWriter.finish();
            }
            try {
                RandomAccessInput input = gReader.input();
                for (int w = 0; w < writers.length; w++) {
                    assertExpected(input, expected[w], writers[w].readerProvider());
                }
            } finally {
                gReader.close();
            }
        }
    }

    public void testRandomPositiveDeltas() throws IOException {
        int count = between(1, MAX);
        long[] expected = new long[count];
        try (Directory directory = noFilesLeftBehindDir(); GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
            Writer writer = gWriter.positiveDeltaWriter(count);
            long v = 0;
            for (int i = 0; i < count; i++) {
                v += between(1, 100);
                expected[i] = v;
                writer.add(i, v);
            }
            assertExpected(gWriter, expected, writer);
        }
    }

    public void testSharedPositiveDeltas() throws IOException {
        int count = between(1, MAX);
        long[][] expected = new long[10][count];
        try (Directory directory = noFilesLeftBehindDir()) {
            GroupReader gReader;
            Writer[] writers = new Writer[10];
            try (GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
                for (int w = 0; w < writers.length; w++) {
                    writers[w] = gWriter.positiveDeltaWriter(count);
                }
                long[] v = new long[10];
                for (int i = 0; i < count; i++) {
                    for (int w = 0; w < writers.length; w++) {
                        v[w] += between(1, 100);
                        expected[w][i] = v[w];
                        writers[w].add(i, v[w]);
                    }
                }
                gReader = gWriter.finish();
            }
            try {
                RandomAccessInput input = gReader.input();
                for (int w = 0; w < writers.length; w++) {
                    assertExpected(input, expected[w], writers[w].readerProvider());
                }
            } finally {
                gReader.close();
            }
        }
    }

    public void testRandomNegativeDeltas() throws IOException {
        int count = between(1, MAX);
        long[] expected = new long[count];
        try (Directory directory = noFilesLeftBehindDir(); GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
            Writer writer = negativeDeltaWriter(gWriter, count);
            for (int i = 0; i < count; i++) {
                int v = between(0, i);
                expected[i] = v;
                writer.add(i, v);
            }
            assertExpected(gWriter, expected, writer);
        }
    }

    public void testRandomNegativeSmallDeltas() throws IOException {
        int count = between(1, MAX);
        int maxDelta = (int) PackedInts.maxValue(between(1, 5));
        long[] expected = new long[count];
        try (Directory directory = noFilesLeftBehindDir(); GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
            Writer writer = negativeDeltaWriter(gWriter, count);
            for (int i = 0; i < count; i++) {
                int v = between(Math.max(0, i - maxDelta), i);
                expected[i] = v;
                writer.add(i, v);
            }
            assertExpected(gWriter, expected, writer);
        }
    }

    public void testSharedNegtiveDeltas() throws IOException {
        int count = between(1, MAX);
        long[][] expected = new long[10][count];
        try (Directory directory = noFilesLeftBehindDir()) {
            GroupReader gReader;
            Writer[] writers = new Writer[10];
            try (GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
                for (int w = 0; w < writers.length; w++) {
                    writers[w] = negativeDeltaWriter(gWriter, count);
                }
                for (int i = 0; i < count; i++) {
                    for (int w = 0; w < writers.length; w++) {
                        int v = between(0, i);
                        expected[w][i] = v;
                        writers[w].add(i, v);
                    }
                }
                gReader = gWriter.finish();
            }
            try {
                RandomAccessInput input = gReader.input();
                for (int w = 0; w < writers.length; w++) {
                    assertExpected(input, expected[w], writers[w].readerProvider());
                }
            } finally {
                gReader.close();
            }
        }
    }

    /**
     * Writes all three supported sequences to the same output.
     */
    public void testSharedMix() throws IOException {
        int count = between(1, MAX);
        long[] nonNegativeExpected = new long[count];
        long[] positiveDeltaExpected = new long[count];
        long[] negativeDeltaExpected = new long[count];
        try (Directory directory = noFilesLeftBehindDir()) {
            GroupReader gReader;
            Writer nonNegativeWriter, positiveDeltaWriter, negativeDeltaWriter;
            try (GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
                nonNegativeWriter = nonNegativeWriter(gWriter, count);
                positiveDeltaWriter = gWriter.positiveDeltaWriter(count);
                negativeDeltaWriter = negativeDeltaWriter(gWriter, count);
                long positiveDeltaValue = 0;
                for (int i = 0; i < count; i++) {
                    nonNegativeExpected[i] = randomLongBetween(0, Long.MAX_VALUE);
                    nonNegativeWriter.add(i, nonNegativeExpected[i]);
                    positiveDeltaValue += between(1, 100);
                    positiveDeltaExpected[i] = positiveDeltaValue;
                    positiveDeltaWriter.add(i, positiveDeltaValue);
                    negativeDeltaExpected[i] = between(0, i);
                    negativeDeltaWriter.add(i, negativeDeltaExpected[i]);
                }
                gReader = gWriter.finish();
            }
            try {
                RandomAccessInput input = gReader.input();
                assertExpected(input, nonNegativeExpected, nonNegativeWriter.readerProvider());
                assertExpected(input, positiveDeltaExpected, positiveDeltaWriter.readerProvider());
                assertExpected(input, negativeDeltaExpected, negativeDeltaWriter.readerProvider());
            } finally {
                gReader.close();
            }
        }
    }

    public void testNonNegativeLeadingZeros() throws IOException {
        int leadingZeros = between(1, MAX);
        int count = between(1, MAX);
        long[] expected = new long[leadingZeros + count];
        try (Directory directory = noFilesLeftBehindDir(); GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
            Writer writer = nonNegativeWriter(gWriter, leadingZeros + count);
            for (int i = 0; i < leadingZeros; i++) {
                expected[i] = 0;
                writer.add(i, 0);
            }
            for (int i = 0; i < count; i++) {
                long v = randomLongBetween(0, Long.MAX_VALUE);
                expected[leadingZeros + i] = v;
                writer.add(leadingZeros + i, v);
            }
            assertExpected(gWriter, expected, writer);
        }
    }

    public void testPositiveDeltaLeadingIdentity() throws IOException {
        int leadingIdentity = between(1, MAX);
        int count = between(1, MAX);
        long[] expected = new long[leadingIdentity + count];
        try (Directory directory = noFilesLeftBehindDir(); GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
            Writer writer = gWriter.positiveDeltaWriter(leadingIdentity + count);
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
            assertExpected(gWriter, expected, writer);
        }
    }

    public void testNegativeDeltaLeadingIdentity() throws IOException {
        int leadingIdentity = between(1, MAX);
        int count = between(1, MAX);
        long[] expected = new long[leadingIdentity + count];
        try (Directory directory = noFilesLeftBehindDir(); GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
            Writer writer = negativeDeltaWriter(gWriter, leadingIdentity + count);
            for (int i = 0; i < leadingIdentity; i++) {
                expected[i] = i;
                writer.add(i, i);
            }
            for (int i = 0; i < count; i++) {
                int v = between(0, leadingIdentity + i);
                expected[leadingIdentity + i] = v;
                writer.add(leadingIdentity + i, v);
            }
            assertExpected(gWriter, expected, writer);
        }
    }

    public void testNonNegativeFailure() throws IOException {
        int count = between(1, MAX);
        try (Directory directory = noFilesLeftBehindDir(); GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
            Writer writer = nonNegativeWriter(gWriter, count);
            for (int i = 0; i < count; i++) {
                writer.add(i, randomLongBetween(0, Long.MAX_VALUE));
            }
            /*
             * Closing the OutHelper without calling finish should delete
             * all the files we've made. If it doesn't, then closing directory
             * will ail.
             */
        }
    }

    public void testPositiveDeltaFailure() throws IOException {
        int count = between(1, MAX);
        try (Directory directory = noFilesLeftBehindDir(); GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
            Writer writer = gWriter.positiveDeltaWriter(count);
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
        int count = between(1, MAX);
        try (Directory directory = noFilesLeftBehindDir(); GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
            Writer writer = gWriter.negativeDeltaWriter(count, count + (randomBoolean() ? 0 : between(0, MAX)));
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

    public void testGapsInNonNegative() throws IOException {
        int count = between(1, MAX);
        long[] expected = new long[count];
        try (Directory directory = noFilesLeftBehindDir(); GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
            Writer writer = nonNegativeWriter(gWriter, count);
            for (int i = 0; i < count; i++) {
                if (randomBoolean()) {
                    long v = randomLongBetween(0, Long.MAX_VALUE);
                    expected[i] = v;
                    writer.add(i, v);
                } else {
                    // Gap
                    expected[i] = -1;
                }
            }
            assertExpected(gWriter, expected, writer);
        }
    }

    public void testGapsInPositiveDeltas() throws IOException {
        int count = between(1, MAX);
        long[] expected = new long[count];
        try (Directory directory = noFilesLeftBehindDir(); GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
            Writer writer = gWriter.positiveDeltaWriter(count);
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
            assertExpected(gWriter, expected, writer);
        }
    }

    public void testGapsInNegativeDelta() throws IOException {
        int count = between(1, MAX);
        long[] expected = new long[count];
        try (Directory directory = noFilesLeftBehindDir(); GroupWriter gWriter = GroupWriter.tmpFile("test", "test", directory)) {
            Writer writer = negativeDeltaWriter(gWriter, count);
            for (int i = 0; i < count; i++) {
                if (randomBoolean()) {
                    long v = between(0, i);
                    expected[i] = v;
                    writer.add(i, v);
                } else {
                    // Gap
                    expected[i] = -1;
                }
            }
            assertExpected(gWriter, expected, writer);
        }
    }

    private void assertExpected(GroupWriter gWriter, long[] expected, Writer writer) throws IOException {
        try (GroupReader gReader = gWriter.finish()) {
            ReaderProvider provider = writer.readerProvider();
            gWriter.close();
            assertExpected(gReader.input(), expected, provider);
        }
    }

    private void assertExpected(RandomAccessInput input, long[] expected, ReaderProvider provider) throws IOException {
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

    static GroupWriter neverWrites() {
        return new GroupWriter() {
            @Override
            protected IndexOutput buildOutput() throws IOException {
                throw new AssertionError();
            }

            @Override
            protected GroupReader buildReader(IndexOutput out) throws IOException {
                throw new AssertionError();
            }
        };
    }

    public static BaseDirectoryWrapper noFilesLeftBehindDir() {
        return new BaseDirectoryWrapper(newDirectory(random())) {
            @Override
            public void close() throws IOException {
                List<String> files = Arrays.stream(listAll())
                    // Strip any "extra" files added by the test randomization. We only care about our files.
                    .filter(name -> false == name.startsWith("extra"))
                    .collect(toList());
                assertThat("deleted all files", files, empty());
                super.close();
            }
        };
    }

    /**
     * Build a non-negative writer that underestimates the minimum count half
     * the time and overestimates the maximum count half the time.
     */
    private Writer nonNegativeWriter(GroupWriter gWriter, int count) {
        return gWriter.nonNegativeWriter(
            randomBoolean() ? count : between(0, count - 1),
            randomBoolean() ? count : between(count + 1, 10 * count)
        );
    }

    /**
     * Build a positive delta writer that underestimates the minimum count half
     * the time and overestimates the maximum count half the time.
     */
    private Writer negativeDeltaWriter(GroupWriter gWriter, int count) {
        return gWriter.negativeDeltaWriter(
            randomBoolean() ? count : between(0, count - 1),
            randomBoolean() ? count : between(count + 1, 10 * count)
        );
    }
    
}
