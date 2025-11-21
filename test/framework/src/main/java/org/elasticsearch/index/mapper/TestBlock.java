/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.exponentialhistogram.CompressedExponentialHistogram;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestBlock implements BlockLoader.Block {
    public static BlockLoader.BlockFactory factory() {
        return new BlockLoader.BlockFactory() {
            @Override
            public void adjustBreaker(long delta) throws CircuitBreakingException {
                // Intentionally NOOP
            }

            @Override
            public BlockLoader.BooleanBuilder booleansFromDocValues(int expectedCount) {
                return booleans(expectedCount);
            }

            @Override
            public BlockLoader.BooleanBuilder booleans(int expectedCount) {
                class BooleansBuilder extends TestBlock.Builder implements BlockLoader.BooleanBuilder {
                    private BooleansBuilder() {
                        super(expectedCount);
                    }

                    @Override
                    public BooleansBuilder appendBoolean(boolean value) {
                        add(value);
                        return this;
                    }
                }
                return new BooleansBuilder();
            }

            @Override
            public BlockLoader.BytesRefBuilder bytesRefsFromDocValues(int expectedCount) {
                class BytesRefsFromDocValuesBuilder extends TestBlock.Builder implements BlockLoader.BytesRefBuilder {

                    private static final int SINGLE_DOC = 1;

                    private BytesRefsFromDocValuesBuilder() {
                        // this is hard coded bc bytesRefsFromDocValues() is currently only used for singe-doc multi-valued fields
                        super(SINGLE_DOC);
                    }

                    @Override
                    public BytesRefsFromDocValuesBuilder appendBytesRef(BytesRef value) {
                        add(BytesRef.deepCopyOf(value));
                        return this;
                    }

                    @Override
                    public TestBlock build() {
                        TestBlock result = super.build();
                        List<?> r;
                        // we have a single, multi-valued document, so extract all those values into a list
                        if (result.values.get(0) instanceof List<?> l) {
                            r = l;
                        } else {
                            r = List.of(result.values.get(0));
                        }
                        assertThat(r, hasSize(expectedCount));
                        return result;
                    }

                }
                return new BytesRefsFromDocValuesBuilder();
            }

            @Override
            public BlockLoader.BytesRefBuilder bytesRefs(int expectedCount) {
                class BytesRefsBuilder extends TestBlock.Builder implements BlockLoader.BytesRefBuilder {
                    private BytesRefsBuilder() {
                        super(expectedCount);
                    }

                    @Override
                    public BytesRefsBuilder appendBytesRef(BytesRef value) {
                        add(BytesRef.deepCopyOf(value));
                        return this;
                    }
                }
                return new BytesRefsBuilder();
            }

            @Override
            public BlockLoader.SingletonBytesRefBuilder singletonBytesRefs(int expectedCount) {
                class BytesRefsBuilder extends TestBlock.Builder implements BlockLoader.SingletonBytesRefBuilder {
                    private final int count = expectedCount;

                    private BytesRefsBuilder() {
                        super(expectedCount);
                    }

                    @Override
                    public BlockLoader.SingletonBytesRefBuilder appendBytesRefs(byte[] bytes, long[] offsets) throws IOException {
                        for (int i = 0; i < offsets.length - 1; i++) {
                            BytesRef ref = new BytesRef(bytes, (int) offsets[i], (int) (offsets[i + 1] - offsets[i]));
                            add(BytesRef.deepCopyOf(ref));
                        }
                        return this;
                    }

                    @Override
                    public BlockLoader.SingletonBytesRefBuilder appendBytesRefs(byte[] bytes, long bytesRefLengths) throws IOException {
                        for (int i = 0; i < count; i++) {
                            BytesRef ref = new BytesRef(bytes, (int) (i * bytesRefLengths), (int) bytesRefLengths);
                            add(BytesRef.deepCopyOf(ref));
                        }
                        return this;
                    }
                }
                return new BytesRefsBuilder();
            }

            @Override
            public BlockLoader.DoubleBuilder doublesFromDocValues(int expectedCount) {
                return doubles(expectedCount);
            }

            @Override
            public BlockLoader.DoubleBuilder doubles(int expectedCount) {
                class DoublesBuilder extends TestBlock.Builder implements BlockLoader.DoubleBuilder {
                    private DoublesBuilder() {
                        super(expectedCount);
                    }

                    @Override
                    public DoublesBuilder appendDouble(double value) {
                        add(value);
                        return this;
                    }
                }
                return new DoublesBuilder();
            }

            @Override
            public BlockLoader.FloatBuilder denseVectors(int expectedCount, int dimensions) {
                class FloatsBuilder extends TestBlock.Builder implements BlockLoader.FloatBuilder {
                    int numElements = 0;

                    private FloatsBuilder() {
                        super(expectedCount);
                    }

                    @Override
                    public BlockLoader.FloatBuilder appendFloat(float value) {
                        add(value);
                        numElements++;
                        return this;
                    }

                    @Override
                    public Builder appendNull() {
                        throw new IllegalArgumentException("dense vectors should not have null values");
                    }

                    @Override
                    public Builder endPositionEntry() {
                        assert numElements == dimensions : "expected " + dimensions + " dimensions, but got " + numElements;
                        numElements = 0;
                        return super.endPositionEntry();
                    }

                    @Override
                    public TestBlock build() {
                        assert numElements == 0 : "endPositionEntry() was not called for the last entry";
                        return super.build();
                    }
                }
                return new FloatsBuilder();
            }

            @Override
            public BlockLoader.IntBuilder intsFromDocValues(int expectedCount) {
                return ints(expectedCount);
            }

            @Override
            public BlockLoader.IntBuilder ints(int expectedCount) {
                class IntsBuilder extends TestBlock.Builder implements BlockLoader.IntBuilder {
                    private IntsBuilder() {
                        super(expectedCount);
                    }

                    @Override
                    public IntsBuilder appendInt(int value) {
                        add(value);
                        return this;
                    }
                }
                return new IntsBuilder();
            }

            @Override
            public BlockLoader.LongBuilder longsFromDocValues(int expectedCount) {
                return longs(expectedCount);
            }

            @Override
            public BlockLoader.LongBuilder longs(int expectedCount) {
                class LongsBuilder extends TestBlock.Builder implements BlockLoader.LongBuilder {
                    private LongsBuilder() {
                        super(expectedCount);
                    }

                    @Override
                    public LongsBuilder appendLong(long value) {
                        add(value);
                        return this;
                    }
                }
                return new LongsBuilder();
            }

            @Override
            public BlockLoader.SingletonLongBuilder singletonLongs(int expectedCount) {
                final long[] values = new long[expectedCount];

                return new BlockLoader.SingletonLongBuilder() {

                    private int count;
                    private BlockDocValuesReader.ToDouble toDouble = null;

                    @Override
                    public BlockLoader.Block build() {
                        if (toDouble != null) {
                            return new TestBlock(
                                Arrays.stream(values).mapToDouble(toDouble::convert).boxed().collect(Collectors.toUnmodifiableList())
                            );
                        }
                        return new TestBlock(Arrays.stream(values).boxed().collect(Collectors.toUnmodifiableList()));
                    }

                    @Override
                    public BlockLoader.SingletonLongBuilder appendLongs(long[] newValues, int from, int length) {
                        System.arraycopy(newValues, from, values, count, length);
                        count += length;
                        return this;
                    }

                    @Override
                    public BlockLoader.SingletonLongBuilder appendLong(long value) {
                        values[count++] = value;
                        return this;
                    }

                    @Override
                    public BlockLoader.Builder appendNull() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public BlockLoader.Builder beginPositionEntry() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public BlockLoader.Builder endPositionEntry() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void close() {}
                };
            }

            @Override
            public BlockLoader.SingletonIntBuilder singletonInts(int expectedCount) {
                final int[] values = new int[expectedCount];

                return new BlockLoader.SingletonIntBuilder() {

                    private int count;

                    @Override
                    public BlockLoader.Block build() {
                        return new TestBlock(Arrays.stream(values).boxed().collect(Collectors.toUnmodifiableList()));
                    }

                    @Override
                    public BlockLoader.SingletonIntBuilder appendLongs(long[] newValues, int from, int length) {
                        for (int i = 0; i < length; i++) {
                            values[count + i] = Math.toIntExact(newValues[from + i]);
                        }
                        this.count += length;
                        return this;
                    }

                    @Override
                    public BlockLoader.Builder appendNull() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public BlockLoader.Builder beginPositionEntry() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public BlockLoader.Builder endPositionEntry() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void close() {}
                };
            }

            @Override
            public BlockLoader.SingletonDoubleBuilder singletonDoubles(int expectedCount) {
                final double[] values = new double[expectedCount];

                return new BlockLoader.SingletonDoubleBuilder() {
                    private int count;

                    @Override
                    public BlockLoader.Block build() {
                        return new TestBlock(Arrays.stream(values).boxed().collect(Collectors.toUnmodifiableList()));
                    }

                    @Override
                    public BlockLoader.SingletonDoubleBuilder appendLongs(
                        BlockDocValuesReader.ToDouble toDouble,
                        long[] longValues,
                        int from,
                        int length
                    ) {
                        for (int i = 0; i < length; i++) {
                            values[count + i] = toDouble.convert(longValues[from + i]);
                        }
                        this.count += length;
                        return this;
                    }

                    @Override
                    public BlockLoader.Builder appendNull() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public BlockLoader.Builder beginPositionEntry() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public BlockLoader.Builder endPositionEntry() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void close() {}
                };
            }

            @Override
            public BlockLoader.Builder nulls(int expectedCount) {
                return longs(expectedCount);
            }

            @Override
            public BlockLoader.Block constantNulls(int count) {
                BlockLoader.LongBuilder builder = longs(count);
                for (int i = 0; i < count; i++) {
                    builder.appendNull();
                }
                return builder.build();
            }

            @Override
            public BlockLoader.Block constantBytes(BytesRef value, int count) {
                BlockLoader.BytesRefBuilder builder = bytesRefs(count);
                for (int i = 0; i < count; i++) {
                    builder.appendBytesRef(value);
                }
                return builder.build();
            }

            @Override
            public BlockLoader.Block constantInt(int value, int count) {
                BlockLoader.IntBuilder builder = ints(count);
                for (int i = 0; i < count; i++) {
                    builder.appendInt(value);
                }
                return builder.build();
            }

            @Override
            public BlockLoader.SingletonOrdinalsBuilder singletonOrdinalsBuilder(
                SortedDocValues ordinals,
                int expectedCount,
                boolean isDense
            ) {
                class SingletonOrdsBuilder extends TestBlock.Builder implements BlockLoader.SingletonOrdinalsBuilder {
                    private SingletonOrdsBuilder() {
                        super(expectedCount);
                    }

                    @Override
                    public SingletonOrdsBuilder appendOrd(int value) {
                        try {
                            add(BytesRef.deepCopyOf(ordinals.lookupOrd(value)));
                            return this;
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }

                    @Override
                    public BlockLoader.SingletonOrdinalsBuilder appendOrds(int[] values, int from, int length, int minOrd, int maxOrd) {
                        for (int i = from; i < from + length; i++) {
                            appendOrd(values[i]);
                        }
                        return this;
                    }

                    @Override
                    public BlockLoader.SingletonOrdinalsBuilder appendOrds(int ord, int length) {
                        for (int i = 0; i < length; i++) {
                            appendOrd(ord);
                        }
                        return this;
                    }
                }
                return new SingletonOrdsBuilder();
            }

            @Override
            public BlockLoader.SortedSetOrdinalsBuilder sortedSetOrdinalsBuilder(SortedSetDocValues ordinals, int expectedSize) {
                class SortedSetOrdinalBuilder extends TestBlock.Builder implements BlockLoader.SortedSetOrdinalsBuilder {
                    private SortedSetOrdinalBuilder() {
                        super(expectedSize);
                    }

                    @Override
                    public SortedSetOrdinalBuilder appendOrd(int value) {
                        try {
                            add(BytesRef.deepCopyOf(ordinals.lookupOrd(value)));
                            return this;
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                }
                return new SortedSetOrdinalBuilder();
            }

            public BlockLoader.AggregateMetricDoubleBuilder aggregateMetricDoubleBuilder(int expectedSize) {
                return new AggregateMetricDoubleBlockBuilder(expectedSize);
            }

            @Override
            public BlockLoader.Block buildAggregateMetricDoubleDirect(
                BlockLoader.Block minBlock,
                BlockLoader.Block maxBlock,
                BlockLoader.Block sumBlock,
                BlockLoader.Block countBlock
            ) {
                return AggregateMetricDoubleBlockBuilder.parseAggMetricsToBlock(
                    (TestBlock) minBlock,
                    (TestBlock) maxBlock,
                    (TestBlock) sumBlock,
                    (TestBlock) countBlock
                );
            }

            @Override
            public BlockLoader.ExponentialHistogramBuilder exponentialHistogramBlockBuilder(int count) {
                return new ExponentialHistogramBlockBuilder(this, count);
            }

            @Override
            public BlockLoader.Block buildExponentialHistogramBlockDirect(
                BlockLoader.Block minima,
                BlockLoader.Block maxima,
                BlockLoader.Block sums,
                BlockLoader.Block valueCounts,
                BlockLoader.Block zeroThresholds,
                BlockLoader.Block encodedHistograms
            ) {
                return ExponentialHistogramBlockBuilder.parseHistogramsToBlock(
                    minima,
                    maxima,
                    sums,
                    valueCounts,
                    zeroThresholds,
                    encodedHistograms
                );
            }

            @Override
            public BlockLoader.Block buildTDigestBlockDirect(
                BlockLoader.Block encodedDigests,
                BlockLoader.Block minima,
                BlockLoader.Block maxima,
                BlockLoader.Block sums,
                BlockLoader.Block valueCounts
            ) {
                TestBlock minBlock = (TestBlock) minima;
                TestBlock maxBlock = (TestBlock) maxima;
                TestBlock sumBlock = (TestBlock) sums;
                TestBlock countBlock = (TestBlock) valueCounts;
                TestBlock digestBlock = (TestBlock) encodedDigests;

                assert minBlock.size() == digestBlock.size();
                assert maxBlock.size() == digestBlock.size();
                assert sumBlock.size() == digestBlock.size();
                assert countBlock.size() == digestBlock.size();

                var values = new ArrayList<>(minBlock.size());

                for (int i = 0; i < minBlock.size(); i++) {
                    // we need to represent this complex block somehow
                    HashMap<String, Object> value = new HashMap<>();
                    value.put("min", minBlock.values.get(i));
                    value.put("max", maxBlock.values.get(i));
                    value.put("sum", sumBlock.values.get(i));
                    value.put("value_count", countBlock.values.get(i));
                    value.put("encoded_digest", digestBlock.values.get(i));

                    values.add(value);
                }

                return new TestBlock(values);
            }
        };
    }

    public static final BlockLoader.Docs docs(int... docs) {
        return new BlockLoader.Docs() {
            @Override
            public int count() {
                return docs.length;
            }

            @Override
            public int get(int i) {
                return docs[i];
            }
        };
    }

    public static final BlockLoader.Docs docs(LeafReaderContext ctx) {
        return new BlockLoader.Docs() {
            @Override
            public int count() {
                return ctx.reader().numDocs();
            }

            @Override
            public int get(int i) {
                return i;
            }
        };
    }

    private final List<Object> values;

    private TestBlock(List<Object> values) {
        this.values = values;
    }

    public Object get(int i) {
        return values.get(i);
    }

    public int size() {
        return values.size();
    }

    @Override
    public void close() {
        // TODO assert that we close the test blocks
    }

    @Override
    public String toString() {
        return "TestBlock" + values;
    }

    private abstract static class Builder implements BlockLoader.Builder {
        private final List<Object> values = new ArrayList<>();

        private Matcher<Integer> expectedSize;

        private List<Object> currentPosition = null;

        private Builder(int expectedSize) {
            this.expectedSize = equalTo(expectedSize);
        }

        @Override
        public Builder appendNull() {
            assertNull(currentPosition);
            values.add(null);
            return this;
        }

        @Override
        public Builder beginPositionEntry() {
            assertNull(currentPosition);
            currentPosition = new ArrayList<>();
            values.add(currentPosition);
            return this;
        }

        @Override
        public Builder endPositionEntry() {
            assertNotNull(currentPosition);
            currentPosition = null;
            return this;
        }

        protected void add(Object value) {
            (currentPosition == null ? values : currentPosition).add(value);
        }

        @Override
        public TestBlock build() {
            assertThat(values, hasSize(expectedSize));
            return new TestBlock(values);
        }

        @Override
        public void close() {
            // TODO assert that we close the test block builders
        }
    }

    /**
     * Test implementation of {@link org.elasticsearch.index.mapper.BlockLoader.AggregateMetricDoubleBuilder}.
     * The implementation here is fairly close to the production one.
     */
    private static class AggregateMetricDoubleBlockBuilder implements BlockLoader.AggregateMetricDoubleBuilder {
        private final DoubleBuilder min;
        private final DoubleBuilder max;
        private final DoubleBuilder sum;
        private final IntBuilder count;

        private AggregateMetricDoubleBlockBuilder(int expectedSize) {
            min = new DoubleBuilder(expectedSize);
            max = new DoubleBuilder(expectedSize);
            sum = new DoubleBuilder(expectedSize);
            count = new IntBuilder(expectedSize);
        }

        private static class DoubleBuilder extends TestBlock.Builder implements BlockLoader.DoubleBuilder {
            private DoubleBuilder(int expectedSize) {
                super(expectedSize);
            }

            @Override
            public BlockLoader.DoubleBuilder appendDouble(double value) {
                add(value);
                return this;
            }
        }

        private static class IntBuilder extends TestBlock.Builder implements BlockLoader.IntBuilder {
            private IntBuilder(int expectedSize) {
                super(expectedSize);
            }

            @Override
            public BlockLoader.IntBuilder appendInt(int value) {
                add(value);
                return this;
            }
        }

        @Override
        public BlockLoader.DoubleBuilder min() {
            return min;
        }

        @Override
        public BlockLoader.DoubleBuilder max() {
            return max;
        }

        @Override
        public BlockLoader.DoubleBuilder sum() {
            return sum;
        }

        @Override
        public BlockLoader.IntBuilder count() {
            return count;
        }

        @Override
        public BlockLoader.Block build() {
            var minBlock = min.build();
            var maxBlock = max.build();
            var sumBlock = sum.build();
            var countBlock = count.build();

            return parseAggMetricsToBlock(minBlock, maxBlock, sumBlock, countBlock);
        }

        public static TestBlock parseAggMetricsToBlock(TestBlock minBlock, TestBlock maxBlock, TestBlock sumBlock, TestBlock countBlock) {
            assert minBlock.size() == maxBlock.size();
            assert maxBlock.size() == sumBlock.size();
            assert sumBlock.size() == countBlock.size();

            var values = new ArrayList<>(minBlock.size());

            for (int i = 0; i < minBlock.size(); i++) {
                // we need to represent this complex block somehow
                var value = new HashMap<String, Object>();
                value.put("min", minBlock.values.get(i));
                value.put("max", maxBlock.values.get(i));
                value.put("sum", sumBlock.values.get(i));
                value.put("value_count", countBlock.values.get(i));

                values.add(value);
            }

            return new TestBlock(values);
        }

        @Override
        public BlockLoader.Builder appendNull() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.Builder beginPositionEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.Builder endPositionEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {

        }
    }

    private static class ExponentialHistogramBlockBuilder implements BlockLoader.ExponentialHistogramBuilder {

        private final BlockLoader.DoubleBuilder minima;
        private final BlockLoader.DoubleBuilder maxima;
        private final BlockLoader.DoubleBuilder sums;
        private final BlockLoader.LongBuilder valueCounts;
        private final BlockLoader.DoubleBuilder zeroThresholds;
        private final BlockLoader.BytesRefBuilder encodedHistograms;

        private ExponentialHistogramBlockBuilder(BlockLoader.BlockFactory testFactory, int expectedSize) {
            minima = testFactory.doubles(expectedSize);
            maxima = testFactory.doubles(expectedSize);
            sums = testFactory.doubles(expectedSize);
            valueCounts = testFactory.longs(expectedSize);
            zeroThresholds = testFactory.doubles(expectedSize);
            encodedHistograms = testFactory.bytesRefs(expectedSize);
        }

        @Override
        public BlockLoader.Block build() {
            BlockLoader.Block minimaBlock = minima.build();
            BlockLoader.Block maximaBlock = maxima.build();
            BlockLoader.Block sumsBlock = sums.build();
            BlockLoader.Block valueCountsBlock = valueCounts.build();
            BlockLoader.Block zeroThresholdsBlock = zeroThresholds.build();
            BlockLoader.Block encodedHistogramsBlock = encodedHistograms.build();
            return parseHistogramsToBlock(
                minimaBlock,
                maximaBlock,
                sumsBlock,
                valueCountsBlock,
                zeroThresholdsBlock,
                encodedHistogramsBlock
            );
        }

        public static TestBlock parseHistogramsToBlock(
            BlockLoader.Block minimaBlock,
            BlockLoader.Block maximaBlock,
            BlockLoader.Block sumsBlock,
            BlockLoader.Block valueCountsBlock,
            BlockLoader.Block zeroThresholdsBlock,
            BlockLoader.Block encodedHistogramsBlock
        ) {
            TestBlock minima = (TestBlock) minimaBlock;
            TestBlock maxima = (TestBlock) maximaBlock;
            TestBlock sums = (TestBlock) sumsBlock;
            TestBlock valueCounts = (TestBlock) valueCountsBlock;
            TestBlock zeroThresholds = (TestBlock) zeroThresholdsBlock;
            TestBlock encodedHistograms = (TestBlock) encodedHistogramsBlock;
            int count = minima.values.size();
            assert count == maxima.values.size();
            assert count == sums.values.size();
            assert count == valueCounts.values.size();
            assert count == zeroThresholds.values.size();
            assert count == encodedHistograms.values.size();

            return new TestBlock(IntStream.range(0, count).mapToObj(i -> {
                if (encodedHistograms.get(i) == null) {
                    return null;
                }
                CompressedExponentialHistogram result = new CompressedExponentialHistogram();
                try {
                    Double sum = (Double) sums.get(i);
                    Double min = (Double) minima.get(i);
                    Double max = (Double) maxima.get(i);
                    result.reset(
                        (Double) zeroThresholds.get(i),
                        (Long) valueCounts.get(i),
                        sum == null ? 0.0 : sum,
                        min == null ? Double.NaN : min,
                        max == null ? Double.NaN : max,
                        (BytesRef) encodedHistograms.get(i)
                    );
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return (Object) result;
            }).toList());
        }

        @Override
        public BlockLoader.Builder appendNull() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.Builder beginPositionEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.Builder endPositionEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {

        }

        @Override
        public BlockLoader.DoubleBuilder minima() {
            return minima;
        }

        @Override
        public BlockLoader.DoubleBuilder maxima() {
            return maxima;
        }

        @Override
        public BlockLoader.DoubleBuilder sums() {
            return sums;
        }

        @Override
        public BlockLoader.LongBuilder valueCounts() {
            return valueCounts;
        }

        @Override
        public BlockLoader.DoubleBuilder zeroThresholds() {
            return zeroThresholds;
        }

        @Override
        public BlockLoader.BytesRefBuilder encodedHistograms() {
            return encodedHistograms;
        }
    }
}
