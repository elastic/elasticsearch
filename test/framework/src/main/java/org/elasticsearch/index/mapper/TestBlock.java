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
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestBlock implements BlockLoader.Block {
    public static BlockLoader.BlockFactory factory() {
        return new BlockLoader.BlockFactory() {
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
                    private BytesRefsFromDocValuesBuilder() {
                        super(1);
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

                    @Override
                    public BlockLoader.Block build() {
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
}
