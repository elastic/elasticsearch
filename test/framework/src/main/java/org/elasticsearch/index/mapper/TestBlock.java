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
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestBlock implements BlockLoader.Block {
    public static BlockLoader.BlockFactory factory(int pageSize) {
        return new BlockLoader.BlockFactory() {
            @Override
            public BlockLoader.BooleanBuilder booleansFromDocValues(int expectedCount) {
                return booleans(expectedCount);
            }

            @Override
            public BlockLoader.BooleanBuilder booleans(int expectedCount) {
                class BooleansBuilder extends TestBlock.Builder implements BlockLoader.BooleanBuilder {
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
                return bytesRefs(expectedCount);
            }

            @Override
            public BlockLoader.BytesRefBuilder bytesRefs(int expectedCount) {
                class BytesRefsBuilder extends TestBlock.Builder implements BlockLoader.BytesRefBuilder {
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
                    @Override
                    public DoublesBuilder appendDouble(double value) {
                        add(value);
                        return this;
                    }
                }
                return new DoublesBuilder();
            }

            @Override
            public BlockLoader.IntBuilder intsFromDocValues(int expectedCount) {
                return ints(expectedCount);
            }

            @Override
            public BlockLoader.IntBuilder ints(int expectedCount) {
                class IntsBuilder extends TestBlock.Builder implements BlockLoader.IntBuilder {
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
                    @Override
                    public LongsBuilder appendLong(long value) {
                        add(value);
                        return this;
                    }
                }
                return new LongsBuilder();
            }

            @Override
            public BlockLoader.Builder nulls(int expectedCount) {
                return longs(expectedCount);
            }

            @Override
            public BlockLoader.Block constantNulls() {
                BlockLoader.LongBuilder builder = longs(pageSize);
                for (int i = 0; i < pageSize; i++) {
                    builder.appendNull();
                }
                return builder.build();
            }

            @Override
            public BlockLoader.Block constantBytes(BytesRef value) {
                BlockLoader.BytesRefBuilder builder = bytesRefs(pageSize);
                for (int i = 0; i < pageSize; i++) {
                    builder.appendBytesRef(value);
                }
                return builder.build();
            }

            @Override
            public BlockLoader.SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count) {
                class SingletonOrdsBuilder extends TestBlock.Builder implements BlockLoader.SingletonOrdinalsBuilder {
                    @Override
                    public SingletonOrdsBuilder appendOrd(int value) {
                        try {
                            add(ordinals.lookupOrd(value));
                            return this;
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                }
                return new SingletonOrdsBuilder();
            }

            @Override
            public BlockLoader.AggregateMetricDoubleBuilder aggregateMetricDoubleBuilder(int count) {
                return new AggregateMetricDoubleBlockBuilder();
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

        private List<Object> currentPosition = null;

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
        private final DoubleBuilder min = new DoubleBuilder();
        private final DoubleBuilder max = new DoubleBuilder();
        private final DoubleBuilder sum = new DoubleBuilder();
        private final IntBuilder count = new IntBuilder();

        private static class DoubleBuilder extends TestBlock.Builder implements BlockLoader.DoubleBuilder {
            @Override
            public BlockLoader.DoubleBuilder appendDouble(double value) {
                add(value);
                return this;
            }
        }

        private static class IntBuilder extends TestBlock.Builder implements BlockLoader.IntBuilder {
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
