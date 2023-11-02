/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestBlock
    implements
        BlockLoader.BooleanBuilder,
        BlockLoader.BytesRefBuilder,
        BlockLoader.DoubleBuilder,
        BlockLoader.IntBuilder,
        BlockLoader.LongBuilder,
        BlockLoader.SingletonOrdinalsBuilder,
        BlockLoader.Block {
    public static BlockLoader.BuilderFactory FACTORY = new BlockLoader.BuilderFactory() {
        @Override
        public BlockLoader.BooleanBuilder booleansFromDocValues(int expectedCount) {
            return new TestBlock(null);
        }

        @Override
        public BlockLoader.BooleanBuilder booleans(int expectedCount) {
            return new TestBlock(null);
        }

        @Override
        public BlockLoader.BytesRefBuilder bytesRefsFromDocValues(int expectedCount) {
            return new TestBlock(null);
        }

        @Override
        public BlockLoader.BytesRefBuilder bytesRefs(int expectedCount) {
            return new TestBlock(null);
        }

        @Override
        public BlockLoader.DoubleBuilder doublesFromDocValues(int expectedCount) {
            return new TestBlock(null);
        }

        @Override
        public BlockLoader.DoubleBuilder doubles(int expectedCount) {
            return new TestBlock(null);
        }

        @Override
        public BlockLoader.IntBuilder intsFromDocValues(int expectedCount) {
            return new TestBlock(null);
        }

        @Override
        public BlockLoader.IntBuilder ints(int expectedCount) {
            return new TestBlock(null);
        }

        @Override
        public BlockLoader.LongBuilder longsFromDocValues(int expectedCount) {
            return new TestBlock(null);
        }

        @Override
        public BlockLoader.LongBuilder longs(int expectedCount) {
            return new TestBlock(null);
        }

        @Override
        public BlockLoader.Builder nulls(int expectedCount) {
            return new TestBlock(null);
        }

        @Override
        public BlockLoader.SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count) {
            return new TestBlock(ordinals);
        }
    };

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

    private final SortedDocValues sortedDocValues;
    private final List<Object> values = new ArrayList<>();

    private List<Object> currentPosition = null;

    private TestBlock(@Nullable SortedDocValues sortedDocValues) {
        this.sortedDocValues = sortedDocValues;
    }

    public Object get(int i) {
        return values.get(i);
    }

    public int size() {
        return values.size();
    }

    @Override
    public TestBlock appendNull() {
        assertNull(currentPosition);
        values.add(null);
        return this;
    }

    @Override
    public TestBlock beginPositionEntry() {
        assertNull(currentPosition);
        currentPosition = new ArrayList<>();
        values.add(currentPosition);
        return this;
    }

    @Override
    public TestBlock endPositionEntry() {
        assertNotNull(currentPosition);
        currentPosition = null;
        return this;
    }

    @Override
    public TestBlock appendBoolean(boolean value) {
        return add(value);
    }

    @Override
    public TestBlock appendBytesRef(BytesRef value) {
        return add(BytesRef.deepCopyOf(value));
    }

    @Override
    public TestBlock appendDouble(double value) {
        return add(value);
    }

    @Override
    public TestBlock appendInt(int value) {
        return add(value);
    }

    @Override
    public TestBlock appendLong(long value) {
        return add(value);
    }

    @Override
    public TestBlock appendOrd(int value) {
        try {
            return add(sortedDocValues.lookupOrd(value));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public TestBlock build() {
        return this;
    }

    private TestBlock add(Object value) {
        (currentPosition == null ? values : currentPosition).add(value);
        return this;
    }

    @Override
    public void close() {
        // TODO assert that we close the test blocks
    }
}
