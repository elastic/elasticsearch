/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;

/**
 * Reads a runtime field whose name falls under a nested object for each child from the child's parent document. A runtime
 * field is a script on the parent, whatever its name, so the child's own document has none of the values it reads. Lucene
 * stores each parent right after its children, so a child's parent is the next parent document.
 * <p>
 * Only wraps a loader that reads each document by itself, with no stored fields loaded for it: those would be the child's.
 * </p>
 */
final class NestedParentBlockLoader implements BlockLoader {
    private final BlockLoader delegate;
    private final BitSetProducer parents;

    NestedParentBlockLoader(BlockLoader delegate, BitSetProducer parents) {
        if (delegate.rowStrideStoredFieldSpec().noRequirements() == false) {
            throw new IllegalArgumentException("cannot read [" + delegate + "] from the parent document of a nested child");
        }
        this.delegate = delegate;
        this.parents = parents;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return delegate.builder(factory, expectedCount);
    }

    @Override
    public IOFunction<CircuitBreaker, ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) {
        return null;
    }

    @Override
    public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        BitSet parentBits = parents.getBitSet(context);
        return new Reader(parentBits, delegate.rowStrideReader(breaker, context));
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        return StoredFieldsSpec.NO_REQUIREMENTS;
    }

    @Override
    public boolean supportsOrdinals() {
        return false;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "NestedParent[" + delegate + "]";
    }

    private record Reader(BitSet parents, RowStrideReader delegate) implements RowStrideReader {
        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            delegate.read(parents.nextSetBit(docId), storedFields, builder);
        }

        @Override
        public boolean canReuse(int startingDocID) {
            return delegate.canReuse(parents.nextSetBit(startingDocID));
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public String toString() {
            return "NestedParent[" + delegate + "]";
        }
    }
}
