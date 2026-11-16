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
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockLoaderStoredFieldsFromLeafLoader;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Reads a runtime field whose name falls under a nested object for each child from the child's parent document. A runtime
 * field is a script on the parent, whatever its name, so the child's own document has none of the values it reads. Lucene
 * stores each parent right after its children, so a child's parent is the next parent document.
 * <p>
 * The stored fields and {@code _source} the delegate needs are the parent's, so this loads them for the parent rather than
 * taking the child's, and keeps them while it reads the parent's other children.
 * </p>
 */
final class NestedParentBlockLoader implements BlockLoader {
    private final BlockLoader delegate;
    private final BitSetProducer parents;
    private final Function<Set<String>, SourceLoader> sourceLoaders;

    /**
     * @param sourceLoaders builds the shard's loader for the parent's {@code _source}, given the paths to load or {@code null}
     *                      for all of them
     */
    NestedParentBlockLoader(BlockLoader delegate, BitSetProducer parents, Function<Set<String>, SourceLoader> sourceLoaders) {
        this.delegate = delegate;
        this.parents = parents;
        this.sourceLoaders = sourceLoaders;
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
        StoredFieldsSpec spec = delegate.rowStrideStoredFieldSpec();
        BlockLoaderStoredFieldsFromLeafLoader parentFields = null;
        SourceLoader.Leaf sourceLeaf = null;
        if (spec.noRequirements() == false) {
            if (spec.requiresSource()) {
                SourceLoader sourceLoader = sourceLoaders.apply(spec.sourcePaths().isEmpty() ? null : spec.sourcePaths());
                Set<String> fields = new HashSet<>(spec.requiredStoredFields());
                fields.addAll(sourceLoader.requiredStoredFields());
                spec = spec.merge(new StoredFieldsSpec(false, false, fields));
                sourceLeaf = sourceLoader.leaf(context, null);
            }
            parentFields = new BlockLoaderStoredFieldsFromLeafLoader(
                StoredFieldLoader.fromSpec(spec).getLoader(context, null),
                sourceLeaf,
                spec.sourcePaths()
            );
        }
        RowStrideReader reader = delegate.rowStrideReader(breaker, context);
        return new Reader(breaker, parentBits, reader, parentFields);
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        // Nothing of the child's: whatever the delegate needs is loaded for the parent
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

    private static final class Reader implements RowStrideReader {
        private final CircuitBreaker breaker;
        private final BitSet parents;
        private final RowStrideReader delegate;
        /** The parent's stored fields, or {@code null} when the delegate reads none. */
        private final BlockLoaderStoredFieldsFromLeafLoader parentFields;
        private int lastParent = -1;
        /** Bytes reserved on {@link #breaker} for the largest parent {@code _source} held so far. */
        private long reserved;

        Reader(CircuitBreaker breaker, BitSet parents, RowStrideReader delegate, BlockLoaderStoredFieldsFromLeafLoader parentFields) {
            this.breaker = breaker;
            this.parents = parents;
            this.delegate = delegate;
            this.parentFields = parentFields;
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            int parent = parents.nextSetBit(docId);
            if (parentFields == null) {
                delegate.read(parent, storedFields, builder);
                lastParent = parent;
                return;
            }
            if (parent != lastParent) {
                parentFields.releaseParsedSource();
                parentFields.advanceTo(parent);
                lastParent = parent;
            }
            delegate.read(parent, parentFields, builder);
            long held = parentFields.lastSourceBytesSize();
            if (held > reserved) {
                breaker.addEstimateBytesAndMaybeBreak(held - reserved, "load nested parent source");
                reserved = held;
            }
        }

        /** The parent's stored fields and synthetic source are read in increasing doc ID order. */
        @Override
        public boolean canReuse(int startingDocID) {
            int parent = parents.nextSetBit(startingDocID);
            return parent >= lastParent && delegate.canReuse(parent);
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-reserved);
            reserved = 0;
            delegate.close();
        }

        @Override
        public String toString() {
            return "NestedParent[" + delegate + "]";
        }
    }
}
