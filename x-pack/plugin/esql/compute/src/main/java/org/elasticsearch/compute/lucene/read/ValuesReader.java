/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.BlockLoaderStoredFieldsFromLeafLoader;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

public abstract class ValuesReader implements ReleasableIterator<Block[]> {
    protected final ValuesSourceReaderOperator operator;
    protected final DocVector docs;
    private int offset;

    ValuesReader(ValuesSourceReaderOperator operator, DocVector docs) {
        this.operator = operator;
        this.docs = docs;
    }

    @Override
    public boolean hasNext() {
        return offset < docs.getPositionCount();
    }

    @Override
    public Block[] next() {
        Block[] target = new Block[operator.fields.length];
        boolean success = false;
        try {
            load(target, offset);
            success = true;
            for (Block b : target) {
                operator.valuesLoaded += b.getTotalValueCount();
            }
            offset += target[0].getPositionCount();
            return target;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(target);
            }
        }
    }

    protected abstract void load(Block[] target, int offset) throws IOException;

    @Override
    public void close() {}

    /**
     * Shared per-load-call work state used by {@link ValuesFromManyReader} and
     * {@link ValuesFromDocSequence}. Holds the builders, readers, and stored-fields
     * loader for a single invocation of {@link #load}.
     */
    abstract class Run implements Releasable {
        final ComputeBlockLoaderFactory blockFactory = new ComputeBlockLoaderFactory(operator.driverContext.blockFactory());
        final Block[] target;
        /**
         * The "final" builder for the block we're going to return. See {@link #current} for
         * how these are built.
         */
        final Block.Builder[] finalBuilders;

        /**
         * The builders for the current shard. These start {@code null} and are filled in when we move
         * to the shard for the first time. When we finish with the shard we build a {@link Block}
         * and convert it to the target type and add it to {@link #finalBuilders}. Then we fill these
         * in for the next shard.
         * <p>
         *     Important: We load in {@code (shard, segment, doc)} sorted order. So we load all values
         *     for a shard at once, meaning once we move to the next shard, we'll never visit the same
         *     shard again.
         * </p>
         */
        final CurrentWork[] current;

        final List<CurrentWork> columnAtATime;
        final List<CurrentWork> rowStride;
        int currentShard = -1;
        BlockLoaderStoredFieldsFromLeafLoader storedFields;

        Run(Block[] target) {
            this.target = target;
            finalBuilders = new Block.Builder[target.length];
            current = new CurrentWork[target.length];
            columnAtATime = new ArrayList<>(target.length);
            rowStride = new ArrayList<>(target.length);
        }

        /**
         * Initializes the final block builders for all fields. Each field has a desired type which
         * might not match the mapped type (in the case of union-types). We create the final block
         * builders using the desired type, one for each field, but then also use inner builders
         * (one for each field and shard), and converters (again one for each field and shard) to
         * actually perform the field loading in a way that is correct for the mapped field type,
         * and then convert between that type and the desired type.
         */
        void initFinalBuilders(int offset) {
            for (int f = 0; f < operator.fields.length; f++) {
                finalBuilders[f] = operator.fields[f].info.type()
                    .newBlockBuilder(docs.getPositionCount() - offset, operator.driverContext.blockFactory());
            }
        }

        void readRowStride(int doc) throws IOException {
            storedFields.advanceTo(doc);
            for (CurrentWork r : rowStride) {
                assert r.columnAtATime == null;
                r.rowStride.read(doc, storedFields, r.builder);
            }
            operator.trackSourceBytesAndRelease(storedFields);
        }

        long estimatedRamBytesUsed() {
            long sum = 0;
            for (int f = 0; f < current.length; f++) {
                sum += finalBuilders[f].estimatedBytes();
                if (current[f].builder != finalBuilders[f]) {
                    sum += current[f].builder.estimatedBytes();
                }
            }
            return sum;
        }

        void fieldsMoved(LeafReaderContext ctx, int shard) throws IOException {
            if (currentShard != shard) {
                if (currentShard >= 0) {
                    convertAndAccumulate();
                }
                moveBuildersAndLoadersToShard();
                currentShard = shard;
            }
            StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
            columnAtATime.clear();
            rowStride.clear();
            for (CurrentWork field : current) {
                field.columnAtATime = field.field.columnAtATime(ctx);
                if (field.columnAtATime != null) {
                    columnAtATime.add(field);
                } else {
                    field.rowStride = field.field.rowStride(ctx);
                    storedFieldsSpec = storedFieldsSpec.merge(field.field.loader.rowStrideStoredFieldSpec());
                    rowStride.add(field);
                }
            }
            SourceLoader sourceLoader = null;
            if (storedFieldsSpec.requiresSource()) {
                sourceLoader = operator.shardContexts.get(shard).newSourceLoader().apply(storedFieldsSpec.sourcePaths());
                storedFieldsSpec = storedFieldsSpec.merge(new StoredFieldsSpec(true, false, sourceLoader.requiredStoredFields()));
            }
            storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
                StoredFieldLoader.fromSpec(storedFieldsSpec).getLoader(ctx, null),
                sourceLoader != null ? sourceLoader.leaf(ctx.reader(), null) : null
            );
            if (false == storedFieldsSpec.equals(StoredFieldsSpec.NO_REQUIREMENTS)) {
                operator.trackStoredFields(storedFieldsSpec, false);
            }
        }

        void convertAndAccumulate() {
            for (CurrentWork currentWork : current) {
                try {
                    currentWork.convertAndAccumulate();
                } finally {
                    currentWork.close();
                    /*
                     * Calling currentWork.close() here is redundant. Once you call
                     * `BlockBuilder#build`, `BlockBuilder#close` becomes a noop. Safe to call
                     * but not required. But calling it is more idiomatic, so we do it.
                     *
                     * In many cases this is the last consumer from of the doc vector, so we
                     * *could* aggressively free the shard context right here - as soon as we're
                     * done with it. But we don't because:
                     * 1. We don't know if we're the last user.
                     * 2. We don't have a code path to free just a single segment's worth of
                     *    references from the doc vector. It'd be easy to build, but much harder
                     *    to build the path that causes us to *NOT* double free.
                     */
                }
            }
        }

        void moveBuildersAndLoadersToShard() {
            for (int f = 0; f < operator.fields.length; f++) {
                current[f] = new CurrentWork(blockFactory, docs, operator.fields[f], finalBuilders[f]);
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(blockFactory, Releasables.wrap(finalBuilders), Releasables.wrap(current));
        }
    }
}
