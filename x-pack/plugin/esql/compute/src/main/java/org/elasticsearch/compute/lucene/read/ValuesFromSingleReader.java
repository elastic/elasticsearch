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
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockLoaderStoredFieldsFromLeafLoader;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads values from a single leaf. Much more efficient than {@link ValuesFromManyReader}.
 */
class ValuesFromSingleReader extends ValuesReader {
    /**
     * Minimum number of documents for which it is more efficient to use a
     * sequential stored field reader when reading stored fields.
     * <p>
     *     The sequential stored field reader decompresses a whole block of docs
     *     at a time so for very short lists it won't be faster to use it. We use
     *     {@code 10} documents as the boundary for "very short" because it's what
     *     search does, not because we've done extensive testing on the number.
     * </p>
     */
    static final int SEQUENTIAL_BOUNDARY = 10;

    private final int shard;
    private final int segment;

    ValuesFromSingleReader(ValuesSourceReaderOperator operator, DocVector docs) {
        super(operator, docs);
        this.shard = docs.shards().getInt(0);
        this.segment = docs.segments().getInt(0);
    }

    @Override
    protected void load(Block[] target, int offset) throws IOException {
        assert offset == 0; // TODO allow non-0 offset to support splitting pages
        if (docs.singleSegmentNonDecreasing()) {
            loadFromSingleLeaf(target, new BlockLoader.Docs() {
                @Override
                public int count() {
                    return docs.getPositionCount();
                }

                @Override
                public int get(int i) {
                    return docs.docs().getInt(i);
                }
            });
            return;
        }
        int[] forwards = docs.shardSegmentDocMapForwards();
        loadFromSingleLeaf(target, new BlockLoader.Docs() {
            @Override
            public int count() {
                return docs.getPositionCount();
            }

            @Override
            public int get(int i) {
                return docs.docs().getInt(forwards[i]);
            }
        });
        final int[] backwards = docs.shardSegmentDocMapBackwards();
        for (int i = 0; i < target.length; i++) {
            try (Block in = target[i]) {
                target[i] = in.filter(backwards);
            }
        }
    }

    private void loadFromSingleLeaf(Block[] target, BlockLoader.Docs docs) throws IOException {
        int firstDoc = docs.get(0);
        operator.positionFieldWork(shard, segment, firstDoc);
        StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
        List<RowStrideReaderWork> rowStrideReaders = new ArrayList<>(operator.fields.length);
        LeafReaderContext ctx = operator.ctx(shard, segment);
        try (ComputeBlockLoaderFactory loaderBlockFactory = new ComputeBlockLoaderFactory(operator.blockFactory, docs.count())) {
            for (int f = 0; f < operator.fields.length; f++) {
                ValuesSourceReaderOperator.FieldWork field = operator.fields[f];
                BlockLoader.ColumnAtATimeReader columnAtATime = field.columnAtATime(ctx);
                if (columnAtATime != null) {
                    target[f] = (Block) columnAtATime.read(loaderBlockFactory, docs);
                    operator.sanityCheckBlock(columnAtATime, docs.count(), target[f], f);
                } else {
                    rowStrideReaders.add(
                        new RowStrideReaderWork(
                            field.rowStride(ctx),
                            (Block.Builder) field.loader.builder(loaderBlockFactory, docs.count()),
                            field.loader,
                            f
                        )
                    );
                    storedFieldsSpec = storedFieldsSpec.merge(field.loader.rowStrideStoredFieldSpec());
                }
            }

            if (rowStrideReaders.isEmpty() == false) {
                loadFromRowStrideReaders(target, storedFieldsSpec, rowStrideReaders, ctx, docs);
            }
        } finally {
            Releasables.close(rowStrideReaders);
        }
    }

    private void loadFromRowStrideReaders(
        Block[] target,
        StoredFieldsSpec storedFieldsSpec,
        List<RowStrideReaderWork> rowStrideReaders,
        LeafReaderContext ctx,
        BlockLoader.Docs docs
    ) throws IOException {
        SourceLoader sourceLoader = null;
        ValuesSourceReaderOperator.ShardContext shardContext = operator.shardContexts.get(shard);
        if (storedFieldsSpec.requiresSource()) {
            sourceLoader = shardContext.newSourceLoader().get();
            storedFieldsSpec = storedFieldsSpec.merge(new StoredFieldsSpec(true, false, sourceLoader.requiredStoredFields()));
        }
        if (storedFieldsSpec.equals(StoredFieldsSpec.NO_REQUIREMENTS)) {
            throw new IllegalStateException(
                "found row stride readers [" + rowStrideReaders + "] without stored fields [" + storedFieldsSpec + "]"
            );
        }
        StoredFieldLoader storedFieldLoader;
        if (useSequentialStoredFieldsReader(docs, shardContext.storedFieldsSequentialProportion())) {
            storedFieldLoader = StoredFieldLoader.fromSpecSequential(storedFieldsSpec);
            operator.trackStoredFields(storedFieldsSpec, true);
        } else {
            storedFieldLoader = StoredFieldLoader.fromSpec(storedFieldsSpec);
            operator.trackStoredFields(storedFieldsSpec, false);
        }
        BlockLoaderStoredFieldsFromLeafLoader storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
            storedFieldLoader.getLoader(ctx, null),
            sourceLoader != null ? sourceLoader.leaf(ctx.reader(), null) : null
        );
        int p = 0;
        while (p < docs.count()) {
            int doc = docs.get(p++);
            storedFields.advanceTo(doc);
            for (RowStrideReaderWork work : rowStrideReaders) {
                work.read(doc, storedFields);
            }
        }
        for (RowStrideReaderWork work : rowStrideReaders) {
            target[work.offset] = work.build();
            operator.sanityCheckBlock(work.reader, p, target[work.offset], work.offset);
        }
    }

    /**
     * Is it more efficient to use a sequential stored field reader
     * when reading stored fields for the documents contained in {@code docIds}?
     */
    private boolean useSequentialStoredFieldsReader(BlockLoader.Docs docs, double storedFieldsSequentialProportion) {
        int count = docs.count();
        if (count < SEQUENTIAL_BOUNDARY) {
            return false;
        }
        int range = docs.get(count - 1) - docs.get(0);
        return range * storedFieldsSequentialProportion <= count;
    }

    private record RowStrideReaderWork(BlockLoader.RowStrideReader reader, Block.Builder builder, BlockLoader loader, int offset)
        implements
            Releasable {
        void read(int doc, BlockLoaderStoredFieldsFromLeafLoader storedFields) throws IOException {
            reader.read(doc, storedFields, builder);
        }

        Block build() {
            return (Block) loader.convert(builder.build());
        }

        @Override
        public void close() {
            builder.close();
        }
    }
}
