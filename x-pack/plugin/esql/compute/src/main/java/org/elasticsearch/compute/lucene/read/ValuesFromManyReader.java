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

/**
 * Loads values from a many leaves. Much less efficient than {@link ValuesFromSingleReader}.
 */
class ValuesFromManyReader extends ValuesReader {
    private final int[] forwards;
    private final int[] backwards;
    private final BlockLoader.RowStrideReader[] rowStride;

    private BlockLoaderStoredFieldsFromLeafLoader storedFields;

    ValuesFromManyReader(ValuesSourceReaderOperator operator, DocVector docs) {
        super(operator, docs);
        forwards = docs.shardSegmentDocMapForwards();
        backwards = docs.shardSegmentDocMapBackwards();
        rowStride = new BlockLoader.RowStrideReader[operator.fields.length];
    }

    @Override
    protected void load(Block[] target, int offset) throws IOException {
        try (Run run = new Run(target)) {
            run.run(offset);
        }
    }

    class Run implements Releasable {
        private final Block[] target;
        private final Block.Builder[][] builders;
        private final BlockLoader[][] converters;
        private final Block.Builder[] fieldTypeBuilders;

        Run(Block[] target) {
            this.target = target;
            fieldTypeBuilders = new Block.Builder[target.length];
            builders = new Block.Builder[target.length][operator.shardContexts.size()];
            converters = new BlockLoader[target.length][operator.shardContexts.size()];
        }

        void run(int offset) throws IOException {
            assert offset == 0; // TODO allow non-0 offset to support splitting pages
            for (int f = 0; f < operator.fields.length; f++) {
                /*
                 * Important note: each field has a desired type, which might not match the mapped type (in the case of union-types).
                 * We create the final block builders using the desired type, one for each field, but then also use inner builders
                 * (one for each field and shard), and converters (again one for each field and shard) to actually perform the field
                 * loading in a way that is correct for the mapped field type, and then convert between that type and the desired type.
                 */
                fieldTypeBuilders[f] = operator.fields[f].info.type().newBlockBuilder(docs.getPositionCount(), operator.blockFactory);
                builders[f] = new Block.Builder[operator.shardContexts.size()];
                converters[f] = new BlockLoader[operator.shardContexts.size()];
            }
            try (
                ComputeBlockLoaderFactory loaderBlockFactory = new ComputeBlockLoaderFactory(operator.blockFactory, docs.getPositionCount())
            ) {
                int p = forwards[offset];
                int shard = docs.shards().getInt(p);
                int segment = docs.segments().getInt(p);
                int firstDoc = docs.docs().getInt(p);
                operator.positionFieldWork(shard, segment, firstDoc);
                LeafReaderContext ctx = operator.ctx(shard, segment);
                fieldsMoved(ctx, shard);
                verifyBuilders(loaderBlockFactory, shard);
                read(firstDoc, shard);

                int i = offset + 1;
                while (i < forwards.length) {
                    p = forwards[i];
                    shard = docs.shards().getInt(p);
                    segment = docs.segments().getInt(p);
                    boolean changedSegment = operator.positionFieldWorkDocGuaranteedAscending(shard, segment);
                    if (changedSegment) {
                        ctx = operator.ctx(shard, segment);
                        fieldsMoved(ctx, shard);
                    }
                    verifyBuilders(loaderBlockFactory, shard);
                    read(docs.docs().getInt(p), shard);
                    i++;
                }
                buildBlocks();
            }
        }

        private void buildBlocks() {
            for (int f = 0; f < target.length; f++) {
                for (int s = 0; s < operator.shardContexts.size(); s++) {
                    if (builders[f][s] != null) {
                        try (Block orig = (Block) converters[f][s].convert(builders[f][s].build())) {
                            fieldTypeBuilders[f].copyFrom(orig, 0, orig.getPositionCount());
                        }
                    }
                }
                try (Block targetBlock = fieldTypeBuilders[f].build()) {
                    target[f] = targetBlock.filter(backwards);
                }
                operator.sanityCheckBlock(rowStride[f], backwards.length, target[f], f);
            }
        }

        private void verifyBuilders(ComputeBlockLoaderFactory loaderBlockFactory, int shard) {
            for (int f = 0; f < operator.fields.length; f++) {
                if (builders[f][shard] == null) {
                    // Note that this relies on field.newShard() to set the loader and converter correctly for the current shard
                    builders[f][shard] = (Block.Builder) operator.fields[f].loader.builder(loaderBlockFactory, docs.getPositionCount());
                    converters[f][shard] = operator.fields[f].loader;
                }
            }
        }

        private void read(int doc, int shard) throws IOException {
            storedFields.advanceTo(doc);
            for (int f = 0; f < builders.length; f++) {
                rowStride[f].read(doc, storedFields, builders[f][shard]);
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(fieldTypeBuilders);
            for (int f = 0; f < operator.fields.length; f++) {
                Releasables.closeExpectNoException(builders[f]);
            }
        }
    }

    private void fieldsMoved(LeafReaderContext ctx, int shard) throws IOException {
        StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
        for (int f = 0; f < operator.fields.length; f++) {
            ValuesSourceReaderOperator.FieldWork field = operator.fields[f];
            rowStride[f] = field.rowStride(ctx);
            storedFieldsSpec = storedFieldsSpec.merge(field.loader.rowStrideStoredFieldSpec());
        }
        SourceLoader sourceLoader = null;
        if (storedFieldsSpec.requiresSource()) {
            sourceLoader = operator.shardContexts.get(shard).newSourceLoader().get();
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
}
