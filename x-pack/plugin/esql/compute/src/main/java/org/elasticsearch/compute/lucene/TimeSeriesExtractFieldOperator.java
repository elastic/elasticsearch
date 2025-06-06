/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockLoaderStoredFieldsFromLeafLoader;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;

/**
 * A variant of {@link ValuesSourceReaderOperator} for extracting fields in time-series indices. The differences are:
 * 1. Caches all segments of the last shard instead of only the last segment, since data in time-series can come from
 *    any segment at any time
 * 2. Although docs do not arrive in the global order (by shard, then segment, then docId), they are still sorted
 *    within each segment; hence, this reader does not perform sorting and regrouping, which are expensive.
 * 3. For dimension fields, values are read only once per tsid.
 * These changes are made purely for performance reasons. We should look into consolidating this operator with
 * {@link ValuesSourceReaderOperator} by adding some metadata to the {@link DocVector} and handling them accordingly.
 */
public class TimeSeriesExtractFieldOperator extends AbstractPageMappingOperator {

    public record Factory(List<ValuesSourceReaderOperator.FieldInfo> fields, List<? extends ShardContext> shardContexts)
        implements
            OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new TimeSeriesExtractFieldOperator(driverContext.blockFactory(), fields, shardContexts);
        }

        @Override
        public String describe() {
            StringBuilder sb = new StringBuilder();
            sb.append("TimeSeriesExtractFieldOperator[fields = [");
            if (fields.size() < 10) {
                boolean first = true;
                for (var f : fields) {
                    if (first) {
                        first = false;
                    } else {
                        sb.append(", ");
                    }
                    sb.append(f.name());
                }
            } else {
                sb.append(fields.size()).append(" fields");
            }
            return sb.append("]]").toString();
        }
    }

    private final BlockFactory blockFactory;
    private final List<ValuesSourceReaderOperator.FieldInfo> fields;
    private final List<? extends ShardContext> shardContexts;

    private ShardLevelFieldsReader fieldsReader;

    public TimeSeriesExtractFieldOperator(
        BlockFactory blockFactory,
        List<ValuesSourceReaderOperator.FieldInfo> fields,
        List<? extends ShardContext> shardContexts
    ) {
        this.blockFactory = blockFactory;
        this.fields = fields;
        this.shardContexts = shardContexts;
    }

    private OrdinalBytesRefVector getTsid(Page page, int channel) {
        BytesRefBlock block = page.getBlock(channel);
        OrdinalBytesRefBlock ordinals = block.asOrdinals();
        if (ordinals == null) {
            throw new IllegalArgumentException("tsid must be an ordinals block, got: " + block.getClass().getName());
        }
        OrdinalBytesRefVector vector = ordinals.asVector();
        if (vector == null) {
            throw new IllegalArgumentException("tsid must be an ordinals vector, got: " + block.getClass().getName());
        }
        return vector;
    }

    private DocVector getDocVector(Page page, int channel) {
        DocBlock docBlock = page.getBlock(channel);
        DocVector docVector = docBlock.asVector();
        if (docVector == null) {
            throw new IllegalArgumentException("doc must be a doc vector, got: " + docBlock.getClass().getName());
        }
        return docVector;
    }

    @Override
    protected Page process(Page page) {
        try {
            return processUnchecked(page);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Page processUnchecked(Page page) throws IOException {
        DocVector docVector = getDocVector(page, 0);
        IntVector shards = docVector.shards();
        if (shards.isConstant() == false) {
            throw new IllegalArgumentException("shards must be a constant vector, got: " + shards.getClass().getName());
        }
        OrdinalBytesRefVector tsidVector = getTsid(page, 1);
        IntVector tsidOrdinals = tsidVector.getOrdinalsVector();
        int shardIndex = shards.getInt(0);
        if (fieldsReader == null || fieldsReader.shardIndex != shardIndex) {
            Releasables.close(fieldsReader);
            fieldsReader = new ShardLevelFieldsReader(shardIndex, blockFactory, shardContexts.get(shardIndex), fields);
        }
        fieldsReader.prepareForReading(page.getPositionCount());
        IntVector docs = docVector.docs();
        IntVector segments = docVector.segments();
        int lastTsidOrdinal = -1;
        for (int p = 0; p < docs.getPositionCount(); p++) {
            int doc = docs.getInt(p);
            int segment = segments.getInt(p);
            int tsidOrd = tsidOrdinals.getInt(p);
            if (tsidOrd == lastTsidOrdinal) {
                fieldsReader.readValues(segment, doc, true);
            } else {
                fieldsReader.readValues(segment, doc, false);
                lastTsidOrdinal = tsidOrd;
            }
        }
        Block[] blocks = new Block[fields.size()];
        Page result = null;
        try {
            fieldsReader.buildBlocks(blocks, tsidOrdinals);
            result = page.appendBlocks(blocks);
            return result;
        } finally {
            if (result == null) {
                Releasables.close(blocks);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TimeSeriesExtractFieldOperator[fields = [");
        if (fields.size() < 10) {
            boolean first = true;
            for (var f : fields) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                sb.append(f.name());
            }
        } else {
            sb.append(fields.size()).append(" fields");
        }
        return sb.append("]]").toString();
    }

    @Override
    public void close() {
        Releasables.close(fieldsReader, super::close);
    }

    static class BlockLoaderFactory extends ValuesSourceReaderOperator.DelegatingBlockLoaderFactory {
        BlockLoaderFactory(BlockFactory factory) {
            super(factory);
        }

        @Override
        public BlockLoader.Block constantNulls() {
            throw new UnsupportedOperationException("must not be used by column readers");
        }

        @Override
        public BlockLoader.Block constantBytes(BytesRef value) {
            throw new UnsupportedOperationException("must not be used by column readers");
        }

        @Override
        public BlockLoader.SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count) {
            throw new UnsupportedOperationException("must not be used by column readers");
        }
    }

    static final class ShardLevelFieldsReader implements Releasable {
        final int shardIndex;
        private final BlockLoaderFactory blockFactory;
        private final SegmentLevelFieldsReader[] segments;
        private final BlockLoader[] loaders;
        private final boolean[] dimensions;
        private final Block.Builder[] builders;
        private final StoredFieldsSpec storedFieldsSpec;
        private final SourceLoader sourceLoader;

        ShardLevelFieldsReader(
            int shardIndex,
            BlockFactory blockFactory,
            ShardContext shardContext,
            List<ValuesSourceReaderOperator.FieldInfo> fields
        ) {
            this.shardIndex = shardIndex;
            this.blockFactory = new BlockLoaderFactory(blockFactory);
            final IndexReader indexReader = shardContext.searcher().getIndexReader();
            this.segments = new SegmentLevelFieldsReader[indexReader.leaves().size()];
            this.loaders = new BlockLoader[fields.size()];
            this.builders = new Block.Builder[loaders.length];
            StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
            for (int i = 0; i < fields.size(); i++) {
                BlockLoader loader = fields.get(i).blockLoader().apply(shardIndex);
                storedFieldsSpec = storedFieldsSpec.merge(loader.rowStrideStoredFieldSpec());
                loaders[i] = loader;
            }
            for (int i = 0; i < indexReader.leaves().size(); i++) {
                LeafReaderContext leafReaderContext = indexReader.leaves().get(i);
                segments[i] = new SegmentLevelFieldsReader(leafReaderContext, loaders);
            }
            if (storedFieldsSpec.requiresSource()) {
                sourceLoader = shardContext.newSourceLoader();
                storedFieldsSpec = storedFieldsSpec.merge(new StoredFieldsSpec(false, false, sourceLoader.requiredStoredFields()));
            } else {
                sourceLoader = null;
            }
            this.storedFieldsSpec = storedFieldsSpec;
            this.dimensions = new boolean[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                dimensions[i] = shardContext.fieldType(fields.get(i).name()).isDimension();
            }
        }

        /**
         * For dimension fields, skips reading them when {@code nonDimensionFieldsOnly} is true,
         * since they only need to be read once per tsid.
         */
        void readValues(int segment, int docID, boolean nonDimensionFieldsOnly) throws IOException {
            segments[segment].read(docID, builders, nonDimensionFieldsOnly, dimensions);
        }

        void prepareForReading(int estimatedSize) throws IOException {
            if (this.builders.length > 0 && this.builders[0] == null) {
                for (int f = 0; f < builders.length; f++) {
                    builders[f] = (Block.Builder) loaders[f].builder(blockFactory, estimatedSize);
                }
            }
            for (SegmentLevelFieldsReader segment : segments) {
                segment.reinitializeIfNeeded(sourceLoader, storedFieldsSpec);
            }
        }

        void buildBlocks(Block[] blocks, IntVector tsidOrdinals) {
            for (int i = 0; i < builders.length; i++) {
                if (dimensions[i]) {
                    blocks[i] = buildBlockForDimensionField(builders[i], tsidOrdinals);
                } else {
                    blocks[i] = builders[i].build();
                }
            }
            Arrays.fill(builders, null);
        }

        private Block buildBlockForDimensionField(Block.Builder builder, IntVector tsidOrdinals) {
            try (var values = builder.build()) {
                if (values.asVector() instanceof BytesRefVector bytes) {
                    tsidOrdinals.incRef();
                    values.incRef();
                    return new OrdinalBytesRefVector(tsidOrdinals, bytes).asBlock();
                } else if (values.areAllValuesNull()) {
                    return blockFactory.factory.newConstantNullBlock(tsidOrdinals.getPositionCount());
                } else {
                    final int positionCount = tsidOrdinals.getPositionCount();
                    try (var newBuilder = values.elementType().newBlockBuilder(positionCount, blockFactory.factory)) {
                        for (int p = 0; p < positionCount; p++) {
                            int pos = tsidOrdinals.getInt(p);
                            newBuilder.copyFrom(values, pos, pos + 1);
                        }
                        return newBuilder.build();
                    }
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(builders);
        }
    }

    static final class SegmentLevelFieldsReader {
        private final BlockLoader.RowStrideReader[] rowStride;
        private final BlockLoader[] loaders;
        private final LeafReaderContext leafContext;
        private BlockLoaderStoredFieldsFromLeafLoader storedFields;
        private Thread loadedThread = null;

        SegmentLevelFieldsReader(LeafReaderContext leafContext, BlockLoader[] loaders) {
            this.leafContext = leafContext;
            this.loaders = loaders;
            this.rowStride = new BlockLoader.RowStrideReader[loaders.length];
        }

        private void reinitializeIfNeeded(SourceLoader sourceLoader, StoredFieldsSpec storedFieldsSpec) throws IOException {
            final Thread currentThread = Thread.currentThread();
            if (loadedThread != currentThread) {
                loadedThread = currentThread;
                for (int f = 0; f < loaders.length; f++) {
                    rowStride[f] = loaders[f].rowStrideReader(leafContext);
                }
                storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
                    StoredFieldLoader.fromSpec(storedFieldsSpec).getLoader(leafContext, null),
                    sourceLoader != null ? sourceLoader.leaf(leafContext.reader(), null) : null
                );
            }
        }

        void read(int docId, Block.Builder[] builder, boolean nonDimensionFieldsOnly, boolean[] dimensions) throws IOException {
            storedFields.advanceTo(docId);
            if (nonDimensionFieldsOnly) {
                for (int i = 0; i < rowStride.length; i++) {
                    if (dimensions[i] == false) {
                        rowStride[i].read(docId, storedFields, builder[i]);
                    }
                }
            } else {
                for (int i = 0; i < rowStride.length; i++) {
                    rowStride[i].read(docId, storedFields, builder[i]);
                }
            }
        }
    }
}
