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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.SingletonOrdinalsBuilder;
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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * Operator that extracts doc_values from a Lucene index out of pages that have been produced by {@link LuceneSourceOperator}
 * and outputs them to a new column.
 */
public class ValuesSourceReaderOperator extends AbstractPageMappingOperator {
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

    /**
     * Creates a factory for {@link ValuesSourceReaderOperator}.
     * @param fields fields to load
     * @param shardContexts per-shard loading information
     * @param docChannel the channel containing the shard, leaf/segment and doc id
     */
    public record Factory(List<FieldInfo> fields, List<ShardContext> shardContexts, int docChannel) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new ValuesSourceReaderOperator(driverContext.blockFactory(), fields, shardContexts, docChannel);
        }

        @Override
        public String describe() {
            StringBuilder sb = new StringBuilder();
            sb.append("ValuesSourceReaderOperator[fields = [");
            if (fields.size() < 10) {
                boolean first = true;
                for (FieldInfo f : fields) {
                    if (first) {
                        first = false;
                    } else {
                        sb.append(", ");
                    }
                    sb.append(f.name);
                }
            } else {
                sb.append(fields.size()).append(" fields");
            }
            return sb.append("]]").toString();
        }
    }

    public record ShardContext(IndexReader reader, Supplier<SourceLoader> newSourceLoader) {}

    private final List<FieldWork> fields;
    private final List<ShardContext> shardContexts;
    private final int docChannel;
    private final BlockFactory blockFactory;

    private final Map<String, Integer> readersBuilt = new TreeMap<>();

    /**
     * Configuration for a field to load.
     *
     * {@code blockLoaders} is a list, one entry per shard, of
     * {@link BlockLoader}s which load the actual blocks.
     */
    public record FieldInfo(String name, List<BlockLoader> blockLoaders) {}

    /**
     * Creates a new extractor
     * @param fields fields to load
     * @param docChannel the channel containing the shard, leaf/segment and doc id
     */
    public ValuesSourceReaderOperator(BlockFactory blockFactory, List<FieldInfo> fields, List<ShardContext> shardContexts, int docChannel) {
        this.fields = fields.stream().map(f -> new FieldWork(f)).toList();
        this.shardContexts = shardContexts;
        this.docChannel = docChannel;
        this.blockFactory = blockFactory;
    }

    @Override
    protected Page process(Page page) {
        DocVector docVector = page.<DocBlock>getBlock(docChannel).asVector();

        Block[] blocks = new Block[fields.size()];
        boolean success = false;
        try {
            if (docVector.singleSegmentNonDecreasing()) {
                loadFromSingleLeaf(blocks, docVector);
            } else {
                loadFromManyLeaves(blocks, docVector);
            }
            success = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
        return page.appendBlocks(blocks);
    }

    private void loadFromSingleLeaf(Block[] blocks, DocVector docVector) throws IOException {
        int shard = docVector.shards().getInt(0);
        int segment = docVector.segments().getInt(0);
        int firstDoc = docVector.docs().getInt(0);
        IntVector docs = docVector.docs();
        BlockLoader.Docs loaderDocs = new BlockLoader.Docs() {
            @Override
            public int count() {
                return docs.getPositionCount();
            }

            @Override
            public int get(int i) {
                return docs.getInt(i);
            }
        };
        StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
        List<RowStrideReaderWork> rowStrideReaders = new ArrayList<>(fields.size());
        ComputeBlockLoaderFactory loaderBlockFactory = new ComputeBlockLoaderFactory(blockFactory, docs.getPositionCount());
        try {
            for (int b = 0; b < fields.size(); b++) {
                FieldWork field = fields.get(b);
                BlockLoader.ColumnAtATimeReader columnAtATime = field.columnAtATime.reader(shard, segment, firstDoc);
                if (columnAtATime != null) {
                    blocks[b] = (Block) columnAtATime.read(loaderBlockFactory, loaderDocs);
                } else {
                    BlockLoader.RowStrideReader rowStride = field.rowStride.reader(shard, segment, firstDoc);
                    rowStrideReaders.add(
                        new RowStrideReaderWork(
                            rowStride,
                            (Block.Builder) field.info.blockLoaders.get(shard).builder(loaderBlockFactory, docs.getPositionCount()),
                            b
                        )
                    );
                    storedFieldsSpec = storedFieldsSpec.merge(field.info.blockLoaders.get(shard).rowStrideStoredFieldSpec());
                }
            }

            if (rowStrideReaders.isEmpty()) {
                return;
            }
            if (storedFieldsSpec.equals(StoredFieldsSpec.NO_REQUIREMENTS)) {
                throw new IllegalStateException(
                    "found row stride readers [" + rowStrideReaders + "] without stored fields [" + storedFieldsSpec + "]"
                );
            }
            LeafReaderContext ctx = ctx(shard, segment);
            StoredFieldLoader storedFieldLoader;
            if (useSequentialStoredFieldsReader(docVector.docs())) {
                storedFieldLoader = StoredFieldLoader.fromSpecSequential(storedFieldsSpec);
                trackStoredFields(storedFieldsSpec, true);
            } else {
                storedFieldLoader = StoredFieldLoader.fromSpec(storedFieldsSpec);
                trackStoredFields(storedFieldsSpec, false);
            }
            BlockLoaderStoredFieldsFromLeafLoader storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
                // TODO enable the optimization by passing non-null to docs if correct
                storedFieldLoader.getLoader(ctx, null),
                storedFieldsSpec.requiresSource() ? shardContexts.get(shard).newSourceLoader.get().leaf(ctx.reader(), null) : null
            );
            for (int p = 0; p < docs.getPositionCount(); p++) {
                int doc = docs.getInt(p);
                if (storedFields != null) {
                    storedFields.advanceTo(doc);
                }
                for (int r = 0; r < rowStrideReaders.size(); r++) {
                    RowStrideReaderWork work = rowStrideReaders.get(r);
                    work.reader.read(doc, storedFields, work.builder);
                }
            }
            for (int r = 0; r < rowStrideReaders.size(); r++) {
                RowStrideReaderWork work = rowStrideReaders.get(r);
                blocks[work.offset] = work.builder.build();
            }
        } finally {
            Releasables.close(rowStrideReaders);
        }
    }

    private void loadFromManyLeaves(Block[] blocks, DocVector docVector) throws IOException {
        IntVector shards = docVector.shards();
        IntVector segments = docVector.segments();
        IntVector docs = docVector.docs();
        Block.Builder[] builders = new Block.Builder[blocks.length];
        int[] forwards = docVector.shardSegmentDocMapForwards();
        ComputeBlockLoaderFactory loaderBlockFactory = new ComputeBlockLoaderFactory(blockFactory, docs.getPositionCount());
        try {
            for (int b = 0; b < fields.size(); b++) {
                FieldWork field = fields.get(b);
                builders[b] = builderFromFirstNonNull(loaderBlockFactory, field, docs.getPositionCount());
            }
            int lastShard = -1;
            int lastSegment = -1;
            BlockLoaderStoredFieldsFromLeafLoader storedFields = null;
            for (int i = 0; i < forwards.length; i++) {
                int p = forwards[i];
                int shard = shards.getInt(p);
                int segment = segments.getInt(p);
                int doc = docs.getInt(p);
                if (shard != lastShard || segment != lastSegment) {
                    lastShard = shard;
                    lastSegment = segment;
                    StoredFieldsSpec storedFieldsSpec = storedFieldsSpecForShard(shard);
                    LeafReaderContext ctx = ctx(shard, segment);
                    storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
                        StoredFieldLoader.fromSpec(storedFieldsSpec).getLoader(ctx, null),
                        storedFieldsSpec.requiresSource() ? shardContexts.get(shard).newSourceLoader.get().leaf(ctx.reader(), null) : null
                    );
                    if (false == storedFieldsSpec.equals(StoredFieldsSpec.NO_REQUIREMENTS)) {
                        trackStoredFields(storedFieldsSpec, false);
                    }
                }
                storedFields.advanceTo(doc);
                for (int r = 0; r < blocks.length; r++) {
                    fields.get(r).rowStride.reader(shard, segment, doc).read(doc, storedFields, builders[r]);
                }
            }
            for (int r = 0; r < blocks.length; r++) {
                try (Block orig = builders[r].build()) {
                    blocks[r] = orig.filter(docVector.shardSegmentDocMapBackwards());
                }
            }
        } finally {
            Releasables.closeExpectNoException(builders);
        }
    }

    /**
     * Is it more efficient to use a sequential stored field reader
     * when reading stored fields for the documents contained in {@code docIds}?
     */
    private boolean useSequentialStoredFieldsReader(IntVector docIds) {
        return docIds.getPositionCount() >= SEQUENTIAL_BOUNDARY
            && docIds.getInt(docIds.getPositionCount() - 1) - docIds.getInt(0) == docIds.getPositionCount() - 1;
    }

    private void trackStoredFields(StoredFieldsSpec spec, boolean sequential) {
        readersBuilt.merge(
            "stored_fields["
                + "requires_source:"
                + spec.requiresSource()
                + ", fields:"
                + spec.requiredStoredFields().size()
                + ", sequential: "
                + sequential
                + "]",
            1,
            (prev, one) -> prev + one
        );
    }

    /**
     * Returns a builder from the first non - {@link BlockLoader#CONSTANT_NULLS} loader
     * in the list. If they are all the null loader then returns a null builder.
     */
    private Block.Builder builderFromFirstNonNull(BlockLoader.BlockFactory loaderBlockFactory, FieldWork field, int positionCount) {
        for (BlockLoader loader : field.info.blockLoaders) {
            if (loader != BlockLoader.CONSTANT_NULLS) {
                return (Block.Builder) loader.builder(loaderBlockFactory, positionCount);
            }
        }
        // All null, just let the first one build the null block loader.
        return (Block.Builder) field.info.blockLoaders.get(0).builder(loaderBlockFactory, positionCount);
    }

    private StoredFieldsSpec storedFieldsSpecForShard(int shard) {
        StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
        for (int b = 0; b < fields.size(); b++) {
            FieldWork field = fields.get(b);
            storedFieldsSpec = storedFieldsSpec.merge(field.info.blockLoaders.get(shard).rowStrideStoredFieldSpec());
        }
        return storedFieldsSpec;
    }

    private class FieldWork {
        final FieldInfo info;
        final GuardedReader<BlockLoader.ColumnAtATimeReader> columnAtATime = new GuardedReader<>() {
            @Override
            BlockLoader.ColumnAtATimeReader build(BlockLoader loader, LeafReaderContext ctx) throws IOException {
                return loader.columnAtATimeReader(ctx);
            }

            @Override
            String type() {
                return "column_at_a_time";
            }
        };

        final GuardedReader<BlockLoader.RowStrideReader> rowStride = new GuardedReader<>() {
            @Override
            BlockLoader.RowStrideReader build(BlockLoader loader, LeafReaderContext ctx) throws IOException {
                return loader.rowStrideReader(ctx);
            }

            @Override
            String type() {
                return "row_stride";
            }
        };

        FieldWork(FieldInfo info) {
            this.info = info;
        }

        private abstract class GuardedReader<V extends BlockLoader.Reader> {
            private int lastShard = -1;
            private int lastSegment = -1;
            V lastReader;

            V reader(int shard, int segment, int startingDocId) throws IOException {
                if (lastShard == shard && lastSegment == segment) {
                    if (lastReader == null) {
                        return null;
                    }
                    if (lastReader.canReuse(startingDocId)) {
                        return lastReader;
                    }
                }
                lastShard = shard;
                lastSegment = segment;
                lastReader = build(info.blockLoaders.get(shard), ctx(shard, segment));
                readersBuilt.merge(info.name + ":" + type() + ":" + lastReader, 1, (prev, one) -> prev + one);
                return lastReader;
            }

            abstract V build(BlockLoader loader, LeafReaderContext ctx) throws IOException;

            abstract String type();
        }
    }

    private record RowStrideReaderWork(BlockLoader.RowStrideReader reader, Block.Builder builder, int offset) implements Releasable {
        @Override
        public void close() {
            builder.close();
        }
    }

    private LeafReaderContext ctx(int shard, int segment) {
        return shardContexts.get(shard).reader.leaves().get(segment);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ValuesSourceReaderOperator[fields = [");
        if (fields.size() < 10) {
            boolean first = true;
            for (FieldWork f : fields) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                sb.append(f.info.name);
            }
        } else {
            sb.append(fields.size()).append(" fields");
        }
        return sb.append("]]").toString();
    }

    @Override
    protected Status status(int pagesProcessed) {
        return new Status(new TreeMap<>(readersBuilt), pagesProcessed);
    }

    public static class Status extends AbstractPageMappingOperator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "values_source_reader",
            Status::new
        );

        private final Map<String, Integer> readersBuilt;

        Status(Map<String, Integer> readersBuilt, int pagesProcessed) {
            super(pagesProcessed);
            this.readersBuilt = readersBuilt;
        }

        Status(StreamInput in) throws IOException {
            super(in);
            readersBuilt = in.readOrderedMap(StreamInput::readString, StreamInput::readVInt);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(readersBuilt, StreamOutput::writeVInt);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public Map<String, Integer> readersBuilt() {
            return readersBuilt;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject("readers_built");
            for (Map.Entry<String, Integer> e : readersBuilt.entrySet()) {
                builder.field(e.getKey(), e.getValue());
            }
            builder.endObject();
            builder.field("pages_processed", pagesProcessed());
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return pagesProcessed() == status.pagesProcessed() && readersBuilt.equals(status.readersBuilt);
        }

        @Override
        public int hashCode() {
            return Objects.hash(readersBuilt, pagesProcessed());
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    private static class ComputeBlockLoaderFactory implements BlockLoader.BlockFactory {
        private final BlockFactory factory;
        private final int pageSize;
        private Block nullBlock;

        private ComputeBlockLoaderFactory(BlockFactory factory, int pageSize) {
            this.factory = factory;
            this.pageSize = pageSize;
        }

        @Override
        public BlockLoader.BooleanBuilder booleansFromDocValues(int expectedCount) {
            return factory.newBooleanBlockBuilder(expectedCount).mvOrdering(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
        }

        @Override
        public BlockLoader.BooleanBuilder booleans(int expectedCount) {
            return factory.newBooleanBlockBuilder(expectedCount);
        }

        @Override
        public BlockLoader.BytesRefBuilder bytesRefsFromDocValues(int expectedCount) {
            return factory.newBytesRefBlockBuilder(expectedCount).mvOrdering(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
        }

        @Override
        public BlockLoader.BytesRefBuilder bytesRefs(int expectedCount) {
            return factory.newBytesRefBlockBuilder(expectedCount);
        }

        @Override
        public BlockLoader.DoubleBuilder doublesFromDocValues(int expectedCount) {
            return factory.newDoubleBlockBuilder(expectedCount).mvOrdering(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
        }

        @Override
        public BlockLoader.DoubleBuilder doubles(int expectedCount) {
            return factory.newDoubleBlockBuilder(expectedCount);
        }

        @Override
        public BlockLoader.IntBuilder intsFromDocValues(int expectedCount) {
            return factory.newIntBlockBuilder(expectedCount).mvOrdering(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
        }

        @Override
        public BlockLoader.IntBuilder ints(int expectedCount) {
            return factory.newIntBlockBuilder(expectedCount);
        }

        @Override
        public BlockLoader.LongBuilder longsFromDocValues(int expectedCount) {
            return factory.newLongBlockBuilder(expectedCount).mvOrdering(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
        }

        @Override
        public BlockLoader.LongBuilder longs(int expectedCount) {
            return factory.newLongBlockBuilder(expectedCount);
        }

        @Override
        public BlockLoader.Builder nulls(int expectedCount) {
            return ElementType.NULL.newBlockBuilder(expectedCount, factory);
        }

        @Override
        public Block constantNulls() {
            if (nullBlock == null) {
                nullBlock = factory.newConstantNullBlock(pageSize);
            } else {
                nullBlock.incRef();
            }
            return nullBlock;
        }

        @Override
        public BytesRefBlock constantBytes(BytesRef value) {
            return factory.newConstantBytesRefBlockWith(value, pageSize);
        }

        @Override
        public BlockLoader.SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count) {
            return new SingletonOrdinalsBuilder(factory, ordinals, count);
        }
    }

    // TODO tests that mix source loaded fields and doc values in the same block
}
