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
import java.util.stream.Collectors;

/**
 * Operator that extracts doc_values from a Lucene index out of pages that have been produced by {@link LuceneSourceOperator}
 * and outputs them to a new column.
 */
public class ValuesSourceReaderOperator extends AbstractPageMappingOperator {
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
            return "ValuesSourceReaderOperator[field = " + fields.stream().map(f -> f.name).collect(Collectors.joining(", ")) + "]";
        }
    }

    public record ShardContext(IndexReader reader, Supplier<SourceLoader> newSourceLoader) {}

    private final List<FieldWork> fields;
    private final List<ShardContext> shardContexts;
    private final int docChannel;
    private final ComputeBlockLoaderFactory blockFactory;

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
        this.blockFactory = new ComputeBlockLoaderFactory(blockFactory);
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
        try {
            for (int b = 0; b < fields.size(); b++) {
                FieldWork field = fields.get(b);
                BlockLoader.ColumnAtATimeReader columnAtATime = field.columnAtATime.reader(shard, segment, firstDoc);
                if (columnAtATime != null) {
                    blocks[b] = (Block) columnAtATime.read(blockFactory, loaderDocs);
                } else {
                    BlockLoader.RowStrideReader rowStride = field.rowStride.reader(shard, segment, firstDoc);
                    rowStrideReaders.add(
                        new RowStrideReaderWork(
                            rowStride,
                            (Block.Builder) field.info.blockLoaders.get(shard).builder(blockFactory, docs.getPositionCount()),
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
            BlockLoaderStoredFieldsFromLeafLoader storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
                // TODO enable the optimization by passing non-null to docs if correct
                StoredFieldLoader.fromSpec(storedFieldsSpec).getLoader(ctx, null),
                storedFieldsSpec.requiresSource() ? shardContexts.get(shard).newSourceLoader.get().leaf(ctx.reader(), null) : null
            );
            trackStoredFields(storedFieldsSpec); // TODO when optimization is enabled add it to tracking
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
        try {
            for (int b = 0; b < fields.size(); b++) {
                FieldWork field = fields.get(b);
                builders[b] = builderFromFirstNonNull(field, docs.getPositionCount());
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
                        trackStoredFields(storedFieldsSpec);
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

    private void trackStoredFields(StoredFieldsSpec spec) {
        readersBuilt.merge(
            "stored_fields[" + "requires_source:" + spec.requiresSource() + ", fields:" + spec.requiredStoredFields().size() + "]",
            1,
            (prev, one) -> prev + one
        );
    }

    /**
     * Returns a builder from the first non - {@link BlockLoader#CONSTANT_NULLS} loader
     * in the list. If they are all the null loader then returns a null builder.
     */
    private Block.Builder builderFromFirstNonNull(FieldWork field, int positionCount) {
        for (BlockLoader loader : field.info.blockLoaders) {
            if (loader != BlockLoader.CONSTANT_NULLS) {
                return (Block.Builder) loader.builder(blockFactory, positionCount);
            }
        }
        // All null, just let the first one build the null block loader.
        return (Block.Builder) field.info.blockLoaders.get(0).builder(blockFactory, positionCount);
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
        return "ValuesSourceReaderOperator[field = " + fields.stream().map(f -> f.info.name).collect(Collectors.joining(", ")) + "]";
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

        private ComputeBlockLoaderFactory(BlockFactory factory) {
            this.factory = factory;
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
        public Block constantNulls(int size) {
            return factory.newConstantNullBlock(size);
        }

        @Override
        public BytesRefBlock constantBytes(BytesRef value, int size) {
            return factory.newConstantBytesRefBlockWith(value, size);
        }

        @Override
        public BlockLoader.SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count) {
            return new SingletonOrdinalsBuilder(factory, ordinals, count);
        }
    }

    // TODO tests that mix source loaded fields and doc values in the same block
}
