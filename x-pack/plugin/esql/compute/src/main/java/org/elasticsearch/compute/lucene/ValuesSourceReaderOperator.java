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
import org.elasticsearch.core.Assertions;
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
import java.util.function.IntFunction;
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

    /**
     * Configuration for a field to load.
     *
     * {@code blockLoader} maps shard index to the {@link BlockLoader}s
     * which load the actual blocks.
     */
    public record FieldInfo(String name, ElementType type, IntFunction<BlockLoader> blockLoader) {}

    public record ShardContext(IndexReader reader, Supplier<SourceLoader> newSourceLoader) {}

    private final FieldWork[] fields;
    private final List<ShardContext> shardContexts;
    private final int docChannel;
    private final BlockFactory blockFactory;

    private final Map<String, Integer> readersBuilt = new TreeMap<>();

    int lastShard = -1;
    int lastSegment = -1;

    /**
     * Creates a new extractor
     * @param fields fields to load
     * @param docChannel the channel containing the shard, leaf/segment and doc id
     */
    public ValuesSourceReaderOperator(BlockFactory blockFactory, List<FieldInfo> fields, List<ShardContext> shardContexts, int docChannel) {
        this.fields = fields.stream().map(f -> new FieldWork(f)).toArray(FieldWork[]::new);
        this.shardContexts = shardContexts;
        this.docChannel = docChannel;
        this.blockFactory = blockFactory;
    }

    @Override
    protected Page process(Page page) {
        DocVector docVector = page.<DocBlock>getBlock(docChannel).asVector();

        Block[] blocks = new Block[fields.length];
        boolean success = false;
        try {
            if (docVector.singleSegmentNonDecreasing()) {
                IntVector docs = docVector.docs();
                int shard = docVector.shards().getInt(0);
                int segment = docVector.segments().getInt(0);
                loadFromSingleLeaf(blocks, shard, segment, new BlockLoader.Docs() {
                    @Override
                    public int count() {
                        return docs.getPositionCount();
                    }

                    @Override
                    public int get(int i) {
                        return docs.getInt(i);
                    }
                });
            } else if (docVector.singleSegment()) {
                loadFromSingleLeafUnsorted(blocks, docVector);
            } else {
                try (LoadFromMany many = new LoadFromMany(blocks, docVector)) {
                    many.run();
                }
            }
            if (Assertions.ENABLED) {
                for (int f = 0; f < fields.length; f++) {
                    assert blocks[f].elementType() == ElementType.NULL || blocks[f].elementType() == fields[f].info.type
                        : blocks[f].elementType() + " NOT IN (NULL, " + fields[f].info.type + ")";
                }
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

    private void positionFieldWork(int shard, int segment, int firstDoc) {
        if (lastShard == shard) {
            if (lastSegment == segment) {
                for (FieldWork w : fields) {
                    w.sameSegment(firstDoc);
                }
                return;
            }
            lastSegment = segment;
            for (FieldWork w : fields) {
                w.sameShardNewSegment();
            }
            return;
        }
        lastShard = shard;
        lastSegment = segment;
        for (FieldWork w : fields) {
            w.newShard(shard);
        }
    }

    private boolean positionFieldWorkDocGuarteedAscending(int shard, int segment) {
        if (lastShard == shard) {
            if (lastSegment == segment) {
                return false;
            }
            lastSegment = segment;
            for (FieldWork w : fields) {
                w.sameShardNewSegment();
            }
            return true;
        }
        lastShard = shard;
        lastSegment = segment;
        for (FieldWork w : fields) {
            w.newShard(shard);
        }
        return true;
    }

    private void loadFromSingleLeaf(Block[] blocks, int shard, int segment, BlockLoader.Docs docs) throws IOException {
        int firstDoc = docs.get(0);
        positionFieldWork(shard, segment, firstDoc);
        StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
        List<RowStrideReaderWork> rowStrideReaders = new ArrayList<>(fields.length);
        ComputeBlockLoaderFactory loaderBlockFactory = new ComputeBlockLoaderFactory(blockFactory, docs.count());
        LeafReaderContext ctx = ctx(shard, segment);
        try {
            for (int f = 0; f < fields.length; f++) {
                FieldWork field = fields[f];
                BlockLoader.ColumnAtATimeReader columnAtATime = field.columnAtATime(ctx);
                if (columnAtATime != null) {
                    blocks[f] = (Block) columnAtATime.read(loaderBlockFactory, docs);
                } else {
                    rowStrideReaders.add(
                        new RowStrideReaderWork(
                            field.rowStride(ctx),
                            (Block.Builder) field.loader.builder(loaderBlockFactory, docs.count()),
                            f
                        )
                    );
                    storedFieldsSpec = storedFieldsSpec.merge(field.loader.rowStrideStoredFieldSpec());
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
            StoredFieldLoader storedFieldLoader;
            if (useSequentialStoredFieldsReader(docs)) {
                storedFieldLoader = StoredFieldLoader.fromSpecSequential(storedFieldsSpec);
                trackStoredFields(storedFieldsSpec, true);
            } else {
                storedFieldLoader = StoredFieldLoader.fromSpec(storedFieldsSpec);
                trackStoredFields(storedFieldsSpec, false);
            }
            BlockLoaderStoredFieldsFromLeafLoader storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
                storedFieldLoader.getLoader(ctx, null),
                storedFieldsSpec.requiresSource() ? shardContexts.get(shard).newSourceLoader.get().leaf(ctx.reader(), null) : null
            );
            for (int p = 0; p < docs.count(); p++) {
                int doc = docs.get(p);
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

    private void loadFromSingleLeafUnsorted(Block[] blocks, DocVector docVector) throws IOException {
        IntVector docs = docVector.docs();
        int[] forwards = docVector.shardSegmentDocMapForwards();
        int shard = docVector.shards().getInt(0);
        int segment = docVector.segments().getInt(0);
        loadFromSingleLeaf(blocks, shard, segment, new BlockLoader.Docs() {
            @Override
            public int count() {
                return docs.getPositionCount();
            }

            @Override
            public int get(int i) {
                return docs.getInt(forwards[i]);
            }
        });
        final int[] backwards = docVector.shardSegmentDocMapBackwards();
        for (int i = 0; i < blocks.length; i++) {
            Block in = blocks[i];
            blocks[i] = in.filter(backwards);
            in.close();
        }
    }

    private class LoadFromMany implements Releasable {
        private final Block[] target;
        private final IntVector shards;
        private final IntVector segments;
        private final IntVector docs;
        private final int[] forwards;
        private final int[] backwards;
        private final Block.Builder[] builders;
        private final BlockLoader.RowStrideReader[] rowStride;

        BlockLoaderStoredFieldsFromLeafLoader storedFields;

        LoadFromMany(Block[] target, DocVector docVector) {
            this.target = target;
            shards = docVector.shards();
            segments = docVector.segments();
            docs = docVector.docs();
            forwards = docVector.shardSegmentDocMapForwards();
            backwards = docVector.shardSegmentDocMapBackwards();
            builders = new Block.Builder[target.length];
            rowStride = new BlockLoader.RowStrideReader[target.length];
        }

        void run() throws IOException {
            for (int f = 0; f < fields.length; f++) {
                /*
                 * Important note: each block loader has a method to build an
                 * optimized block loader, but we have *many* fields and some
                 * of those block loaders may not be compatible with each other.
                 * So! We take the least common denominator which is the loader
                 * from the element expected element type.
                 */
                builders[f] = fields[f].info.type.newBlockBuilder(docs.getPositionCount(), blockFactory);
            }
            int p = forwards[0];
            int shard = shards.getInt(p);
            int segment = segments.getInt(p);
            int firstDoc = docs.getInt(p);
            positionFieldWork(shard, segment, firstDoc);
            LeafReaderContext ctx = ctx(shard, segment);
            fieldsMoved(ctx, shard);
            read(firstDoc);
            for (int i = 1; i < forwards.length; i++) {
                p = forwards[i];
                shard = shards.getInt(p);
                segment = segments.getInt(p);
                boolean changedSegment = positionFieldWorkDocGuarteedAscending(shard, segment);
                if (changedSegment) {
                    ctx = ctx(shard, segment);
                    fieldsMoved(ctx, shard);
                }
                read(docs.getInt(p));
            }
            for (int f = 0; f < builders.length; f++) {
                try (Block orig = builders[f].build()) {
                    target[f] = orig.filter(backwards);
                }
            }
        }

        private void fieldsMoved(LeafReaderContext ctx, int shard) throws IOException {
            StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
            for (int f = 0; f < fields.length; f++) {
                FieldWork field = fields[f];
                rowStride[f] = field.rowStride(ctx);
                storedFieldsSpec = storedFieldsSpec.merge(field.loader.rowStrideStoredFieldSpec());
                storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
                    StoredFieldLoader.fromSpec(storedFieldsSpec).getLoader(ctx, null),
                    storedFieldsSpec.requiresSource() ? shardContexts.get(shard).newSourceLoader.get().leaf(ctx.reader(), null) : null
                );
                if (false == storedFieldsSpec.equals(StoredFieldsSpec.NO_REQUIREMENTS)) {
                    trackStoredFields(storedFieldsSpec, false);
                }
            }
        }

        private void read(int doc) throws IOException {
            storedFields.advanceTo(doc);
            for (int f = 0; f < builders.length; f++) {
                rowStride[f].read(doc, storedFields, builders[f]);
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(builders);
        }
    }

    /**
     * Is it more efficient to use a sequential stored field reader
     * when reading stored fields for the documents contained in {@code docIds}?
     */
    private boolean useSequentialStoredFieldsReader(BlockLoader.Docs docs) {
        int count = docs.count();
        return count >= SEQUENTIAL_BOUNDARY && docs.get(count - 1) - docs.get(0) == count - 1;
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

    private class FieldWork {
        final FieldInfo info;

        BlockLoader loader;
        BlockLoader.ColumnAtATimeReader columnAtATime;
        BlockLoader.RowStrideReader rowStride;

        FieldWork(FieldInfo info) {
            this.info = info;
        }

        void sameSegment(int firstDoc) {
            if (columnAtATime != null && columnAtATime.canReuse(firstDoc) == false) {
                columnAtATime = null;
            }
            if (rowStride != null && rowStride.canReuse(firstDoc) == false) {
                rowStride = null;
            }
        }

        void sameShardNewSegment() {
            columnAtATime = null;
            rowStride = null;
        }

        void newShard(int shard) {
            loader = info.blockLoader.apply(shard);
            columnAtATime = null;
            rowStride = null;
        }

        BlockLoader.ColumnAtATimeReader columnAtATime(LeafReaderContext ctx) throws IOException {
            if (columnAtATime == null) {
                columnAtATime = loader.columnAtATimeReader(ctx);
                trackReader("column_at_a_time", this.columnAtATime);
            }
            return columnAtATime;
        }

        BlockLoader.RowStrideReader rowStride(LeafReaderContext ctx) throws IOException {
            if (rowStride == null) {
                rowStride = loader.rowStrideReader(ctx);
                trackReader("row_stride", this.rowStride);
            }
            return rowStride;
        }

        private void trackReader(String type, BlockLoader.Reader reader) {
            readersBuilt.merge(info.name + ":" + type + ":" + reader, 1, (prev, one) -> prev + one);
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
        if (fields.length < 10) {
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
            sb.append(fields.length).append(" fields");
        }
        return sb.append("]]").toString();
    }

    @Override
    protected Status status(long processNanos, int pagesProcessed) {
        return new Status(new TreeMap<>(readersBuilt), processNanos, pagesProcessed);
    }

    public static class Status extends AbstractPageMappingOperator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "values_source_reader",
            Status::new
        );

        private final Map<String, Integer> readersBuilt;

        Status(Map<String, Integer> readersBuilt, long processNanos, int pagesProcessed) {
            super(processNanos, pagesProcessed);
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
            innerToXContent(builder);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) return false;
            Status status = (Status) o;
            return readersBuilt.equals(status.readersBuilt);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), readersBuilt);
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
