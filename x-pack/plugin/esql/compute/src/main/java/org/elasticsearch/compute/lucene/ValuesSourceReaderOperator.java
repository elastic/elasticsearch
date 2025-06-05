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
import java.util.function.IntFunction;
import java.util.function.Supplier;

import static org.elasticsearch.TransportVersions.ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED;

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

    public record ShardContext(IndexReader reader, Supplier<SourceLoader> newSourceLoader, double storedFieldsSequentialProportion) {}

    private final FieldWork[] fields;
    private final List<ShardContext> shardContexts;
    private final int docChannel;
    private final BlockFactory blockFactory;

    private final Map<String, Integer> readersBuilt = new TreeMap<>();
    private long valuesLoaded;

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
            success = true;
            for (Block b : blocks) {
                valuesLoaded += b.getTotalValueCount();
            }
            return page.appendBlocks(blocks);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
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
        LeafReaderContext ctx = ctx(shard, segment);
        try (ComputeBlockLoaderFactory loaderBlockFactory = new ComputeBlockLoaderFactory(blockFactory, docs.count())) {
            for (int f = 0; f < fields.length; f++) {
                FieldWork field = fields[f];
                BlockLoader.ColumnAtATimeReader columnAtATime = field.columnAtATime(ctx);
                if (columnAtATime != null) {
                    blocks[f] = (Block) columnAtATime.read(loaderBlockFactory, docs);
                    sanityCheckBlock(columnAtATime, docs.count(), blocks[f], f);
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

            SourceLoader sourceLoader = null;
            ShardContext shardContext = shardContexts.get(shard);
            if (storedFieldsSpec.requiresSource()) {
                sourceLoader = shardContext.newSourceLoader.get();
                storedFieldsSpec = storedFieldsSpec.merge(new StoredFieldsSpec(true, false, sourceLoader.requiredStoredFields()));
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
            if (useSequentialStoredFieldsReader(docs, shardContext.storedFieldsSequentialProportion())) {
                storedFieldLoader = StoredFieldLoader.fromSpecSequential(storedFieldsSpec);
                trackStoredFields(storedFieldsSpec, true);
            } else {
                storedFieldLoader = StoredFieldLoader.fromSpec(storedFieldsSpec);
                trackStoredFields(storedFieldsSpec, false);
            }
            BlockLoaderStoredFieldsFromLeafLoader storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
                storedFieldLoader.getLoader(ctx, null),
                sourceLoader != null ? sourceLoader.leaf(ctx.reader(), null) : null
            );
            for (int p = 0; p < docs.count(); p++) {
                int doc = docs.get(p);
                storedFields.advanceTo(doc);
                for (RowStrideReaderWork work : rowStrideReaders) {
                    work.read(doc, storedFields);
                }
            }
            for (RowStrideReaderWork work : rowStrideReaders) {
                blocks[work.offset] = work.build();
                sanityCheckBlock(work.reader, docs.count(), blocks[work.offset], work.offset);
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
        private final Block.Builder[][] builders;
        private final BlockLoader[][] converters;
        private final Block.Builder[] fieldTypeBuilders;
        private final BlockLoader.RowStrideReader[] rowStride;

        BlockLoaderStoredFieldsFromLeafLoader storedFields;

        LoadFromMany(Block[] target, DocVector docVector) {
            this.target = target;
            shards = docVector.shards();
            segments = docVector.segments();
            docs = docVector.docs();
            forwards = docVector.shardSegmentDocMapForwards();
            backwards = docVector.shardSegmentDocMapBackwards();
            fieldTypeBuilders = new Block.Builder[target.length];
            builders = new Block.Builder[target.length][shardContexts.size()];
            converters = new BlockLoader[target.length][shardContexts.size()];
            rowStride = new BlockLoader.RowStrideReader[target.length];
        }

        void run() throws IOException {
            for (int f = 0; f < fields.length; f++) {
                /*
                 * Important note: each field has a desired type, which might not match the mapped type (in the case of union-types).
                 * We create the final block builders using the desired type, one for each field, but then also use inner builders
                 * (one for each field and shard), and converters (again one for each field and shard) to actually perform the field
                 * loading in a way that is correct for the mapped field type, and then convert between that type and the desired type.
                 */
                fieldTypeBuilders[f] = fields[f].info.type.newBlockBuilder(docs.getPositionCount(), blockFactory);
                builders[f] = new Block.Builder[shardContexts.size()];
                converters[f] = new BlockLoader[shardContexts.size()];
            }
            try (ComputeBlockLoaderFactory loaderBlockFactory = new ComputeBlockLoaderFactory(blockFactory, docs.getPositionCount())) {
                int p = forwards[0];
                int shard = shards.getInt(p);
                int segment = segments.getInt(p);
                int firstDoc = docs.getInt(p);
                positionFieldWork(shard, segment, firstDoc);
                LeafReaderContext ctx = ctx(shard, segment);
                fieldsMoved(ctx, shard);
                verifyBuilders(loaderBlockFactory, shard);
                read(firstDoc, shard);
                for (int i = 1; i < forwards.length; i++) {
                    p = forwards[i];
                    shard = shards.getInt(p);
                    segment = segments.getInt(p);
                    boolean changedSegment = positionFieldWorkDocGuarteedAscending(shard, segment);
                    if (changedSegment) {
                        ctx = ctx(shard, segment);
                        fieldsMoved(ctx, shard);
                    }
                    verifyBuilders(loaderBlockFactory, shard);
                    read(docs.getInt(p), shard);
                }
            }
            for (int f = 0; f < target.length; f++) {
                for (int s = 0; s < shardContexts.size(); s++) {
                    if (builders[f][s] != null) {
                        try (Block orig = (Block) converters[f][s].convert(builders[f][s].build())) {
                            fieldTypeBuilders[f].copyFrom(orig, 0, orig.getPositionCount());
                        }
                    }
                }
                try (Block targetBlock = fieldTypeBuilders[f].build()) {
                    target[f] = targetBlock.filter(backwards);
                }
                sanityCheckBlock(rowStride[f], docs.getPositionCount(), target[f], f);
            }
        }

        private void fieldsMoved(LeafReaderContext ctx, int shard) throws IOException {
            StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
            for (int f = 0; f < fields.length; f++) {
                FieldWork field = fields[f];
                rowStride[f] = field.rowStride(ctx);
                storedFieldsSpec = storedFieldsSpec.merge(field.loader.rowStrideStoredFieldSpec());
            }
            SourceLoader sourceLoader = null;
            if (storedFieldsSpec.requiresSource()) {
                sourceLoader = shardContexts.get(shard).newSourceLoader.get();
                storedFieldsSpec = storedFieldsSpec.merge(new StoredFieldsSpec(true, false, sourceLoader.requiredStoredFields()));
            }
            storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
                StoredFieldLoader.fromSpec(storedFieldsSpec).getLoader(ctx, null),
                sourceLoader != null ? sourceLoader.leaf(ctx.reader(), null) : null
            );
            if (false == storedFieldsSpec.equals(StoredFieldsSpec.NO_REQUIREMENTS)) {
                trackStoredFields(storedFieldsSpec, false);
            }
        }

        private void verifyBuilders(ComputeBlockLoaderFactory loaderBlockFactory, int shard) {
            for (int f = 0; f < fields.length; f++) {
                if (builders[f][shard] == null) {
                    // Note that this relies on field.newShard() to set the loader and converter correctly for the current shard
                    builders[f][shard] = (Block.Builder) fields[f].loader.builder(loaderBlockFactory, docs.getPositionCount());
                    converters[f][shard] = fields[f].loader;
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
            for (int f = 0; f < fields.length; f++) {
                Releasables.closeExpectNoException(builders[f]);
            }
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
    protected Status status(long processNanos, int pagesProcessed, long rowsReceived, long rowsEmitted) {
        return new Status(new TreeMap<>(readersBuilt), processNanos, pagesProcessed, rowsReceived, rowsEmitted, valuesLoaded);
    }

    /**
     * Quick checks for on the loaded block to make sure it looks reasonable.
     * @param loader the object that did the loading - we use it to make error messages if the block is busted
     * @param expectedPositions how many positions the block should have - it's as many as the incoming {@link Page} has
     * @param block the block to sanity check
     * @param field offset into the {@link #fields} array for the block being loaded
     */
    private void sanityCheckBlock(Object loader, int expectedPositions, Block block, int field) {
        if (block.getPositionCount() != expectedPositions) {
            throw new IllegalStateException(loader + ": " + block + " didn't have [" + expectedPositions + "] positions");
        }
        if (block.elementType() != ElementType.NULL && block.elementType() != fields[field].info.type) {
            throw new IllegalStateException(
                loader + ": " + block + "'s element_type [" + block.elementType() + "] NOT IN (NULL, " + fields[field].info.type + ")"
            );
        }
    }

    public static class Status extends AbstractPageMappingOperator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "values_source_reader",
            Status::new
        );

        private final Map<String, Integer> readersBuilt;
        private final long valuesLoaded;

        Status(
            Map<String, Integer> readersBuilt,
            long processNanos,
            int pagesProcessed,
            long rowsReceived,
            long rowsEmitted,
            long valuesLoaded
        ) {
            super(processNanos, pagesProcessed, rowsReceived, rowsEmitted);
            this.readersBuilt = readersBuilt;
            this.valuesLoaded = valuesLoaded;
        }

        Status(StreamInput in) throws IOException {
            super(in);
            readersBuilt = in.readOrderedMap(StreamInput::readString, StreamInput::readVInt);
            valuesLoaded = in.getTransportVersion().onOrAfter(ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED) ? in.readVLong() : 0;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(readersBuilt, StreamOutput::writeVInt);
            if (out.getTransportVersion().onOrAfter(ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED)) {
                out.writeVLong(valuesLoaded);
            }
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public Map<String, Integer> readersBuilt() {
            return readersBuilt;
        }

        @Override
        public long valuesLoaded() {
            return valuesLoaded;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject("readers_built");
            for (Map.Entry<String, Integer> e : readersBuilt.entrySet()) {
                builder.field(e.getKey(), e.getValue());
            }
            builder.endObject();
            builder.field("values_loaded", valuesLoaded);
            innerToXContent(builder);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) return false;
            Status status = (Status) o;
            return readersBuilt.equals(status.readersBuilt) && valuesLoaded == status.valuesLoaded;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), readersBuilt, valuesLoaded);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    private static class ComputeBlockLoaderFactory extends DelegatingBlockLoaderFactory implements Releasable {
        private final int pageSize;
        private Block nullBlock;

        private ComputeBlockLoaderFactory(BlockFactory factory, int pageSize) {
            super(factory);
            this.pageSize = pageSize;
        }

        @Override
        public Block constantNulls() {
            if (nullBlock == null) {
                nullBlock = factory.newConstantNullBlock(pageSize);
            }
            nullBlock.incRef();
            return nullBlock;
        }

        @Override
        public void close() {
            if (nullBlock != null) {
                nullBlock.close();
            }
        }

        @Override
        public BytesRefBlock constantBytes(BytesRef value) {
            return factory.newConstantBytesRefBlockWith(value, pageSize);
        }
    }

    public abstract static class DelegatingBlockLoaderFactory implements BlockLoader.BlockFactory {
        protected final BlockFactory factory;

        protected DelegatingBlockLoaderFactory(BlockFactory factory) {
            this.factory = factory;
        }

        @Override
        public BlockLoader.BooleanBuilder booleansFromDocValues(int expectedCount) {
            return factory.newBooleanBlockBuilder(expectedCount).mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
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
            return factory.newDoubleBlockBuilder(expectedCount).mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
        }

        @Override
        public BlockLoader.DoubleBuilder doubles(int expectedCount) {
            return factory.newDoubleBlockBuilder(expectedCount);
        }

        @Override
        public BlockLoader.FloatBuilder denseVectors(int expectedVectorsCount, int dimensions) {
            return factory.newFloatBlockBuilder(expectedVectorsCount * dimensions);
        }

        @Override
        public BlockLoader.IntBuilder intsFromDocValues(int expectedCount) {
            return factory.newIntBlockBuilder(expectedCount).mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
        }

        @Override
        public BlockLoader.IntBuilder ints(int expectedCount) {
            return factory.newIntBlockBuilder(expectedCount);
        }

        @Override
        public BlockLoader.LongBuilder longsFromDocValues(int expectedCount) {
            return factory.newLongBlockBuilder(expectedCount).mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
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
        public BlockLoader.SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count) {
            return new SingletonOrdinalsBuilder(factory, ordinals, count);
        }

        @Override
        public BlockLoader.AggregateMetricDoubleBuilder aggregateMetricDoubleBuilder(int count) {
            return factory.newAggregateMetricDoubleBlockBuilder(count);
        }
    }
}
