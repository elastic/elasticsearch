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
import org.apache.lucene.index.StoredFields;
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
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Operator that extracts doc_values from a Lucene index out of pages that have been produced by {@link LuceneSourceOperator}
 * and outputs them to a new column.
 */
public class ValuesSourceReaderOperator extends AbstractPageMappingOperator {
    /**
     * Creates a factory for {@link ValuesSourceReaderOperator}.
     * @param blockLoaders the value source, type and index readers to use for extraction
     * @param docChannel the channel containing the shard, leaf/segment and doc id
     * @param field the lucene field being loaded
     */
    public record Factory(List<BlockLoader> blockLoaders, List<IndexReader> readers, int docChannel, String field)
        implements
            OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new ValuesSourceReaderOperator(driverContext.blockFactory(), blockLoaders, readers, docChannel, field);
        }

        @Override
        public String describe() {
            return "ValuesSourceReaderOperator[field = " + field + "]";
        }
    }

    private final List<FieldWork> fields;
    private final List<IndexReader> readers;
    private final int docChannel;
    private final String field;
    private final ComputeBlockLoaderFactory blockFactory;

    private BlockLoader.DocAtATimeReader lastSingleDocReader;

    private final Map<String, Integer> readersBuilt = new TreeMap<>();

    /**
     * Configuration for a field to load.
     *
     * {@code blockLoaders} is a list, one entry per shard, of
     * {@link BlockLoader}s which load the actual blocks.
     */
    record FieldInfo(String name, List<BlockLoader> blockLoaders) {}

    /**
     * Creates a new extractor
     * @param blockLoaders actually loads the blocks
     * @param docChannel the channel containing the shard, leaf/segment and doc id
     * @param field the lucene field being loaded
     */
    public ValuesSourceReaderOperator(
        BlockFactory blockFactory,
        List<BlockLoader> blockLoaders,
        List<IndexReader> readers,
        int docChannel,
        String field
    ) {
        this.blockLoaders = blockLoaders;
        this.readers = readers;
        this.docChannel = docChannel;
        this.field = field;
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
        List<BlockLoader.RowStrideReader> rowStrideReaders = new ArrayList<>(fields.size());
        for (int b = 0; b < fields.size(); b++) {
            FieldWork field = fields.get(b);
            BlockLoader.ColumnAtATimeReader columnAtATime = field.columnAtATime.reader(shard, segment, firstDoc);
            if (columnAtATime != null) {
                blocks[b] = (Block) columnAtATime.read(blockFactory, loaderDocs);
            } else {
                rowStrideReaders.add(field.rowStride.reader(shard, segment, firstDoc));
                storedFieldsSpec = storedFieldsSpec.merge(field.info.blockLoaders.get(shard).rowStrideStoredFieldSpec());
            }
        }

        if (rowStrideReaders.isEmpty()) {
            return;
        }
        if (storedFieldsSpec.equals(StoredFieldsSpec.NO_REQUIREMENTS)) {
            Block.Builder[] builders = new Block.Builder[rowStrideReaders.size()];
            for (int p = 0; p < docs.getPositionCount(); p++) {
                int doc = docs.getInt(p);
                for (BlockLoader.RowStrideReader reader : rowStrideReaders) {
                    reader.read(doc, );
                }
            }
        }
        return
        // TODO enable the optimization by passing non-null to docs if correct
        LeafStoredFieldLoader loader = StoredFieldLoader.fromSpec(storedFieldsSpec).getLoader(ctx(shard, segment), null);
        for (int p = 0; p < docs.getPositionCount(); p++) {
            loader.advanceTo(docs.getInt(p));

        }
    }

    private void loadFromManyLeaves(Block[] blocks, DocVector docVector) throws IOException {
        int[] forwards = docVector.shardSegmentDocMapForwards();
        setupSingleDocReader(docVector.shards().getInt(forwards[0]), docVector.segments().getInt(forwards[0]), doc);
        try (Block.Builder builder = (Block.Builder) lastLoader.builder(blockFactory, forwards.length)) {
            for (int i = 0; i < forwards.length; i++) {
                int shard = docVector.shards().getInt(forwards[i]);
                int segment = docVector.segments().getInt(forwards[i]);
                int doc = docVector.docs().getInt(forwards[i]);
                setupSingleDocReader(shard, segment, doc);
                lastSingleDocReader.read(doc, builder);
            }
            try (Block orig = builder.build()) {
                return orig.filter(docVector.shardSegmentDocMapBackwards());
            }
        }
    }

    private class FieldWork {
        final FieldInfo info;
        final GuardedReader<BlockLoader.ColumnAtATimeReader> columnAtATime = new GuardedReader<>() {
            @Override
            BlockLoader.ColumnAtATimeReader build(BlockLoader loader, LeafReaderContext ctx) throws IOException {
                return loader.columnAtATimeReader(ctx);
            }
        };

        final GuardedReader<BlockLoader.RowStrideReader> rowStride = new GuardedReader<>() {
            @Override
            BlockLoader.RowStrideReader build(BlockLoader loader, LeafReaderContext ctx) throws IOException {
                return loader.rowStrideReader(ctx);
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
                if (lastShard == shard && lastSegment == segment && lastReader != null && lastReader.canReuse(startingDocId)) {
                    return lastReader;
                }
                return build(info.blockLoaders.get(shard), ctx(shard, segment));
            }

            abstract V build(BlockLoader loader, LeafReaderContext ctx) throws IOException;
        }
    }

    private LeafReaderContext ctx(int shard, int segment) {
        return readers.get(shard).leaves().get(segment);
    }

    /**
     * Sets {@link FieldWork#lastLoader} to the loader for the passed shard, returning
     * {@code true} if the loader changed.
     */
    private boolean setupLoaders(int shard) {
        if (lastShard == shard) {
            return false;
        }
        lastShard = shard;
        for (FieldWork f : fields) {
            f.lastLoader = f.info.blockLoaders.get(shard);
        }
        return true;
    }

    @Override
    public String toString() {
        return "ValuesSourceReaderOperator[field = " + field + "]";
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
