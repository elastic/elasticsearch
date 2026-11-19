/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.AbstractScriptFieldType;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.FallbackSyntheticSourceBlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.expression.function.BlockLoaderWarnings;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * Reads the hidden child documents Lucene stores before each parent, with the block loaders top-level fields use, so a
 * child field of any type a block loader reads gets the same values it would get as a top-level field. Holds an
 * operator, so each driver needs its own instance.
 */
public final class NestedChildren implements Releasable {
    static final String BREAKER_LABEL = "nested_children";

    /**
     * One shard's filters selecting parents and the children of this nested field, both {@code null} where it is not mapped.
     */
    public record Shard(IndexReader reader, @Nullable BitSetProducer parents, @Nullable BitSetProducer children) {}

    /** {@code reader} is {@code null} when no child field is read, and the children are only counted. */
    public record Factory(IndexedByShardId<Shard> shards, @Nullable ValuesSourceReaderOperator.Factory reader) {
        public NestedChildren get(DriverContext context) {
            if (reader == null) {
                return new NestedChildren(context, shards, null, new ElementType[0]);
            }
            ElementType[] types = reader.fields().stream().map(ValuesSourceReaderOperator.FieldInfo::type).toArray(ElementType[]::new);
            return new NestedChildren(context, shards, (ValuesSourceReaderOperator) reader.get(context), types);
        }
    }

    /**
     * Reads the child fields {@code paths}, relative to the nested field {@code path}, as {@code types}. A field without doc
     * values reads the child's object from the parent's {@code _source}, as a top-level field reads its document's. A shard
     * that does not map {@code path} as nested gives its parents no children, and one that does not map a field gives nulls
     * for it. Each shard is only looked at once a page of its parents is read: a driver may get the factory after the
     * shards of other drivers are released. The children load with the same {@code settings} as their parents. A shard that
     * holds a field as a key of a {@code flattened} field converts its keyword values to the field's type with the field's
     * {@code keyCasts} entry, and gives nulls without one.
     */
    public static Factory factory(
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        String path,
        List<String> paths,
        List<DataType> types,
        List<UnionTypeEsField> unionTypes,
        PlannerSettings settings,
        Configuration configuration
    ) {
        List<String> fields = paths.stream().map(p -> path + "." + p).toList();
        IndexedByShardId<Shard> shards = shardContexts.map(shard -> {
            if (shard instanceof EsPhysicalOperationProviders.DefaultShardContext == false) {
                return new Shard(shard.searcher().getIndexReader(), null, null);
            }
            SearchExecutionContext context = ((EsPhysicalOperationProviders.DefaultShardContext) shard).executionContext();
            NestedObjectMapper nested = context.nestedLookup().getNestedMappers().get(path);
            if (nested == null) {
                return new Shard(shard.searcher().getIndexReader(), null, null);
            }
            return new Shard(
                shard.searcher().getIndexReader(),
                context.bitsetFilter(Queries.newNonNestedFilter(context.indexVersionCreated())),
                context.bitsetFilter(nested.nestedTypeFilter())
            );
        });
        if (fields.isEmpty()) {
            return new Factory(shards, null);
        }
        List<ValuesSourceReaderOperator.FieldInfo> infos = new ArrayList<>(fields.size());
        for (int f = 0; f < fields.size(); f++) {
            String name = fields.get(f);
            DataType type = types.get(f);
            UnionTypeEsField union = unionTypes.get(f);
            infos.add(new ValuesSourceReaderOperator.FieldInfo(name, PlannerUtils.toElementType(type), false, (driverContext, shard) -> {
                var context = (EsPhysicalOperationProviders.DefaultShardContext) shardContexts.get(shard);
                if (type == DataType.UNSUPPORTED) {
                    // As for a top-level column, an unsupported child reads as nulls without asking its mapper for a loader
                    return ValuesSourceReaderOperator.LOAD_CONSTANT_NULLS;
                }
                MappedFieldType fieldType = context.fieldType(name);
                if (fieldType instanceof FlattenedFieldMapper.KeyedFlattenedFieldType) {
                    return keyChild(context, driverContext, name, type, union, settings, configuration);
                }
                BlockLoader loader = context.nestedChildBlockLoader(
                    name,
                    new BlockLoaderWarnings(driverContext, Source.EMPTY),
                    settings.blockLoaderSizeOrdinals(),
                    settings.blockLoaderSizeScript()
                );
                BitSetProducer parents = shards.get(shard).parents();
                if (loader instanceof FallbackSyntheticSourceBlockLoader fallback) {
                    // The child's own _ignored_source misses the values the nested object or an object above it keeps whole, so
                    // they are read from the _source. It holds them under the paths of the field, not of an alias to it, and
                    // under those of the fields that copy to it.
                    Set<String> sourcePaths = context.executionContext().sourcePath(fieldType.name());
                    loader = new NestedChildSourceBlockLoader(fallback, sourcePaths.isEmpty() ? Set.of(fieldType.name()) : sourcePaths);
                }
                if (fieldType instanceof AbstractScriptFieldType && parents != null) {
                    // This shard maps the name as a runtime field, which runs on the parent rather than on the child
                    loader = new NestedParentBlockLoader(
                        loader,
                        parents,
                        context::newSourceLoader,
                        NestedParentBlockLoader.valueBlockFactory(driverContext.blockFactory())
                    );
                } else if (loader.rowStrideStoredFieldSpec().requiresSource() && context.storesFilteredSource()) {
                    driverContext.addWarning(
                        Strings.format(
                            "Field [%s] cannot be retrieved from the filtered _source of index [%s]; returning null",
                            name,
                            context.executionContext().getFullyQualifiedIndex().getName()
                        )
                    );
                    loader = ConstantNull.INSTANCE;
                }
                if (union != null) {
                    return context.convertedNestedChild(union, name, loader);
                }
                return ValuesSourceReaderOperator.load(loader);
            }));
        }
        var readers = shardContexts.map(
            s -> new ValuesSourceReaderOperator.ShardContext(
                s.searcher().getIndexReader(),
                sourcePaths -> ((EsPhysicalOperationProviders.DefaultShardContext) s).newNestedChildSourceLoader(),
                s.storedFieldsSequentialProportion()
            )
        );
        return new Factory(
            shards,
            // A page of children can still hold more bytes than a page may, so the reader splits it like any other
            new ValuesSourceReaderOperator.Factory(
                settings.valuesLoadingJumboSize(),
                infos,
                readers,
                true,
                0,
                settings.sourceReservationFactor(),
                () -> 0
            )
        );
    }

    /**
     * Reads {@code name} as the key of a {@code flattened} field, a {@code keyword}, as a top-level column does: a column of another
     * type, or one that indices map with different types, reads it through a cast, and a shard that does not read keys gives nulls.
     */
    private static ValuesSourceReaderOperator.LoaderAndConverter keyChild(
        EsPhysicalOperationProviders.DefaultShardContext context,
        DriverContext driverContext,
        String name,
        DataType type,
        @Nullable UnionTypeEsField union,
        PlannerSettings settings,
        Configuration configuration
    ) {
        if (context.readsKey(name) == false) {
            return ValuesSourceReaderOperator.LOAD_CONSTANT_NULLS;
        }
        BlockLoader keys = context.nestedChildBlockLoader(
            name,
            new BlockLoaderWarnings(driverContext, Source.EMPTY),
            settings.blockLoaderSizeOrdinals(),
            settings.blockLoaderSizeScript()
        );
        if (union == null && DataType.isString(type)) {
            return ValuesSourceReaderOperator.load(keys);
        }
        return context.convertedNestedKey(union, name, type, keys, configuration);
    }

    private final DriverContext context;
    private final IndexedByShardId<Shard> shards;
    @Nullable
    private final ValuesSourceReaderOperator reader;
    private final ElementType[] types;
    /** Block 0 of each page the reader gives is the child documents and the child fields follow it. */
    private final int[] fieldsProjection;

    private NestedChildren(
        DriverContext context,
        IndexedByShardId<Shard> shards,
        @Nullable ValuesSourceReaderOperator reader,
        ElementType[] types
    ) {
        this.context = context;
        this.shards = shards;
        this.reader = reader;
        this.types = types;
        this.fieldsProjection = IntStream.range(1, 1 + types.length).toArray();
    }

    /**
     * A page of rows {@link Expansion#next} gives: row {@code i} of the page belongs to the parent at position
     * {@code positions[offset + i]}. {@code fields} holds a block of {@code count} rows for each child field and is owned
     * by the caller. {@code positions} is only valid until the next call to {@link Expansion#next}.
     */
    record Rows(int[] positions, int offset, int count, Block[] fields) {}

    /**
     * Expands the parents of {@code parents} into at most {@code maxRows} rows per call, one row per child in parent order,
     * and one row of nulls for a parent without children. It keeps its place in the parents, so a parent with more
     * children than {@code maxRows} is read over several calls.
     */
    Expansion expand(DocVector parents, int maxRows) {
        parents.ensureAttached("NESTED");
        return new Expansion(parents, maxRows);
    }

    final class Expansion implements Releasable {
        private static final int ARRAYS = 5;

        private final DocVector parents;
        private final int maxRows;
        private final long reserved;
        /** Parent position of each row of the batch. */
        private final int[] rowParents;
        /** Index in {@link #docs} of the child each row of the batch holds, or -1 for the row of a parent without children. */
        private final int[] rowChildren;
        private final int[] childShards;
        private final int[] childSegments;
        private final int[] docs;
        private int rows;
        private int children;
        private int nextRow;
        private int pendingChildren;
        private boolean closed;

        private int parent;
        private boolean entered;
        @Nullable
        private BitSet siblings;
        private int parentDoc;
        private int nextChild;
        private int shardId;
        private int segment;

        private Expansion(DocVector parents, int maxRows) {
            if (maxRows < 1) {
                throw new IllegalArgumentException("maxRows must be positive but was [" + maxRows + "]");
            }
            this.parents = parents;
            this.maxRows = maxRows;
            this.reserved = (long) ARRAYS * maxRows * Integer.BYTES;
            context.breaker().addEstimateBytesAndMaybeBreak(reserved, BREAKER_LABEL);
            this.rowParents = new int[maxRows];
            this.rowChildren = new int[maxRows];
            this.childShards = new int[maxRows];
            this.childSegments = new int[maxRows];
            this.docs = new int[maxRows];
        }

        boolean hasNext() {
            return nextRow < rows || parent < parents.getPositionCount();
        }

        /**
         * The next rows, at most {@code maxRows} of them, fewer when the reader splits the children it loaded into several pages.
         */
        Rows next() {
            context.checkForEarlyTermination();
            if (nextRow == rows) {
                fill();
            }
            int start = nextRow;
            if (reader == null) {
                nextRow = rows;
                return new Rows(rowParents, start, rows - start, new Block[0]);
            }
            if (pendingChildren == 0) {
                nextRow = rows;
                return new Rows(rowParents, start, rows - start, nulls(rows - start));
            }
            Page loaded = loadNext();
            try {
                int available = loaded.getPositionCount();
                int end = start;
                for (int taken = 0; end < rows && (rowChildren[end] < 0 || taken < available); end++) {
                    if (rowChildren[end] >= 0) {
                        taken++;
                    }
                }
                pendingChildren -= available;
                nextRow = end;
                Block[] fields = fields(loaded, start, end);
                loaded = null;
                return new Rows(rowParents, start, end - start, fields);
            } finally {
                if (loaded != null) {
                    loaded.releaseBlocks();
                }
            }
        }

        private Block[] nulls(int count) {
            Block[] nulls = new Block[types.length];
            try {
                for (int f = 0; f < nulls.length; f++) {
                    nulls[f] = context.blockFactory().newConstantNullBlock(count);
                }
                Block[] result = nulls;
                nulls = null;
                return result;
            } finally {
                if (nulls != null) {
                    Releasables.closeExpectNoException(nulls);
                }
            }
        }

        /** The fields of rows {@code start} to {@code end}, which take their children from {@code loaded} in order. */
        private Block[] fields(Page loaded, int start, int end) {
            int count = end - start;
            if (count == loaded.getPositionCount()) {
                Block[] blocks = new Block[types.length];
                for (int f = 0; f < blocks.length; f++) {
                    blocks[f] = loaded.getBlock(f);
                    blocks[f].incRef();
                }
                loaded.releaseBlocks();
                return blocks;
            }
            Block.Builder[] builders = new Block.Builder[types.length];
            Block[] blocks = new Block[types.length];
            try {
                for (int f = 0; f < builders.length; f++) {
                    builders[f] = types[f].newBlockBuilder(count, context.blockFactory());
                }
                int from = 0;
                for (int row = start; row < end;) {
                    if (rowChildren[row] < 0) {
                        for (Block.Builder builder : builders) {
                            builder.appendNull();
                        }
                        row++;
                        continue;
                    }
                    int run = row;
                    while (row < end && rowChildren[row] >= 0) {
                        row++;
                    }
                    for (int f = 0; f < builders.length; f++) {
                        builders[f].copyFrom(loaded.getBlock(f), from, from + row - run);
                    }
                    from += row - run;
                }
                for (int f = 0; f < blocks.length; f++) {
                    blocks[f] = builders[f].build();
                }
                loaded.releaseBlocks();
                Block[] result = blocks;
                blocks = null;
                return result;
            } finally {
                Releasables.closeExpectNoException(builders);
                if (blocks != null) {
                    Releasables.closeExpectNoException(blocks);
                }
            }
        }

        private Page loadNext() {
            Page out = reader.getOutput();
            if (out == null) {
                throw new IllegalStateException("the reader gave fewer children than were asked for");
            }
            try {
                return out.projectBlocks(fieldsProjection);
            } finally {
                out.releaseBlocks();
            }
        }

        /** Takes the rows of the next {@code maxRows} children and parents without children, and hands the children to the reader. */
        private void fill() {
            rows = 0;
            children = 0;
            nextRow = 0;
            try {
                while (rows < maxRows && parent < parents.getPositionCount()) {
                    if (entered == false) {
                        enter();
                    }
                    rowParents[rows] = parent;
                    if (siblings == null) {
                        rowChildren[rows++] = -1;
                        parent++;
                        entered = false;
                        continue;
                    }
                    rowChildren[rows++] = children;
                    childShards[children] = shardId;
                    childSegments[children] = segment;
                    docs[children++] = nextChild;
                    nextChild = siblings.nextSetBit(nextChild + 1);
                    if (nextChild >= parentDoc) {
                        parent++;
                        entered = false;
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            pendingChildren = children;
            if (reader != null && children > 0) {
                feed();
            }
        }

        /** Finds the first child of the parent at {@link #parent}, if it has one. */
        private void enter() throws IOException {
            entered = true;
            siblings = null;
            shardId = parents.shards().getInt(parent);
            segment = parents.segments().getInt(parent);
            parentDoc = parents.docs().getInt(parent);
            Shard shard = shards.get(shardId);
            if (shard.children() == null || parentDoc == 0) {
                return;
            }
            LeafReaderContext leaf = shard.reader().leaves().get(segment);
            BitSet bits = shard.children().getBitSet(leaf);
            if (bits == null) {
                return;
            }
            int first = shard.parents().getBitSet(leaf).prevSetBit(parentDoc - 1) + 1;
            int child = bits.nextSetBit(first);
            if (child < parentDoc) {
                siblings = bits;
                nextChild = child;
            }
        }

        private void feed() {
            BlockFactory blockFactory = context.blockFactory();
            IntVector shardVector = null;
            IntVector segmentVector = null;
            IntVector docVector = null;
            DocVector childDocs = null;
            try {
                shardVector = blockFactory.newIntArrayVector(Arrays.copyOf(childShards, children), children);
                segmentVector = blockFactory.newIntArrayVector(Arrays.copyOf(childSegments, children), children);
                docVector = blockFactory.newIntArrayVector(Arrays.copyOf(docs, children), children);
                // The parent page holds the shards while its children are read. A parent repeated by a join or an expansion
                // repeats its children.
                childDocs = new DocVector(
                    AlwaysReferencedIndexedByShardId.INSTANCE,
                    shardVector,
                    segmentVector,
                    docVector,
                    DocVector.config().mayContainDuplicates(parents.mayContainDuplicates())
                );
                shardVector = segmentVector = docVector = null;
                Page input = new Page(childDocs.asBlock());
                childDocs = null;
                reader.addInput(input);
            } finally {
                Releasables.closeExpectNoException(shardVector, segmentVector, docVector, childDocs);
            }
        }

        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                context.breaker().addWithoutBreaking(-reserved);
            }
        }
    }

    @Override
    public void close() {
        Releasables.close(reader);
    }
}
