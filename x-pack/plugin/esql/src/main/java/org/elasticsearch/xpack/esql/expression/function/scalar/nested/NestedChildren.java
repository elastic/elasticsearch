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
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
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
import java.util.function.Consumer;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;

/**
 * Reads the hidden child documents Lucene stores before each parent, with the block loaders top-level fields use, so a
 * child field of any type a block loader reads gets the same values it would get as a top-level field. Holds an
 * operator, so each driver needs its own instance.
 */
public final class NestedChildren implements Releasable {
    /** Children per page, so a parent with many objects does not need them all in memory at once. */
    static final int PAGE_SIZE = 1024;

    /**
     * One shard's filters selecting parents and the children of this nested field, both {@code null} where it is not mapped.
     */
    public record Shard(IndexReader reader, @Nullable BitSetProducer parents, @Nullable BitSetProducer children) {}

    /** {@code reader} is {@code null} when no child field is read, and the children are only counted. */
    public record Factory(IndexedByShardId<Shard> shards, @Nullable ValuesSourceReaderOperator.Factory reader) {
        public NestedChildren get(DriverContext context) {
            if (reader == null) {
                return new NestedChildren(context, shards, null, 0);
            }
            return new NestedChildren(context, shards, (ValuesSourceReaderOperator) reader.get(context), reader.fields().size());
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
            // A page of PAGE_SIZE children can still hold more bytes than a page may, so the reader splits it like any other
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
    /** Block 0 of each loaded page is the parent positions and the child fields follow it. */
    private final int[] projection;

    private NestedChildren(DriverContext context, IndexedByShardId<Shard> shards, @Nullable ValuesSourceReaderOperator reader, int fields) {
        this.context = context;
        this.shards = shards;
        this.reader = reader;
        this.projection = IntStream.range(1, 2 + fields).toArray();
    }

    /**
     * Reads the children of the parents at the positions of {@code parents} that are {@code included} and hands them, in
     * parent order, to {@code consumer} in pages of at most {@link #PAGE_SIZE} children. Block 0 of each page is an
     * {@link IntVector} of each child's parent position, and the child fields follow it. Each page is released once
     * {@code consumer} returns.
     */
    void read(DocVector parents, IntPredicate included, Consumer<Page> consumer) {
        read(parents, included, position -> true, consumer);
    }

    /**
     * As {@link #read(DocVector, IntPredicate, Consumer)}, but a parent's remaining children are skipped once
     * {@code keepReading} says its position needs no more. It is asked after each page is consumed, so a parent whose
     * children fit in one page is read whole.
     */
    void read(DocVector parents, IntPredicate included, IntPredicate keepReading, Consumer<Page> consumer) {
        parents.ensureAttached("NESTED");
        int[] shardIds = new int[PAGE_SIZE];
        int[] segments = new int[PAGE_SIZE];
        int[] docs = new int[PAGE_SIZE];
        int[] positions = new int[PAGE_SIZE];
        int count = 0;
        try {
            for (int p = 0; p < parents.getPositionCount(); p++) {
                if (included.test(p) == false) {
                    continue;
                }
                int shardId = parents.shards().getInt(p);
                Shard shard = shards.get(shardId);
                int segment = parents.segments().getInt(p);
                int parent = parents.docs().getInt(p);
                if (shard.children() == null || parent == 0) {
                    continue;
                }
                LeafReaderContext leaf = shard.reader().leaves().get(segment);
                BitSet children = shard.children().getBitSet(leaf);
                if (children == null) {
                    continue;
                }
                int first = shard.parents().getBitSet(leaf).prevSetBit(parent - 1) + 1;
                int child = children.nextSetBit(first);
                while (child < parent && keepReading.test(p)) {
                    shardIds[count] = shardId;
                    segments[count] = segment;
                    docs[count] = child;
                    positions[count] = p;
                    if (++count == PAGE_SIZE) {
                        emit(shardIds, segments, docs, positions, count, parents.mayContainDuplicates(), consumer);
                        count = 0;
                    }
                    child = children.nextSetBit(child + 1);
                }
            }
            if (count > 0) {
                emit(shardIds, segments, docs, positions, count, parents.mayContainDuplicates(), consumer);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void emit(
        int[] shardIds,
        int[] segments,
        int[] docs,
        int[] positions,
        int count,
        boolean mayContainDuplicates,
        Consumer<Page> consumer
    ) {
        context.checkForEarlyTermination();
        BlockFactory blockFactory = context.blockFactory();
        IntVector shardVector = null;
        IntVector segmentVector = null;
        IntVector docVector = null;
        IntVector positionVector = null;
        DocVector children = null;
        try {
            shardVector = blockFactory.newIntArrayVector(Arrays.copyOf(shardIds, count), count);
            segmentVector = blockFactory.newIntArrayVector(Arrays.copyOf(segments, count), count);
            docVector = blockFactory.newIntArrayVector(Arrays.copyOf(docs, count), count);
            positionVector = blockFactory.newIntArrayVector(Arrays.copyOf(positions, count), count);
            // The parent page holds the shards while its children are read. A parent repeated by a join or an expansion
            // repeats its children.
            children = new DocVector(
                AlwaysReferencedIndexedByShardId.INSTANCE,
                shardVector,
                segmentVector,
                docVector,
                DocVector.config().mayContainDuplicates(mayContainDuplicates)
            );
            shardVector = segmentVector = docVector = null;
            Page input = new Page(children.asBlock(), positionVector.asBlock());
            children = null;
            positionVector = null;
            if (reader == null) {
                Page counted;
                try {
                    counted = input.projectBlocks(projection);
                } finally {
                    input.releaseBlocks();
                }
                accept(counted, consumer);
                return;
            }
            reader.addInput(input);
            for (Page out = reader.getOutput(); out != null; out = reader.getOutput()) {
                Page loaded;
                try {
                    loaded = out.projectBlocks(projection);
                } finally {
                    out.releaseBlocks();
                }
                accept(loaded, consumer);
            }
        } finally {
            Releasables.closeExpectNoException(shardVector, segmentVector, docVector, positionVector, children);
        }
    }

    private static void accept(Page page, Consumer<Page> consumer) {
        try {
            consumer.accept(page);
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public void close() {
        Releasables.close(reader);
    }
}
