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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.BlockLoaderWarnings;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
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
     * One shard's filters selecting parents and the children of this nested field, {@code null} where it is not mapped.
     * {@code parents} is only read with {@code children}. {@code warning} is added to each driver that reads the shard.
     */
    public record Shard(IndexReader reader, @Nullable BitSetProducer parents, @Nullable BitSetProducer children, @Nullable String warning) {
        public Shard(IndexReader reader, @Nullable BitSetProducer parents, @Nullable BitSetProducer children) {
            this(reader, parents, children, null);
        }
    }

    /** @param reader loads the child fields; {@code null} when no child field is read, as for {@code COUNT(*)} */
    public record Factory(IndexedByShardId<Shard> shards, @Nullable ValuesSourceReaderOperator.Factory reader) {
        public NestedChildren get(DriverContext context) {
            return new NestedChildren(
                context,
                shards,
                reader == null ? null : (ValuesSourceReaderOperator) reader.get(context),
                reader == null ? 0 : reader.fields().size()
            );
        }
    }

    /**
     * Reads the child fields {@code paths}, relative to the nested field {@code path}, as {@code types}. Returns {@code null},
     * meaning the values must come from {@code _source}, when no shards are local or a shard maps one of those fields
     * without doc values.
     */
    @Nullable
    public static Factory factory(
        @Nullable IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        String path,
        List<String> paths,
        List<DataType> types
    ) {
        return factory(shardContexts, context -> path, paths.stream().map(p -> path + "." + p).toList(), types);
    }

    /**
     * Reads {@code field} from the child documents of each parent on the shards that map it under a nested object. Each
     * shard decides when its first page arrives, as the shards of a data node are opened and closed while it computes.
     * Where the children can't be read, because the field has no doc values there, {@code converted} says its values are
     * converted as they are loaded, or values of {@code type} can't be flattened, the field reads as null with a warning.
     */
    public static Factory flattened(
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        String field,
        DataType type,
        boolean converted
    ) {
        boolean readable = converted == false && NestedFlattenOperator.canFlatten(PlannerUtils.toElementType(type));
        IndexedByShardId<Shard> shards = shardContexts.map(shard -> {
            if (shard instanceof EsPhysicalOperationProviders.DefaultShardContext == false) {
                return new Shard(shard.searcher().getIndexReader(), null, null);
            }
            SearchExecutionContext context = ((EsPhysicalOperationProviders.DefaultShardContext) shard).executionContext();
            NestedObjectMapper nested = nested(shard, pathFor(field));
            if (nested != null && (readable == false || withoutDocValues(shard, List.of(field)) != null)) {
                return new Shard(
                    shard.searcher().getIndexReader(),
                    null,
                    null,
                    Strings.format("Field [%s] cannot be retrieved, it is unsupported or not indexed; returning null", field)
                );
            }
            if (nested == null) {
                return new Shard(shard.searcher().getIndexReader(), null, null);
            }
            return new Shard(
                shard.searcher().getIndexReader(),
                context.bitsetFilter(Queries.newNonNestedFilter(context.indexVersionCreated())),
                context.bitsetFilter(nested.nestedTypeFilter())
            );
        });
        return new Factory(shards, readable ? reader(shardContexts, List.of(field), List.of(type)) : null);
    }

    /** A field the shard doesn't map, such as one loaded from {@code _source}, has no child values even under a nested path. */
    private static Function<SearchExecutionContext, String> pathFor(String field) {
        return context -> context.getMappingLookup().getFieldType(field) != null ? context.nestedLookup().getNestedParent(field) : null;
    }

    /** @param pathFor the nested object to read the children of on the shard, {@code null} where there is none */
    @Nullable
    private static Factory factory(
        @Nullable IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        Function<SearchExecutionContext, String> pathFor,
        List<String> fields,
        List<DataType> types
    ) {
        if (shardContexts == null || shardContexts.isEmpty()) {
            return null;
        }
        for (var shard : shardContexts.iterable()) {
            if (shard instanceof EsPhysicalOperationProviders.DefaultShardContext == false) {
                return null;
            }
            if (nested(shard, pathFor) != null && withoutDocValues(shard, fields) != null) {
                return null;
            }
        }
        IndexedByShardId<Shard> shards = shardContexts.map(shard -> {
            SearchExecutionContext context = ((EsPhysicalOperationProviders.DefaultShardContext) shard).executionContext();
            NestedObjectMapper nested = nested(shard, pathFor);
            if (nested != null) {
                String missing = withoutDocValues(shard, fields);
                if (missing != null) {
                    throw new IllegalStateException("nested child field [" + missing + "] has no doc values on a later shard");
                }
            }
            return new Shard(
                shard.searcher().getIndexReader(),
                context.bitsetFilter(Queries.newNonNestedFilter(context.indexVersionCreated())),
                nested == null ? null : context.bitsetFilter(nested.nestedTypeFilter())
            );
        });
        return new Factory(shards, fields.isEmpty() ? null : reader(shardContexts, fields, types));
    }

    private static ValuesSourceReaderOperator.Factory reader(
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        List<String> fields,
        List<DataType> types
    ) {
        PlannerSettings settings = PlannerSettings.DEFAULTS;
        List<ValuesSourceReaderOperator.FieldInfo> infos = new ArrayList<>(fields.size());
        for (int f = 0; f < fields.size(); f++) {
            String name = fields.get(f);
            infos.add(
                new ValuesSourceReaderOperator.FieldInfo(
                    name,
                    PlannerUtils.toElementType(types.get(f)),
                    false,
                    (driverContext, shard) -> ValuesSourceReaderOperator.load(
                        ((EsPhysicalOperationProviders.DefaultShardContext) shardContexts.get(shard)).nestedChildBlockLoader(
                            name,
                            new BlockLoaderWarnings(driverContext, Source.EMPTY),
                            settings.blockLoaderSizeOrdinals(),
                            settings.blockLoaderSizeScript()
                        )
                    )
                )
            );
        }
        var readers = shardContexts.map(
            s -> new ValuesSourceReaderOperator.ShardContext(
                s.searcher().getIndexReader(),
                s::newSourceLoader,
                s.storedFieldsSequentialProportion()
            )
        );
        // Pages are bounded by PAGE_SIZE already.
        return new ValuesSourceReaderOperator.Factory(
            ByteSizeValue.ofBytes(Long.MAX_VALUE),
            infos,
            readers,
            true,
            0,
            settings.sourceReservationFactor(),
            () -> 0
        );
    }

    @Nullable
    private static NestedObjectMapper nested(
        EsPhysicalOperationProviders.ShardContext shard,
        Function<SearchExecutionContext, String> pathFor
    ) {
        SearchExecutionContext context = ((EsPhysicalOperationProviders.DefaultShardContext) shard).executionContext();
        String path = pathFor.apply(context);
        return path == null ? null : context.nestedLookup().getNestedMappers().get(path);
    }

    @Nullable
    private static String withoutDocValues(EsPhysicalOperationProviders.ShardContext shard, List<String> fields) {
        for (String field : fields) {
            MappedFieldType type = shard.fieldType(field);
            if (type != null && type.hasDocValues() == false) {
                return field;
            }
        }
        return null;
    }

    /** The {@code _doc} vector of {@code page}, or {@code null} when the page has no doc ids to read the index with. */
    @Nullable
    static DocVector docs(Page page) {
        for (int b = 0; b < page.getBlockCount(); b++) {
            if (page.getBlock(b) instanceof DocBlock doc) {
                return doc.asVector();
            }
        }
        return null;
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

    /** Whether the shard maps the nested object, so its parents have children to read. */
    boolean hasChildren(int shard) {
        Shard s = shards.get(shard);
        if (s.warning() != null && context.warningsMode() == DriverContext.WarningsMode.COLLECT) {
            context.addWarning(s.warning());
        }
        return s.children() != null;
    }

    /**
     * Reads the children of the parents at the positions of {@code parents} that {@code included} accepts and hands them,
     * in parent order, to {@code consumer} in pages of at most {@link #PAGE_SIZE} children. Block 0 of each page is an
     * {@link IntVector} of each child's parent position, and the child fields follow it. Each page is released once
     * {@code consumer} returns.
     */
    void read(DocVector parents, IntPredicate included, Consumer<Page> consumer) {
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
                for (int child = children.nextSetBit(first); child < parent; child = children.nextSetBit(child + 1)) {
                    shardIds[count] = shardId;
                    segments[count] = segment;
                    docs[count] = child;
                    positions[count] = p;
                    if (++count == PAGE_SIZE) {
                        emit(shardIds, segments, docs, positions, count, parents.mayContainDuplicates(), consumer);
                        count = 0;
                    }
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
                Page loaded;
                try {
                    loaded = input.projectBlocks(projection);
                } finally {
                    input.releaseBlocks();
                }
                accept(loaded, consumer);
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
