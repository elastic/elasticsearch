/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Reads the fields under a nested object as ordinary columns: one multivalued position per parent holding the values of all
 * of its objects, in document order. Field extraction loads nulls for such a field, as the parent document holds none of its
 * values; this replaces them on the shards that map the field under a nested object and keeps what extraction loaded on the
 * others. The fields under one nested object read its children once, together.
 */
public final class NestedFlattenOperator extends AbstractPageMappingOperator {
    /**
     * A field that may be under a nested object on some shard, in block {@code channel}. {@code converted} is a field
     * converted as it is loaded, as a cast across a type conflict is, which the child values are not.
     */
    public record Leaf(String name, int channel, DataType type, boolean converted) {
        /** Whether the child values can be read; otherwise the field is null on the shards that nest it. */
        boolean readable() {
            return converted == false && canFlatten(PlannerUtils.toElementType(type));
        }
    }

    public record Factory(
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shards,
        int docChannel,
        List<Leaf> leaves,
        PlannerSettings settings
    ) implements Operator.OperatorFactory {
        @Override
        public Operator get(DriverContext context) {
            return new NestedFlattenOperator(context, this);
        }

        @Override
        public String describe() {
            return "NestedFlattenOperator[fields=" + leaves.stream().map(Leaf::name).collect(Collectors.joining(", ", "[", "]")) + "]";
        }
    }

    /** Whether values of {@code elementType} can be flattened. A dense vector, say, is already one multivalued position. */
    static boolean canFlatten(ElementType elementType) {
        return switch (elementType) {
            case BOOLEAN, INT, LONG, DOUBLE, BYTES_REF -> true;
            default -> false;
        };
    }

    /**
     * The nested object the shard maps {@code field} under, or {@code null}. A field the shard doesn't map, such as one loaded
     * from {@code _source}, has no child values.
     */
    @Nullable
    public static String nestedPath(EsPhysicalOperationProviders.ShardContext shard, String field) {
        if (shard instanceof EsPhysicalOperationProviders.DefaultShardContext == false) {
            return null;
        }
        SearchExecutionContext context = ((EsPhysicalOperationProviders.DefaultShardContext) shard).executionContext();
        return context.getMappingLookup().getFieldType(field) == null ? null : context.nestedLookup().getNestedParent(field);
    }

    /** The leaves one read of the children of the nested object {@code path} loads, by index in {@link Factory#leaves}. */
    private record Group(String path, List<Integer> leaves) {}

    /** How one shard reads the leaves: the groups it reads together, and the leaves it nests but can't read. */
    private record ShardLeaves(List<Group> groups, List<Integer> unreadable) {
        boolean isEmpty() {
            return groups.isEmpty() && unreadable.isEmpty();
        }
    }

    private final DriverContext context;
    private final Factory factory;
    private final Map<Integer, ShardLeaves> byShard = new HashMap<>();
    private final Map<Group, NestedChildren> readers = new HashMap<>();

    private NestedFlattenOperator(DriverContext context, Factory factory) {
        this.context = context;
        this.factory = factory;
    }

    @Override
    protected Page process(Page page) {
        DocVector docs = page.<DocBlock>getBlock(factory.docChannel()).asVector();
        Map<Group, boolean[]> groups = new LinkedHashMap<>();
        boolean[][] unreadable = new boolean[factory.leaves().size()][];
        boolean any = false;
        for (int p = 0; p < docs.getPositionCount(); p++) {
            ShardLeaves shard = shard(docs.shards().getInt(p));
            if (shard.isEmpty()) {
                continue;
            }
            any = true;
            for (Group group : shard.groups()) {
                groups.computeIfAbsent(group, g -> new boolean[docs.getPositionCount()])[p] = true;
            }
            for (int leaf : shard.unreadable()) {
                if (unreadable[leaf] == null) {
                    unreadable[leaf] = new boolean[docs.getPositionCount()];
                }
                unreadable[leaf][p] = true;
            }
        }
        if (any == false) {
            return page;
        }
        Block[] blocks = new Block[page.getBlockCount()];
        for (int b = 0; b < blocks.length; b++) {
            blocks[b] = page.getBlock(b);
        }
        boolean success = false;
        try {
            for (var e : groups.entrySet()) {
                flatten(page, docs, reader(e.getKey()), e.getKey().leaves(), e.getValue(), blocks);
            }
            for (int leaf = 0; leaf < unreadable.length; leaf++) {
                if (unreadable[leaf] != null) {
                    flatten(page, docs, null, List.of(leaf), unreadable[leaf], blocks);
                }
            }
            success = true;
        } finally {
            // Until it returns, the page belongs to AbstractPageMappingOperator, which releases it if this throws
            for (int b = 0; b < blocks.length; b++) {
                if (blocks[b] != page.getBlock(b)) {
                    Releasables.closeExpectNoException(success ? page.getBlock(b) : blocks[b]);
                }
            }
        }
        return new Page(page.getPositionCount(), blocks);
    }

    private ShardLeaves shard(int shardId) {
        return byShard.computeIfAbsent(shardId, id -> {
            var shard = factory.shards().get(id);
            Map<String, List<Integer>> byPath = new LinkedHashMap<>();
            List<Integer> unreadable = new ArrayList<>();
            for (int l = 0; l < factory.leaves().size(); l++) {
                Leaf leaf = factory.leaves().get(l);
                String path = nestedPath(shard, leaf.name());
                if (path == null) {
                    continue;
                }
                if (leaf.readable()) {
                    byPath.computeIfAbsent(path, k -> new ArrayList<>()).add(l);
                } else {
                    // Not HeaderWarning: the exchange drops response headers from remote nodes
                    context.addWarning(
                        Strings.format("Field [%s] cannot be retrieved, it is unsupported or not indexed; returning null", leaf.name())
                    );
                    unreadable.add(l);
                }
            }
            List<Group> groups = new ArrayList<>(byPath.size());
            byPath.forEach((path, leaves) -> groups.add(new Group(path, List.copyOf(leaves))));
            return new ShardLeaves(groups, unreadable);
        });
    }

    private NestedChildren reader(Group group) {
        NestedChildren reader = readers.get(group);
        if (reader == null) {
            List<String> paths = new ArrayList<>(group.leaves().size());
            List<DataType> types = new ArrayList<>(group.leaves().size());
            for (int l : group.leaves()) {
                Leaf leaf = factory.leaves().get(l);
                paths.add(leaf.name().substring(group.path().length() + 1));
                types.add(leaf.type());
            }
            reader = NestedChildren.factory(factory.shards(), group.path(), paths, types, factory.settings()).get(context);
            readers.put(group, reader);
        }
        return reader;
    }

    /**
     * Replaces the blocks of {@code leaves} at the {@code included} positions with the values of the children {@code reader}
     * reads, or with nulls when it is {@code null}, and keeps the other positions. Only releases the blocks it replaces that
     * it built itself, not those of {@code page}.
     */
    private void flatten(
        Page page,
        DocVector docs,
        @Nullable NestedChildren reader,
        List<Integer> leaves,
        boolean[] included,
        Block[] blocks
    ) {
        int positions = docs.getPositionCount();
        Block.Builder[] builders = new Block.Builder[leaves.size()];
        try {
            for (int l = 0; l < builders.length; l++) {
                Leaf leaf = factory.leaves().get(leaves.get(l));
                builders[l] = PlannerUtils.toElementType(leaf.type()).newBlockBuilder(positions, context.blockFactory());
            }
            var rows = new Object() {
                final BytesRef scratch = new BytesRef();
                final boolean[] started = new boolean[builders.length];
                /** Positions before this one are written. */
                int next;
                /** The parent whose values are being written, or -1. */
                int open = -1;

                void add(Page page) {
                    IntVector parents = page.<IntBlock>getBlock(0).asVector();
                    for (int c = 0; c < page.getPositionCount(); c++) {
                        int parent = parents.getInt(c);
                        if (parent != open) {
                            fillTo(parent);
                            open = parent;
                        }
                        for (int l = 0; l < builders.length; l++) {
                            Block values = page.getBlock(1 + l);
                            if (values.isNull(c)) {
                                continue;
                            }
                            if (started[l] == false) {
                                builders[l].beginPositionEntry();
                                started[l] = true;
                            }
                            int first = values.getFirstValueIndex(c);
                            for (int v = first; v < first + values.getValueCount(c); v++) {
                                append(builders[l], values, v, scratch);
                            }
                        }
                    }
                }

                /** Writes every position before {@code end}: the parents that aren't included keep what was loaded. */
                void fillTo(int end) {
                    if (open >= 0) {
                        for (int l = 0; l < builders.length; l++) {
                            if (started[l]) {
                                builders[l].endPositionEntry();
                                started[l] = false;
                            } else {
                                builders[l].appendNull();
                            }
                        }
                        next = open + 1;
                        open = -1;
                    }
                    for (; next < end; next++) {
                        for (int l = 0; l < builders.length; l++) {
                            if (included[next]) {
                                builders[l].appendNull();
                            } else {
                                builders[l].copyFrom(blocks[factory.leaves().get(leaves.get(l)).channel()], next, next + 1);
                            }
                        }
                    }
                }
            };
            if (reader != null) {
                reader.read(docs, p -> included[p], rows::add);
            }
            rows.fillTo(positions);
            for (int l = 0; l < builders.length; l++) {
                int channel = factory.leaves().get(leaves.get(l)).channel();
                Block built = builders[l].build();
                if (blocks[channel] != page.getBlock(channel)) {
                    blocks[channel].close();
                }
                blocks[channel] = built;
            }
        } finally {
            Releasables.closeExpectNoException(builders);
        }
    }

    private static void append(Block.Builder builder, Block values, int v, BytesRef scratch) {
        switch (values.elementType()) {
            case BOOLEAN -> ((BooleanBlock.Builder) builder).appendBoolean(((BooleanBlock) values).getBoolean(v));
            case INT -> ((IntBlock.Builder) builder).appendInt(((IntBlock) values).getInt(v));
            case LONG -> ((LongBlock.Builder) builder).appendLong(((LongBlock) values).getLong(v));
            case DOUBLE -> ((DoubleBlock.Builder) builder).appendDouble(((DoubleBlock) values).getDouble(v));
            case BYTES_REF -> ((BytesRefBlock.Builder) builder).appendBytesRef(((BytesRefBlock) values).getBytesRef(v, scratch));
            default -> throw new IllegalArgumentException("cannot flatten [" + values.elementType() + "]");
        }
    }

    @Override
    public String toString() {
        return factory.describe();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(Releasables.wrap(readers.values()), super::close);
    }
}
