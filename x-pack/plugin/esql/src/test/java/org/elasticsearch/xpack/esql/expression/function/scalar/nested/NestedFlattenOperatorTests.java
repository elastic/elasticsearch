/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Reads {@code items.price} and {@code items.note} of each parent as one multivalued position, in one read of the children.
 * Field extraction loads nulls for them, so the input pages hold null blocks in their place.
 */
public class NestedFlattenOperatorTests extends NestedIndexOperatorTestCase {
    private static final List<NestedFlattenOperator.Leaf> LEAVES = List.of(
        new NestedFlattenOperator.Leaf("items.price", 1, DataType.LONG, false),
        new NestedFlattenOperator.Leaf("items.note", 2, DataType.TEXT, false)
    );

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new NestedFlattenOperator.Factory(new IndexedByShardIdFromSingleton<>(shard), 0, LEAVES);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<Page> pages = new ArrayList<>();
        int parents = Math.min(size, PARENTS);
        for (int start = 0; start < parents;) {
            int count = Math.min(between(1, 200), parents - start);
            Page page = parents(blockFactory, start, count);
            pages.add(
                page.appendBlocks(new Block[] { blockFactory.newConstantNullBlock(count), blockFactory.newConstantNullBlock(count) })
            );
            start += count;
        }
        return new CannedSourceOperator(pages.iterator());
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        List<List<Object>> expected = new ArrayList<>();
        for (Page page : input) {
            for (int p = 0; p < page.getPositionCount(); p++) {
                int parent = parent(page, p);
                List<Object> prices = new ArrayList<>();
                List<Object> notes = new ArrayList<>();
                for (int j = 0; j < parent % 4; j++) {
                    prices.add(10L * parent + j);
                    notes.add("n" + j);
                }
                expected.add(Arrays.asList(parent, values(prices), values(notes)));
            }
        }
        List<List<Object>> actual = new ArrayList<>();
        for (Page page : results) {
            for (int p = 0; p < page.getPositionCount(); p++) {
                actual.add(Arrays.asList(parent(page, p), prices(page.getBlock(1), p), notes(page.getBlock(2), p)));
            }
        }
        assertThat(actual, equalTo(expected));
    }

    /**
     * A page can mix shards, as after a TopN. A shard that maps {@code items} as a plain object keeps what extraction loaded,
     * and the nested shard's parents get their children's values.
     */
    public void testShardWithoutTheNestedObjectKeepsTheLoadedValues() throws IOException {
        try (ObjectShard other = new ObjectShard()) {
            BlockFactory blockFactory = driverContext().blockFactory();
            int count = 8;
            Page page;
            try (
                DocBlock.Builder docs = DocBlock.newBlockBuilder(blockFactory, count);
                LongBlock.Builder prices = blockFactory.newLongBlockBuilder(count);
                BytesRefBlock.Builder notes = blockFactory.newBytesRefBlockBuilder(count)
            ) {
                for (int i = 0; i < count; i++) {
                    if (i % 2 == 0) {
                        docs.appendShard(0).appendSegment(0).appendDoc(parentDoc(i));
                        prices.appendNull();
                        notes.appendNull();
                    } else {
                        docs.appendShard(1).appendSegment(0).appendDoc(i);
                        prices.appendLong(-i);
                        notes.appendBytesRef(new BytesRef("o" + i));
                    }
                }
                page = new Page(docs.build(), prices.build(), notes.build());
            }
            var shards = new IndexedByShardIdFromList<EsPhysicalOperationProviders.ShardContext>(List.of(shard, other.shard));
            try (Operator operator = new NestedFlattenOperator.Factory(shards, 0, LEAVES).get(driverContext())) {
                operator.addInput(page);
                Page result = operator.getOutput();
                try {
                    List<List<Object>> actual = new ArrayList<>();
                    for (int p = 0; p < count; p++) {
                        actual.add(Arrays.asList(prices(result.getBlock(1), p), notes(result.getBlock(2), p)));
                    }
                    assertThat(
                        actual,
                        equalTo(
                            List.of(
                                Arrays.asList(null, null),
                                List.of(-1L, "o1"),
                                List.of(List.of(20L, 21L), List.of("n0", "n1")),
                                List.of(-3L, "o3"),
                                Arrays.asList(null, null),
                                List.of(-5L, "o5"),
                                List.of(List.of(60L, 61L), List.of("n0", "n1")),
                                List.of(-7L, "o7")
                            )
                        )
                    );
                } finally {
                    result.releaseBlocks();
                }
            }
        }
    }

    /**
     * The node-reduce driver reads pages of the shards it is given, while the data drivers of the node's other shards may
     * have released theirs: only the shards of its pages may be looked at.
     */
    public void testOnlyLooksAtTheShardsOfItsPages() {
        var shards = new LiveShards<EsPhysicalOperationProviders.ShardContext>(id -> {
            if (id != 0) {
                throw new AssertionError("shard [" + id + "] was released");
            }
            return shard;
        });
        BlockFactory blockFactory = driverContext().blockFactory();
        Page page = parents(blockFactory, 2, 1).appendBlocks(
            new Block[] { blockFactory.newConstantNullBlock(1), blockFactory.newConstantNullBlock(1) }
        );
        try (Operator operator = new NestedFlattenOperator.Factory(shards, 0, LEAVES).get(driverContext())) {
            operator.addInput(page);
            Page result = operator.getOutput();
            try {
                assertThat(prices(result.getBlock(1), 0), equalTo(List.of(20L, 21L)));
            } finally {
                result.releaseBlocks();
            }
        }
    }

    /** Shards that can only be looked at one at a time, by id, as released ones can't be listed. */
    private record LiveShards<T>(IntFunction<T> shard) implements IndexedByShardId<T> {
        @Override
        public T get(int shardId) {
            return shard.apply(shardId);
        }

        @Override
        public Iterable<? extends T> iterable() {
            throw new AssertionError("listing the shards would look at released ones");
        }

        @Override
        public int size() {
            return 2;
        }

        @Override
        public <S> IndexedByShardId<S> map(Function<T, S> mapper) {
            return new LiveShards<>(id -> mapper.apply(shard.apply(id)));
        }
    }

    /** A page with no parent on a shard that nests the leaves is passed on as it is. */
    public void testPageWithoutNestedShardIsUnchanged() throws IOException {
        try (ObjectShard other = new ObjectShard()) {
            BlockFactory blockFactory = driverContext().blockFactory();
            Page page;
            try (DocBlock.Builder docs = DocBlock.newBlockBuilder(blockFactory, 1)) {
                docs.appendShard(0).appendSegment(0).appendDoc(0);
                page = new Page(docs.build(), blockFactory.newConstantLongBlockWith(5, 1), blockFactory.newConstantNullBlock(1));
            }
            var shards = new IndexedByShardIdFromSingleton<EsPhysicalOperationProviders.ShardContext>(other.shard);
            try (Operator operator = new NestedFlattenOperator.Factory(shards, 0, LEAVES).get(driverContext())) {
                operator.addInput(page);
                Page result = operator.getOutput();
                try {
                    assertThat(result, sameInstance(page));
                } finally {
                    result.releaseBlocks();
                }
            }
        }
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("NestedFlattenOperator[fields=[items.price, items.note]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    private static Object values(List<Object> values) {
        return switch (values.size()) {
            case 0 -> null;
            case 1 -> values.getFirst();
            default -> values;
        };
    }

    private static Object prices(LongBlock block, int p) {
        List<Object> values = new ArrayList<>();
        for (int v = block.getFirstValueIndex(p); v < block.getFirstValueIndex(p) + block.getValueCount(p); v++) {
            values.add(block.getLong(v));
        }
        return block.isNull(p) ? null : values(values);
    }

    private static Object notes(BytesRefBlock block, int p) {
        List<Object> values = new ArrayList<>();
        for (int v = block.getFirstValueIndex(p); v < block.getFirstValueIndex(p) + block.getValueCount(p); v++) {
            values.add(block.getBytesRef(v, new BytesRef()).utf8ToString());
        }
        return block.isNull(p) ? null : values(values);
    }

    /** An empty shard 1 that maps {@code items} as a plain object, so the operator never reads it. */
    private class ObjectShard implements AutoCloseable {
        private final Directory directory = newDirectory();
        private final MapperServiceTestCase helper = new MapperServiceTestCase() {};
        private final MapperService mapperService;
        private final DirectoryReader reader;
        final EsPhysicalOperationProviders.DefaultShardContext shard;

        ObjectShard() throws IOException {
            mapperService = helper.createMapperService("""
                { "_doc": { "properties": { "items": { "properties": {
                  "price": { "type": "long" }, "note": { "type": "text" } } } } } }
                """);
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                writer.commit();
            }
            reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), new ShardId(mapperService.index(), 1));
            shard = new EsPhysicalOperationProviders.DefaultShardContext(
                1,
                new NoOpReleasable(),
                helper.createSearchExecutionContext(mapperService, newSearcher(reader, false)),
                AliasFilter.EMPTY
            );
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(reader, mapperService, directory);
        }
    }
}
