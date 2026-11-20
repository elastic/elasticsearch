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
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Runs an operator that reads nested children over a real index, so the circuit breaker and random allocation failures
 * reach the child reads through {@link NestedChildren}, including the text field that loads from the parent's source.
 * Parent {@code i} has {@code i % 4} children, child {@code j} with price {@code 10 * i + j} and note {@code "n<j>"}.
 * Parents are in one segment of one shard, unless a subclass overrides {@link #parentsPerSegment} or {@link #shardCount}.
 */
public abstract class NestedIndexOperatorTestCase extends OperatorTestCase {
    protected static final int PARENTS = 1_000;
    private static final String MAPPING = """
        {
          "_doc": {
            "properties": {
              "items": {
                "type": "nested",
                "properties": {
                  "price": { "type": "long" },
                  "note": { "type": "text" }
                }
              }
            }
          }
        }
        """;

    private final Directory directory = newDirectory();
    private final MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {};
    private MapperService mapperService;
    private DirectoryReader reader;
    protected EsPhysicalOperationProviders.DefaultShardContext shard;
    private final List<EsPhysicalOperationProviders.DefaultShardContext> shardContexts = new ArrayList<>();
    /** The doc id of each parent in its segment, which follows its children. */
    private final int[] parentDocs = new int[PARENTS];
    private final int[] parentSegments = new int[PARENTS];
    private final Map<Long, Integer> parentOfDoc = new HashMap<>();

    /** How many parents each segment holds. */
    protected int parentsPerSegment() {
        return PARENTS;
    }

    /**
     * How many shards the parents are spread over, parent {@code i} in shard {@code i % shardCount()}. Every shard reads the
     * same index.
     */
    protected int shardCount() {
        return 1;
    }

    @Before
    public void buildIndex() throws IOException {
        mapperService = mapperHelper.createMapperService(MAPPING);
        IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setRAMBufferSizeMB(256);
        int doc = 0;
        int segment = 0;
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < PARENTS; i++) {
                int parent = i;
                ParsedDocument parsed = mapperService.documentMapper().parse(mapperHelper.source(b -> {
                    b.startArray("items");
                    for (int j = 0; j < parent % 4; j++) {
                        b.startObject().field("price", 10L * parent + j).field("note", "n" + j).endObject();
                    }
                    b.endArray();
                }));
                writer.addDocuments(parsed.docs());
                doc += parsed.docs().size();
                parentDocs[i] = doc - 1;
                parentSegments[i] = segment;
                parentOfDoc.put(docKey(segment, doc - 1), i);
                if ((i + 1) % parentsPerSegment() == 0 && i + 1 < PARENTS) {
                    writer.flush();
                    segment++;
                    doc = 0;
                }
            }
        }
        reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), new ShardId(mapperService.index(), 0));
        assertThat(reader.leaves().size(), equalTo(segment + 1));
        SearchExecutionContext context = mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader, false));
        for (int s = 0; s < shardCount(); s++) {
            shardContexts.add(new EsPhysicalOperationProviders.DefaultShardContext(s, new NoOpReleasable(), context, AliasFilter.EMPTY));
        }
        shard = shardContexts.getFirst();
    }

    protected final IndexedByShardId<EsPhysicalOperationProviders.DefaultShardContext> shards() {
        return shardContexts.size() == 1 ? new IndexedByShardIdFromSingleton<>(shard) : new IndexedByShardIdFromList<>(shardContexts);
    }

    private static long docKey(int segment, int doc) {
        return ((long) segment << 32) | doc;
    }

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, mapperService, directory);
    }

    @Override
    protected int largeInputSize() {
        return PARENTS;
    }

    /** The first {@code size} parents, in doc order, in pages of random size. */
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<Page> pages = new ArrayList<>();
        int parents = Math.min(size, PARENTS);
        for (int start = 0; start < parents;) {
            int count = Math.min(between(1, 200), parents - start);
            pages.add(parents(blockFactory, start, count));
            start += count;
        }
        return new CannedSourceOperator(pages.iterator());
    }

    /** One page of the {@code count} parents from {@code start}, in doc order. */
    protected final Page parents(BlockFactory blockFactory, int start, int count) {
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, count)) {
            for (int i = start; i < start + count; i++) {
                appendParent(builder, i);
            }
            return new Page(builder.build());
        }
    }

    /** One page of the parents at {@code indexes}, in that order, which may repeat a parent. */
    protected final Page parentsAt(BlockFactory blockFactory, int... indexes) {
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, indexes.length)) {
            for (int i : indexes) {
                appendParent(builder, i);
            }
            return new Page(builder.build(DocVector.config().mayContainDuplicates(true)));
        }
    }

    private void appendParent(DocBlock.Builder builder, int parent) {
        builder.appendShard(parent % shardCount()).appendSegment(parentSegments[parent]).appendDoc(parentDocs[parent]);
    }

    /** The parent at each position of {@code page}, whose block 0 holds the parents' doc ids. */
    protected final int parent(Page page, int position) {
        DocVector docs = page.<DocBlock>getBlock(0).asVector();
        return parentOfDoc.get(docKey(docs.segments().getInt(position), docs.docs().getInt(position)));
    }
}
