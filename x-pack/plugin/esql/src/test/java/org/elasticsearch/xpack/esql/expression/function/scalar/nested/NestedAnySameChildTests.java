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
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * {@code NESTED_ANY(items, sku == "a" AND qty * price > 100)} on real nested documents. The sku term selects child
 * positions before {@code qty} and {@code price} are loaded, and a parent is true only when one child satisfies both.
 */
public class NestedAnySameChildTests extends ComputeTestCase {
    private static final String MAPPING = """
        {
          "_doc": {
            "properties": {
              "items": {
                "type": "nested",
                "properties": {
                  "sku": { "type": "keyword" },
                  "qty": { "type": "long" },
                  "price": { "type": "long" }
                }
              }
            }
          }
        }
        """;

    private final MapperHelper mapperHelper = new MapperHelper();
    private final Directory directory = newDirectory();
    private MapperService mapperService;
    private DirectoryReader reader;
    private EsPhysicalOperationProviders.DefaultShardContext shard;
    private int[] parentDocs;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, mapperService, directory);
    }

    public void testSkuAndProductMustHoldOnTheSameChild() throws IOException {
        index(parentWithSplitChildren(), parentWithMatchOnTheNextPage(), parentWithNullPrice());
        SearchExecutionContext context = shard.executionContext();
        Query sku = context.getFieldType("items.sku").termQuery("a", context);
        NestedChildren.Factory factory = NestedChildren.factory(
            new IndexedByShardIdFromSingleton<>(shard),
            "items",
            List.of("qty", "price"),
            List.of(DataType.LONG, DataType.LONG),
            Collections.nCopies(2, (UnionTypeEsField) null),
            PlannerSettings.DEFAULTS,
            EsqlTestUtils.TEST_CFG
        );
        FieldAttribute qty = field("qty");
        FieldAttribute price = field("price");
        Layout layout = new Layout.Builder().append(qty).append(price).build();
        var product = new GreaterThan(
            Source.EMPTY,
            new Mul(Source.EMPTY, qty, price),
            new Literal(Source.EMPTY, 100L, DataType.LONG),
            null
        );
        var blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        boolean[] matched = new boolean[parentDocs.length];
        List<List<Object>> loaded = new ArrayList<>();
        try (
            NestedChildren children = factory.get(driverContext);
            DocBlock parents = parents(parentDocs);
            var evaluator = EvalMapper.toEvaluator(FoldContext.small(), product, layout).get(driverContext)
        ) {
            children.read(parents.asVector(), position -> true, position -> matched[position] == false, sku, page -> {
                IntVector parentPositions = page.<IntBlock>getBlock(0).asVector();
                Page fields = page.projectBlocks(new int[] { 1, 2 });
                Block values = null;
                try {
                    values = evaluator.eval(fields);
                    for (int c = 0; c < page.getPositionCount(); c++) {
                        loaded.add(
                            row(
                                parentPositions.getInt(c),
                                BlockUtils.toJavaObject(page.<Block>getBlock(1), c),
                                BlockUtils.toJavaObject(page.<Block>getBlock(2), c)
                            )
                        );
                        if (values.getValueCount(c) == 1 && Boolean.TRUE.equals(BlockUtils.toJavaObject(values, c))) {
                            matched[parentPositions.getInt(c)] = true;
                        }
                    }
                } finally {
                    if (values != null) {
                        values.close();
                    }
                    fields.releaseBlocks();
                }
            });
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        assertThat(loaded, equalTo(List.of(row(0, 1L, 10L), row(1, 10L, 20L), row(2, 10L, null))));
        assertThat(matched, equalTo(new boolean[] { false, true, false }));
    }

    /** One child matches the sku and another, different, child is the expensive product. */
    private String parentWithSplitChildren() throws IOException {
        return document(items -> {
            items.startObject().field("sku", "a").field("qty", 1).field("price", 10).endObject();
            items.startObject().field("sku", "b").field("qty", 50).field("price", 50).endObject();
        });
    }

    /**
     * The child that satisfies both is past {@link NestedChildren#PAGE_SIZE}, so it arrives in a later page than this
     * parent's first children.
     */
    private String parentWithMatchOnTheNextPage() throws IOException {
        return document(items -> {
            for (int i = 0; i < NestedChildren.PAGE_SIZE; i++) {
                items.startObject().field("sku", "b").field("qty", 1).field("price", 1).endObject();
            }
            items.startObject().field("sku", "a").field("qty", 10).field("price", 20).endObject();
        });
    }

    /** The sku matches and the price is missing, so the product is null and the parent stays false. */
    private String parentWithNullPrice() throws IOException {
        return document(items -> items.startObject().field("sku", "a").field("qty", 10).endObject());
    }

    private String document(CheckedConsumer<XContentBuilder, IOException> items) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject().startArray("items");
        items.accept(builder);
        builder.endArray().endObject();
        return org.elasticsearch.common.Strings.toString(builder);
    }

    private void index(String... documents) throws IOException {
        mapperService = mapperHelper.create(Settings.EMPTY, MAPPING);
        IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setRAMBufferSizeMB(256);
        parentDocs = new int[documents.length];
        int doc = 0;
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < documents.length; i++) {
                ParsedDocument parsed = mapperService.documentMapper()
                    .parse(new SourceToParse(Integer.toString(i), new BytesArray(documents[i]), XContentType.JSON));
                writer.addDocuments(parsed.docs());
                doc += parsed.docs().size();
                parentDocs[i] = doc - 1;
            }
        }
        reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), new ShardId(mapperService.index(), 0));
        assertThat(reader.leaves().size(), equalTo(1));
        SearchExecutionContext context = mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader, false));
        shard = new EsPhysicalOperationProviders.DefaultShardContext(0, new NoOpReleasable(), context, AliasFilter.EMPTY);
    }

    private DocBlock parents(int[] docs) {
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory(), docs.length)) {
            for (int doc : docs) {
                builder.appendShard(0).appendSegment(0).appendDoc(doc);
            }
            return builder.build();
        }
    }

    private static FieldAttribute field(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, DataType.LONG, Collections.emptyMap(), false, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    private static final class MapperHelper extends MapperServiceTestCase {
        MapperService create(Settings settings, String mapping) throws IOException {
            return createMapperService(settings, mapping);
        }
    }
}
