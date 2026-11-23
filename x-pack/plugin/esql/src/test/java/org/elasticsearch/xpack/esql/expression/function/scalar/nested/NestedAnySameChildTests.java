/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.LowercaseNormalizer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.EvaluateNestedPredicateInLucene;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.EsqlSearchExecutionContext;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.stats.SearchContextStats;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

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
    private static final String NORMALIZED_MAPPING = """
        {
          "_doc": {
            "properties": {
              "items": {
                "type": "nested",
                "properties": {
                  "sku": { "type": "keyword", "normalizer": "lowercase" }
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
            children.read(
                parents.asVector(),
                position -> true,
                position -> matched[position] == false,
                new IndexedByShardIdFromSingleton<>(sku),
                page -> {
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
                }
            );
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        assertThat(loaded, equalTo(List.of(row(0, 1L, 10L), row(1, 10L, 20L), row(2, 10L, null))));
        assertThat(matched, equalTo(new boolean[] { false, true, false }));
    }

    public void testPredicateEvaluatesTheResidualOnlyOnMatchingChildren() throws IOException {
        index(parentWithSplitChildren(), parentWithMatchOnTheNextPage(), parentWithNullPrice());
        FieldAttribute qty = field("qty");
        FieldAttribute price = field("price");
        var product = new GreaterThan(
            Source.EMPTY,
            new Mul(Source.EMPTY, qty, price),
            new Literal(Source.EMPTY, 100L, DataType.LONG),
            null
        );
        NestedPredicate predicate = new NestedPredicate(
            Source.EMPTY,
            "items",
            product,
            List.of(qty, price),
            List.of("qty", "price"),
            null,
            new SingleValueQuery(new TermQuery(Source.EMPTY, "items.sku", "a"), "items.sku", false).toQueryBuilder(),
            EsqlTestUtils.TEST_CFG
        );
        Layout layout = new Layout.Builder().append(new ReferenceAttribute(Source.EMPTY, "_doc", DataType.DOC_DATA_TYPE)).build();
        var blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (
            var evaluator = EvalMapper.toEvaluator(
                FoldContext.small(),
                predicate,
                layout,
                new IndexedByShardIdFromSingleton<>(shard),
                null,
                PlannerSettings.DEFAULTS
            ).get(driverContext);
            Page page = new Page(parents(parentDocs));
            Block result = evaluator.eval(page)
        ) {
            BooleanBlock flags = (BooleanBlock) result;
            assertThat(flags.getBoolean(flags.getFirstValueIndex(0)), equalTo(false));
            assertThat(flags.getBoolean(flags.getFirstValueIndex(1)), equalTo(true));
            assertThat(flags.getBoolean(flags.getFirstValueIndex(2)), equalTo(false));
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    /**
     * The plain keyword indexes {@code A} and the normalized keyword indexes it as {@code a}. A query built on the first
     * shard looks for {@code A} and misses the second shard's child.
     */
    public void testEachShardBuildsItsOwnChildQuery() throws IOException {
        try (
            OpenIndex plain = openIndex(0, MAPPING, "{\"items\":[{\"sku\":\"A\"}]}");
            OpenIndex normalized = openIndex(1, NORMALIZED_MAPPING, "{\"items\":[{\"sku\":\"A\"}]}")
        ) {
            var childFilter = QueryBuilders.termQuery("items.sku", "A");
            Query plainQuery = plain.shard.executionContext().toQuery(childFilter).query();
            Query normalizedQuery = normalized.shard.executionContext().toQuery(childFilter).query();
            assertThat(plainQuery, not(equalTo(normalizedQuery)));
            assertThat(new IndexSearcher(normalized.reader).count(plainQuery), equalTo(0));
            assertThat(new IndexSearcher(normalized.reader).count(normalizedQuery), equalTo(1));
            NestedPredicate predicate = new NestedPredicate(
                Source.EMPTY,
                "items",
                Literal.TRUE,
                List.of(),
                List.of(),
                null,
                childFilter,
                EsqlTestUtils.TEST_CFG
            );
            Layout layout = new Layout.Builder().append(new ReferenceAttribute(Source.EMPTY, "_doc", DataType.DOC_DATA_TYPE)).build();
            var blockFactory = blockFactory();
            DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
            try (
                var evaluator = EvalMapper.toEvaluator(
                    FoldContext.small(),
                    predicate,
                    layout,
                    new IndexedByShardIdFromList<>(List.of(plain.shard, normalized.shard)),
                    null,
                    PlannerSettings.DEFAULTS
                ).get(driverContext);
                DocBlock.Builder parents = DocBlock.newBlockBuilder(blockFactory, 2);
                Page page = parentPage(parents, plain, normalized);
                Block result = evaluator.eval(page)
            ) {
                BooleanBlock flags = (BooleanBlock) result;
                assertThat(flags.getBoolean(flags.getFirstValueIndex(0)), equalTo(true));
                assertThat(flags.getBoolean(flags.getFirstValueIndex(1)), equalTo(true));
            }
            assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        }
    }

    /**
     * The term matches both children. The single-value check confirms only the child that holds {@code a} alone, so the
     * multivalued child does not make its parent true.
     */
    public void testAMultivaluedChildFailsTheSingleValueCheck() throws IOException {
        try (OpenIndex index = openIndex(0, MAPPING, true, "{\"items\":[{\"sku\":[\"a\",\"b\"]}]}", "{\"items\":[{\"sku\":\"a\"}]}")) {
            var childFilter = new SingleValueQuery(new TermQuery(Source.EMPTY, "items.sku", "a"), "items.sku", false).toQueryBuilder();
            NestedPredicate predicate = new NestedPredicate(
                Source.EMPTY,
                "items",
                Literal.TRUE,
                List.of(),
                List.of(),
                null,
                childFilter,
                EsqlTestUtils.TEST_CFG
            );
            Layout layout = new Layout.Builder().append(new ReferenceAttribute(Source.EMPTY, "_doc", DataType.DOC_DATA_TYPE)).build();
            var blockFactory = blockFactory();
            DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
            try (
                var evaluator = EvalMapper.toEvaluator(
                    FoldContext.small(),
                    predicate,
                    layout,
                    new IndexedByShardIdFromSingleton<>(index.shard),
                    null,
                    PlannerSettings.DEFAULTS
                ).get(driverContext);
                DocBlock.Builder parents = DocBlock.newBlockBuilder(blockFactory, 2);
                Page page = parentsOf(parents, index);
                Block result = evaluator.eval(page)
            ) {
                BooleanBlock flags = (BooleanBlock) result;
                assertThat(flags.getBoolean(flags.getFirstValueIndex(0)), equalTo(false));
                assertThat(flags.getBoolean(flags.getFirstValueIndex(1)), equalTo(true));
            }
            assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        }
    }

    /**
     * The reference predicate, planned against this index and then evaluated. The sku comparison becomes the child filter and
     * the product stays in the predicate, including the match past one page of children and the null price. When expensive
     * queries are refused, the conjunction stays whole and the same parents match.
     */
    public void testTheReferencePredicatePlansAndEvaluates() throws IOException {
        index(parentWithSplitChildren(), parentWithMatchOnTheNextPage(), parentWithNullPrice());
        NestedPredicate predicate = referencePredicate();
        NestedPredicate planned = plan(predicate, SearchContextStats.from(List.of(shard.executionContext())));

        assertThat(planned.queryBuilder(), nullValue());
        assertThat(planned.childFilter(), instanceOf(SingleValueQuery.Builder.class));
        assertThat(planned.relativePaths(), equalTo(List.of("qty", "price")));
        assertParents(planned, new IndexedByShardIdFromSingleton<>(shard), parents(parentDocs), false, true, false);

        SearchExecutionContext refused = new SearchExecutionContext(shard.executionContext()) {
            @Override
            public boolean allowExpensiveQueries() {
                return false;
            }
        };
        assertThat(plan(predicate, SearchContextStats.from(List.of(refused))), equalTo(predicate));
        assertParents(predicate, new IndexedByShardIdFromSingleton<>(shard), parents(parentDocs), false, true, false);
    }

    /**
     * A lowercase normalizer on one shard makes the sku not queryable, so the comparison stays in the predicate. The stored
     * value is {@code a}, and comparing it with {@code A} does not match that parent.
     */
    public void testANormalizedShardKeepsTheComparisonOnTheValueAsRead() throws IOException {
        try (
            OpenIndex plain = openIndex(0, MAPPING, "{\"items\":[{\"sku\":\"A\"}]}");
            OpenIndex normalized = openIndex(1, NORMALIZED_MAPPING, "{\"items\":[{\"sku\":\"A\"}]}")
        ) {
            FieldAttribute sku = field("sku", DataType.KEYWORD);
            NestedPredicate predicate = new NestedPredicate(
                Source.EMPTY,
                "items",
                new Equals(Source.EMPTY, sku, Literal.keyword(Source.EMPTY, "A")),
                List.of(sku),
                List.of("sku"),
                null,
                null,
                EsqlTestUtils.TEST_CFG
            );
            SearchStats stats = SearchContextStats.from(List.of(plain.shard.executionContext(), normalized.shard.executionContext()));

            assertThat(plan(predicate, stats), equalTo(predicate));
            try (DocBlock.Builder parents = DocBlock.newBlockBuilder(blockFactory(), 2)) {
                parents.appendShard(plain.shard.index()).appendSegment(0).appendDoc(plain.parentDocs[0]);
                parents.appendShard(normalized.shard.index()).appendSegment(0).appendDoc(normalized.parentDocs[0]);
                assertParents(
                    predicate,
                    new IndexedByShardIdFromList<>(List.of(plain.shard, normalized.shard)),
                    parents.build(),
                    true,
                    false
                );
            }
        }
    }

    /** {@code items.sku == "a" AND items.qty * items.price > 100}, before the planner splits it. */
    private NestedPredicate referencePredicate() {
        FieldAttribute sku = field("sku", DataType.KEYWORD);
        FieldAttribute qty = field("qty");
        FieldAttribute price = field("price");
        var skuMatches = new Equals(Source.EMPTY, sku, Literal.keyword(Source.EMPTY, "a"));
        var product = new GreaterThan(
            Source.EMPTY,
            new Mul(Source.EMPTY, qty, price),
            new Literal(Source.EMPTY, 100L, DataType.LONG),
            null
        );
        return new NestedPredicate(
            Source.EMPTY,
            "items",
            new And(Source.EMPTY, skuMatches, product),
            List.of(sku, qty, price),
            List.of("sku", "qty", "price"),
            null,
            null,
            EsqlTestUtils.TEST_CFG
        );
    }

    private NestedPredicate plan(NestedPredicate predicate, SearchStats stats) {
        var planned = new EvaluateNestedPredicateInLucene().apply(
            new EvalExec(
                Source.EMPTY,
                new LocalSourceExec(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY),
                List.of(new Alias(Source.EMPTY, "h", predicate))
            ),
            new LocalPhysicalOptimizerContext(
                PlannerSettings.DEFAULTS,
                new EsqlFlags(true),
                EsqlTestUtils.TEST_CFG,
                FoldContext.small(),
                stats
            )
        );
        List<NestedPredicate> found = new ArrayList<>();
        planned.forEachExpression(NestedPredicate.class, found::add);
        assertThat(found, hasSize(1));
        return found.getFirst();
    }

    private void assertParents(
        NestedPredicate predicate,
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shards,
        Block parents,
        boolean... expected
    ) {
        Layout layout = new Layout.Builder().append(new ReferenceAttribute(Source.EMPTY, "_doc", DataType.DOC_DATA_TYPE)).build();
        var blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (
            var evaluator = EvalMapper.toEvaluator(FoldContext.small(), predicate, layout, shards, null, PlannerSettings.DEFAULTS)
                .get(driverContext);
            Page page = new Page(parents);
            Block result = evaluator.eval(page)
        ) {
            BooleanBlock flags = (BooleanBlock) result;
            for (int i = 0; i < expected.length; i++) {
                assertThat(flags.getBoolean(flags.getFirstValueIndex(i)), equalTo(expected[i]));
            }
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
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
        context = new EsqlSearchExecutionContext(context, QueryWarnings.EMIT, Set.of());
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
        return field(name, DataType.LONG);
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, type, Collections.emptyMap(), false, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    private OpenIndex openIndex(int shardId, String mapping, String source) throws IOException {
        return openIndex(shardId, mapping, false, source);
    }

    private OpenIndex openIndex(int shardId, String mapping, boolean esqlContext, String... sources) throws IOException {
        MapperService mapperService = mapperHelper.create(Settings.EMPTY, mapping);
        Directory directory = newDirectory();
        int[] parentDocs = new int[sources.length];
        IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setRAMBufferSizeMB(256);
        int doc = 0;
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < sources.length; i++) {
                ParsedDocument parsed = mapperService.documentMapper()
                    .parse(new SourceToParse(Integer.toString(i), new BytesArray(sources[i]), XContentType.JSON));
                writer.addDocuments(parsed.docs());
                doc += parsed.docs().size();
                parentDocs[i] = doc - 1;
            }
        }
        DirectoryReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), new ShardId(mapperService.index(), 0));
        SearchExecutionContext context = mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader, false));
        if (esqlContext) {
            context = new EsqlSearchExecutionContext(context, QueryWarnings.EMIT, Set.of());
        }
        var shard = new EsPhysicalOperationProviders.DefaultShardContext(shardId, new NoOpReleasable(), context, AliasFilter.EMPTY);
        return new OpenIndex(mapperService, directory, reader, shard, parentDocs);
    }

    private Page parentPage(DocBlock.Builder parents, OpenIndex plain, OpenIndex normalized) {
        parents.appendShard(plain.shard.index()).appendSegment(0).appendDoc(plain.parentDocs[0]);
        parents.appendShard(normalized.shard.index()).appendSegment(0).appendDoc(normalized.parentDocs[0]);
        return new Page(parents.build());
    }

    private Page parentsOf(DocBlock.Builder parents, OpenIndex index) {
        for (int parentDoc : index.parentDocs) {
            parents.appendShard(index.shard.index()).appendSegment(0).appendDoc(parentDoc);
        }
        return new Page(parents.build());
    }

    private record OpenIndex(
        MapperService mapperService,
        Directory directory,
        DirectoryReader reader,
        EsPhysicalOperationProviders.DefaultShardContext shard,
        int[] parentDocs
    ) implements java.io.Closeable {
        @Override
        public void close() throws IOException {
            IOUtils.close(reader, mapperService, directory);
        }
    }

    private static final class MapperHelper extends MapperServiceTestCase {
        MapperService create(Settings settings, String mapping) throws IOException {
            return createMapperService(settings, mapping);
        }

        @Override
        protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
            return IndexAnalyzers.of(
                Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
                Map.of("lowercase", new NamedAnalyzer("lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer())),
                Map.of()
            );
        }
    }
}
