/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.LuceneTopNSourceOperator;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockSourceReader;
import org.elasticsearch.index.mapper.FallbackSyntheticSourceBlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.ParallelExec;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class LocalExecutionPlannerTests extends MapperServiceTestCase {

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[] { false });
        params.add(new Object[] { true });
        return params;
    }

    private final QueryPragmas pragmas = new QueryPragmas(Settings.EMPTY);
    private final boolean estimatedRowSizeIsHuge;

    private Directory directory = newDirectory();
    private IndexReader reader;

    private final ArrayList<Releasable> releasables = new ArrayList<>();

    private Settings settings = SETTINGS;

    public LocalExecutionPlannerTests(@Name("estimatedRowSizeIsHuge") boolean estimatedRowSizeIsHuge) {
        this.estimatedRowSizeIsHuge = estimatedRowSizeIsHuge;
    }

    @Override
    protected Settings getIndexSettings() {
        return settings;
    }

    @Override
    protected Collection<Plugin> getPlugins() {
        var plugin = new SpatialPlugin();
        plugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                return List.of();
            }
        });

        return Collections.singletonList(plugin);
    }

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory, () -> Releasables.close(releasables), releasables::clear);
    }

    public void testLuceneSourceOperatorHugeRowSize() throws IOException {
        int estimatedRowSize = randomEstimatedRowSize(estimatedRowSizeIsHuge);
        LocalExecutionPlanner.LocalExecutionPlan plan = planner().plan(
            "test",
            FoldContext.small(),
            new EsQueryExec(
                Source.EMPTY,
                index().name(),
                IndexMode.STANDARD,
                index().indexNameWithModes(),
                List.of(),
                null,
                null,
                null,
                estimatedRowSize
            )
        );
        assertThat(plan.driverFactories.size(), lessThanOrEqualTo(pragmas.taskConcurrency()));
        LocalExecutionPlanner.DriverSupplier supplier = plan.driverFactories.get(0).driverSupplier();
        var factory = (LuceneSourceOperator.Factory) supplier.physicalOperation().sourceOperatorFactory;
        assertThat(factory.maxPageSize(), maxPageSizeMatcher(estimatedRowSizeIsHuge, estimatedRowSize));
        assertThat(factory.limit(), equalTo(Integer.MAX_VALUE));
    }

    public void testLuceneTopNSourceOperator() throws IOException {
        int estimatedRowSize = randomEstimatedRowSize(estimatedRowSizeIsHuge);
        FieldAttribute sortField = new FieldAttribute(Source.EMPTY, "field", new EsField("field", DataType.INTEGER, Map.of(), true));
        EsQueryExec.FieldSort sort = new EsQueryExec.FieldSort(sortField, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        Literal limit = new Literal(Source.EMPTY, 10, DataType.INTEGER);
        LocalExecutionPlanner.LocalExecutionPlan plan = planner().plan(
            "test",
            FoldContext.small(),
            new EsQueryExec(
                Source.EMPTY,
                index().name(),
                IndexMode.STANDARD,
                index().indexNameWithModes(),
                List.of(),
                null,
                limit,
                List.of(sort),
                estimatedRowSize
            )
        );
        assertThat(plan.driverFactories.size(), lessThanOrEqualTo(pragmas.taskConcurrency()));
        LocalExecutionPlanner.DriverSupplier supplier = plan.driverFactories.get(0).driverSupplier();
        var factory = (LuceneTopNSourceOperator.Factory) supplier.physicalOperation().sourceOperatorFactory;
        assertThat(factory.maxPageSize(), maxPageSizeMatcher(estimatedRowSizeIsHuge, estimatedRowSize));
        assertThat(factory.limit(), equalTo(10));
    }

    public void testLuceneTopNSourceOperatorDistanceSort() throws IOException {
        int estimatedRowSize = randomEstimatedRowSize(estimatedRowSizeIsHuge);
        FieldAttribute sortField = new FieldAttribute(Source.EMPTY, "point", new EsField("point", DataType.GEO_POINT, Map.of(), true));
        EsQueryExec.GeoDistanceSort sort = new EsQueryExec.GeoDistanceSort(sortField, Order.OrderDirection.ASC, 1, -1);
        Literal limit = new Literal(Source.EMPTY, 10, DataType.INTEGER);
        LocalExecutionPlanner.LocalExecutionPlan plan = planner().plan(
            "test",
            FoldContext.small(),
            new EsQueryExec(
                Source.EMPTY,
                index().name(),
                IndexMode.STANDARD,
                index().indexNameWithModes(),
                List.of(),
                null,
                limit,
                List.of(sort),
                estimatedRowSize
            )
        );
        assertThat(plan.driverFactories.size(), lessThanOrEqualTo(pragmas.taskConcurrency()));
        LocalExecutionPlanner.DriverSupplier supplier = plan.driverFactories.get(0).driverSupplier();
        var factory = (LuceneTopNSourceOperator.Factory) supplier.physicalOperation().sourceOperatorFactory;
        assertThat(factory.maxPageSize(), maxPageSizeMatcher(estimatedRowSizeIsHuge, estimatedRowSize));
        assertThat(factory.limit(), equalTo(10));
    }

    public void testDriverClusterAndNodeName() throws IOException {
        int estimatedRowSize = randomEstimatedRowSize(estimatedRowSizeIsHuge);
        LocalExecutionPlanner.LocalExecutionPlan plan = planner().plan(
            "test",
            FoldContext.small(),
            new EsQueryExec(
                Source.EMPTY,
                index().name(),
                IndexMode.STANDARD,
                index().indexNameWithModes(),
                List.of(),
                null,
                null,
                null,
                estimatedRowSize
            )
        );
        assertThat(plan.driverFactories.size(), lessThanOrEqualTo(pragmas.taskConcurrency()));
        LocalExecutionPlanner.DriverSupplier supplier = plan.driverFactories.get(0).driverSupplier();
        assertThat(supplier.clusterName(), equalTo("dev-cluster"));
        assertThat(supplier.nodeName(), equalTo("node-1"));
    }

    public void testParallel() throws Exception {
        EsQueryExec queryExec = new EsQueryExec(
            Source.EMPTY,
            index().name(),
            IndexMode.STANDARD,
            index().indexNameWithModes(),
            List.of(),
            null,
            null,
            null,
            between(1, 1000)
        );
        var limitExec = new LimitExec(
            Source.EMPTY,
            new ParallelExec(queryExec.source(), queryExec),
            new Literal(Source.EMPTY, between(1, 100), DataType.INTEGER),
            randomEstimatedRowSize(estimatedRowSizeIsHuge)
        );
        LocalExecutionPlanner.LocalExecutionPlan plan = planner().plan("test", FoldContext.small(), limitExec);
        assertThat(plan.driverFactories, hasSize(2));
    }

    public void testPlanUnmappedFieldExtractStoredSource() throws Exception {
        var blockLoader = constructBlockLoader();
        // In case of stored source we expect bytes based block source loader (this loads source from _source)
        assertThat(blockLoader, instanceOf(BlockSourceReader.BytesRefsBlockLoader.class));
    }

    public void testPlanUnmappedFieldExtractSyntheticSource() throws Exception {
        // Enables synthetic source, so that fallback synthetic source blocker loader is used:
        settings = Settings.builder().put(settings).put("index.mapping.source.mode", "synthetic").build();

        var blockLoader = constructBlockLoader();
        // In case of synthetic source we expect bytes based block source loader (this loads source from _ignored_source)
        assertThat(blockLoader, instanceOf(FallbackSyntheticSourceBlockLoader.class));
    }

    private BlockLoader constructBlockLoader() throws IOException {
        EsQueryExec queryExec = new EsQueryExec(
            Source.EMPTY,
            index().name(),
            IndexMode.STANDARD,
            index().indexNameWithModes(),
            List.of(new FieldAttribute(Source.EMPTY, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD)),
            null,
            null,
            null,
            between(1, 1000)
        );
        FieldExtractExec fieldExtractExec = new FieldExtractExec(
            Source.EMPTY,
            queryExec,
            List.of(
                new FieldAttribute(Source.EMPTY, "potentially_unmapped", new PotentiallyUnmappedKeywordEsField("potentially_unmapped"))
            ),
            MappedFieldType.FieldExtractPreference.NONE
        );
        LocalExecutionPlanner.LocalExecutionPlan plan = planner().plan("test", FoldContext.small(), fieldExtractExec);
        var p = plan.driverFactories.get(0).driverSupplier().physicalOperation();
        var fieldInfo = ((ValuesSourceReaderOperator.Factory) p.intermediateOperatorFactories.get(0)).fields().get(0);
        return fieldInfo.blockLoader().apply(0);
    }

    private int randomEstimatedRowSize(boolean huge) {
        int hugeBoundary = SourceOperator.MIN_TARGET_PAGE_SIZE * 10;
        return huge ? between(hugeBoundary, Integer.MAX_VALUE) : between(1, hugeBoundary);
    }

    private Matcher<Integer> maxPageSizeMatcher(boolean estimatedRowSizeIsHuge, int estimatedRowSize) {
        if (estimatedRowSizeIsHuge) {
            return equalTo(SourceOperator.MIN_TARGET_PAGE_SIZE);
        }
        return equalTo(SourceOperator.TARGET_PAGE_SIZE / estimatedRowSize);
    }

    private LocalExecutionPlanner planner() throws IOException {
        List<EsPhysicalOperationProviders.ShardContext> shardContexts = createShardContexts();
        return new LocalExecutionPlanner(
            "test",
            "",
            null,
            BigArrays.NON_RECYCLING_INSTANCE,
            TestBlockFactory.getNonBreakingInstance(),
            Settings.builder()
                .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "dev-cluster")
                .put(Node.NODE_NAME_SETTING.getKey(), "node-1")
                .build(),
            config(),
            null,
            null,
            null,
            null,
            null,
            esPhysicalOperationProviders(shardContexts),
            shardContexts
        );
    }

    private Configuration config() {
        return new Configuration(
            randomZone(),
            randomLocale(random()),
            "test_user",
            "test_cluster",
            pragmas,
            EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(null),
            EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(null),
            StringUtils.EMPTY,
            false,
            Map.of(),
            System.nanoTime(),
            randomBoolean()
        );
    }

    private EsPhysicalOperationProviders esPhysicalOperationProviders(List<EsPhysicalOperationProviders.ShardContext> shardContexts) {
        return new EsPhysicalOperationProviders(FoldContext.small(), shardContexts, null, DataPartitioning.AUTO);
    }

    private List<EsPhysicalOperationProviders.ShardContext> createShardContexts() throws IOException {
        int numShards = randomIntBetween(1, 1000);
        List<EsPhysicalOperationProviders.ShardContext> shardContexts = new ArrayList<>(numShards);
        var searcher = new ContextIndexSearcher(
            reader(),
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            TrivialQueryCachingPolicy.NEVER,
            true
        );
        for (int i = 0; i < numShards; i++) {
            SearchExecutionContext searchExecutionContext = createSearchExecutionContext(createMapperService(mapping(b -> {
                b.startObject("point").field("type", "geo_point").endObject();
            })), searcher);
            shardContexts.add(
                new EsPhysicalOperationProviders.DefaultShardContext(i, new NoOpReleasable(), searchExecutionContext, AliasFilter.EMPTY)
            );
        }
        releasables.add(searcher);
        return shardContexts;
    }

    private IndexReader reader() {
        if (reader != null) {
            return reader;
        }
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int d = 0; d < 10; d++) {
                List<IndexableField> doc = new ArrayList<>();
                doc.add(new SortedNumericDocValuesField("s", d));
                writer.addDocument(doc);
            }
            reader = writer.getReader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return reader;
    }

    private EsIndex index() {
        return new EsIndex("test", Map.of());
    }
}
