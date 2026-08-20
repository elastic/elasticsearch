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
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.query.DataPartitioning;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.query.LuceneTopNSourceOperator;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.ColumnLoadOperator;
import org.elasticsearch.compute.operator.DistinctByOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.FilterOperator;
import org.elasticsearch.compute.operator.LocalSourceOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.compute.operator.ProjectOperator;
import org.elasticsearch.compute.operator.RowInTableLookupOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.StreamingPageOperator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.mapper.BlockSourceReader;
import org.elasticsearch.index.mapper.FallbackSyntheticSourceBlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.datasources.CoalescedSplit;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.OperatorFactoryRegistry;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorFactoryProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.ProjectAwayColumns;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.DistinctByExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MetricsInfoExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.StreamingOutputExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

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
            PlannerSettings.DEFAULTS,
            new EsQueryExec(
                Source.EMPTY,
                EsIndexGenerator.esIndex("test").name(),
                IndexMode.STANDARD,
                List.of(),
                null,
                null,
                estimatedRowSize,
                List.of(new EsQueryExec.QueryBuilderAndTags(null, List.of()))
            ),
            EmptyIndexedByShardId.instance()
        );
        assertThat(plan.driverFactories.size(), lessThanOrEqualTo(pragmas.taskConcurrency()));
        LocalExecutionPlanner.DriverSupplier supplier = plan.driverFactories.get(0).driverSupplier();
        var factory = (LuceneSourceOperator.Factory) supplier.physicalOperation().sourceOperatorFactory;
        assertThat(factory.maxPageSize(), maxPageSizeMatcher(estimatedRowSizeIsHuge, estimatedRowSize));
        assertThat(factory.limit(), equalTo(Integer.MAX_VALUE));
    }

    public void testLuceneTopNSourceOperator() throws IOException {
        int estimatedRowSize = randomEstimatedRowSize(estimatedRowSizeIsHuge);
        FieldAttribute sortField = new FieldAttribute(
            Source.EMPTY,
            "field",
            new EsField("field", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        EsQueryExec.FieldSort sort = new EsQueryExec.FieldSort(sortField, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        Literal limit = new Literal(Source.EMPTY, 10, DataType.INTEGER);
        LocalExecutionPlanner.LocalExecutionPlan plan = planner().plan(
            "test",
            FoldContext.small(),
            PlannerSettings.DEFAULTS,
            new EsQueryExec(
                Source.EMPTY,
                EsIndexGenerator.esIndex("test").name(),
                IndexMode.STANDARD,
                List.of(),
                limit,
                List.of(sort),
                estimatedRowSize,
                List.of(new EsQueryExec.QueryBuilderAndTags(null, List.of()))
            ),
            EmptyIndexedByShardId.instance()
        );
        assertThat(plan.driverFactories.size(), lessThanOrEqualTo(pragmas.taskConcurrency()));
        LocalExecutionPlanner.DriverSupplier supplier = plan.driverFactories.get(0).driverSupplier();
        var factory = (LuceneTopNSourceOperator.Factory) supplier.physicalOperation().sourceOperatorFactory;
        assertThat(factory.maxPageSize(), maxPageSizeMatcher(estimatedRowSizeIsHuge, estimatedRowSize));
        assertThat(factory.limit(), equalTo(10));
    }

    public void testLuceneTopNSourceOperatorDistanceSort() throws IOException {
        int estimatedRowSize = randomEstimatedRowSize(estimatedRowSizeIsHuge);
        FieldAttribute sortField = new FieldAttribute(
            Source.EMPTY,
            "point",
            new EsField("point", DataType.GEO_POINT, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        EsQueryExec.GeoDistanceSort sort = new EsQueryExec.GeoDistanceSort(sortField, Order.OrderDirection.ASC, 1, -1);
        Literal limit = new Literal(Source.EMPTY, 10, DataType.INTEGER);
        LocalExecutionPlanner.LocalExecutionPlan plan = planner().plan(
            "test",
            FoldContext.small(),
            PlannerSettings.DEFAULTS,
            new EsQueryExec(
                Source.EMPTY,
                EsIndexGenerator.esIndex("test").name(),
                IndexMode.STANDARD,
                List.of(),
                limit,
                List.of(sort),
                estimatedRowSize,
                List.of(new EsQueryExec.QueryBuilderAndTags(null, List.of()))
            ),
            EmptyIndexedByShardId.instance()
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
            PlannerSettings.DEFAULTS,
            new EsQueryExec(
                Source.EMPTY,
                EsIndexGenerator.esIndex("test").name(),
                IndexMode.STANDARD,
                List.of(),
                null,
                null,
                estimatedRowSize,
                List.of(new EsQueryExec.QueryBuilderAndTags(null, List.of()))
            ),
            EmptyIndexedByShardId.instance()
        );
        assertThat(plan.driverFactories.size(), lessThanOrEqualTo(pragmas.taskConcurrency()));
        LocalExecutionPlanner.DriverSupplier supplier = plan.driverFactories.get(0).driverSupplier();
        assertThat(supplier.clusterName(), equalTo("dev-cluster"));
        assertThat(supplier.nodeName(), equalTo("node-1"));
    }

    public void testExternalSourceUsesSliceQueueWhenGenericFileListIsUnresolved() throws IOException {
        AtomicReference<SourceOperatorContext> captured = new AtomicReference<>();
        SourceOperatorFactoryProvider provider = capturingProvider(captured);
        OperatorFactoryRegistry operatorFactoryRegistry = new OperatorFactoryRegistry(Map.of(), Map.of("file", provider), Runnable::run);

        List<Attribute> attrs = List.of(
            new FieldAttribute(Source.EMPTY, "a", new EsField("a", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE))
        );
        ExternalSplit child1 = new FileSplit(
            "file",
            StoragePath.of("s3://test-bucket/warehouse/stress/part-00000.csv"),
            0,
            10,
            ".csv",
            Map.of(),
            Map.of()
        );
        ExternalSplit child2 = new FileSplit(
            "file",
            StoragePath.of("s3://test-bucket/warehouse/stress/part-00001.csv"),
            0,
            10,
            ".csv",
            Map.of(),
            Map.of()
        );
        ExternalSplit coalesced = new CoalescedSplit("file", List.of(child1, child2));

        ExternalSourceExec exec = new ExternalSourceExec(
            Source.EMPTY,
            "s3://test-bucket/warehouse/stress/*.csv",
            "file",
            attrs,
            Map.of(),
            Map.of(),
            null,
            10
        ).withSplits(List.of(coalesced));

        planner(operatorFactoryRegistry).plan(
            "test",
            FoldContext.small(),
            PlannerSettings.DEFAULTS,
            exec,
            EmptyIndexedByShardId.instance()
        );

        assertThat(captured.get(), notNullValue());
        assertThat(captured.get().sliceQueue(), notNullValue());
        assertThat(captured.get().sliceQueue().totalSlices(), equalTo(1));
    }

    /**
     * Regression guard for the multi-file over-read. When an instance is assigned splits AND also carries a
     * resolved {@link FileList} (the coordinator keeps one; data nodes don't), it must route through the
     * slice queue and read only the assigned splits — NOT fall through to the resolved-FileList multi-file
     * read, which would re-read the entire glob behind the splits and double-count rows that the slice-queue
     * instances also read. Before the fix, {@code useSliceQueue} was false for a single split + resolved
     * FileList, so {@code sliceQueue} was null here and the operator took the whole-glob path.
     */
    public void testExternalSourceUsesSliceQueueWhenResolvedFileListHasAssignedSplits() throws IOException {
        AtomicReference<SourceOperatorContext> captured = new AtomicReference<>();
        SourceOperatorFactoryProvider provider = capturingProvider(captured);
        OperatorFactoryRegistry operatorFactoryRegistry = new OperatorFactoryRegistry(Map.of(), Map.of("file", provider), Runnable::run);

        List<Attribute> attrs = List.of(
            new FieldAttribute(Source.EMPTY, "a", new EsField("a", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE))
        );
        StoragePath p1 = StoragePath.of("s3://test-bucket/warehouse/stress/part-00000.csv");
        StoragePath p2 = StoragePath.of("s3://test-bucket/warehouse/stress/part-00001.csv");
        // Resolved FileList over both files — the shape a coordinator holds.
        FileList resolved = GlobExpander.fileListOf(
            List.of(new StorageEntry(p1, 10, Instant.EPOCH), new StorageEntry(p2, 10, Instant.EPOCH)),
            "s3://test-bucket/warehouse/stress/*.csv"
        );
        assertThat("precondition: FileList must be resolved", resolved.isResolved(), equalTo(true));

        // A single (coalesced) split assigned to this instance — splitCount == 1.
        ExternalSplit child1 = new FileSplit("file", p1, 0, 10, ".csv", Map.of(), Map.of());
        ExternalSplit child2 = new FileSplit("file", p2, 0, 10, ".csv", Map.of(), Map.of());
        ExternalSplit coalesced = new CoalescedSplit("file", List.of(child1, child2));

        ExternalSourceExec exec = new ExternalSourceExec(
            Source.EMPTY,
            "s3://test-bucket/warehouse/stress/*.csv",
            "file",
            attrs,
            Map.of(),
            Map.of(),
            null,
            10
        ).withFileList(resolved).withSplits(List.of(coalesced));

        planner(operatorFactoryRegistry).plan(
            "test",
            FoldContext.small(),
            PlannerSettings.DEFAULTS,
            exec,
            EmptyIndexedByShardId.instance()
        );

        assertThat(captured.get(), notNullValue());
        assertThat(
            "a resolved FileList with assigned splits must still route through the slice queue (read only the "
                + "splits), not the whole-glob multi-file read",
            captured.get().sliceQueue(),
            notNullValue()
        );
        assertThat(captured.get().sliceQueue().totalSlices(), equalTo(1));
    }

    /**
     * The data-node backstop: building the operator for an external source is refused on a node that does not have
     * federation, whoever planned the query. An already-rewritten {@link ExternalSourceExec} can arrive from an
     * enabled coordinator, from a remote cluster, or from a rolling restart that has not reached this node yet.
     */
    public void testExternalSourceRefusedWhenFederationIsNotAvailable() throws IOException {
        SourceOperatorFactoryProvider provider = capturingProvider(new AtomicReference<>());
        OperatorFactoryRegistry operatorFactoryRegistry = new OperatorFactoryRegistry(Map.of(), Map.of("file", provider), Runnable::run);

        List<Attribute> attrs = List.of(
            new FieldAttribute(Source.EMPTY, "a", new EsField("a", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE))
        );
        ExternalSourceExec exec = new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/data.ndjson",
            "file",
            attrs,
            Map.of(),
            Map.of(),
            null,
            10
        );

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> planner(operatorFactoryRegistry, false).plan(
                "test",
                FoldContext.small(),
                PlannerSettings.DEFAULTS,
                exec,
                EmptyIndexedByShardId.instance()
            )
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("external data sources are not available"));
    }

    /**
     * {@link LocalExecutionPlanner} must pass {@link OperatorFactoryRegistry#executor()} and
     * {@link OperatorFactoryRegistry#fileReadExecutor()} into {@link SourceOperatorContext} separately.
     * Production wires the main executor to external-source work and {@code fileReadExecutor} to
     * {@link org.elasticsearch.threadpool.ThreadPool.Names#GENERIC} via
     * {@link org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction}.
     */
    public void testPlanExternalSourcePassesDistinctExecutorsToSourceOperatorContext() throws IOException {
        AtomicReference<SourceOperatorContext> captured = new AtomicReference<>();
        SourceOperatorFactoryProvider provider = capturingProvider(captured);
        Executor mainExecutor = r -> r.run();
        Executor fileReadExecutor = r -> r.run();
        OperatorFactoryRegistry operatorFactoryRegistry = new OperatorFactoryRegistry(
            Map.of(),
            Map.of("file", provider),
            mainExecutor,
            fileReadExecutor
        );

        List<Attribute> attrs = List.of(
            new FieldAttribute(Source.EMPTY, "a", new EsField("a", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE))
        );
        ExternalSplit child1 = new FileSplit(
            "file",
            StoragePath.of("s3://test-bucket/warehouse/stress/part-00000.csv"),
            0,
            10,
            ".csv",
            Map.of(),
            Map.of()
        );
        ExternalSplit child2 = new FileSplit(
            "file",
            StoragePath.of("s3://test-bucket/warehouse/stress/part-00001.csv"),
            0,
            10,
            ".csv",
            Map.of(),
            Map.of()
        );
        ExternalSplit coalesced = new CoalescedSplit("file", List.of(child1, child2));

        ExternalSourceExec exec = new ExternalSourceExec(
            Source.EMPTY,
            "s3://test-bucket/warehouse/stress/*.csv",
            "file",
            attrs,
            Map.of(),
            Map.of(),
            null,
            10
        ).withSplits(List.of(coalesced));

        planner(operatorFactoryRegistry).plan(
            "test",
            FoldContext.small(),
            PlannerSettings.DEFAULTS,
            exec,
            EmptyIndexedByShardId.instance()
        );

        assertThat(captured.get(), notNullValue());
        assertThat(captured.get().executor(), sameInstance(mainExecutor));
        assertThat(captured.get().fileReadExecutor(), sameInstance(fileReadExecutor));
    }

    /**
     * When every column is pruned (e.g. {@code COUNT(*)} with no referenced fields), {@link ProjectAwayColumns}
     * inserts a single synthetic {@code "<all-fields-projected>"} attribute into the {@link ExternalSourceExec}
     * output. The planner must translate that sentinel into an empty {@code projectedColumns} list before
     * handing it to the format reader, so the reader can take its row-count-only fast path.
     */
    public void testExternalSourceCountStarYieldsEmptyProjection() throws IOException {
        AtomicReference<SourceOperatorContext> captured = new AtomicReference<>();
        SourceOperatorFactoryProvider provider = capturingProvider(captured);
        OperatorFactoryRegistry operatorFactoryRegistry = new OperatorFactoryRegistry(Map.of(), Map.of("file", provider), Runnable::run);

        // Mirrors what ProjectAwayColumns inserts when COUNT(*) prunes every real column.
        List<Attribute> attrs = List.of(new ReferenceAttribute(Source.EMPTY, null, ProjectAwayColumns.ALL_FIELDS_PROJECTED, DataType.NULL));
        ExternalSourceExec exec = new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/data.ndjson",
            "file",
            attrs,
            Map.of(),
            Map.of(),
            null,
            10
        );

        planner(operatorFactoryRegistry).plan(
            "test",
            FoldContext.small(),
            PlannerSettings.DEFAULTS,
            exec,
            EmptyIndexedByShardId.instance()
        );

        assertThat(captured.get(), notNullValue());
        assertThat(
            "COUNT(*) sentinel must arrive at the format reader as an empty projection",
            captured.get().projectedColumns(),
            equalTo(List.of())
        );
    }

    /**
     * Guards the partition-column seeding in {@link LocalExecutionPlanner} {@code planExternalSource}: on a data node
     * the coordinator's {@link FileList} is not serialized ({@code ExternalSourceExec.writeTo} drops it, so it
     * deserializes to {@code null}), so the Hive partition-column NAMES must instead be recovered from the serialized
     * {@code _partition.columns} stamp in {@code sourceMetadata} — read through the node-safe
     * {@code ExternalSourceExec.partitionColumnNames()} accessor. Without it
     * {@link SourceOperatorContext#partitionColumnNames()} is empty on the data node, {@code VirtualColumnIterator}
     * never materialises the partition column, and a distributed partition-column read attaches SQL {@code NULL}.
     * <p>
     * Here {@code fileList} is deliberately left {@code null} (the data-node shape) and the partition name {@code p} is
     * present in NEITHER the output attributes NOR a {@code FileList} — its only possible source is the stamp, so
     * seeing it in the resolved set pins exactly this read. The end-to-end value-attachment twin is
     * {@code ExternalHivePartitionDistributedValueIT}.
     */
    public void testExternalSourceReadsPartitionColumnNamesFromSourceMetadataStamp() throws IOException {
        AtomicReference<SourceOperatorContext> captured = new AtomicReference<>();
        SourceOperatorFactoryProvider provider = capturingProvider(captured);
        OperatorFactoryRegistry operatorFactoryRegistry = new OperatorFactoryRegistry(Map.of(), Map.of("file", provider), Runnable::run);

        // Only the data column 'id' is in the output — the partition column 'p' is NOT, so the sole path that can put
        // it into partitionColumnNames is the serialized stamp read below.
        List<Attribute> attrs = List.of(
            new FieldAttribute(Source.EMPTY, "id", new EsField("id", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE))
        );
        ExternalSplit split = new FileSplit(
            "file",
            StoragePath.of("s3://test-bucket/warehouse/p=a/part-00000.parquet"),
            0,
            10,
            ".parquet",
            Map.of(),
            Map.of("p", "a")
        );

        // fileList left null (the data-node shape: the coordinator's resolved FileList is not serialized), so the
        // fileList partition-metadata branch contributes nothing and 'p' can only come from the _partition.columns stamp.
        ExternalSourceExec exec = new ExternalSourceExec(
            Source.EMPTY,
            "s3://test-bucket/warehouse/*.parquet",
            "file",
            attrs,
            Map.of(),
            Map.of(SourceStatisticsSerializer.PARTITION_COLUMNS_KEY, List.of("p")),
            null, // pushedFilter
            10
        ).withSplits(List.of(split));

        planner(operatorFactoryRegistry).plan(
            "test",
            FoldContext.small(),
            PlannerSettings.DEFAULTS,
            exec,
            EmptyIndexedByShardId.instance()
        );

        assertThat(captured.get(), notNullValue());
        assertThat(
            "partition column names must be recovered from the serialized _partition.columns stamp when the "
                + "coordinator FileList is absent (data-node read)",
            captured.get().partitionColumnNames(),
            equalTo(Set.of("p"))
        );
    }

    public void testPlanUnmappedFieldExtractStoredSource() throws Exception {
        var blockLoader = constructBlockLoader();
        // In case of stored source we expect bytes based block source loader (this loads source from _source)
        assertThat(blockLoader.loader(), instanceOf(BlockSourceReader.BytesRefsBlockLoader.class));
    }

    public void testPlanUnmappedFieldExtractSyntheticSource() throws Exception {
        // Enables synthetic source, so that fallback synthetic source blocker loader is used:
        settings = Settings.builder().put(settings).put("index.mapping.source.mode", "synthetic").build();

        var blockLoader = constructBlockLoader();
        // In case of synthetic source we expect bytes based block source loader (this loads source from _ignored_source)
        assertThat(blockLoader.loader(), instanceOf(FallbackSyntheticSourceBlockLoader.class));
    }

    public void testTimeSeries() throws IOException {
        int estimatedRowSize = estimatedRowSizeIsHuge ? randomIntBetween(20000, Integer.MAX_VALUE) : randomIntBetween(1, 50);
        EsQueryExec queryExec = new EsQueryExec(
            Source.EMPTY,
            EsIndexGenerator.esIndex("test").name(),
            IndexMode.STANDARD,
            List.of(),
            new Literal(Source.EMPTY, 10, DataType.INTEGER),
            List.of(),
            estimatedRowSize,
            List.of(new EsQueryExec.QueryBuilderAndTags(null, List.of()))
        );
        TimeSeriesAggregateExec aggExec = new TimeSeriesAggregateExec(
            Source.EMPTY,
            queryExec,
            List.of(),
            List.of(new Alias(Source.EMPTY, "count(*)", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*")))),
            AggregatorMode.SINGLE,
            List.of(),
            10,
            null
        );
        PlannerSettings plannerSettings = new PlannerSettings(
            DataPartitioning.AUTO,
            PlannerSettings.DEFAULTS.docsThresholdForAutoPartitioning(),
            ByteSizeValue.ofMb(1),
            10_000,
            ByteSizeValue.ofMb(1),
            between(1, 10000),
            randomDoubleBetween(0.1, 1.0, true),
            PlannerSettings.TIME_SERIES_TARGET_CHUNK_ROWS.getDefault(Settings.EMPTY),
            between(0, 1000),
            MappedFieldType.BlockLoaderContext.DEFAULT_ORDINALS_BYTE_SIZE,
            MappedFieldType.BlockLoaderContext.DEFAULT_SCRIPT_BYTE_SIZE,
            10,
            PlannerSettings.SOURCE_RESERVATION_FACTOR.getDefault(Settings.EMPTY),
            PlannerSettings.BYTES_REF_RAM_OVERESTIMATE_THRESHOLD.getDefault(Settings.EMPTY),
            PlannerSettings.BYTES_REF_RAM_OVERESTIMATE_FACTOR.getDefault(Settings.EMPTY),
            PlannerSettings.DOC_SEQUENCE_BYTES_REF_FIELD_THRESHOLD.getDefault(Settings.EMPTY),
            PlannerSettings.PARALLEL_OPERATOR_PROMOTION_THRESHOLD_ROWS.getDefault(Settings.EMPTY),
            PlannerSettings.PARALLEL_OPERATOR_MAX_WORKERS.getDefault(Settings.EMPTY),
            PlannerSettings.IN_SUBQUERY_HASH_JOIN_THRESHOLD.getDefault(Settings.EMPTY)
        );
        LocalExecutionPlanner.LocalExecutionPlan plan = planner().plan(
            "test",
            FoldContext.small(),
            plannerSettings,
            aggExec,
            EmptyIndexedByShardId.instance()
        );
        assertThat(plan.driverFactories.size(), lessThanOrEqualTo(pragmas.taskConcurrency()));
        LocalExecutionPlanner.DriverSupplier supplier = plan.driverFactories.get(0).driverSupplier();
        var factory = (LuceneSourceOperator.Factory) supplier.physicalOperation().sourceOperatorFactory;
        if (estimatedRowSizeIsHuge) {
            assertThat(factory.maxPageSize(), equalTo(128));
        } else {
            assertThat(factory.maxPageSize(), equalTo(2048));
        }
    }

    /**
     * When the child EsQueryExec contains a {@code _doc} attribute, the planner should
     * build the full LuceneSourceOperator pipeline for MetricsInfoExec.
     */
    public void testPlanMetricsInfoBuildsLuceneSourceWhenDocAttributePresent() throws IOException {
        FieldAttribute docAttr = new FieldAttribute(Source.EMPTY, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
        EsQueryExec queryExec = new EsQueryExec(
            Source.EMPTY,
            EsIndexGenerator.esIndex("test").name(),
            IndexMode.STANDARD,
            List.of(docAttr),
            null,
            null,
            1,
            List.of(new EsQueryExec.QueryBuilderAndTags(null, List.of()))
        );

        List<Attribute> outputAttrs = buildMetricsInfoAttributes();
        MetricsInfoExec metricsInfoExec = new MetricsInfoExec(
            Source.EMPTY,
            queryExec,
            outputAttrs,
            outputAttrs,
            MetricsInfoExec.Mode.INITIAL
        );

        LocalExecutionPlanner.LocalExecutionPlan plan = planner().plan(
            "test",
            FoldContext.small(),
            PlannerSettings.DEFAULTS,
            metricsInfoExec,
            EmptyIndexedByShardId.instance()
        );
        assertThat(plan.driverFactories.size(), equalTo(1));
        var sourceFactory = plan.driverFactories.get(0).driverSupplier().physicalOperation().sourceOperatorFactory;
        assertThat(
            "Expected LuceneSourceOperator when EsQueryExec child has _doc",
            sourceFactory,
            instanceOf(LuceneSourceOperator.Factory.class)
        );
    }

    /**
     * When the child plan does not contain a {@code _doc} attribute,
     * the planner should correctly fall back to an empty source.
     */
    public void testPlanMetricsInfoEmptySourceWhenNoDocAttribute() throws IOException {
        EsQueryExec queryExec = new EsQueryExec(
            Source.EMPTY,
            EsIndexGenerator.esIndex("test").name(),
            IndexMode.STANDARD,
            List.of(),
            null,
            null,
            1,
            List.of(new EsQueryExec.QueryBuilderAndTags(null, List.of()))
        );

        List<Attribute> outputAttrs = buildMetricsInfoAttributes();
        MetricsInfoExec metricsInfoExec = new MetricsInfoExec(
            Source.EMPTY,
            queryExec,
            outputAttrs,
            outputAttrs,
            MetricsInfoExec.Mode.INITIAL
        );

        LocalExecutionPlanner.LocalExecutionPlan plan = planner().plan(
            "test",
            FoldContext.small(),
            PlannerSettings.DEFAULTS,
            metricsInfoExec,
            EmptyIndexedByShardId.instance()
        );
        assertThat(plan.driverFactories.size(), equalTo(1));
        var sourceFactory = plan.driverFactories.get(0).driverSupplier().physicalOperation().sourceOperatorFactory;
        assertThat(
            "Expected LocalSourceOperator (empty source) when no _doc attribute is present",
            sourceFactory,
            instanceOf(LocalSourceOperator.LocalSourceFactory.class)
        );
    }

    public void testStreamingOutput() throws IOException {
        int estimatedRowSize = randomEstimatedRowSize(estimatedRowSizeIsHuge);
        EsQueryExec esQueryExec = new EsQueryExec(
            Source.EMPTY,
            EsIndexGenerator.esIndex("test").name(),
            IndexMode.STANDARD,
            List.of(),
            null,
            null,
            estimatedRowSize,
            List.of(new EsQueryExec.QueryBuilderAndTags(null, List.of()))
        );
        PageStreamPublisher pageStream = new PageStreamPublisher(randomIntBetween(1, 1000));
        StreamingOutputExec streamingOutput = new StreamingOutputExec(Source.EMPTY, esQueryExec, pageStream);

        LocalExecutionPlanner.LocalExecutionPlan plan = planner().plan(
            "test",
            FoldContext.small(),
            PlannerSettings.DEFAULTS,
            streamingOutput,
            EmptyIndexedByShardId.instance()
        );

        assertThat(plan.driverFactories.size(), lessThanOrEqualTo(pragmas.taskConcurrency()));
        var physicalOperation = plan.driverFactories.get(0).driverSupplier().physicalOperation();
        assertThat(physicalOperation.sourceOperatorFactory, instanceOf(LuceneSourceOperator.Factory.class));
        var sinkFactory = (StreamingPageOperator.Factory) physicalOperation.sinkOperatorFactory;
        assertThat(sinkFactory.stream(), sameInstance(pageStream));
        assertThat(sinkFactory.alignment(), notNullValue());
    }

    private static List<Attribute> buildMetricsInfoAttributes() {
        List<Attribute> attributes = new ArrayList<>();
        for (String name : MetricsInfo.ATTRIBUTES) {
            attributes.add(new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD));
        }
        return attributes;
    }

    private ValuesSourceReaderOperator.LoaderAndConverter constructBlockLoader() throws IOException {
        EsQueryExec queryExec = new EsQueryExec(
            Source.EMPTY,
            EsIndexGenerator.esIndex("test").name(),
            IndexMode.STANDARD,
            List.of(new FieldAttribute(Source.EMPTY, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD)),
            null,
            null,
            between(1, 1000),
            List.of(new EsQueryExec.QueryBuilderAndTags(null, List.of()))
        );
        FieldExtractExec fieldExtractExec = new FieldExtractExec(
            Source.EMPTY,
            queryExec,
            List.of(
                new FieldAttribute(Source.EMPTY, "potentially_unmapped", new PotentiallyUnmappedKeywordEsField("potentially_unmapped"))
            ),
            MappedFieldType.FieldExtractPreference.NONE
        );
        var plan = planner().plan(
            "test",
            FoldContext.small(),
            PlannerSettings.DEFAULTS,
            fieldExtractExec,
            EmptyIndexedByShardId.instance()
        );
        var p = plan.driverFactories.get(0).driverSupplier().physicalOperation();
        var fieldInfo = ((ValuesSourceReaderOperator.Factory) p.intermediateOperatorFactories.get(0)).fields().get(0);
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance(), null);
        return fieldInfo.buildLoader().build(driverContext, 0);
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

    /**
     * A {@link SourceOperatorFactoryProvider} that captures the {@link SourceOperatorContext} the planner hands it and
     * returns a no-op {@link SourceOperator} (never produces a page). Lets a test assert on the context the planner
     * built for an external source without running a real read.
     */
    private static SourceOperatorFactoryProvider capturingProvider(AtomicReference<SourceOperatorContext> captured) {
        return context -> {
            captured.set(context);
            return new SourceOperator.SourceOperatorFactory() {
                @Override
                public SourceOperator get(DriverContext driverContext) {
                    return new SourceOperator() {
                        @Override
                        public Page getOutput() {
                            return null;
                        }

                        @Override
                        public boolean isFinished() {
                            return true;
                        }

                        @Override
                        public void finish() {}

                        @Override
                        public void close() {}
                    };
                }

                @Override
                public String describe() {
                    return "test-source";
                }
            };
        };
    }

    public void testPlanInnerJoinManyToOne() throws IOException {
        // LEFT hash-join emits the lookup ordinal and loads values; FilterExec drops misses (null ordinal);
        // the final Project drops the ordinal.
        assertThat(
            planInnerJoinFactories(false),
            contains(
                RowInTableLookupOperator.Factory.class,
                ColumnLoadOperator.Factory.class,
                FilterOperator.FilterOperatorFactory.class,
                ProjectOperator.ProjectOperatorFactory.class
            )
        );
    }

    public void testPlanInnerJoinOneToOneHasDistinctByGuard() throws IOException {
        // unique=true adds an OrdinalIntKeyFactory guard after the filter, keyed on the lookup ordinal
        // (unique per build row), independent of the join key types and count.
        assertThat(
            planInnerJoinFactories(true),
            contains(
                RowInTableLookupOperator.Factory.class,
                ColumnLoadOperator.Factory.class,
                FilterOperator.FilterOperatorFactory.class,
                DistinctByOperator.OrdinalIntKeyFactory.class,
                ProjectOperator.ProjectOperatorFactory.class
            )
        );
    }

    public void testInnerJoinUniqueDuplicateBuildKeyThrows() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> runInnerJoin(innerJoinExec(true, new long[] { 10 }, new long[] { 10, 10, 20 }, new long[] { 100, 100, 200 }))
        );
        assertThat(e.getMessage(), containsString("found a duplicate row"));
    }

    public void testInnerJoinManyToOneGathersAndDropsMisses() throws IOException {
        // group_left: many probe rows per build row; the miss (99) is dropped by the inner-join filter.
        // Final page order matches InnerJoin.output(): added build columns, then left join keys.
        List<Page> results = runInnerJoin(
            innerJoinExec(false, new long[] { 10, 20, 10, 99, 30 }, new long[] { 10, 20, 30 }, new long[] { 100, 200, 300 })
        );
        assertInnerJoinOutput(results, List.of(100L, 200L, 100L, 300L), List.of(10L, 20L, 10L, 30L));
    }

    public void testInnerJoinOneToOneUniqueProbe() throws IOException {
        List<Page> results = runInnerJoin(
            innerJoinExec(true, new long[] { 10, 20, 30 }, new long[] { 10, 20, 30 }, new long[] { 100, 200, 300 })
        );
        assertInnerJoinOutput(results, List.of(100L, 200L, 300L), List.of(10L, 20L, 30L));
    }

    public void testInnerJoinOneToOneProbeDuplicatesThrows() {
        // unique=true requires probe-side uniqueness; duplicate probe keys matching the same build row throw.
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> runInnerJoin(innerJoinExec(true, new long[] { 10, 20, 10 }, new long[] { 10, 20, 30 }, new long[] { 100, 200, 300 }))
        );
        assertThat(e.getMessage(), equalTo("input must not have duplicates when [failOnDuplicate] set to [true]"));
    }

    public void testInnerJoinDuplicateBuildKeyThrows() {
        // A duplicate key on the build ("one") side is rejected when the lookup table is built.
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> runInnerJoin(innerJoinExec(false, new long[] { 10 }, new long[] { 10, 10, 20 }, new long[] { 100, 100, 200 }))
        );
        assertThat(e.getMessage(), containsString("found a duplicate row"));
    }

    public void testInnerJoinManyToOneFanOutAllowsProbeDuplicates() throws IOException {
        // group_left: several probe rows share one build key. Unlike 1:1 there is no guard, so probe
        // duplicates are allowed and each row fans out carrying the same build value.
        List<Page> results = runInnerJoin(
            innerJoinExec(false, new long[] { 10, 10, 10, 20 }, new long[] { 10, 20 }, new long[] { 100, 200 })
        );
        assertInnerJoinOutput(results, List.of(100L, 100L, 100L, 200L), List.of(10L, 10L, 10L, 20L));
    }

    public void testInnerJoinMultiColumnKey() throws IOException {
        // The real join key spans (labels..., step): a match requires equality on BOTH key columns.
        // Probe (10, 2) matches only the first column of build (10, 1) -> miss, dropped.
        // Output after Project: [v0, k0, k1] per InnerJoin.output().
        PhysicalPlan innerJoin = innerJoinExec(
            false,
            List.of(new long[] { 10, 20, 10 }, new long[] { 1, 1, 2 }), // probe (k0, k1)
            List.of(new long[] { 10, 20, 30 }, new long[] { 1, 1, 1 }), // build (k0, k1)
            List.of(new long[] { 100, 200, 300 })                       // build v0
        );
        assertInnerJoinRows(runInnerJoin(innerJoin), List.of(List.of(100L, 10L, 1L), List.of(200L, 20L, 1L)));
    }

    public void testInnerJoinCopiesMultipleBuildColumns() throws IOException {
        // group_left(l1, l2): more than one build column is gathered onto each surviving probe row.
        // Output after Project: [v0, v1, k0] per InnerJoin.output().
        PhysicalPlan innerJoin = innerJoinExec(
            false,
            List.of(new long[] { 10, 20, 10 }),                        // probe (k0)
            List.of(new long[] { 10, 20 }),                            // build (k0)
            List.of(new long[] { 100, 200 }, new long[] { 111, 222 })  // build (v0, v1)
        );
        assertInnerJoinRows(runInnerJoin(innerJoin), List.of(List.of(100L, 111L, 10L), List.of(200L, 222L, 20L), List.of(100L, 111L, 10L)));
    }

    public void testInnerJoinEmptyBuildProducesNoRows() throws IOException {
        // Empty "one" side -> every probe row misses -> the inner join drops everything.
        List<Page> results = runInnerJoin(innerJoinExec(false, new long[] { 10, 20 }, new long[] {}, new long[] {}));
        assertInnerJoinOutput(results, List.of(), List.of());
    }

    public void testInnerJoinEmptyProbeProducesNoRows() throws IOException {
        List<Page> results = runInnerJoin(innerJoinExec(false, new long[] {}, new long[] { 10, 20 }, new long[] { 100, 200 }));
        assertInnerJoinOutput(results, List.of(), List.of());
    }

    public void testInnerJoinAllProbeRowsMissProduceNoRows() throws IOException {
        // Non-empty build, but no probe key exists on the build side -> empty inner-join result.
        List<Page> results = runInnerJoin(innerJoinExec(false, new long[] { 1, 2, 3 }, new long[] { 10, 20 }, new long[] { 100, 200 }));
        assertInnerJoinOutput(results, List.of(), List.of());
    }

    public void testInnerJoinMultivaluedBuildKeyThrows() {
        // The lookup table only supports single-valued keys; a multivalued build key is rejected.
        var blockFactory = TestBlockFactory.getNonBreakingInstance();
        ReferenceAttribute buildKey = new ReferenceAttribute(Source.EMPTY, "k0", DataType.LONG);
        ReferenceAttribute buildValue = new ReferenceAttribute(Source.EMPTY, "v0", DataType.LONG);
        LongBlock.Builder keyBuilder = blockFactory.newLongBlockBuilder(2);
        keyBuilder.beginPositionEntry().appendLong(10).appendLong(20).endPositionEntry(); // multivalued key
        keyBuilder.appendLong(30);
        LocalSourceExec build = new LocalSourceExec(
            Source.EMPTY,
            List.of(buildKey, buildValue),
            LocalSupplier.of(new Page(keyBuilder.build(), blockFactory.newLongArrayVector(new long[] { 100, 200 }, 2).asBlock()))
        );
        ReferenceAttribute probeKey = new ReferenceAttribute(Source.EMPTY, "k0", DataType.LONG);
        LocalSourceExec probe = new LocalSourceExec(
            Source.EMPTY,
            List.of(probeKey),
            LocalSupplier.of(new Page(blockFactory.newLongArrayVector(new long[] { 10 }, 1).asBlock()))
        );
        PhysicalPlan innerJoin = innerJoinPhysical(false, probe, build, List.of(probeKey), List.of(buildKey), List.of(buildValue));
        var e = expectThrows(IllegalArgumentException.class, () -> runInnerJoin(innerJoin));
        assertThat(e.getMessage(), containsString("only single valued keys are supported"));
    }

    /**
     * Builds the physical plan shape Mapper produces for InnerJoin:
     * LEFT HashJoin(lookup ordinal as added field) -> Filter(ordinal IS NOT NULL) -> [DistinctBy(ordinal) when unique] -> Project.
     */
    private PhysicalPlan innerJoinExec(boolean unique, long[] probeKeys, long[] buildKeys, long[] buildValues) {
        return innerJoinExec(unique, List.of(probeKeys), List.of(buildKeys), List.of(buildValues));
    }

    private PhysicalPlan innerJoinExec(boolean unique, List<long[]> probeKeyCols, List<long[]> buildKeyCols, List<long[]> buildValueCols) {
        var blockFactory = TestBlockFactory.getNonBreakingInstance();

        List<Attribute> buildKeyAttrs = new ArrayList<>();
        for (int c = 0; c < buildKeyCols.size(); c++) {
            buildKeyAttrs.add(new ReferenceAttribute(Source.EMPTY, "k" + c, DataType.LONG));
        }
        List<Attribute> buildValueAttrs = new ArrayList<>();
        for (int c = 0; c < buildValueCols.size(); c++) {
            buildValueAttrs.add(new ReferenceAttribute(Source.EMPTY, "v" + c, DataType.LONG));
        }
        List<Block> buildBlocks = new ArrayList<>();
        for (long[] col : buildKeyCols) {
            buildBlocks.add(blockFactory.newLongArrayVector(col, col.length).asBlock());
        }
        for (long[] col : buildValueCols) {
            buildBlocks.add(blockFactory.newLongArrayVector(col, col.length).asBlock());
        }
        List<Attribute> buildOutput = new ArrayList<>(buildKeyAttrs);
        buildOutput.addAll(buildValueAttrs);
        LocalSourceExec build = new LocalSourceExec(
            Source.EMPTY,
            buildOutput,
            LocalSupplier.of(new Page(buildBlocks.toArray(new Block[0])))
        );

        List<Attribute> probeKeyAttrs = new ArrayList<>();
        for (int c = 0; c < probeKeyCols.size(); c++) {
            probeKeyAttrs.add(new ReferenceAttribute(Source.EMPTY, "k" + c, DataType.LONG));
        }
        List<Block> probeBlocks = new ArrayList<>();
        for (long[] col : probeKeyCols) {
            probeBlocks.add(blockFactory.newLongArrayVector(col, col.length).asBlock());
        }
        LocalSourceExec probe = new LocalSourceExec(
            Source.EMPTY,
            probeKeyAttrs,
            LocalSupplier.of(new Page(probeBlocks.toArray(new Block[0])))
        );

        return innerJoinPhysical(unique, probe, build, probeKeyAttrs, buildKeyAttrs, buildValueAttrs);
    }

    private static PhysicalPlan innerJoinPhysical(
        boolean unique,
        LocalSourceExec probe,
        LocalSourceExec build,
        List<Attribute> probeKeyAttrs,
        List<Attribute> buildKeyAttrs,
        List<Attribute> buildValueAttrs
    ) {
        ReferenceAttribute ordinal = Mapper.newJoinMarker(Source.EMPTY);
        List<Attribute> addedFields = new ArrayList<>(buildValueAttrs);
        addedFields.add(ordinal);
        PhysicalPlan join = new HashJoinExec(Source.EMPTY, probe, build, probeKeyAttrs, buildKeyAttrs, addedFields);
        join = new FilterExec(Source.EMPTY, join, new IsNotNull(Source.EMPTY, ordinal));
        if (unique) {
            join = new DistinctByExec(Source.EMPTY, join, ordinal, true);
        }
        List<Attribute> leftOutputWithoutKeys = probe.output().stream().filter(attr -> probeKeyAttrs.contains(attr) == false).toList();
        List<Attribute> rightWithAppendedKeys = new ArrayList<>(build.output());
        rightWithAppendedKeys.removeAll(buildKeyAttrs);
        rightWithAppendedKeys.addAll(probeKeyAttrs);
        List<Attribute> output = new ArrayList<>(
            org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes(rightWithAppendedKeys, leftOutputWithoutKeys)
        );
        return new ProjectExec(Source.EMPTY, join, output);
    }

    private LocalExecutionPlanner.LocalExecutionPlan planInnerJoin(PhysicalPlan innerJoin) throws IOException {
        return planner().plan("test", FoldContext.small(), PlannerSettings.DEFAULTS, innerJoin, EmptyIndexedByShardId.instance());
    }

    /**
     * Classes of the intermediate operator factories for the Mapper-shaped InnerJoin physical plan.
     */
    private List<Class<?>> planInnerJoinFactories(boolean unique) throws IOException {
        PhysicalPlan innerJoin = innerJoinExec(unique, new long[] { 10, 20, 10 }, new long[] { 10, 20, 30 }, new long[] { 100, 200, 300 });
        List<Class<?>> factories = new ArrayList<>();
        for (var factory : planInnerJoin(innerJoin).driverFactories.get(0)
            .driverSupplier()
            .physicalOperation().intermediateOperatorFactories) {
            factories.add(factory.getClass());
        }
        return factories;
    }

    /**
     * Plans then runs a Mapper-shaped InnerJoin physical plan end to end.
     */
    private List<Page> runInnerJoin(PhysicalPlan innerJoin) throws IOException {
        var op = planInnerJoin(innerJoin).driverFactories.get(0).driverSupplier().physicalOperation();
        var blockFactory = TestBlockFactory.getNonBreakingInstance();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        var runner = new TestDriverRunner().builder(driverContext);
        runner.input(op.sourceOperatorFactory.get(driverContext));
        return runner.run(op.intermediateOperatorFactories.toArray(new Operator.OperatorFactory[0]));
    }

    /** Asserts single-key InnerJoin pages in InnerJoin.output() order: build value, then left key. */
    private void assertInnerJoinOutput(List<Page> results, List<Long> expectedValues, List<Long> expectedKeys) {
        List<Long> values = new ArrayList<>();
        List<Long> keys = new ArrayList<>();
        for (Page page : results) {
            LongBlock valueBlock = page.getBlock(0);
            LongBlock keyBlock = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                values.add(valueBlock.getLong(valueBlock.getFirstValueIndex(p)));
                keys.add(keyBlock.getLong(keyBlock.getFirstValueIndex(p)));
            }
        }
        assertThat(values, equalTo(expectedValues));
        assertThat(keys, equalTo(expectedKeys));
    }

    /**
     * Asserts the full output rows of a Mapper-shaped InnerJoin plan whose output is entirely {@code LONG}
     * columns, in InnerJoin.output() order (added build columns, then left join keys).
     */
    private void assertInnerJoinRows(List<Page> results, List<List<Long>> expectedRows) {
        List<List<Long>> rows = new ArrayList<>();
        for (Page page : results) {
            for (int p = 0; p < page.getPositionCount(); p++) {
                List<Long> row = new ArrayList<>();
                for (int b = 0; b < page.getBlockCount(); b++) {
                    LongBlock block = page.getBlock(b);
                    row.add(block.getLong(block.getFirstValueIndex(p)));
                }
                rows.add(row);
            }
        }
        assertThat(rows, equalTo(expectedRows));
    }

    private LocalExecutionPlanner planner() throws IOException {
        return planner(null);
    }

    private LocalExecutionPlanner planner(OperatorFactoryRegistry operatorFactoryRegistry) throws IOException {
        return planner(operatorFactoryRegistry, true);
    }

    private LocalExecutionPlanner planner(OperatorFactoryRegistry operatorFactoryRegistry, boolean federationEnabled) throws IOException {
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
                // several tests here plan an ExternalSourceExec, which the federation gate refuses unless it is enabled
                .put(Federation.FEDERATION_ENABLED.getKey(), federationEnabled)
                .build(),
            config(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            esPhysicalOperationProviders(shardContexts),
            operatorFactoryRegistry,
            null, // parallelWorkerExecutor - not needed for these tests
            0,    // esqlWorkerPoolSize - not needed for these tests
            MatcherWatchdog.noop()
        );
    }

    private Configuration config() {
        return new Configuration(
            randomInstantBetween(Instant.EPOCH, Instant.ofEpochMilli(Long.MAX_VALUE)),
            randomLocale(random()),
            "test_user",
            "test_cluster",
            pragmas,
            AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(null),
            AnalyzerSettings.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(null),
            StringUtils.EMPTY,
            false,
            Map.of(),
            System.nanoTime(),
            randomBoolean(),
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_MAX_SIZE.getDefault(null),
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(null),
            ResolvedSettings.EMPTY.withOverride(QuerySettings.TIME_ZONE, randomZone().normalized()),
            Map.of()
        );
    }

    private EsPhysicalOperationProviders esPhysicalOperationProviders(List<EsPhysicalOperationProviders.ShardContext> shardContexts) {
        return new EsPhysicalOperationProviders(
            FoldContext.small(),
            new IndexedByShardIdFromList<>(shardContexts),
            null,
            PlannerSettings.DEFAULTS,
            () -> 0L,
            QueryWarnings.EMIT
        );
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
}
