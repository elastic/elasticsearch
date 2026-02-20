/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.AsyncOperatorTestCase;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.mockito.Mockito.mock;

public class LookupFromIndexOperatorTests extends AsyncOperatorTestCase {
    private static final int LOOKUP_SIZE = 1000;
    private static final int LESS_THAN_VALUE = 40;
    private final ThreadPool threadPool = threadPool();
    private final Directory lookupIndexDirectory = newDirectory();
    private final List<Releasable> releasables = new ArrayList<>();
    private final boolean applyRightFilterAsJoinOnFilter;
    private int numberOfJoinColumns; // we only allow 1 or 2 columns due to simpleInput() implementation
    private EsqlBinaryComparison.BinaryComparisonOperation operation;

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<Object[]> operations = new ArrayList<>();
        operations.add(new Object[] { null });
        if (EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()) {
            for (EsqlBinaryComparison.BinaryComparisonOperation operation : EsqlBinaryComparison.BinaryComparisonOperation.values()) {
                // we skip NEQ because there are too many matches and the test can timeout
                if (operation != EsqlBinaryComparison.BinaryComparisonOperation.NEQ) {
                    operations.add(new Object[] { operation });
                }
            }
        }
        return operations;
    }

    public LookupFromIndexOperatorTests(EsqlBinaryComparison.BinaryComparisonOperation operation) {
        super();
        this.operation = operation;
        this.applyRightFilterAsJoinOnFilter = randomBoolean();
    }

    @Before
    public void buildLookupIndex() throws IOException {
        numberOfJoinColumns = 1 + randomInt(1); // 1 or 2 join columns
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), lookupIndexDirectory)) {
            String suffix = (operation == null) ? "" : ("_" + "right");
            for (int i = 0; i < LOOKUP_SIZE; i++) {
                List<IndexableField> fields = new ArrayList<>();
                fields.add(new LongField("match0" + suffix, i, Field.Store.NO));
                if (numberOfJoinColumns == 2) {
                    fields.add(new LongField("match1" + suffix, i + 1, Field.Store.NO));
                }
                fields.add(new KeywordFieldMapper.KeywordField("lkwd", new BytesRef("l" + i), KeywordFieldMapper.Defaults.FIELD_TYPE));
                fields.add(new IntField("lint", i, Field.Store.NO));
                writer.addDocument(fields);
            }
        }
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        if (numberOfJoinColumns == 1) {
            return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> l % LOOKUP_SIZE));
        } else if (numberOfJoinColumns == 2) {
            return new TupleLongLongBlockSourceOperator(
                blockFactory,
                LongStream.range(0, size).mapToObj(l -> Tuple.tuple(l % LOOKUP_SIZE, l % LOOKUP_SIZE + 1))
            );
        } else {
            throw new IllegalStateException("numberOfJoinColumns must be 1 or 2, got: " + numberOfJoinColumns);
        }
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        /*
         * We've configured there to be just a single result per input so the total
         * row count is the same. But lookup cuts into pages of length 256 so the
         * result is going to have more pages usually.
         */
        int inputCount = input.stream().mapToInt(Page::getPositionCount).sum();
        int outputCount = results.stream().mapToInt(Page::getPositionCount).sum();

        if (operation == null || operation.equals(EsqlBinaryComparison.BinaryComparisonOperation.EQ)) {
            assertThat(outputCount, equalTo(input.stream().mapToInt(Page::getPositionCount).sum()));
        } else {
            // For lookup join on non-equality, output count should be >= input count
            assertThat("Output count should be >= input count for left outer join", outputCount, greaterThanOrEqualTo(inputCount));
        }

        for (Page r : results) {
            assertThat(r.getBlockCount(), equalTo(numberOfJoinColumns + 2));
            LongVector match = r.<LongBlock>getBlock(0).asVector();
            BytesRefBlock lkwdBlock = r.getBlock(numberOfJoinColumns);
            IntBlock lintBlock = r.getBlock(numberOfJoinColumns + 1);
            for (int p = 0; p < r.getPositionCount(); p++) {
                long m = match.getLong(p);
                if (lkwdBlock.isNull(p) || lintBlock.isNull(p)) {
                    // If the joined values are null, this means no match was found (or it was filtered out)
                    // verify that both the columns are null
                    assertTrue("at " + p, lkwdBlock.isNull(p));
                    assertTrue("at " + p, lintBlock.isNull(p));
                } else {
                    String joinedLkwd = lkwdBlock.getBytesRef(lkwdBlock.getFirstValueIndex(p), new BytesRef()).utf8ToString();
                    // there was a match, verify that the join on condition was satisfied
                    int joinedLint = lintBlock.getInt(lintBlock.getFirstValueIndex(p));
                    boolean conditionSatisfied = compare(m, joinedLint, operation);
                    assertTrue("Join condition not satisfied: " + m + " " + operation + " " + joinedLint, conditionSatisfied);
                    // Verify that the joined lkwd matches the lint value
                    assertThat(joinedLkwd, equalTo("l" + joinedLint));
                }
            }
        }
    }

    /**
     * Compares two values using the specified binary comparison operation.
     *
     * @param left the left operand
     * @param right the right operand
     * @param op the binary comparison operation (null means equality join)
     * @return true if the comparison condition is satisfied, false otherwise
     */
    private boolean compare(long left, long right, EsqlBinaryComparison.BinaryComparisonOperation op) {
        if (op == null) {
            // field based join is the same as equals comparison
            op = EsqlBinaryComparison.BinaryComparisonOperation.EQ;
        }
        // Use the operator's fold method for comparison
        Literal leftLiteral = new Literal(Source.EMPTY, left, DataType.LONG);
        Literal rightLiteral = new Literal(Source.EMPTY, right, DataType.LONG);
        EsqlBinaryComparison operatorInstance = op.buildNewInstance(Source.EMPTY, leftLiteral, rightLiteral);
        Object result = operatorInstance.fold(FoldContext.small());
        if (result instanceof Boolean) {
            return (Boolean) result;
        }
        throw new IllegalArgumentException("Operator fold did not return a boolean");
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        String sessionId = "test";
        CancellableTask parentTask = new CancellableTask(0, "test", "test", "test", TaskId.EMPTY_TASK_ID, Map.of());
        int maxOutstandingRequests = 1;
        DataType inputDataType = DataType.LONG;
        String lookupIndex = "idx";
        List<NamedExpression> loadFields = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "lkwd",
                new EsField("lkwd", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            ),
            new FieldAttribute(
                Source.EMPTY,
                "lint",
                new EsField("lint", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            )
        );

        List<MatchConfig> matchFields = new ArrayList<>();
        String suffix = (operation == null) ? "" : ("_left");
        for (int i = 0; i < numberOfJoinColumns; i++) {
            String matchField = "match" + i + suffix;
            matchFields.add(new MatchConfig(matchField, i, inputDataType));
        }
        Expression joinOnExpression = null;
        FragmentExec rightPlanWithOptionalPreJoinFilter = buildLessThanFilter(LESS_THAN_VALUE);
        if (operation != null) {
            List<Expression> conditions = new ArrayList<>();
            for (int i = 0; i < numberOfJoinColumns; i++) {
                String matchFieldLeft = "match" + i + "_left";
                String matchFieldRight = "match" + i + "_right";
                FieldAttribute left = new FieldAttribute(
                    Source.EMPTY,
                    matchFieldLeft,
                    new EsField(matchFieldLeft, inputDataType, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
                );
                FieldAttribute right = new FieldAttribute(
                    Source.EMPTY,
                    matchFieldRight,
                    new EsField(matchFieldRight.replace("left", "right"), inputDataType, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
                );
                conditions.add(operation.buildNewInstance(Source.EMPTY, left, right));
            }
            if (applyRightFilterAsJoinOnFilter) {
                if (rightPlanWithOptionalPreJoinFilter instanceof FragmentExec fragmentExec
                    && fragmentExec.fragment() instanceof Filter filterPlan) {
                    conditions.add(filterPlan.condition());
                    rightPlanWithOptionalPreJoinFilter = null;
                }
            }
            joinOnExpression = Predicates.combineAnd(conditions);
        }

        return new LookupFromIndexOperator.Factory(
            matchFields,
            sessionId,
            parentTask,
            maxOutstandingRequests,
            this::lookupService,
            lookupIndex,
            lookupIndex,
            loadFields,
            Source.EMPTY,
            rightPlanWithOptionalPreJoinFilter,
            joinOnExpression
        );
    }

    private FragmentExec buildLessThanFilter(int value) {
        FieldAttribute filterAttribute = new FieldAttribute(
            Source.EMPTY,
            "lint",
            new EsField("lint", DataType.INTEGER, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Expression lessThan = new LessThan(Source.EMPTY, filterAttribute, new Literal(Source.EMPTY, value, DataType.INTEGER));
        EsRelation esRelation = new EsRelation(Source.EMPTY, "test", IndexMode.LOOKUP, Map.of(), Map.of(), Map.of(), List.of());
        Filter filter = new Filter(Source.EMPTY, esRelation, lessThan);
        return new FragmentExec(filter);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return expectedToStringOfSimple();
    }

    @Override
    public void testSimpleDescription() {
        Operator.OperatorFactory factory = simple();
        String description = factory.describe();
        assertThat(description, expectedDescriptionOfSimple());
        try (Operator op = factory.get(driverContext())) {
            // we use a special pattern here because the description can contain new lines for the right_pre_join_plan
            String pattern = "^\\w*\\[[\\s\\S]*\\]$";
            assertThat(description, matchesPattern(pattern));
        }
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        StringBuilder sb = new StringBuilder();
        String suffix = (operation == null) ? "" : ("_left");
        sb.append("LookupOperator\\[index=idx load_fields=\\[lkwd\\{f}#\\d+, lint\\{f}#\\d+] ");
        for (int i = 0; i < numberOfJoinColumns; i++) {
            // match_field=match<i>_left (index first, then suffix)
            sb.append("input_type=LONG match_field=match").append(i).append(suffix).append(" inputChannel=").append(i).append(" ");
        }

        if (applyRightFilterAsJoinOnFilter && operation != null) {
            // When applyRightFilterAsJoinOnFilter is true, right_pre_join_plan should be null
            sb.append("right_pre_join_plan=null");
        } else {
            // Accept either the legacy physical plan rendering (FilterExec/EsQueryExec) or the new FragmentExec rendering
            sb.append("right_pre_join_plan=(?:");
            // Legacy pattern
            sb.append("FilterExec\\[lint\\{f}#\\d+ < ")
                .append(LESS_THAN_VALUE)
                .append(
                    "\\[INTEGER]]\\n\\\\_EsQueryExec\\[test], indexMode\\[lookup],\\s*(?:query\\[\\]|\\[\\])?,?\\s*"
                        + "limit\\[\\],?\\s*sort\\[(?:\\[\\])?\\]\\s*estimatedRowSize\\[null\\]\\s*queryBuilderAndTags \\[(?:\\[\\]\\])\\]"
                );
            sb.append("|");
            // New FragmentExec pattern - match the actual output format
            sb.append("FragmentExec\\[filter=null, estimatedRowSize=\\d+, reducer=\\[\\], fragment=\\[<>\\n")
                .append("Filter\\[lint\\{f}#\\d+ < ")
                .append(LESS_THAN_VALUE)
                .append("\\[INTEGER]]\\n")
                .append("\\\\_EsRelation\\[test]\\[LOOKUP]\\[\\]<>\\]\\]");
            sb.append(")");
        }

        // Accept join_on_expression=null or a valid join predicate
        if (applyRightFilterAsJoinOnFilter && operation != null) {
            // When applyRightFilterAsJoinOnFilter is true and operation is not null, the join expression includes the filter condition
            sb.append(
                " join_on_expression=(match\\d+left [=!<>]+ match\\d+right( "
                    + "AND match\\d+left [=!<>]+ match\\d+right)* AND lint\\{f}#\\d+ < "
            ).append(LESS_THAN_VALUE).append("\\[INTEGER]|)\\]");
        } else {
            // Standard pattern for other cases
            sb.append(" join_on_expression=(null|match\\d+left [=!<>]+ match\\d+right( AND match\\d+left [=!<>]+ match\\d+right)*|)\\]");
        }
        return matchesPattern(sb.toString());
    }

    private LookupFromIndexService lookupService(DriverContext mainContext) {
        boolean beCranky = mainContext.bigArrays().breakerService() instanceof CrankyCircuitBreakerService;
        DiscoveryNode localNode = DiscoveryNodeUtils.create("node", "node");
        var builtInClusterSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        builtInClusterSettings.addAll(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS);
        ClusterService clusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            localNode,
            Settings.builder()
                // Reserve 0 bytes in the sub-driver so we are more likely to hit the cranky breaker in it.
                .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING, ByteSizeValue.ofKb(0))
                .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING, ByteSizeValue.ofKb(0))
                .build(),
            new ClusterSettings(Settings.EMPTY, builtInClusterSettings)
        );
        IndicesService indicesService = mock(IndicesService.class);
        IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();
        releasables.add(clusterService::stop);
        final var projectId = randomProjectIdOrDefault();
        ClusterServiceUtils.setState(clusterService, ClusterStateCreationUtils.state(projectId, "idx", 1, 1));
        if (beCranky) {
            logger.info("building a cranky lookup");
        }
        DriverContext ctx = beCranky ? crankyDriverContext() : driverContext();
        BigArrays bigArrays = ctx.bigArrays();
        BlockFactory blockFactory = ctx.blockFactory();
        return new LookupFromIndexService(
            clusterService,
            indicesService,
            lookupShardContextFactory(),
            transportService(clusterService),
            indexNameExpressionResolver,
            bigArrays,
            blockFactory,
            TestProjectResolvers.singleProject(projectId)
        );
    }

    private ThreadPool threadPool() {
        return new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME,
                1,
                1024,
                "esql",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
    }

    private TransportService transportService(ClusterService clusterService) {
        MockTransport mockTransport = new MockTransport();
        releasables.add(mockTransport);
        TransportService transportService = mockTransport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            clusterService.getClusterSettings(),
            Set.of()
        );
        releasables.add(transportService);
        transportService.start();
        transportService.acceptIncomingRequests();
        return transportService;
    }

    private AbstractLookupService.LookupShardContextFactory lookupShardContextFactory() {
        return shardId -> {
            MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {};
            String suffix = (operation == null) ? "" : ("_right");
            StringBuilder props = new StringBuilder();
            props.append(String.format(Locale.ROOT, "\"match0%s\": { \"type\": \"long\" }", suffix));
            if (numberOfJoinColumns == 2) {
                props.append(String.format(Locale.ROOT, ", \"match1%s\": { \"type\": \"long\" }", suffix));
            }
            props.append(", \"lkwd\": { \"type\": \"keyword\" }, \"lint\": { \"type\": \"integer\" }");
            String mapping = String.format(Locale.ROOT, "{\n  \"doc\": { \"properties\": { %s } }\n}", props.toString());
            MapperService mapperService = mapperHelper.createMapperService(mapping);
            DirectoryReader reader = DirectoryReader.open(lookupIndexDirectory);
            SearchExecutionContext executionCtx = mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader));
            var ctx = new EsPhysicalOperationProviders.DefaultShardContext(0, new NoOpReleasable(), executionCtx, AliasFilter.EMPTY);
            return new AbstractLookupService.LookupShardContext(ctx, executionCtx, () -> {
                try {
                    IOUtils.close(reader, mapperService);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        };
    }

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(lookupIndexDirectory);
    }

    @After
    public void release() {
        Releasables.close(Releasables.wrap(releasables.reversed()), () -> terminate(threadPool));
    }

    @Override
    protected MapMatcher extendStatusMatcher(MapMatcher mapMatcher, List<Page> input, List<Page> output) {
        var totalInputRows = input.stream().mapToInt(Page::getPositionCount).sum();
        var totalOutputRows = output.stream().mapToInt(Page::getPositionCount).sum();

        return mapMatcher.entry("total_rows", totalInputRows).entry("pages_emitted", output.size()).entry("rows_emitted", totalOutputRows);
    }

    @Override
    public void testSimpleCircuitBreaking() throws Exception {
        // only test field based join and EQ to prevents timeouts in Ci
        if (operation == null || operation.equals(EsqlBinaryComparison.BinaryComparisonOperation.EQ)) {
            super.testSimpleCircuitBreaking();
        }
    }
}
