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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.test.TupleLongLongBlockSourceOperator;
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
import org.elasticsearch.test.junit.annotations.TestLogging;
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
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
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

@TestLogging(
    value = "org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeClient:DEBUG,"
        + "org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeServer:DEBUG,"
        + "org.elasticsearch.compute.operator.exchange.BatchSortedExchangeSource:DEBUG,"
        + "org.elasticsearch.xpack.esql.enrich.StreamingLookupFromIndexOperator:DEBUG",
    reason = "debugging streaming lookup performance"
)
public class StreamingLookupFromIndexOperatorTests extends OperatorTestCase {
    private static final int LOOKUP_SIZE = 1000;
    private static final int LESS_THAN_VALUE = 40;
    private final ThreadPool threadPool = threadPool();
    private final Directory lookupIndexDirectory = newDirectory();
    private final List<Releasable> releasables = new ArrayList<>();
    private final boolean applyRightFilterAsJoinOnFilter;
    private int numberOfJoinColumns;
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

        // Add 100 instances of GTE (temporary to test failing scenario)
        // for (int i = 0; i < 100; i++) {
        // operations.add(new Object[] { EsqlBinaryComparison.BinaryComparisonOperation.GTE });
        // }
        return operations;
    }

    public StreamingLookupFromIndexOperatorTests(EsqlBinaryComparison.BinaryComparisonOperation operation) {
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
        int inputCount = input.stream().mapToInt(Page::getPositionCount).sum();
        int outputCount = results.stream().mapToInt(Page::getPositionCount).sum();

        if (operation == null || operation.equals(EsqlBinaryComparison.BinaryComparisonOperation.EQ)) {
            assertThat(outputCount, equalTo(input.stream().mapToInt(Page::getPositionCount).sum()));
        } else {
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
                    assertTrue("at " + p, lkwdBlock.isNull(p));
                    assertTrue("at " + p, lintBlock.isNull(p));
                } else {
                    String joinedLkwd = lkwdBlock.getBytesRef(lkwdBlock.getFirstValueIndex(p), new BytesRef()).utf8ToString();
                    int joinedLint = lintBlock.getInt(lintBlock.getFirstValueIndex(p));
                    boolean conditionSatisfied = compare(m, joinedLint, operation);
                    assertTrue("Join condition not satisfied: " + m + " " + operation + " " + joinedLint, conditionSatisfied);
                    assertThat(joinedLkwd, equalTo("l" + joinedLint));
                }
            }
        }
    }

    private boolean compare(long left, long right, EsqlBinaryComparison.BinaryComparisonOperation op) {
        if (op == null) {
            op = EsqlBinaryComparison.BinaryComparisonOperation.EQ;
        }
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

        // Capture variables for lambda
        final List<MatchConfig> finalMatchFields = matchFields;
        final FragmentExec finalRightPlan = rightPlanWithOptionalPreJoinFilter;
        final Expression finalJoinOnExpression = joinOnExpression;
        final int exchangeBufferSize = QueryPragmas.EXCHANGE_BUFFER_SIZE.getDefault(Settings.EMPTY);

        // Create a factory that produces StreamingLookupFromIndexOperator
        return new Operator.OperatorFactory() {
            @Override
            public Operator get(DriverContext driverContext) {
                LookupFromIndexService service = lookupService(driverContext);
                return new StreamingLookupFromIndexOperator(
                    finalMatchFields,
                    sessionId,
                    parentTask,
                    service,
                    lookupIndex,
                    lookupIndex,
                    loadFields,
                    Source.EMPTY,
                    finalRightPlan,
                    finalJoinOnExpression,
                    exchangeBufferSize,
                    false // profile
                );
            }

            @Override
            public String describe() {
                return "StreamingLookupOperator[index=" + lookupIndex + "]";
            }
        };
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
        return matchesPattern("StreamingLookupOperator\\[index=idx\\]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return matchesPattern("StreamingLookupOperator\\[index=idx\\]");
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
            // Building a cranky lookup for randomized failure testing
        }
        // For cranky tests, use a separate cranky context (original behavior) to properly handle random failures
        // For circuit breaking tests and normal tests, use mainContext to share the limited breaker
        BigArrays bigArrays;
        BlockFactory blockFactory;
        if (beCranky) {
            DriverContext ctx = crankyDriverContext();
            bigArrays = ctx.bigArrays();
            blockFactory = ctx.blockFactory();
        } else {
            bigArrays = mainContext.bigArrays();
            blockFactory = mainContext.blockFactory();
        }
        TransportService transportService = transportService(clusterService);
        ExchangeService exchangeService = new ExchangeService(Settings.EMPTY, threadPool, ThreadPool.Names.SEARCH, blockFactory);
        exchangeService.registerTransportHandler(transportService);
        releasables.add(exchangeService);
        return new LookupFromIndexService(
            clusterService,
            indicesService,
            lookupShardContextFactory(),
            transportService,
            indexNameExpressionResolver,
            bigArrays,
            blockFactory,
            TestProjectResolvers.singleProject(projectId),
            exchangeService
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
            MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {
            };
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
            Releasable releasable = () -> {
                try {
                    IOUtils.close(reader, mapperService);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
            return new AbstractLookupService.LookupShardContext(ctx, executionCtx, releasable);
        };
    }

    @After
    public void release() throws IOException {
        Releasables.close(Releasables.wrap(releasables.reversed()), () -> terminate(threadPool));
        IOUtils.close(lookupIndexDirectory);
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        long inputRows = input.stream().mapToLong(Page::getPositionCount).sum();
        // Check page counts
        assertThat(map.get("pages_received"), equalTo(input.size()));
        assertThat(map.get("pages_emitted"), equalTo(input.size()));
        // Check row counts (use Number to handle Integer/Long from JSON)
        assertThat(((Number) map.get("rows_received")).longValue(), equalTo(inputRows));
        assertThat(((Number) map.get("rows_emitted")).longValue(), greaterThanOrEqualTo(0L));
        // Check timing fields
        assertThat(((Number) map.get("planning_nanos")).longValue(), greaterThanOrEqualTo(0L));
        assertThat(((Number) map.get("process_nanos")).longValue(), greaterThanOrEqualTo(0L));
    }

    @Override
    public void testSimpleCircuitBreaking() {
        // Only test field-based join and EQ to prevent timeouts in CI
        // (Same as LookupFromIndexOperatorTests - non-EQ operations are slower and can timeout)
        if (operation == null || operation.equals(EsqlBinaryComparison.BinaryComparisonOperation.EQ)) {
            // super.testSimpleCircuitBreaking();
            // The streaming operator has non-deterministic memory usage due to async page streaming,
            // so the binary search in BreakerTestUtil doesn't work reliably. Instead, we test:
            // 1. Very low memory should throw CircuitBreakingException
            // 2. Plenty of memory should succeed
            testCircuitBreakingWithLowMemory();
            testCircuitBreakingWithEnoughMemory();
        }
    }

    /**
     * Test that the operator throws CircuitBreakingException with very low memory.
     */
    private void testCircuitBreakingWithLowMemory() {
        DriverContext inputFactoryContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(inputFactoryContext.blockFactory(), between(1, 10)));
        Operator.OperatorFactory factory = simple();

        // Use very low memory - should definitely break
        ByteSizeValue lowMemory = ByteSizeValue.ofBytes(100);

        try {
            Exception e = expectThrows(CircuitBreakingException.class, () -> runWithLimit(factory, input, lowMemory));
            assertThat(e.getMessage(), equalTo(MockBigArrays.ERROR_MESSAGE));
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(input.iterator(), p -> p::releaseBlocks)));
        }
        assertThat(inputFactoryContext.breaker().getUsed(), equalTo(0L));
    }

    /**
     * Test that the operator succeeds with plenty of memory.
     */
    private void testCircuitBreakingWithEnoughMemory() {
        DriverContext inputFactoryContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(inputFactoryContext.blockFactory(), between(1, 10)));
        Operator.OperatorFactory factory = simple();

        // Use plenty of memory - should succeed
        ByteSizeValue plentyMemory = enoughMemoryForSimple();

        try {
            // Should not throw - just verify it completes without exception
            runWithLimit(factory, input, plentyMemory);
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(input.iterator(), p -> p::releaseBlocks)));
        }
        assertThat(inputFactoryContext.breaker().getUsed(), equalTo(0L));
    }

    /**
     * Run the operator with a specific memory limit.
     */
    private void runWithLimit(Operator.OperatorFactory factory, List<Page> input, ByteSizeValue limit) {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, limit).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        MockBlockFactory blockFactory = new MockBlockFactory(breaker, bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory, LocalCircuitBreaker.SizeSettings.DEFAULT_SETTINGS);
        List<Page> localInput = CannedSourceOperator.deepCopyOf(blockFactory, input);
        boolean driverStarted = false;
        try {
            var operator = factory.get(driverContext);
            driverStarted = true;
            drive(operator, localInput.iterator(), driverContext);
        } finally {
            if (driverStarted == false) {
                Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(localInput.iterator(), p -> p::releaseBlocks)));
            }
            blockFactory.ensureAllBlocksAreReleased();
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }

    @Override
    public void testSimpleDescription() {
        Operator.OperatorFactory factory = simple();
        String description = factory.describe();
        assertThat(description, expectedDescriptionOfSimple());
        try (Operator op = factory.get(driverContext())) {
            assertTrue("Expected StreamingLookupFromIndexOperator instance", op instanceof StreamingLookupFromIndexOperator);
        }
    }

    /**
     * Verify the MAX_CONCURRENT_BATCHES constant is accessible and has a reasonable value.
     */
    public void testMaxConcurrentBatchesConstant() {
        assertThat(
            "MAX_CONCURRENT_BATCHES should be positive",
            StreamingLookupFromIndexOperator.MAX_CONCURRENT_BATCHES,
            greaterThanOrEqualTo(1)
        );
    }

    /**
     * Test that the operator accepts exactly MAX_CONCURRENT_BATCHES before blocking.
     * This verifies that pipelining allows multiple batches in flight simultaneously.
     */
    public void testConcurrentBatchPipelining() {
        // Only test with null operation (field-based join) or EQ to avoid timeout
        if (operation != null && operation.equals(EsqlBinaryComparison.BinaryComparisonOperation.EQ) == false) {
            return;
        }

        int maxBatches = StreamingLookupFromIndexOperator.MAX_CONCURRENT_BATCHES;
        int numPages = maxBatches + 2; // More pages than max concurrent

        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();

        // Create individual single-row pages so each page = one batch
        List<Page> inputPages = new ArrayList<>();
        for (int i = 0; i < numPages; i++) {
            long value = i % LOOKUP_SIZE;
            LongBlock block1 = blockFactory.newConstantLongBlockWith(value, 1);
            if (numberOfJoinColumns == 1) {
                inputPages.add(new Page(block1));
            } else {
                LongBlock block2 = blockFactory.newConstantLongBlockWith(value + 1, 1);
                inputPages.add(new Page(block1, block2));
            }
        }

        // Verify we created the expected number of single-row pages
        assertThat("Should create " + numPages + " pages", inputPages.size(), equalTo(numPages));
        for (Page p : inputPages) {
            assertThat("Each page should have 1 row", p.getPositionCount(), equalTo(1));
        }

        Operator.OperatorFactory factory = simple();
        Operator operator = factory.get(driverContext);
        List<Page> results = new ArrayList<>();

        try {
            // Wait for client to be ready by polling isBlocked()
            int maxWaitIterations = 1000;
            int waitIterations = 0;
            while (operator.isBlocked().listener().isDone() == false && waitIterations < maxWaitIterations) {
                Thread.sleep(10);
                waitIterations++;
            }
            assertTrue("Client should be ready within timeout", waitIterations < maxWaitIterations);

            // Send pages one at a time and track how many are accepted before blocking
            int pagesAccepted = 0;
            int pageIndex = 0;

            // Send up to MAX_CONCURRENT_BATCHES pages - all should be accepted
            for (int i = 0; i < maxBatches && pageIndex < inputPages.size(); i++) {
                assertTrue("Should accept batch " + i, operator.needsInput());
                operator.addInput(inputPages.get(pageIndex++));
                pagesAccepted++;
            }

            // Verify we accepted exactly MAX_CONCURRENT_BATCHES
            assertThat("Should accept exactly MAX_CONCURRENT_BATCHES before blocking", pagesAccepted, equalTo(maxBatches));

            // After MAX_CONCURRENT_BATCHES, needsInput should return false
            assertFalse("Should NOT accept more than MAX_CONCURRENT_BATCHES in flight", operator.needsInput());

            // Now drive the operator to completion, feeding remaining pages as space becomes available
            while (operator.isFinished() == false) {
                // Wait if blocked
                while (operator.isBlocked().listener().isDone() == false) {
                    Thread.sleep(1);
                }

                // Get output first to free up batch slots
                Page output = operator.getOutput();
                if (output != null) {
                    results.add(output);
                }

                // Try to add remaining pages when operator needs input
                while (pageIndex < inputPages.size() && operator.needsInput()) {
                    operator.addInput(inputPages.get(pageIndex++));
                }

                // If all pages sent and operator needs input, finish
                if (pageIndex >= inputPages.size() && operator.needsInput()) {
                    operator.finish();
                }
            }

            // Verify we got output for all input rows
            int totalInputRows = inputPages.size(); // 1 row per page
            int totalOutputRows = results.stream().mapToInt(Page::getPositionCount).sum();
            assertThat("All input rows should produce output", totalOutputRows, equalTo(totalInputRows));

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Test interrupted");
        } finally {
            // Cleanup
            operator.close();
            for (Page page : results) {
                page.releaseBlocks();
            }
        }
    }
}
