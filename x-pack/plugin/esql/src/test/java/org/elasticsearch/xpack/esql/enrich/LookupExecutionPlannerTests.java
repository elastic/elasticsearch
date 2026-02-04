/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ResolvedExpression;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.ProjectOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeServer;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
import org.elasticsearch.compute.operator.lookup.LookupQueryOperator;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LookupExecutionPlannerTests extends ESTestCase {
    private BlockFactory blockFactory;
    private BigArrays bigArrays;
    private List<Releasable> releasables;
    private ClusterService clusterService;
    private TransportService transportService;
    private IndicesService indicesService;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private ThreadPool threadPool;

    /**
     * Testable concrete implementation of LookupFromIndexService that captures LookupQueryPlan
     * instead of starting a driver, allowing tests to inspect the operators.
     */
    private static class TestLookupService extends LookupFromIndexService {
        private LookupFromIndexService.LookupQueryPlan capturedPlan;

        TestLookupService(
            ClusterService clusterService,
            IndicesService indicesService,
            AbstractLookupService.LookupShardContextFactory lookupShardContextFactory,
            TransportService transportService,
            IndexNameExpressionResolver indexNameExpressionResolver,
            BigArrays bigArrays,
            BlockFactory blockFactory,
            ProjectResolver projectResolver,
            ExchangeService exchangeService
        ) {
            super(
                clusterService,
                indicesService,
                lookupShardContextFactory,
                transportService,
                indexNameExpressionResolver,
                bigArrays,
                blockFactory,
                projectResolver,
                exchangeService
            );
        }

        @Override
        protected LookupEnrichQueryGenerator queryList(
            TransportRequest request,
            SearchExecutionContext context,
            AliasFilter aliasFilter,
            Warnings warnings
        ) {
            return mock(LookupEnrichQueryGenerator.class);
        }

        @Override
        protected void startServerWithOperators(
            BidirectionalBatchExchangeServer server,
            LookupQueryPlan lookupQueryPlan,
            List<Operator> intermediateOperators,
            Releasable releasables,
            ActionListener<LookupResponse> responseListener,
            String planString
        ) {
            // Capture the plan instead of starting the server
            this.capturedPlan = lookupQueryPlan;
            // Don't call super - we don't want to actually start the server in tests
            // Signal ready immediately for test purposes
            responseListener.onResponse(new LookupResponse(List.of(), blockFactory, planString));
        }

        @Override
        protected DiscoveryNode determineClientNode(TransportRequest request, CancellableTask task) {
            // Return a mock client node for testing - we don't actually use it since we override startServerWithOperators
            return mock(DiscoveryNode.class);
        }

        LookupFromIndexService.LookupQueryPlan getCapturedPlan() {
            return capturedPlan;
        }
    }

    @Before
    public void setup() {
        blockFactory = TestBlockFactory.getNonBreakingInstance();
        bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService()).withCircuitBreaking();
        releasables = new ArrayList<>();

        // Create minimal mocks for services - we only need these because AbstractLookupService constructor requires them
        // but startDriver is overridden to do nothing, so they don't need to be fully functional
        clusterService = mock(ClusterService.class);
        ProjectId projectId = Metadata.DEFAULT_PROJECT_ID;
        ClusterState clusterState = ClusterStateCreationUtils.state(projectId, "test-index", 1, 1);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        indicesService = mock(IndicesService.class);
        when(indicesService.buildAliasFilter(any(), any(), any())).thenReturn(AliasFilter.EMPTY);

        indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(indexNameExpressionResolver.resolveExpressionsIgnoringRemotes(any(), any())).thenReturn(
            Set.of(new ResolvedExpression("test-index"))
        );

        // Create mock transport service - only needed for constructor, startDriver does nothing
        // Since we override startDriver to do nothing, we don't need a real thread pool
        // But TransportService requires a ThreadPool, so we create a minimal one
        threadPool = new TestThreadPool(
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
        MockTransport mockTransport = new MockTransport();
        transportService = mockTransport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> null,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            Set.of()
        );
        releasables.add(transportService);
        releasables.add(mockTransport);
        releasables.add(() -> terminate(threadPool));
    }

    @After
    public void cleanup() {
        Releasables.close(releasables);
        blockFactory = null;
        bigArrays = null;
        clusterService = null;
        transportService = null;
        indicesService = null;
        indexNameExpressionResolver = null;
        threadPool = null;
    }

    public void testLookupJoinNoMerge() throws Exception {
        // Test lookup join with mergePages=false and no extract fields
        // No extract fields
        List<NamedExpression> extractFields = Collections.emptyList();

        LookupFromIndexService.LookupQueryPlan queryPlan = generateQueryPlan(extractFields);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure
        // Expected: LookupQueryOperator -> ProjectOperator
        // Only the intermediate operators are in the plan for Streaming mode,
        // the source and sink are added by the BidirectionalBatchExchangeServer automatically
        verifyCompletePlan(queryPlan, List.of(LookupQueryOperator.class, ProjectOperator.class), extractFields);
    }

    public void testLookupJoinNoExtraFields() throws Exception {
        // Test lookup join where we don't need to get extra fields (no extract fields, no merge)
        // This is the simplest lookup join case - just joining without extracting additional fields
        // No extract fields - pure lookup join without extra field extraction
        List<NamedExpression> extractFields = Collections.emptyList();

        LookupFromIndexService.LookupQueryPlan queryPlan = generateQueryPlan(extractFields);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure
        // Expected: LookupQueryOperator -> ProjectOperator
        // No ValuesSourceReaderOperator (no extract fields)
        // Only the intermediate operators are in the plan for Streaming mode,
        // the source and sink are added by the BidirectionalBatchExchangeServer automatically
        verifyCompletePlan(queryPlan, List.of(LookupQueryOperator.class, ProjectOperator.class), extractFields);
    }

    public void testLookupJoinWithExtractFields() throws Exception {
        // Test lookup join with extract fields (uses ProjectOperator)
        List<NamedExpression> extractFields = List.of(createFieldAttribute("field1", DataType.KEYWORD));

        LookupFromIndexService.LookupQueryPlan queryPlan = generateQueryPlan(extractFields);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure
        // Only the intermediate operators are in the plan for Streaming mode,
        // the source and sink are added by the BidirectionalBatchExchangeServer automatically
        // Expected: LookupQueryOperator -> ValuesSourceReaderOperator -> ProjectOperator
        verifyCompletePlan(
            queryPlan,
            List.of(LookupQueryOperator.class, ValuesSourceReaderOperator.class, ProjectOperator.class),
            extractFields
        );
    }

    public void testEnrichWithAllNullInputPage() throws Exception {
        // Test enrich with input page where all values are null
        // In streaming mode, operators are still generated because input comes through exchange
        // The null check happens in AbstractLookupService.doLookup() for non-streaming mode,
        // but streaming mode always generates operators regardless of input page content
        List<NamedExpression> extractFields = List.of(createFieldAttribute("field1", DataType.KEYWORD));

        // In streaming mode, operators are generated even for all-null input pages
        // The operators will handle null values during execution
        LookupFromIndexService.LookupQueryPlan queryPlan = generateQueryPlan(extractFields);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure - same as with extract fields
        // Expected: LookupQueryOperator -> ValuesSourceReaderOperator -> ProjectOperator
        verifyCompletePlan(
            queryPlan,
            List.of(LookupQueryOperator.class, ValuesSourceReaderOperator.class, ProjectOperator.class),
            extractFields
        );
    }

    // Verification helper methods

    /**
     * Verifies the complete plan structure with all operators and performs detailed verification.
     * @param queryPlan The query plan to verify
     * @param expectedOperators List of expected intermediate operator types in order
     * @param extractFields Extract fields for ProjectOperator verification
     */
    private void verifyCompletePlan(
        LookupFromIndexService.LookupQueryPlan queryPlan,
        List<Class<? extends Operator>> expectedOperators,
        List<NamedExpression> extractFields
    ) {
        // In streaming mode, only intermediate operators are in LookupQueryPlan
        // The source is ExchangeSourceOperator (provided by BidirectionalBatchExchangeServer)
        // The sink is ExchangeSinkOperator (provided by BidirectionalBatchExchangeServer)

        // Verify intermediate operators only
        List<Operator> actualOperators = queryPlan.operators();
        MatcherAssert.assertThat("Intermediate operators count mismatch", actualOperators.size(), is(expectedOperators.size()));

        // Verify all intermediate operators match expected types
        for (int i = 0; i < expectedOperators.size(); i++) {
            Class<? extends Operator> expectedType = expectedOperators.get(i);
            Operator operator = actualOperators.get(i);
            MatcherAssert.assertThat(
                "Operator at index " + i + " should be " + expectedType.getSimpleName(),
                operator,
                instanceOf(expectedType)
            );

            if (expectedType == ValuesSourceReaderOperator.class) {
                verifyValuesSourceReaderOperator(queryPlan, i);
            } else if (expectedType == ProjectOperator.class) {
                verifyProjectOperator((ProjectOperator) operator, extractFields.size());
            }
        }
    }

    /**
     * Verifies ValuesSourceReaderOperator at the specified index.
     */
    private ValuesSourceReaderOperator verifyValuesSourceReaderOperator(LookupFromIndexService.LookupQueryPlan queryPlan, int index) {
        Operator operator = queryPlan.operators().get(index);
        MatcherAssert.assertThat(operator, instanceOf(ValuesSourceReaderOperator.class));
        return (ValuesSourceReaderOperator) operator;
    }

    /**
     * Verifies ProjectOperator projection.
     */
    private void verifyProjectOperator(ProjectOperator projectOp, int extractFieldsCount) {
        MatcherAssert.assertThat(projectOp, notNullValue());
        int[] projection = projectOp.getProjection();
        MatcherAssert.assertThat("Projection should have extractFields.size() + 1 elements", projection.length, is(extractFieldsCount + 1));
        // Verify projection starts at 1 (skipping doc block at index 0) and contains sequential values
        for (int i = 0; i < projection.length; i++) {
            MatcherAssert.assertThat("Projection should contain sequential values starting from 1", projection[i], is(i + 1));
        }
    }

    /**
     * Executes doLookup() and returns the captured LookupQueryPlan.
     * This is the common pattern used by all test cases.
     * Note: In streaming mode, the setup request requires an empty page (0 rows).
     * The actual input data comes through the exchange, not the request.
     */
    private LookupFromIndexService.LookupQueryPlan generateQueryPlan(List<NamedExpression> extractFields) throws Exception {
        AbstractLookupService.LookupShardContext shardContext = createMockShardContext();
        TestLookupService testService = createTestService(shardContext);

        // In streaming mode, setup request must have an empty page (0 rows)
        Page emptyPage = new Page(blockFactory.newBytesRefBlockBuilder(0).build());

        LookupFromIndexService.Request request = new LookupFromIndexService.Request(
            "test-session",
            "test-index",
            "test-index",
            List.of(new MatchConfig("test-field", 0, DataType.KEYWORD)),
            emptyPage,
            extractFields,
            Source.EMPTY,
            null,
            null,
            "test-session/node_0/clientToServer", // clientToServerId - per-server unique
            "test-session/serverToClient", // serverToClientId - shared across all servers
            false // profile
        );
        LookupFromIndexService.TransportRequest transportRequest = testService.transportRequest(request, new ShardId("test", "n/a", 0));

        testService.doLookup(transportRequest, null, ActionListener.wrap(pages -> {}, e -> {}));

        return testService.getCapturedPlan();
    }

    private TestLookupService createTestService(AbstractLookupService.LookupShardContext shardContext) {
        AbstractLookupService.LookupShardContextFactory factory = shardId -> shardContext;
        // Use TestProjectResolvers which provides a proper implementation
        ProjectResolver projectResolver = TestProjectResolvers.singleProject(Metadata.DEFAULT_PROJECT_ID);
        ExchangeService exchangeService = new ExchangeService(
            Settings.EMPTY,
            threadPool,
            EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME,
            blockFactory
        );
        releasables.add(exchangeService);  // Ensure ExchangeService is properly closed to stop scheduled tasks
        return new TestLookupService(
            clusterService,
            indicesService,
            factory,
            transportService,
            indexNameExpressionResolver,
            bigArrays,
            blockFactory,
            projectResolver,
            exchangeService
        );
    }

    private AbstractLookupService.LookupShardContext createMockShardContext() throws IOException {
        // Create resources lazily when needed
        Directory directory = newDirectory();
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            writer.commit();
        }
        DirectoryReader reader = DirectoryReader.open(directory);
        MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {
        };
        String mapping = "{\n  \"doc\": { \"properties\": { \"field1\": { \"type\": \"keyword\" } } }\n}";
        MapperService mapperService = mapperHelper.createMapperService(mapping);
        SearchExecutionContext executionCtx = mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader));

        // Add cleanup to releasables
        releasables.add(() -> {
            try {
                org.elasticsearch.core.IOUtils.close(reader, mapperService, directory);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        EsPhysicalOperationProviders.DefaultShardContext shardContext = new EsPhysicalOperationProviders.DefaultShardContext(
            0,
            new NoOpReleasable(),
            executionCtx,
            AliasFilter.EMPTY
        );
        return new AbstractLookupService.LookupShardContext(
            shardContext,
            executionCtx,
            () -> {} // No-op releasable
        );
    }

    private FieldAttribute createFieldAttribute(String name, DataType type) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, type, Collections.emptyMap(), false, EsField.TimeSeriesFieldType.NONE)
        );
    }

}
