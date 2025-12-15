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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ResolvedExpression;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.ProjectOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.lookup.BlockOptimization;
import org.elasticsearch.compute.operator.lookup.EnrichQuerySourceOperator;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
import org.elasticsearch.compute.operator.lookup.MergePositionsOperator;
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

public class LookupExecutionMapperTests extends ESTestCase {
    private BlockFactory blockFactory;
    private BigArrays bigArrays;
    private List<Releasable> releasables;
    private ClusterService clusterService;
    private TransportService transportService;
    private IndicesService indicesService;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private ThreadPool threadPool;

    /**
     * Testable concrete implementation of AbstractLookupService that captures LookupQueryPlan
     * instead of starting a driver, allowing tests to inspect the operators.
     */
    private static class TestLookupService extends AbstractLookupService<
        TestLookupService.TestRequest,
        TestLookupService.TestTransportRequest> {
        private AbstractLookupService.LookupQueryPlan capturedPlan;

        TestLookupService(
            ClusterService clusterService,
            IndicesService indicesService,
            AbstractLookupService.LookupShardContextFactory lookupShardContextFactory,
            TransportService transportService,
            IndexNameExpressionResolver indexNameExpressionResolver,
            BigArrays bigArrays,
            BlockFactory blockFactory,
            boolean mergePages,
            ProjectResolver projectResolver
        ) {
            super(
                "test-lookup-action",
                clusterService,
                indicesService,
                lookupShardContextFactory,
                transportService,
                indexNameExpressionResolver,
                bigArrays,
                blockFactory,
                mergePages,
                (in, bf) -> {
                    throw new UnsupportedOperationException("Not used in tests");
                },
                projectResolver
            );
        }

        @Override
        protected TestTransportRequest transportRequest(TestRequest request, ShardId shardId) {
            return new TestTransportRequest(request, shardId);
        }

        @Override
        protected LookupEnrichQueryGenerator queryList(
            TestTransportRequest request,
            SearchExecutionContext context,
            AliasFilter aliasFilter,
            Warnings warnings
        ) {
            return mock(LookupEnrichQueryGenerator.class);
        }

        @Override
        protected AbstractLookupService.LookupResponse createLookupResponse(List<Page> pages, BlockFactory blockFactory) {
            return new AbstractLookupService.LookupResponse(blockFactory) {
                @Override
                public void writeTo(StreamOutput out) {
                    throw new UnsupportedOperationException("Not used in tests");
                }

                @Override
                protected List<Page> takePages() {
                    return pages;
                }

                @Override
                protected void innerRelease() {}
            };
        }

        @Override
        protected AbstractLookupService.LookupResponse readLookupResponse(StreamInput in, BlockFactory blockFactory) {
            throw new UnsupportedOperationException("Not used in tests");
        }

        @Override
        protected void startDriver(
            TestTransportRequest request,
            CancellableTask task,
            ActionListener<List<Page>> listener,
            LookupQueryPlan lookupQueryPlan
        ) {
            // Capture the plan instead of starting the driver
            // We don't want to actually execute the plan in this test class
            this.capturedPlan = lookupQueryPlan;
            // Immediately respond with empty pages to satisfy the listener
            listener.onResponse(List.of());
        }

        AbstractLookupService.LookupQueryPlan getCapturedPlan() {
            return capturedPlan;
        }

        static class TestRequest extends AbstractLookupService.Request {
            TestRequest(Page inputPage, List<NamedExpression> extractFields, Source source) {
                super("test-session", "test-index", "test-index", DataType.KEYWORD, inputPage, extractFields, source);
            }
        }

        static class TestTransportRequest extends AbstractLookupService.TransportRequest {
            TestTransportRequest(TestRequest request, ShardId shardId) {
                super(request.sessionId, shardId, request.indexPattern, request.inputPage, null, request.extractFields, request.source);
            }

            @Override
            protected String extraDescription() {
                return "";
            }
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
        when(indexNameExpressionResolver.resolveExpressions(any(), any())).thenReturn(Set.of(new ResolvedExpression("test-index")));

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
        Page inputPage = createBytesRefPage("a", "b");

        // No extract fields
        List<NamedExpression> extractFields = Collections.emptyList();
        boolean isEnrich = false;

        AbstractLookupService.LookupQueryPlan queryPlan = generateQueryPlan(inputPage, extractFields, isEnrich);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure
        // Expected: EnrichQuerySourceOperator -> ProjectOperator -> OutputOperator
        verifyCompletePlan(
            queryPlan,
            List.of(EnrichQuerySourceOperator.class, ProjectOperator.class, OutputOperator.class),
            extractFields,
            isEnrich,
            null,
            null
        );
    }

    public void testEnrichDictionaryOptimization() throws Exception {
        // Create a BytesRefBlock with ordinals for dictionary optimization
        Page inputPage = createOrdinalBytesRefPage(new String[] { "a", "b" }, new int[] { 0, 1, 0 });

        List<NamedExpression> extractFields = List.of(
            createFieldAttribute("field1", DataType.KEYWORD),
            createFieldAttribute("field2", DataType.INTEGER)
        );
        boolean isEnrich = true;

        AbstractLookupService.LookupQueryPlan queryPlan = generateQueryPlan(inputPage, extractFields, isEnrich);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure
        // Expected: EnrichQuerySourceOperator -> ValuesSourceReaderOperator -> MergePositionsOperator -> OutputOperator
        verifyCompletePlan(
            queryPlan,
            List.of(EnrichQuerySourceOperator.class, ValuesSourceReaderOperator.class, MergePositionsOperator.class, OutputOperator.class),
            extractFields,
            isEnrich,
            null,
            new MergePositionsDetails(3, BlockOptimization.DICTIONARY, 2)
        );
    }

    public void testEnrichRangeBlockOptimization() throws Exception {
        Page inputPage = createBytesRefPage("a", "b", "c");

        List<NamedExpression> extractFields = List.of(
            createFieldAttribute("field1", DataType.KEYWORD),
            createFieldAttribute("field2", DataType.INTEGER),
            createFieldAttribute("field3", DataType.LONG)
        );
        boolean isEnrich = true;

        AbstractLookupService.LookupQueryPlan queryPlan = generateQueryPlan(inputPage, extractFields, isEnrich);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure
        // Expected: EnrichQuerySourceOperator -> ValuesSourceReaderOperator -> MergePositionsOperator -> OutputOperator
        verifyCompletePlan(
            queryPlan,
            List.of(EnrichQuerySourceOperator.class, ValuesSourceReaderOperator.class, MergePositionsOperator.class, OutputOperator.class),
            extractFields,
            isEnrich,
            inputPage,
            new MergePositionsDetails(3, BlockOptimization.RANGE, 0)
        );
    }

    public void testLookupJoinNoExtraFields() throws Exception {
        // Test lookup join where we don't need to get extra fields (no extract fields, no merge)
        // This is the simplest lookup join case - just joining without extracting additional fields
        Page inputPage = createBytesRefPage("key1", "key2", "key3");

        // No extract fields - pure lookup join without extra field extraction
        List<NamedExpression> extractFields = Collections.emptyList();
        boolean isEnrich = false;

        AbstractLookupService.LookupQueryPlan queryPlan = generateQueryPlan(inputPage, extractFields, isEnrich);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure
        // Expected: EnrichQuerySourceOperator -> ProjectOperator -> OutputOperator
        // No ValuesSourceReaderOperator (no extract fields), no MergePositionsOperator (no merge)
        verifyCompletePlan(
            queryPlan,
            List.of(EnrichQuerySourceOperator.class, ProjectOperator.class, OutputOperator.class),
            extractFields,
            isEnrich,
            null,
            null
        );
    }

    public void testLookupJoinWithExtractFields() throws Exception {
        // Test lookup join with mergePages=false but with extract fields (uses ProjectOperator, not MergePositionsOperator)
        Page inputPage = createBytesRefPage("a", "b");

        List<NamedExpression> extractFields = List.of(createFieldAttribute("field1", DataType.KEYWORD));
        boolean isEnrich = false;

        AbstractLookupService.LookupQueryPlan queryPlan = generateQueryPlan(inputPage, extractFields, isEnrich);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure
        // Expected: EnrichQuerySourceOperator -> ValuesSourceReaderOperator -> ProjectOperator -> OutputOperator
        verifyCompletePlan(
            queryPlan,
            List.of(EnrichQuerySourceOperator.class, ValuesSourceReaderOperator.class, ProjectOperator.class, OutputOperator.class),
            extractFields,
            isEnrich,
            null,
            null
        );
    }

    public void testEnrichMergeWithEmptyExtractFields() throws Exception {
        // Test enrich with mergePages=true but empty extract fields
        // This is an edge case: when mergingChannels is empty, we should use dropDocBlockOperator instead of MergePositionsOperator
        Page inputPage = createBytesRefPage("a", "b");

        // Empty extract fields
        List<NamedExpression> extractFields = Collections.emptyList();
        boolean isEnrich = true;

        AbstractLookupService.LookupQueryPlan queryPlan = generateQueryPlan(inputPage, extractFields, isEnrich);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure
        // Expected: EnrichQuerySourceOperator -> ProjectOperator -> OutputOperator
        // Even though mergePages=true, empty mergingChannels means we use dropDocBlockOperator (ProjectOperator) instead of
        // MergePositionsOperator
        verifyCompletePlan(
            queryPlan,
            List.of(EnrichQuerySourceOperator.class, ProjectOperator.class, OutputOperator.class),
            extractFields,
            isEnrich,
            null,
            null
        );
    }

    public void testEnrichWithAllNullInputPage() throws Exception {
        // Test enrich with input page where all values are null
        // doLookup() returns early for null input blocks without generating operators
        Page inputPage = createAllNullBytesRefPage(3);

        List<NamedExpression> extractFields = List.of(createFieldAttribute("field1", DataType.KEYWORD));
        boolean isEnrich = true;

        // With all null values, doLookup() returns early without calling startDriver()
        // Therefore, no operators should be generated
        AbstractLookupService.LookupQueryPlan queryPlan = generateQueryPlan(inputPage, extractFields, isEnrich);
        assertNull("No operators should be generated for all-null input page", queryPlan);
    }

    // Verification helper methods

    /**
     * Verifies the complete plan structure with all operators and performs detailed verification.
     * @param queryPlan The query plan to verify
     * @param expectedOperators List of expected operator types in order: [source, intermediate1, intermediate2, ..., output]
     * @param extractFields Extract fields for OutputOperator and ProjectOperator verification
     * @param isEnrich Whether this is an enrich operation (affects OutputOperator columns)
     * @param inputPage Optional input page for optimization verification
     * @param mergePositionsDetails Optional details for MergePositionsOperator verification
     */
    private void verifyCompletePlan(
        AbstractLookupService.LookupQueryPlan queryPlan,
        List<Class<? extends Operator>> expectedOperators,
        List<NamedExpression> extractFields,
        boolean isEnrich,
        Page inputPage,
        MergePositionsDetails mergePositionsDetails
    ) {
        if (expectedOperators.size() < 2) {
            throw new IllegalArgumentException("Expected operators list must have at least 2 elements (source and output)");
        }

        // Verify source operator (first in list)
        MatcherAssert.assertThat("Source operator type mismatch", queryPlan.queryOperator(), instanceOf(expectedOperators.get(0)));

        // Verify intermediate operators (middle of list)
        List<Operator> actualOperators = queryPlan.operators();
        int expectedIntermediateCount = expectedOperators.size() - 2; // Exclude source and output
        MatcherAssert.assertThat("Intermediate operators count mismatch", actualOperators.size(), is(expectedIntermediateCount));

        for (int i = 0; i < expectedIntermediateCount; i++) {
            Class<? extends Operator> expectedType = expectedOperators.get(i + 1); // +1 to skip source
            MatcherAssert.assertThat(
                "Operator at index " + i + " should be " + expectedType.getSimpleName(),
                actualOperators.get(i),
                instanceOf(expectedType)
            );
        }

        // Verify output operator (last in list)
        MatcherAssert.assertThat(
            "Output operator type mismatch",
            queryPlan.outputOperator(),
            instanceOf(expectedOperators.get(expectedOperators.size() - 1))
        );

        // Perform detailed verification based on operator types
        EnrichQuerySourceOperator sourceOp = verifyEnrichQuerySourceOperator(queryPlan);

        // Verify input page optimization if provided
        if (inputPage != null) {
            verifyNoPageOptimization(sourceOp, inputPage.getBlock(0));
        }

        // Verify intermediate operators
        int operatorIndex = 0;
        for (int i = 1; i < expectedOperators.size() - 1; i++) {
            Class<? extends Operator> expectedType = expectedOperators.get(i);
            Operator operator = actualOperators.get(operatorIndex);

            if (expectedType == ValuesSourceReaderOperator.class) {
                verifyValuesSourceReaderOperator(queryPlan, operatorIndex);
            } else if (expectedType == MergePositionsOperator.class) {
                if (mergePositionsDetails != null) {
                    verifyMergePositionsOperator(
                        (MergePositionsOperator) operator,
                        sourceOp,
                        mergePositionsDetails.positionCount,
                        mergePositionsDetails.blockOptimization,
                        mergePositionsDetails.dictionarySize
                    );
                }
            } else if (expectedType == ProjectOperator.class) {
                verifyProjectOperator((ProjectOperator) operator, extractFields.size());
            }

            operatorIndex++;
        }

        // Always verify OutputOperator
        verifyOutputOperator(queryPlan, extractFields, isEnrich);
    }

    /**
     * Details for MergePositionsOperator verification.
     */
    private record MergePositionsDetails(int positionCount, BlockOptimization blockOptimization, int dictionarySize) {}

    /**
     * Verifies EnrichQuerySourceOperator and returns it.
     */
    private EnrichQuerySourceOperator verifyEnrichQuerySourceOperator(AbstractLookupService.LookupQueryPlan queryPlan) {
        EnrichQuerySourceOperator sourceOp = queryPlan.queryOperator();
        MatcherAssert.assertThat(sourceOp, notNullValue());
        MatcherAssert.assertThat(sourceOp, instanceOf(EnrichQuerySourceOperator.class));
        Page inputPage = sourceOp.getInputPage();
        MatcherAssert.assertThat(inputPage, notNullValue());
        return sourceOp;
    }

    /**
     * Verifies ValuesSourceReaderOperator at the specified index.
     */
    private ValuesSourceReaderOperator verifyValuesSourceReaderOperator(AbstractLookupService.LookupQueryPlan queryPlan, int index) {
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
     * Verifies MergePositionsOperator with range block or dictionary ordinals.
     * @param mergeOp The MergePositionsOperator to verify
     * @param sourceOp The EnrichQuerySourceOperator to verify optimization state
     * @param expectedPositionCount Expected number of positions
     * @param expectedOptimizationState Expected optimization state (DICTIONARY or RANGE)
     * @param expectedDictionarySize Expected dictionary size (only used for DICTIONARY optimization)
     */
    private void verifyMergePositionsOperator(
        MergePositionsOperator mergeOp,
        EnrichQuerySourceOperator sourceOp,
        int expectedPositionCount,
        BlockOptimization expectedOptimizationState,
        int expectedDictionarySize
    ) {
        MatcherAssert.assertThat(mergeOp, notNullValue());
        IntBlock selectedPositions = mergeOp.getSelectedPositions();
        MatcherAssert.assertThat(selectedPositions, notNullValue());
        MatcherAssert.assertThat(
            "Selected positions should have expected position count",
            selectedPositions.getPositionCount(),
            is(expectedPositionCount)
        );

        if (expectedOptimizationState == BlockOptimization.DICTIONARY) {
            // Verify dictionary optimization was applied to the input page
            Page optimizedPage = sourceOp.getInputPage();
            Block optimizedBlock = optimizedPage.getBlock(0);
            MatcherAssert.assertThat(
                "Optimized block should be a BytesRefBlock (dictionary block)",
                optimizedBlock,
                instanceOf(BytesRefBlock.class)
            );
            MatcherAssert.assertThat(
                "Dictionary block should have dictionary size positions",
                optimizedBlock.getPositionCount(),
                is(expectedDictionarySize)
            );

            // Verify the dictionary values are present
            BytesRefBlock dictBlock = (BytesRefBlock) optimizedBlock;
            BytesRef value1 = new BytesRef();
            BytesRef value2 = new BytesRef();
            dictBlock.getBytesRef(0, value1);
            dictBlock.getBytesRef(1, value2);
            // The dictionary should contain "a" and "b" (order may vary)
            MatcherAssert.assertThat(
                "Dictionary block should contain expected values",
                (value1.utf8ToString().equals("a") && value2.utf8ToString().equals("b"))
                    || (value1.utf8ToString().equals("b") && value2.utf8ToString().equals("a")),
                is(true)
            );
        } else if (expectedOptimizationState == BlockOptimization.RANGE) {
            // Verify it's a range block (vector with sequential values 0, 1, 2, ...)
            IntVector selectedPositionsVector = selectedPositions.asVector();
            MatcherAssert.assertThat("Selected positions should be a vector (range block)", selectedPositionsVector, notNullValue());
            for (int i = 0; i < expectedPositionCount; i++) {
                MatcherAssert.assertThat("Range block position " + i + " should be " + i, selectedPositionsVector.getInt(i), is(i));
            }
        } else {
            throw new IllegalArgumentException("Unsupported optimization state: " + expectedOptimizationState);
        }
    }

    private void verifyNoPageOptimization(EnrichQuerySourceOperator sourceOp, Block expectedInputBlock) {
        Page sourceInputPage = sourceOp.getInputPage();
        MatcherAssert.assertThat(
            "Input page should be the original page when no optimization is used",
            sourceInputPage.getBlock(0),
            is(expectedInputBlock)
        );
    }

    private void verifyOutputOperator(
        AbstractLookupService.LookupQueryPlan queryPlan,
        List<NamedExpression> extractFields,
        boolean isEnrich
    ) {
        OutputOperator outputOp = queryPlan.outputOperator();
        MatcherAssert.assertThat(outputOp, notNullValue());
        MatcherAssert.assertThat(outputOp, instanceOf(OutputOperator.class));

        // Build expected columns: for lookup joins (!isEnrich) with no extractFields, include positions
        List<String> expectedColumns = new ArrayList<>();
        if (isEnrich == false) {
            expectedColumns.add("$$Positions$$");
        }
        expectedColumns.addAll(extractFields.stream().map(NamedExpression::name).toList());

        // Verify columns
        List<String> actualColumns = outputOp.getColumns();
        MatcherAssert.assertThat("OutputOperator columns should match expected columns", actualColumns, is(expectedColumns));
    }

    /**
     * Creates a Page with a BytesRefBlock containing the specified string values.
     */
    private Page createBytesRefPage(String... values) {
        BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(values.length);
        for (String value : values) {
            builder.appendBytesRef(new BytesRef(value));
        }
        BytesRefBlock inputBlock = builder.build();
        return new Page(inputBlock);
    }

    /**
     * Creates a Page with an OrdinalBytesRefBlock (dictionary optimization).
     * @param dictionary The dictionary values
     * @param ordinals The ordinal indices into the dictionary
     */
    private Page createOrdinalBytesRefPage(String[] dictionary, int[] ordinals) {
        BytesRefVector.Builder dictBuilder = blockFactory.newBytesRefVectorBuilder(dictionary.length);
        for (String value : dictionary) {
            dictBuilder.appendBytesRef(new BytesRef(value));
        }
        BytesRefVector dictionaryVector = dictBuilder.build();

        IntBlock.Builder ordinalsBuilder = blockFactory.newIntBlockBuilder(ordinals.length);
        for (int ordinal : ordinals) {
            ordinalsBuilder.appendInt(ordinal);
        }
        IntBlock ordinalsBlock = ordinalsBuilder.build();

        OrdinalBytesRefBlock ordinalBlock = new OrdinalBytesRefBlock(ordinalsBlock, dictionaryVector);
        return new Page(ordinalBlock);
    }

    /**
     * Creates a Page with a BytesRefBlock containing all null values.
     * @param positionCount The number of null positions to create
     */
    private Page createAllNullBytesRefPage(int positionCount) {
        BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(positionCount);
        for (int i = 0; i < positionCount; i++) {
            builder.appendNull();
        }
        BytesRefBlock inputBlock = builder.build();
        return new Page(inputBlock);
    }

    /**
     * Executes doLookup() and returns the captured LookupQueryPlan.
     * This is the common pattern used by all test cases.
     */
    private AbstractLookupService.LookupQueryPlan generateQueryPlan(Page inputPage, List<NamedExpression> extractFields, boolean isEnrich)
        throws Exception {
        AbstractLookupService.LookupShardContext shardContext = createMockShardContext();
        TestLookupService testService = createTestService(isEnrich, shardContext);

        TestLookupService.TestRequest request = new TestLookupService.TestRequest(inputPage, extractFields, Source.EMPTY);
        TestLookupService.TestTransportRequest transportRequest = testService.transportRequest(request, new ShardId("test", "n/a", 0));

        testService.doLookup(transportRequest, null, ActionListener.wrap(pages -> {}, e -> {}));

        return testService.getCapturedPlan();
    }

    private TestLookupService createTestService(boolean mergePages, AbstractLookupService.LookupShardContext shardContext) {
        AbstractLookupService.LookupShardContextFactory factory = shardId -> shardContext;
        // Use TestProjectResolvers which provides a proper implementation
        ProjectResolver projectResolver = TestProjectResolvers.singleProject(Metadata.DEFAULT_PROJECT_ID);
        return new TestLookupService(
            clusterService,
            indicesService,
            factory,
            transportService,
            indexNameExpressionResolver,
            bigArrays,
            blockFactory,
            mergePages,
            projectResolver
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
