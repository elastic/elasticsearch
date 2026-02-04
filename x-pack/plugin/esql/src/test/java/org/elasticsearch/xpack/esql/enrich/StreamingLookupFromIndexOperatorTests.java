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
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
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
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.ExpressionWritables;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.plan.PlanWritables;
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
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

@TestLogging(
    value = "org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeClient:DEBUG,"
        + "org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeServer:DEBUG,"
        + "org.elasticsearch.compute.operator.exchange.BatchSortedExchangeSource:DEBUG,"
        + "org.elasticsearch.xpack.esql.enrich.StreamingLookupFromIndexOperator:DEBUG",
    reason = "debugging streaming lookup performance"
)
public class StreamingLookupFromIndexOperatorTests extends OperatorTestCase {
    private static final String MULTI_NODE = "multiNode";
    private static final String SINGLE_NODE = "singleNode";
    private static final int LOOKUP_SIZE = 1000;
    private static final int LESS_THAN_VALUE = 40;
    private final ThreadPool threadPool = threadPool();
    private final Directory lookupIndexDirectory = newDirectory();
    private final List<Releasable> releasables = new ArrayList<>();
    private final boolean applyRightFilterAsJoinOnFilter;
    private final boolean useMultiNode;
    private int numberOfJoinColumns;
    private EsqlBinaryComparison.BinaryComparisonOperation operation;
    // Track node IDs for mode verification in assertStatus
    private String clientNodeId;
    private final List<String> serverNodeIds = new ArrayList<>();

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[] { null, MULTI_NODE });
        params.add(new Object[] { null, SINGLE_NODE });
        if (EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()) {
            for (EsqlBinaryComparison.BinaryComparisonOperation operation : EsqlBinaryComparison.BinaryComparisonOperation.values()) {
                // we skip NEQ because there are too many matches and the test can timeout
                if (operation != EsqlBinaryComparison.BinaryComparisonOperation.NEQ) {
                    params.add(new Object[] { operation, MULTI_NODE });
                    params.add(new Object[] { operation, SINGLE_NODE });
                }
            }
        }
        return params;
    }

    public StreamingLookupFromIndexOperatorTests(EsqlBinaryComparison.BinaryComparisonOperation operation, String nodeMode) {
        super();
        this.operation = operation;
        this.useMultiNode = MULTI_NODE.equals(nodeMode);
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
        return simple(options, QueryPragmas.ENRICH_MAX_WORKERS.getDefault(Settings.EMPTY));
    }

    protected Operator.OperatorFactory simple(SimpleOptions options, int maxOutstandingRequests) {
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
                    maxOutstandingRequests,
                    service,
                    lookupIndex,
                    lookupIndex,
                    loadFields,
                    Source.EMPTY,
                    finalRightPlan,
                    finalJoinOnExpression,
                    exchangeBufferSize,
                    true // profile - enables plan tracking for mode verification
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

    private static final int NUM_SERVER_NODES = 2;

    private LookupFromIndexService lookupService(DriverContext mainContext) {
        // Clear node tracking from previous test runs
        clientNodeId = null;
        serverNodeIds.clear();

        if (useMultiNode == false) {
            return lookupServiceSingleNode(mainContext);
        }
        // Create client (coordinator) transport service
        MockTransportService clientTransport = newMockTransportService("client");
        releasables.add(clientTransport);
        clientNodeId = clientTransport.getLocalNode().getId();

        // Create server transport services (data nodes with shard replicas)
        List<MockTransportService> serverTransports = new ArrayList<>();
        for (int i = 0; i < NUM_SERVER_NODES; i++) {
            MockTransportService serverTransport = newMockTransportService("node_" + i);
            serverTransports.add(serverTransport);
            releasables.add(serverTransport);
            serverNodeIds.add(serverTransport.getLocalNode().getId());
        }

        // Connect all transport services bidirectionally (client <-> servers only, no server-to-server)
        for (MockTransportService serverTransport : serverTransports) {
            AbstractSimpleTransportTestCase.connectToNode(clientTransport, serverTransport.getLocalNode());
            AbstractSimpleTransportTestCase.connectToNode(serverTransport, clientTransport.getLocalNode());
        }

        // Build cluster state with server nodes having shards, client node does NOT have shard
        var builtInClusterSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        builtInClusterSettings.addAll(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS);
        ClusterService clusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            clientTransport.getLocalNode(),
            Settings.builder()
                .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING, ByteSizeValue.ofKb(0))
                .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING, ByteSizeValue.ofKb(0))
                .build(),
            new ClusterSettings(Settings.EMPTY, builtInClusterSettings)
        );
        releasables.add(clusterService::stop);

        final var projectId = randomProjectIdOrDefault();
        // Create cluster state with actual server nodes having shard replicas
        // Client node does NOT have shard - this forces routing to rotate through server nodes
        ClusterState clusterState = buildClusterStateWithNodes(
            projectId,
            "idx",
            clientTransport.getLocalNode(),
            serverTransports.stream().map(MockTransportService::getLocalNode).toList()
        );
        ClusterServiceUtils.setState(clusterService, clusterState);

        // Use non-cranky breaker for exchange services - cranky breaker should NOT affect transport deserialization
        // The cranky tests are meant to test operator-level circuit breaking, not transport layer failures
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

        IndicesService indicesService = mock(IndicesService.class);
        IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();

        // Create LookupFromIndexService on each server to handle incoming requests
        // Each server needs its own ExchangeService and LookupFromIndexService
        for (MockTransportService serverTransport : serverTransports) {
            ExchangeService serverExchangeService = new ExchangeService(Settings.EMPTY, threadPool, ThreadPool.Names.SEARCH, blockFactory);
            serverExchangeService.registerTransportHandler(serverTransport);
            releasables.add(serverExchangeService);

            // Create a cluster service for the server (uses same cluster state)
            ClusterService serverClusterService = ClusterServiceUtils.createClusterService(
                threadPool,
                serverTransport.getLocalNode(),
                Settings.builder()
                    .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING, ByteSizeValue.ofKb(0))
                    .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING, ByteSizeValue.ofKb(0))
                    .build(),
                new ClusterSettings(Settings.EMPTY, builtInClusterSettings)
            );
            ClusterServiceUtils.setState(serverClusterService, clusterService.state());
            releasables.add(serverClusterService::stop);

            // Create LookupFromIndexService on server - this registers the transport handler
            new LookupFromIndexService(
                serverClusterService,
                indicesService,
                lookupShardContextFactory(),
                serverTransport,
                indexNameExpressionResolver,
                bigArrays,
                blockFactory,
                TestProjectResolvers.singleProject(projectId),
                serverExchangeService
            );
        }

        // Create client ExchangeService and LookupFromIndexService
        ExchangeService clientExchangeService = new ExchangeService(Settings.EMPTY, threadPool, ThreadPool.Names.SEARCH, blockFactory);
        clientExchangeService.registerTransportHandler(clientTransport);
        releasables.add(clientExchangeService);

        return new LookupFromIndexService(
            clusterService,
            indicesService,
            lookupShardContextFactory(),
            clientTransport,
            indexNameExpressionResolver,
            bigArrays,
            blockFactory,
            TestProjectResolvers.singleProject(projectId),
            clientExchangeService
        );
    }

    /**
     * Create a LookupFromIndexService for single-node mode where the local node has the lookup index.
     */
    private LookupFromIndexService lookupServiceSingleNode(DriverContext mainContext) {
        // Create single transport service - this node has the lookup index
        MockTransportService localTransport = newMockTransportService("local");
        releasables.add(localTransport);
        // In single-node mode, the local node is the server (has the shard)
        clientNodeId = localTransport.getLocalNode().getId();
        // serverNodeIds stays empty - indicates local node is the server

        // Build cluster state with local node having the shard
        var builtInClusterSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        builtInClusterSettings.addAll(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS);
        ClusterService clusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            localTransport.getLocalNode(),
            Settings.builder()
                .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING, ByteSizeValue.ofKb(0))
                .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING, ByteSizeValue.ofKb(0))
                .build(),
            new ClusterSettings(Settings.EMPTY, builtInClusterSettings)
        );
        releasables.add(clusterService::stop);

        final var projectId = randomProjectIdOrDefault();
        // Create cluster state where the local node has the shard
        ClusterState clusterState = buildClusterStateForSingleNode(projectId, "idx", localTransport.getLocalNode());
        ClusterServiceUtils.setState(clusterService, clusterState);

        // Use non-cranky breaker for exchange services
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

        IndicesService indicesService = mock(IndicesService.class);
        IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();

        // Create ExchangeService and LookupFromIndexService on the local node
        ExchangeService exchangeService = new ExchangeService(Settings.EMPTY, threadPool, ThreadPool.Names.SEARCH, blockFactory);
        exchangeService.registerTransportHandler(localTransport);
        releasables.add(exchangeService);

        return new LookupFromIndexService(
            clusterService,
            indicesService,
            lookupShardContextFactory(),
            localTransport,
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

    private MockTransportService newMockTransportService(String nodeName) {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(ClusterModule.getNamedWriteables());
        // Add ESQL-specific named writeables for transport serialization
        namedWriteables.addAll(ExpressionWritables.getNamedWriteables());
        namedWriteables.addAll(PlanWritables.getNamedWriteables());
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
        // Use node name in settings so it shows up in node info
        Settings nodeSettings = Settings.builder().put("node.name", nodeName).build();
        MockTransportService service = MockTransportService.createNewService(
            nodeSettings,
            MockTransportService.newMockTransport(nodeSettings, TransportVersion.current(), threadPool, namedWriteableRegistry),
            VersionInformation.CURRENT,
            threadPool,
            null,
            Collections.emptySet()
        );
        service.getTaskManager().setTaskCancellationService(new TaskCancellationService(service));
        service.start();
        service.acceptIncomingRequests();
        return service;
    }

    /**
     * Build a cluster state with the actual nodes from our transport services.
     * The index has 1 shard with replicas on all server nodes. The client node does NOT have a copy.
     */
    private ClusterState buildClusterStateWithNodes(
        ProjectId projectId,
        String indexName,
        DiscoveryNode clientNode,
        List<DiscoveryNode> serverNodes
    ) {
        // Build DiscoveryNodes with all nodes, client as local
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(clientNode);
        nodesBuilder.localNodeId(clientNode.getId());
        nodesBuilder.masterNodeId(serverNodes.get(0).getId()); // First server is master
        for (DiscoveryNode serverNode : serverNodes) {
            nodesBuilder.add(serverNode);
        }

        // Create index metadata with 1 shard and N replicas
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, serverNodes.size() - 1)
            )
            .build();

        // Build shard routing - primary on first server, replicas on others
        ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        IndexShardRoutingTable.Builder shardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);

        // Primary shard on first server node
        ShardRouting primaryRouting = ShardRouting.newUnassigned(
            shardId,
            true, // primary
            org.elasticsearch.cluster.routing.RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
            ShardRouting.Role.DEFAULT
        ).initialize(serverNodes.get(0).getId(), null, 0).moveToStarted(0);
        shardRoutingBuilder.addShard(primaryRouting);

        // Replica shards on remaining server nodes
        for (int i = 1; i < serverNodes.size(); i++) {
            ShardRouting replicaRouting = ShardRouting.newUnassigned(
                shardId,
                false, // replica
                org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
                ShardRouting.Role.DEFAULT
            ).initialize(serverNodes.get(i).getId(), null, 0).moveToStarted(0);
            shardRoutingBuilder.addShard(replicaRouting);
        }

        // Build routing table
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addIndexShard(shardRoutingBuilder)
            .build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();

        // Build project metadata
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId);
        projectMetadataBuilder.put(indexMetadata, false);

        // Build metadata
        Metadata metadata = Metadata.builder().put(projectMetadataBuilder).generateClusterUuidIfNeeded().build();

        // Build and return cluster state
        return ClusterState.builder(new ClusterName("test"))
            .nodes(nodesBuilder)
            .metadata(metadata)
            .routingTable(GlobalRoutingTable.builder().put(projectId, routingTable).build())
            .build();
    }

    /**
     * Build a cluster state for single-node mode where the local node has the shard.
     */
    private ClusterState buildClusterStateForSingleNode(ProjectId projectId, String indexName, DiscoveryNode localNode) {
        // Single node is local and master
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(localNode);
        nodesBuilder.localNodeId(localNode.getId());
        nodesBuilder.masterNodeId(localNode.getId());

        // Create index metadata with 1 shard and 0 replicas
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .build();

        // Build shard routing - primary on local node
        ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        IndexShardRoutingTable.Builder shardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);

        ShardRouting primaryRouting = ShardRouting.newUnassigned(
            shardId,
            true, // primary
            org.elasticsearch.cluster.routing.RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
            ShardRouting.Role.DEFAULT
        ).initialize(localNode.getId(), null, 0).moveToStarted(0);
        shardRoutingBuilder.addShard(primaryRouting);

        // Build routing table
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addIndexShard(shardRoutingBuilder)
            .build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();

        // Build project metadata
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId);
        projectMetadataBuilder.put(indexMetadata, false);

        // Build metadata
        Metadata metadata = Metadata.builder().put(projectMetadataBuilder).generateClusterUuidIfNeeded().build();

        // Build and return cluster state
        return ClusterState.builder(new ClusterName("test"))
            .nodes(nodesBuilder)
            .metadata(metadata)
            .routingTable(GlobalRoutingTable.builder().put(projectId, routingTable).build())
            .build();
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

        // Verify mode via lookup_plans (populated because profile=true)
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> lookupPlans = (List<Map<String, Object>>) map.get("lookup_plans");
        if (lookupPlans != null && lookupPlans.isEmpty() == false) {
            Set<String> workerNodeIds = new HashSet<>();
            for (Map<String, Object> planEntry : lookupPlans) {
                @SuppressWarnings("unchecked")
                List<String> workers = (List<String>) planEntry.get("workers");
                for (String workerKey : workers) {
                    // Extract nodeId from "nodeId:workerN"
                    String nodeId = workerKey.substring(0, workerKey.lastIndexOf(":worker"));
                    workerNodeIds.add(nodeId);
                }
            }

            if (useMultiNode) {
                // Multi-node: workers should be on server nodes, NOT on client
                assertThat("Workers should not be on client node", workerNodeIds, not(hasItem(clientNodeId)));
                assertThat("Workers should be on server nodes", workerNodeIds, everyItem(isIn(serverNodeIds)));
            } else {
                // Single-node: workers should be on local node (which is clientNodeId in this mode)
                assertThat("Single-node: all workers should be on local node", workerNodeIds, everyItem(equalTo(clientNodeId)));
            }
        }
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
        // Use at least 10 rows to ensure memory usage exceeds the 100-byte limit
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(inputFactoryContext.blockFactory(), between(10, 20)));
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
     * Test that the operator accepts exactly maxOutstandingRequests batches before blocking.
     * This verifies that pipelining allows multiple batches in flight simultaneously.
     */
    public void testConcurrentBatchPipelining() {
        // Only test with null operation (field-based join) or EQ to avoid timeout
        if (operation != null && operation.equals(EsqlBinaryComparison.BinaryComparisonOperation.EQ) == false) {
            return;
        }

        int maxBatches = 5;
        int numPages = maxBatches * 100 + 1;

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

        Operator.OperatorFactory factory = simple(SimpleOptions.DEFAULT, maxBatches);
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

            // Send up to maxBatches pages - all should be accepted
            for (int i = 0; i < maxBatches && pageIndex < inputPages.size(); i++) {
                assertTrue("Should accept batch " + i, operator.needsInput());
                operator.addInput(inputPages.get(pageIndex++));
                pagesAccepted++;
            }

            // Verify we accepted exactly maxBatches
            assertThat("Should accept exactly maxOutstandingRequests batches before blocking", pagesAccepted, equalTo(maxBatches));

            // After maxBatches, needsInput should return false
            assertFalse("Should NOT accept more than maxOutstandingRequests batches in flight", operator.needsInput());

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
