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
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.ExpressionWritables;
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
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link BroadcastLookupFromIndexOperator}.
 * <p>
 * Builds a lookup index with LOOKUP_SIZE documents, then tests the broadcast join:
 * the operator fetches the entire right-side (filtered by pre-join filter) into a hash table,
 * then joins each left input page locally.
 */
@TestLogging(
    value = "org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeClient:DEBUG,"
        + "org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeServer:DEBUG,"
        + "org.elasticsearch.compute.operator.exchange.BatchSortedExchangeSource:DEBUG,"
        + "org.elasticsearch.xpack.esql.enrich.BroadcastLookupFromIndexOperator:DEBUG",
    reason = "debugging broadcast lookup"
)
public class BroadcastLookupFromIndexOperatorTests extends OperatorTestCase {
    private static final String MULTI_NODE = "multiNode";
    private static final String SINGLE_NODE = "singleNode";
    private static final int LOOKUP_SIZE = 1000;
    private static final int LESS_THAN_VALUE = 40;
    private final ThreadPool threadPool = threadPool();
    private final Directory lookupIndexDirectory = newDirectory();
    private final List<Releasable> releasables = new ArrayList<>();
    private final boolean useMultiNode;

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[] { SINGLE_NODE });
        params.add(new Object[] { MULTI_NODE });
        return params;
    }

    public BroadcastLookupFromIndexOperatorTests(String nodeMode) {
        super();
        this.useMultiNode = MULTI_NODE.equals(nodeMode);
    }

    @Before
    public void buildLookupIndex() throws IOException {
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), lookupIndexDirectory)) {
            for (int i = 0; i < LOOKUP_SIZE; i++) {
                List<IndexableField> fields = new ArrayList<>();
                fields.add(new LongField("match0", i, Field.Store.NO));
                fields.add(new KeywordFieldMapper.KeywordField("lkwd", new BytesRef("l" + i), KeywordFieldMapper.Defaults.FIELD_TYPE));
                fields.add(new IntField("lint", i, Field.Store.NO));
                writer.addDocument(fields);
            }
        }
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        // Single column: the join key (long values 0..size-1 mod LOOKUP_SIZE)
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> l % LOOKUP_SIZE));
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int inputCount = input.stream().mapToInt(Page::getPositionCount).sum();
        int outputCount = results.stream().mapToInt(Page::getPositionCount).sum();

        // Broadcast hash join with equality: each input row appears exactly once
        assertThat(outputCount, equalTo(inputCount));

        for (Page r : results) {
            // Output columns: [left_key, right_match0, right_lkwd, right_lint]
            // The right join key (match0) is included in output — it is part of addedFields
            assertThat(r.getBlockCount(), equalTo(4));
            LongVector leftKey = r.<LongBlock>getBlock(0).asVector();
            LongBlock match0Block = r.getBlock(1);
            BytesRefBlock lkwdBlock = r.getBlock(2);
            IntBlock lintBlock = r.getBlock(3);

            for (int p = 0; p < r.getPositionCount(); p++) {
                long key = leftKey.getLong(p);
                if (key < LESS_THAN_VALUE) {
                    // Should match: pre-join filter is lint < LESS_THAN_VALUE
                    assertFalse("at " + p + " key=" + key + ": match0 should not be null", match0Block.isNull(p));
                    assertThat("at " + p, match0Block.getLong(match0Block.getFirstValueIndex(p)), equalTo(key));

                    assertFalse("at " + p + " key=" + key + ": lkwd should not be null", lkwdBlock.isNull(p));
                    assertFalse("at " + p + " key=" + key + ": lint should not be null", lintBlock.isNull(p));

                    String joinedLkwd = lkwdBlock.getBytesRef(lkwdBlock.getFirstValueIndex(p), new BytesRef()).utf8ToString();
                    assertThat("at " + p, joinedLkwd, equalTo("l" + key));

                    int joinedLint = lintBlock.getInt(lintBlock.getFirstValueIndex(p));
                    assertThat("at " + p, joinedLint, equalTo((int) key));
                } else {
                    // No match: right columns should be null
                    assertTrue("at " + p + " key=" + key + ": match0 should be null", match0Block.isNull(p));
                    assertTrue("at " + p + " key=" + key + ": lkwd should be null", lkwdBlock.isNull(p));
                    assertTrue("at " + p + " key=" + key + ": lint should be null", lintBlock.isNull(p));
                }
            }
        }
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        String sessionId = "test";
        CancellableTask parentTask = new CancellableTask(0, "test", "test", "test", TaskId.EMPTY_TASK_ID, Map.of());
        String lookupIndex = "idx";

        // loadFields includes the join key (match0) so it is fetched from the right index
        List<NamedExpression> loadFields = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "match0",
                new EsField("match0", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            ),
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

        // Empty matchFields: broadcast mode — server returns all (filtered) documents
        List<MatchConfig> matchFields = List.of();

        // Pre-join filter: lint < LESS_THAN_VALUE (applied server-side)
        FragmentExec rightPreJoinFilter = buildLessThanFilter(LESS_THAN_VALUE);

        int exchangeBufferSize = QueryPragmas.EXCHANGE_BUFFER_SIZE.getDefault(Settings.EMPTY);

        // leftJoinKeyChannels: column 0 in the left input page
        int[] leftJoinKeyChannels = new int[] { 0 };
        // joinKeyColumnsInRight: column 1 in right result pages (column 0 = positions, column 1 = first loadField = match0)
        int[] joinKeyColumnsInRight = new int[] { 1 };

        return new Operator.OperatorFactory() {
            @Override
            public Operator get(DriverContext driverContext) {
                LookupFromIndexService service = lookupService(driverContext);
                return new BroadcastLookupFromIndexOperator(
                    matchFields,
                    sessionId,
                    parentTask,
                    service,
                    lookupIndex,
                    lookupIndex,
                    loadFields,
                    Source.EMPTY,
                    rightPreJoinFilter,
                    exchangeBufferSize,
                    false, // profile
                    driverContext.blockFactory(),
                    leftJoinKeyChannels,
                    joinKeyColumnsInRight,
                    loadFields.size(), // addedFieldCount: join key IS in loadFields, so addedFields == loadFields
                    Warnings.createWarnings(driverContext.warningsMode(), Source.EMPTY)
                );
            }

            @Override
            public String describe() {
                return "BroadcastLookupOperator[index=" + lookupIndex + "]";
            }
        };
    }

    private FragmentExec buildLessThanFilter(int value) {
        FieldAttribute filterAttribute = new FieldAttribute(
            Source.EMPTY,
            "lint",
            new EsField("lint", DataType.INTEGER, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
        org.elasticsearch.xpack.esql.core.expression.Expression lessThan = new LessThan(
            Source.EMPTY,
            filterAttribute,
            new Literal(Source.EMPTY, value, DataType.INTEGER)
        );
        EsRelation esRelation = new EsRelation(Source.EMPTY, "test", IndexMode.LOOKUP, Map.of(), Map.of(), Map.of(), List.of());
        Filter filter = new Filter(Source.EMPTY, esRelation, lessThan);
        return new FragmentExec(filter);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return matchesPattern("BroadcastLookupOperator\\[index=idx\\]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return matchesPattern("BroadcastLookupOperator\\[index=idx\\]");
    }

    // --- Lookup service infrastructure (same pattern as StreamingLookupFromIndexOperatorTests) ---

    private static final int NUM_SERVER_NODES = 2;

    private LookupFromIndexService lookupService(DriverContext mainContext) {
        if (useMultiNode == false) {
            return lookupServiceSingleNode(mainContext);
        }
        // Multi-node: client + server nodes
        MockTransportService clientTransport = newMockTransportService("client");
        releasables.add(clientTransport);

        List<MockTransportService> serverTransports = new ArrayList<>();
        for (int i = 0; i < NUM_SERVER_NODES; i++) {
            MockTransportService serverTransport = newMockTransportService("node_" + i);
            serverTransports.add(serverTransport);
            releasables.add(serverTransport);
        }

        // Connect client <-> servers
        for (MockTransportService serverTransport : serverTransports) {
            AbstractSimpleTransportTestCase.connectToNode(clientTransport, serverTransport.getLocalNode());
            AbstractSimpleTransportTestCase.connectToNode(serverTransport, clientTransport.getLocalNode());
        }

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
        ClusterState clusterState = buildClusterStateWithNodes(
            projectId,
            "idx",
            clientTransport.getLocalNode(),
            serverTransports.stream().map(MockTransportService::getLocalNode).toList()
        );
        ClusterServiceUtils.setState(clusterService, clusterState);

        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        IndicesService indicesService = mock(IndicesService.class);
        IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();

        // Create exchange and lookup service on each server node
        for (MockTransportService serverTransport : serverTransports) {
            ExchangeService serverExchangeService = new ExchangeService(Settings.EMPTY, threadPool, ThreadPool.Names.SEARCH, blockFactory);
            serverExchangeService.registerTransportHandler(serverTransport);
            releasables.add(serverExchangeService);

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

        // Client exchange and lookup service
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

    private LookupFromIndexService lookupServiceSingleNode(DriverContext mainContext) {
        MockTransportService localTransport = newMockTransportService("local");
        releasables.add(localTransport);

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
        ClusterState clusterState = buildClusterStateForSingleNode(projectId, "idx", localTransport.getLocalNode());
        ClusterServiceUtils.setState(clusterService, clusterState);

        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        IndicesService indicesService = mock(IndicesService.class);
        IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();

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
        namedWriteables.addAll(ExpressionWritables.getNamedWriteables());
        namedWriteables.addAll(PlanWritables.getNamedWriteables());
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
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

    private ClusterState buildClusterStateWithNodes(
        ProjectId projectId,
        String indexName,
        DiscoveryNode clientNode,
        List<DiscoveryNode> serverNodes
    ) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(clientNode);
        nodesBuilder.localNodeId(clientNode.getId());
        nodesBuilder.masterNodeId(serverNodes.get(0).getId());
        for (DiscoveryNode serverNode : serverNodes) {
            nodesBuilder.add(serverNode);
        }

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, serverNodes.size() - 1)
            )
            .build();

        ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        IndexShardRoutingTable.Builder shardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);

        ShardRouting primaryRouting = ShardRouting.newUnassigned(
            shardId,
            true,
            org.elasticsearch.cluster.routing.RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
            ShardRouting.Role.DEFAULT
        ).initialize(serverNodes.get(0).getId(), null, 0).moveToStarted(0);
        shardRoutingBuilder.addShard(primaryRouting);

        for (int i = 1; i < serverNodes.size(); i++) {
            ShardRouting replicaRouting = ShardRouting.newUnassigned(
                shardId,
                false,
                org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
                ShardRouting.Role.DEFAULT
            ).initialize(serverNodes.get(i).getId(), null, 0).moveToStarted(0);
            shardRoutingBuilder.addShard(replicaRouting);
        }

        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addIndexShard(shardRoutingBuilder)
            .build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId);
        projectMetadataBuilder.put(indexMetadata, false);
        Metadata metadata = Metadata.builder().put(projectMetadataBuilder).generateClusterUuidIfNeeded().build();

        return ClusterState.builder(new ClusterName("test"))
            .nodes(nodesBuilder)
            .metadata(metadata)
            .routingTable(GlobalRoutingTable.builder().put(projectId, routingTable).build())
            .build();
    }

    private ClusterState buildClusterStateForSingleNode(ProjectId projectId, String indexName, DiscoveryNode localNode) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(localNode);
        nodesBuilder.localNodeId(localNode.getId());
        nodesBuilder.masterNodeId(localNode.getId());

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .build();

        ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        IndexShardRoutingTable.Builder shardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);

        ShardRouting primaryRouting = ShardRouting.newUnassigned(
            shardId,
            true,
            org.elasticsearch.cluster.routing.RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
            ShardRouting.Role.DEFAULT
        ).initialize(localNode.getId(), null, 0).moveToStarted(0);
        shardRoutingBuilder.addShard(primaryRouting);

        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addIndexShard(shardRoutingBuilder)
            .build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId);
        projectMetadataBuilder.put(indexMetadata, false);
        Metadata metadata = Metadata.builder().put(projectMetadataBuilder).generateClusterUuidIfNeeded().build();

        return ClusterState.builder(new ClusterName("test"))
            .nodes(nodesBuilder)
            .metadata(metadata)
            .routingTable(GlobalRoutingTable.builder().put(projectId, routingTable).build())
            .build();
    }

    private AbstractLookupService.LookupShardContextFactory lookupShardContextFactory() {
        return shardId -> {
            MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {};
            String mapping = String.format(
                Locale.ROOT,
                "{\n  \"doc\": { \"properties\": { \"match0\": { \"type\": \"long\" }, "
                    + "\"lkwd\": { \"type\": \"keyword\" }, \"lint\": { \"type\": \"integer\" } } }\n}"
            );
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
        // Basic sanity checks on status fields
        assertThat(((Number) map.get("rows_emitted")).longValue(), greaterThanOrEqualTo(0L));
        assertThat(((Number) map.get("planning_nanos")).longValue(), greaterThanOrEqualTo(0L));
        assertThat(((Number) map.get("process_nanos")).longValue(), greaterThanOrEqualTo(0L));
    }

    @Override
    public void testCanProduceMoreDataWithoutExtraInput() {
        // BroadcastLookupFromIndexOperator starts an async exchange in its constructor,
        // so it can't immediately report isFinished() after finish() is called without
        // running through a full driver loop. This test is not applicable.
    }

    @Override
    public void testSimpleFinishClose() {
        // BroadcastLookupFromIndexOperator starts an async exchange in its constructor.
        // The operator's needsInput() returns false during FETCHING_RIGHT phase,
        // so the standard finish/close test flow doesn't apply.
    }

    @Override
    public void testSimpleCircuitBreaking() {
        // The broadcast operator has non-deterministic memory usage due to async page streaming,
        // so the binary search in BreakerTestUtil doesn't work reliably. Instead test:
        // 1. Very low memory should throw CircuitBreakingException
        // 2. Plenty of memory should succeed
        testCircuitBreakingWithLowMemory();
        testCircuitBreakingWithEnoughMemory();
    }

    private void testCircuitBreakingWithLowMemory() {
        DriverContext inputFactoryContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(inputFactoryContext.blockFactory(), between(10, 20)));
        Operator.OperatorFactory factory = simple();

        ByteSizeValue lowMemory = ByteSizeValue.ofBytes(100);

        try {
            expectThrows(CircuitBreakingException.class, () -> runWithLimit(factory, input, lowMemory));
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(input.iterator(), p -> p::releaseBlocks)));
        }
        assertThat(inputFactoryContext.breaker().getUsed(), equalTo(0L));
    }

    private void testCircuitBreakingWithEnoughMemory() {
        DriverContext inputFactoryContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(inputFactoryContext.blockFactory(), between(1, 10)));
        Operator.OperatorFactory factory = simple();

        ByteSizeValue plentyMemory = enoughMemoryForSimple();

        try {
            runWithLimit(factory, input, plentyMemory);
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(input.iterator(), p -> p::releaseBlocks)));
        }
        assertThat(inputFactoryContext.breaker().getUsed(), equalTo(0L));
    }

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
            new TestDriverRunner().builder(driverContext).input(localInput).run(operator);
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
            assertTrue("Expected BroadcastLookupFromIndexOperator instance", op instanceof BroadcastLookupFromIndexOperator);
        }
    }
}
