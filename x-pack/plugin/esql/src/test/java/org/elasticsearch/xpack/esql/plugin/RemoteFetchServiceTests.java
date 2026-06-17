/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeServer;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.ConfigurationTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.LeafExpression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RemoteFetchSource;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.junit.After;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class RemoteFetchServiceTests extends MapperServiceTestCase {
    private Directory directory;
    private IndexReader reader;
    private BlockFactory blockFactory;

    public void testInputPagePreservesHandleCoordinates() {
        blockFactory = blockFactory();
        List<RemoteFetchHandle> handles = List.of(
            new RemoteFetchHandle("node-1", "session-1", 3, 7, 11),
            new RemoteFetchHandle("node-1", "session-1", 5, 13, 17)
        );

        Page page = RemoteFieldLoader.inputPage(blockFactory, handles);
        try {
            DocBlock docBlock = page.getBlock(0);
            assertThat(docBlock.asVector().shards().getInt(0), equalTo(3));
            assertThat(docBlock.asVector().segments().getInt(0), equalTo(7));
            assertThat(docBlock.asVector().docs().getInt(0), equalTo(11));
            assertThat(docBlock.asVector().shards().getInt(1), equalTo(5));
            assertThat(docBlock.asVector().segments().getInt(1), equalTo(13));
            assertThat(docBlock.asVector().docs().getInt(1), equalTo(17));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testExecuteFetchLoadsRequestedFieldsInHandleOrder() throws Exception {
        blockFactory = blockFactory();
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("k").field("type", "keyword").endObject();
            b.startObject("n").field("type", "long").endObject();
        }));

        directory = newDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            addDoc(writer, "k0", 10);
            addDoc(writer, "k1", 20);
            addDoc(writer, "k2", 30);
        }
        reader = DirectoryReader.open(directory);

        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService, null);
        EsPhysicalOperationProviders.DefaultShardContext fetchShardContext = new EsPhysicalOperationProviders.DefaultShardContext(
            0,
            new NoOpReleasable(),
            searchExecutionContext,
            AliasFilter.EMPTY
        );
        List<ValuesSourceReaderOperator.FieldInfo> fieldInfos = RemoteFieldLoader.buildFieldInfos(
            List.of(new RemoteFetchService.FetchField("k", DataType.KEYWORD), new RemoteFetchService.FetchField("n", DataType.LONG)),
            new IndexedByShardIdFromSingleton<>(fetchShardContext),
            PlannerSettings.DEFAULTS
        );

        List<Page> pages = RemoteFieldLoader.executeFetch(
            List.of(
                new RemoteFetchHandle("node-1", "session-1", 0, 0, 2),
                new RemoteFetchHandle("node-1", "session-1", 0, 0, 0),
                new RemoteFetchHandle("node-1", "session-1", 0, 0, 1)
            ),
            fieldInfos,
            new IndexedByShardIdFromSingleton<>(
                new ValuesSourceReaderOperator.ShardContext(reader, sourcePaths -> SourceLoader.FROM_STORED_SOURCE, 0.2)
            ),
            BigArrays.NON_RECYCLING_INSTANCE,
            blockFactory,
            LocalCircuitBreaker.SizeSettings.DEFAULT_SETTINGS,
            PlannerSettings.DEFAULTS
        );

        try {
            assertThat(pages.size(), equalTo(1));
            Page page = pages.getFirst();
            BytesRef scratch = new BytesRef();
            assertThat(page.getBlockCount(), equalTo(2));
            assertThat(
                page.<org.elasticsearch.compute.data.BytesRefBlock>getBlock(0).getBytesRef(0, scratch).utf8ToString(),
                equalTo("k2")
            );
            assertThat(
                page.<org.elasticsearch.compute.data.BytesRefBlock>getBlock(0).getBytesRef(1, scratch).utf8ToString(),
                equalTo("k0")
            );
            assertThat(
                page.<org.elasticsearch.compute.data.BytesRefBlock>getBlock(0).getBytesRef(2, scratch).utf8ToString(),
                equalTo("k1")
            );
            assertThat(page.<org.elasticsearch.compute.data.LongBlock>getBlock(1).getLong(0), equalTo(30L));
            assertThat(page.<org.elasticsearch.compute.data.LongBlock>getBlock(1).getLong(1), equalTo(10L));
            assertThat(page.<org.elasticsearch.compute.data.LongBlock>getBlock(1).getLong(2), equalTo(20L));
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(pages.iterator(), page -> page::releaseBlocks)));
        }
    }

    public void testExecutePushdownForExchangeSupportsFilterExec() {
        blockFactory = blockFactory();
        RemoteFetchPushdownPlanExecutor pushdownExecutor = new RemoteFetchPushdownPlanExecutor(
            blockFactory.bigArrays(),
            LocalCircuitBreaker.SizeSettings.DEFAULT_SETTINGS
        );
        ReferenceAttribute fetchedAttribute = new ReferenceAttribute(Source.EMPTY, null, "n", DataType.LONG);
        ReferenceAttribute positionAttribute = new ReferenceAttribute(Source.EMPTY, null, "_remote_fetch_position", DataType.INTEGER);
        PhysicalPlan pushdownPlan = new FragmentExec(
            new Filter(
                Source.EMPTY,
                new RemoteFetchSource(Source.EMPTY, List.of(fetchedAttribute, positionAttribute)),
                new GreaterThan(Source.EMPTY, fetchedAttribute, new Literal(Source.EMPTY, 15L, DataType.LONG))
            )
        );
        Page inputPage;
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(3)) {
            builder.appendLong(10L);
            builder.appendLong(20L);
            builder.appendLong(30L);
            inputPage = new Page(builder.build());
        }

        List<Page> outputPages = pushdownExecutor.execute(
            List.of(inputPage),
            pushdownPlan,
            new IndexedByShardIdFromSingleton<>(
                Mockito.mock(EsPhysicalOperationProviders.ShardContext.class, Mockito.withSettings().stubOnly())
            ),
            blockFactory,
            FoldContext.small()
        );

        try {
            assertThat(outputPages.size(), equalTo(1));
            Page output = outputPages.getFirst();
            assertThat(output.getPositionCount(), equalTo(2));
            assertThat(output.getBlockCount(), equalTo(2));
            assertThat(output.<LongBlock>getBlock(0).getLong(0), equalTo(20L));
            assertThat(output.<LongBlock>getBlock(0).getLong(1), equalTo(30L));
            assertThat(output.<IntBlock>getBlock(1).getInt(0), equalTo(1));
            assertThat(output.<IntBlock>getBlock(1).getInt(1), equalTo(2));
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(outputPages.iterator(), page -> page::releaseBlocks)));
        }
    }

    public void testPushdownLogicalProjectRejectsScalarAliases() {
        ReferenceAttribute fetchedAttribute = new ReferenceAttribute(Source.EMPTY, null, "n", DataType.LONG);
        ReferenceAttribute positionAttribute = new ReferenceAttribute(Source.EMPTY, null, "_remote_fetch_position", DataType.INTEGER);
        expectThrows(
            AssertionError.class,
            () -> new Project(
                Source.EMPTY,
                new RemoteFetchSource(Source.EMPTY, List.of(fetchedAttribute, positionAttribute)),
                List.of(new Alias(Source.EMPTY, "constant", new Literal(Source.EMPTY, 1, DataType.INTEGER)))
            )
        );
    }

    public void testExecutePushdownProjectPreservesPositionMapping() {
        blockFactory = blockFactory();
        RemoteFetchPushdownPlanExecutor pushdownExecutor = new RemoteFetchPushdownPlanExecutor(
            blockFactory.bigArrays(),
            LocalCircuitBreaker.SizeSettings.DEFAULT_SETTINGS
        );
        ReferenceAttribute fetchedAttribute = new ReferenceAttribute(Source.EMPTY, null, "n", DataType.LONG);
        ReferenceAttribute positionAttribute = new ReferenceAttribute(Source.EMPTY, null, "_remote_fetch_position", DataType.INTEGER);
        PhysicalPlan pushdownPlan = new FragmentExec(
            new Project(
                Source.EMPTY,
                new Filter(
                    Source.EMPTY,
                    new RemoteFetchSource(Source.EMPTY, List.of(fetchedAttribute, positionAttribute)),
                    new GreaterThan(Source.EMPTY, fetchedAttribute, new Literal(Source.EMPTY, 15L, DataType.LONG))
                ),
                List.of(fetchedAttribute)
            )
        );
        Page inputPage;
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(3)) {
            builder.appendLong(10L);
            builder.appendLong(20L);
            builder.appendLong(30L);
            inputPage = new Page(builder.build());
        }

        List<Page> outputPages = pushdownExecutor.execute(
            List.of(inputPage),
            pushdownPlan,
            new IndexedByShardIdFromSingleton<>(
                Mockito.mock(EsPhysicalOperationProviders.ShardContext.class, Mockito.withSettings().stubOnly())
            ),
            blockFactory,
            FoldContext.small()
        );

        try {
            assertThat(outputPages.size(), equalTo(1));
            Page output = outputPages.getFirst();
            assertThat(output.getPositionCount(), equalTo(2));
            assertThat(output.getBlockCount(), equalTo(2));
            assertThat(output.<LongBlock>getBlock(0).getLong(0), equalTo(20L));
            assertThat(output.<LongBlock>getBlock(0).getLong(1), equalTo(30L));
            assertThat(output.<IntBlock>getBlock(1).getInt(0), equalTo(1));
            assertThat(output.<IntBlock>getBlock(1).getInt(1), equalTo(2));
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(outputPages.iterator(), page -> page::releaseBlocks)));
        }
    }

    public void testExecutePushdownUsesSuppliedBlockFactory() {
        blockFactory = blockFactory();
        LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(
            blockFactory.breaker(),
            LocalCircuitBreaker.SizeSettings.DEFAULT_SETTINGS.overReservedBytes(),
            LocalCircuitBreaker.SizeSettings.DEFAULT_SETTINGS.maxOverReservedBytes()
        );
        BlockFactory exchangeBlockFactory = blockFactory.newChildFactory(localBreaker);
        RemoteFetchPushdownPlanExecutor pushdownExecutor = new RemoteFetchPushdownPlanExecutor(
            blockFactory.bigArrays(),
            LocalCircuitBreaker.SizeSettings.DEFAULT_SETTINGS
        );
        ReferenceAttribute fetchedAttribute = new ReferenceAttribute(Source.EMPTY, null, "n", DataType.LONG);
        ReferenceAttribute positionAttribute = new ReferenceAttribute(Source.EMPTY, null, "_remote_fetch_position", DataType.INTEGER);
        PhysicalPlan pushdownPlan = new FragmentExec(new RemoteFetchSource(Source.EMPTY, List.of(fetchedAttribute, positionAttribute)));
        Page inputPage;
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(1)) {
            builder.appendLong(10L);
            inputPage = new Page(builder.build());
        }

        List<Page> outputPages = pushdownExecutor.execute(
            List.of(inputPage),
            pushdownPlan,
            new IndexedByShardIdFromSingleton<>(
                Mockito.mock(EsPhysicalOperationProviders.ShardContext.class, Mockito.withSettings().stubOnly())
            ),
            exchangeBlockFactory,
            FoldContext.small()
        );

        try {
            assertThat(localBreaker.getUsed(), greaterThan(0L));
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(outputPages.iterator(), page -> page::releaseBlocks)));
            localBreaker.close();
        }
        assertThat(localBreaker.getUsed(), equalTo(0L));
    }

    public void testExecutePushdownUsesSuppliedFoldContext() {
        blockFactory = blockFactory();
        RemoteFetchPushdownPlanExecutor pushdownExecutor = new RemoteFetchPushdownPlanExecutor(
            blockFactory.bigArrays(),
            LocalCircuitBreaker.SizeSettings.DEFAULT_SETTINGS
        );
        ReferenceAttribute fetchedAttribute = new ReferenceAttribute(Source.EMPTY, null, "n", DataType.LONG);
        ReferenceAttribute positionAttribute = new ReferenceAttribute(Source.EMPTY, null, "_remote_fetch_position", DataType.INTEGER);
        FoldContext foldContext = new FoldContext(1234L);
        AtomicLong observedFoldLimit = new AtomicLong(-1L);
        PhysicalPlan pushdownPlan = new FragmentExec(
            new Eval(
                Source.EMPTY,
                new RemoteFetchSource(Source.EMPTY, List.of(fetchedAttribute, positionAttribute)),
                List.of(new Alias(Source.EMPTY, "tracked", new FoldContextTrackingExpression(Source.EMPTY, observedFoldLimit)))
            )
        );
        Page inputPage;
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(1)) {
            builder.appendLong(10L);
            inputPage = new Page(builder.build());
        }

        List<Page> outputPages = pushdownExecutor.execute(
            List.of(inputPage),
            pushdownPlan,
            new IndexedByShardIdFromSingleton<>(
                Mockito.mock(EsPhysicalOperationProviders.ShardContext.class, Mockito.withSettings().stubOnly())
            ),
            blockFactory,
            foldContext
        );

        try {
            assertThat(observedFoldLimit.get(), equalTo(foldContext.initialAllowedBytes()));
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(outputPages.iterator(), page -> page::releaseBlocks)));
        }
    }

    public void testExchangeSetupRequestRoundTripsPushdownPlanWithFieldAttributes() throws IOException {
        FieldAttribute fieldAttribute = new FieldAttribute(
            Source.EMPTY,
            "n",
            new EsField("n", DataType.LONG, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        Configuration configuration = ConfigurationTestUtils.randomConfiguration();
        PhysicalPlan pushdownPlan = new FragmentExec(new RemoteFetchSource(Source.EMPTY, List.of(fieldAttribute)));
        RemoteFetchService.ExchangeSetupRequest request = new RemoteFetchService.ExchangeSetupRequest(
            "session-1",
            List.of(new RemoteFetchService.FetchField("n", DataType.LONG)),
            pushdownPlan,
            configuration,
            "clientToServer-1",
            "serverToClient-1"
        );

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (
                StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), SerializationTestUtils.writableRegistry())
            ) {
                RemoteFetchService.ExchangeSetupRequest copy = new RemoteFetchService.ExchangeSetupRequest(in);
                assertThat(copy.retainedSessionId(), equalTo("session-1"));
                assertThat(copy.fields(), equalTo(List.of(new RemoteFetchService.FetchField("n", DataType.LONG))));
                assertThat(
                    copy.pushdownPlan() instanceof FragmentExec fragmentExec && fragmentExec.fragment() instanceof RemoteFetchSource,
                    equalTo(true)
                );
                assertThat(copy.configuration(), equalTo(configuration));
                assertThat(copy.clientToServerId(), equalTo("clientToServer-1"));
                assertThat(copy.serverToClientId(), equalTo("serverToClient-1"));
            }
        }
    }

    public void testExchangeSetupRequestRejectsUnsupportedPushdownPlan() {
        ReferenceAttribute fetchedAttribute = new ReferenceAttribute(Source.EMPTY, null, "n", DataType.LONG);
        ReferenceAttribute positionAttribute = new ReferenceAttribute(Source.EMPTY, null, "_remote_fetch_position", DataType.INTEGER);
        PhysicalPlan pushdownPlan = new LimitExec(
            Source.EMPTY,
            new FragmentExec(new RemoteFetchSource(Source.EMPTY, List.of(fetchedAttribute, positionAttribute))),
            new Literal(Source.EMPTY, 10, DataType.INTEGER),
            null
        );

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new RemoteFetchService.ExchangeSetupRequest(
                "session-1",
                List.of(new RemoteFetchService.FetchField("n", DataType.LONG)),
                pushdownPlan,
                ConfigurationTestUtils.randomConfiguration(),
                "clientToServer-1",
                "serverToClient-1"
            )
        );
        assertThat(exception.getMessage(), containsString("unsupported remote fetch pushdown plan [LimitExec]"));
    }

    public void testExchangeSetupRequestSerializesConfigurationWithoutTables() throws IOException {
        blockFactory = blockFactory();
        Configuration configuration;
        IntBlock values = blockFactory.newConstantIntBlockWith(1, 1);
        try (Column column = new Column(DataType.INTEGER, values)) {
            configuration = ConfigurationTestUtils.randomConfiguration("from test", Map.of("t", Map.of("v", column)));
            RemoteFetchService.ExchangeSetupRequest request = new RemoteFetchService.ExchangeSetupRequest(
                "session-1",
                List.of(new RemoteFetchService.FetchField("n", DataType.LONG)),
                null,
                configuration,
                "clientToServer-1",
                "serverToClient-1"
            );

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                request.writeTo(out);
                try (
                    StreamInput in = new NamedWriteableAwareStreamInput(
                        out.bytes().streamInput(),
                        SerializationTestUtils.writableRegistry()
                    )
                ) {
                    RemoteFetchService.ExchangeSetupRequest copy = new RemoteFetchService.ExchangeSetupRequest(in);
                    assertThat(copy.configuration().tables(), equalTo(Map.of()));
                    assertThat(copy.configuration(), equalTo(configuration.withoutTables()));
                }
            }
        }
    }

    public void testExchangeSetupRequestCreatesCancellableChildTask() {
        Configuration configuration = ConfigurationTestUtils.randomConfiguration();
        RemoteFetchService.ExchangeSetupRequest request = new RemoteFetchService.ExchangeSetupRequest(
            "session-1",
            List.of(new RemoteFetchService.FetchField("n", DataType.LONG)),
            null,
            configuration,
            "clientToServer-1",
            "serverToClient-1"
        );

        TaskId parentTaskId = new TaskId("coordinator-node", randomNonNegativeLong());
        Task task = request.createTask(1L, "transport", RemoteFetchService.EXCHANGE_SETUP_ACTION_NAME, parentTaskId, Map.of());

        assertThat(task instanceof CancellableTask, equalTo(true));
        assertThat(task.getParentTaskId(), equalTo(parentTaskId));
    }

    public void testExchangeSetupRequestRejectsEmptyFields() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new RemoteFetchService.ExchangeSetupRequest(
                "session-1",
                List.of(),
                null,
                ConfigurationTestUtils.randomConfiguration(),
                "clientToServer-1",
                "serverToClient-1"
            )
        );
        assertThat(exception.getMessage(), containsString("remote fetch requires at least one request field"));
    }

    public void testRetainedSessionReleaserDeduplicatesReleases() {
        DiscoveryNode node = DiscoveryNodeUtils.create("node-1");
        List<String> released = new ArrayList<>();
        RemoteFetchService.RetainedSessionReleaser releaser = new RemoteFetchService.RetainedSessionReleaser(
            (targetNode, sessionId) -> released.add(targetNode.getId() + "/" + sessionId)
        );

        releaser.track(node, "session-1");
        releaser.track(node, "session-1");
        releaser.track(node, "session-2");

        releaser.close();

        assertThat(released, equalTo(List.of("node-1/session-1", "node-1/session-2")));
    }

    public void testRetainedSessionReleaserCanReleaseOneTrackedSession() {
        DiscoveryNode node = DiscoveryNodeUtils.create("node-1");
        List<String> released = new ArrayList<>();
        RemoteFetchService.RetainedSessionReleaser releaser = new RemoteFetchService.RetainedSessionReleaser(
            (targetNode, sessionId) -> released.add(targetNode.getId() + "/" + sessionId)
        );

        releaser.track(node, "session-1");
        releaser.track(node, "session-2");
        releaser.release(node, "session-1");
        releaser.release(node, "session-1");
        releaser.close();

        assertThat(released, equalTo(List.of("node-1/session-1", "node-1/session-2")));
    }

    public void testRetainedSessionReleaserReleasesImmediatelyAfterClose() {
        DiscoveryNode node = DiscoveryNodeUtils.create("node-1");
        List<String> released = new ArrayList<>();
        RemoteFetchService.RetainedSessionReleaser releaser = new RemoteFetchService.RetainedSessionReleaser(
            (targetNode, sessionId) -> released.add(targetNode.getId() + "/" + sessionId)
        );

        releaser.close();
        releaser.track(node, "session-1");

        assertThat(released, equalTo(List.of("node-1/session-1")));
    }

    public void testBatchExchangeClientCloseReleasesTrackedSessions() {
        DiscoveryNode node = DiscoveryNodeUtils.create("node-1");
        List<String> released = new ArrayList<>();
        RemoteFetchService.RetainedSessionReleaser releaser = new RemoteFetchService.RetainedSessionReleaser(
            (targetNode, sessionId) -> released.add(targetNode.getId() + "/" + sessionId)
        );
        releaser.track(node, "session-1");
        releaser.track(node, "session-1");

        RemoteFetchService.Client client = remoteFetchService().newBatchExchangeClient(Mockito.mock(CancellableTask.class), releaser);
        client.close();
        client.close();

        assertThat(released, equalTo(List.of("node-1/session-1")));
    }

    public void testRetainedSearchContextsReaperExpiresIdleRegistrations() {
        long[] now = new long[] { 0L };
        RetainedSearchContextsRegistry registry = new RetainedSearchContextsRegistry(() -> now[0], TimeValue.timeValueMillis(10));
        List<Runnable> scheduledCommands = new ArrayList<>();
        RemoteFetchService service = remoteFetchService(registry, scheduledCommands);
        SearchContext searchContext = new TestSearchContext(Mockito.mock(SearchExecutionContext.class, Mockito.withSettings().stubOnly()));

        RetainedSearchContextsRegistry.Handle registration = service.retainSearchContexts("session-1", createContexts(searchContext));
        registration.finishRegistration();
        now[0] = 11L;
        scheduledCommands.forEach(Runnable::run);

        assertThat(service.retainedSessions(), equalTo(0));
        assertTrue(searchContext.isClosed());
    }

    public void testRetainedSearchContextsReaperDoesNotExpireActiveRegistration() {
        long[] now = new long[] { 0L };
        RetainedSearchContextsRegistry registry = new RetainedSearchContextsRegistry(() -> now[0], TimeValue.timeValueMillis(10));
        List<Runnable> scheduledCommands = new ArrayList<>();
        RemoteFetchService service = remoteFetchService(registry, scheduledCommands);
        SearchContext searchContext = new TestSearchContext(Mockito.mock(SearchExecutionContext.class, Mockito.withSettings().stubOnly()));
        RetainedSearchContextsRegistry.Handle registration = service.retainSearchContexts("session-1", createContexts(searchContext));

        now[0] = 11L;
        scheduledCommands.forEach(Runnable::run);

        assertThat(service.retainedSessions(), equalTo(1));
        assertFalse(searchContext.isClosed());
        registration.close();
        assertTrue(searchContext.isClosed());
    }

    public void testStartExchangeFetchServerReleasesLeaseWhenSetupFails() {
        RetainedSearchContextsRegistry registry = new RetainedSearchContextsRegistry();
        RuntimeException setupFailure = new RuntimeException("setup failed");
        RemoteFetchService service = remoteFetchService(
            registry,
            new ArrayList<>(),
            (a, b, c, d, e, f, g, h, i, j) -> { throw setupFailure; }
        );
        SearchContext searchContext = new TestSearchContext(Mockito.mock(SearchExecutionContext.class, Mockito.withSettings().stubOnly()));
        RetainedSearchContextsRegistry.Handle registration = service.retainSearchContexts("session-1", createContexts(searchContext));
        AtomicReference<Exception> failure = new AtomicReference<>();

        service.startExchangeFetchServer(
            new RemoteFetchService.ExchangeSetupRequest(
                "session-1",
                List.of(new RemoteFetchService.FetchField("n", DataType.LONG)),
                null,
                ConfigurationTestUtils.randomConfiguration(),
                "clientToServer-1",
                "serverToClient-1"
            ),
            new CancellableTask(
                1,
                "transport",
                RemoteFetchService.EXCHANGE_SETUP_ACTION_NAME,
                "",
                new TaskId("coordinator-node", 1),
                Map.of()
            ),
            ActionListener.wrap(ignored -> fail("setup should have failed"), failure::set)
        );

        assertThat(failure.get(), equalTo(setupFailure));
        registration.close();
        assertTrue(searchContext.isClosed());
    }

    private static void addDoc(IndexWriter writer, String keyword, long number) throws IOException {
        Document document = new Document();
        document.add(new SortedDocValuesField("k", new BytesRef(keyword)));
        document.add(new NumericDocValuesField("n", number));
        writer.addDocument(document);
    }

    private BlockFactory blockFactory() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(4)).withCircuitBreaking();
        return BlockFactory.builder(bigArrays).build();
    }

    private static RemoteFetchService remoteFetchService() {
        return remoteFetchService(new RetainedSearchContextsRegistry(), new ArrayList<>());
    }

    private static RemoteFetchService remoteFetchService(RetainedSearchContextsRegistry registry, List<Runnable> scheduledCommands) {
        return remoteFetchService(registry, scheduledCommands, BidirectionalBatchExchangeServer::new);
    }

    private static RemoteFetchService remoteFetchService(
        RetainedSearchContextsRegistry registry,
        List<Runnable> scheduledCommands,
        RemoteFetchService.ExchangeServerFactory exchangeServerFactory
    ) {
        TransportService transportService = Mockito.mock(TransportService.class);
        ThreadPool threadPool = Mockito.mock(ThreadPool.class);
        Mockito.when(threadPool.executor(Mockito.anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        Mockito.when(threadPool.scheduleWithFixedDelay(Mockito.any(Runnable.class), Mockito.any(), Mockito.any()))
            .thenAnswer(invocation -> {
                scheduledCommands.add(invocation.getArgument(0));
                return Mockito.mock(Scheduler.Cancellable.class);
            });
        Mockito.when(transportService.getThreadPool()).thenReturn(threadPool);

        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        Mockito.when(clusterService.getClusterName()).thenReturn(ClusterName.DEFAULT);
        DiscoveryNode coordinatorNode = DiscoveryNodeUtils.create("coordinator-node");
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(coordinatorNode).localNodeId(coordinatorNode.getId()).masterNodeId(coordinatorNode.getId()))
            .build();
        Mockito.when(clusterService.state()).thenReturn(clusterState);
        ExchangeService exchangeService = new ExchangeService(
            Settings.EMPTY,
            threadPool,
            ThreadPool.Names.SEARCH,
            TestBlockFactory.getNonBreakingInstance()
        );

        TransportActionServices services = new TransportActionServices(
            transportService,
            null,
            exchangeService,
            clusterService,
            null,
            null,
            null,
            null,
            null,
            null,
            Mockito.mock(PlannerSettings.Holder.class),
            null
        );
        return new RemoteFetchService(
            services,
            BigArrays.NON_RECYCLING_INSTANCE,
            TestBlockFactory.getNonBreakingInstance(),
            registry,
            exchangeServerFactory
        );
    }

    private static AcquiredSearchContexts createContexts(SearchContext searchContext) {
        AcquiredSearchContexts contexts = new AcquiredSearchContexts(1);
        contexts.newSubRangeView(List.of(searchContext));
        return contexts;
    }

    private static class FoldContextTrackingExpression extends LeafExpression implements EvaluatorMapper {
        private final AtomicLong observedFoldLimit;

        FoldContextTrackingExpression(Source source, AtomicLong observedFoldLimit) {
            super(source);
            this.observedFoldLimit = observedFoldLimit;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Nullability nullable() {
            return Nullability.FALSE;
        }

        @Override
        public DataType dataType() {
            return DataType.INTEGER;
        }

        @Override
        protected NodeInfo<? extends Expression> info() {
            return NodeInfo.create(this, FoldContextTrackingExpression::new, observedFoldLimit);
        }

        @Override
        public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
            observedFoldLimit.set(toEvaluator.foldCtx().initialAllowedBytes());
            return driverContext -> new ExpressionEvaluator() {
                @Override
                public Block eval(Page page) {
                    return driverContext.blockFactory().newConstantIntBlockWith(1, page.getPositionCount());
                }

                @Override
                public long baseRamBytesUsed() {
                    return 0;
                }

                @Override
                public void close() {}
            };
        }
    }

    @After
    public void tearDownResources() throws Exception {
        IOUtils.close(reader, directory);
        if (blockFactory != null) {
            assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
            assertThat(blockFactory.breaker().getTrippedCount(), equalTo(0L));
            assertThat(blockFactory.breaker().getName(), equalTo(CircuitBreaker.REQUEST));
        }
        MockBigArrays.ensureAllArraysAreReleased();
        super.tearDown();
    }
}
