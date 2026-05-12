/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BatchMetadata;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.EvalOperatorFactory;
import org.elasticsearch.compute.operator.FilterOperator.FilterOperatorFactory;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.ProjectOperator.ProjectOperatorFactory;
import org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeClient;
import org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeServer;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchSourceExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Internal transport service that fetches field values for coordinator-selected rows from the owning data node.
 * <p>
 * This is the transport half of the remote late-materialization prototype. It intentionally works on a narrow v1
 * contract: a batch of {@link RemoteFetchHandle}s plus a list of plain field specifications to load.
 */
public final class RemoteFetchService {
    private static final String ACTION_PREFIX = EsqlQueryAction.NAME + "/remote_fetch";
    static final String RELEASE_ACTION_NAME = ACTION_PREFIX + "/release";
    static final String EXCHANGE_SETUP_ACTION_NAME = ACTION_PREFIX + "/exchange_setup";

    private static final Logger logger = LogManager.getLogger(RemoteFetchService.class);
    private static final AtomicLong exchangeIdGenerator = new AtomicLong();

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final ExchangeService exchangeService;
    private final BigArrays bigArrays;
    private final BlockFactory blockFactory;
    private final PlannerSettings.Holder plannerSettings;
    private final LocalCircuitBreaker.SizeSettings localBreakerSettings;
    private final RetainedSearchContextsRegistry retainedSearchContexts = new RetainedSearchContextsRegistry();

    RemoteFetchService(TransportActionServices transportActionServices, BigArrays bigArrays, BlockFactory blockFactory) {
        this.clusterService = transportActionServices.clusterService();
        this.transportService = transportActionServices.transportService();
        this.exchangeService = transportActionServices.exchangeService();
        this.bigArrays = bigArrays;
        this.blockFactory = blockFactory;
        this.plannerSettings = transportActionServices.plannerSettings();
        this.localBreakerSettings = new LocalCircuitBreaker.SizeSettings(clusterService.getSettings());
        transportService.registerRequestHandler(
            RELEASE_ACTION_NAME,
            transportService.getThreadPool().executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME),
            ReleaseRequest::new,
            new ReleaseTransportHandler()
        );
        transportService.registerRequestHandler(
            EXCHANGE_SETUP_ACTION_NAME,
            transportService.getThreadPool().executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME),
            ExchangeSetupRequest::new,
            new ExchangeSetupTransportHandler()
        );
    }

    RetainedSearchContextsRegistry.Registration retainSearchContexts(String sessionId, AcquiredSearchContexts searchContexts) {
        return retainedSearchContexts.register(sessionId, searchContexts);
    }

    void releaseAsync(DiscoveryNode targetNode, String sessionId, ActionListener<Void> listener) {
        transportService.sendRequest(
            targetNode,
            RELEASE_ACTION_NAME,
            new ReleaseRequest(sessionId),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(
                listener.map(ignored -> null),
                in -> ActionResponse.Empty.INSTANCE,
                transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
            )
        );
    }

    public ThreadContext threadContext() {
        return transportService.getThreadPool().getThreadContext();
    }

    public RemoteFetchOperator.Client newBatchExchangeClient(CancellableTask parentTask) {
        return new BatchExchangeFetchClient(parentTask);
    }

    private Page buildHandlesPage(List<RemoteFetchHandle> handles, long batchId) {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(handles.size())) {
            for (RemoteFetchHandle handle : handles) {
                builder.appendBytesRef(handle.toBytesRef());
            }
            return new Page(new BatchMetadata(batchId, 0, true), builder.build());
        }
    }

    private static Page stripBatchMetadata(Page page) {
        Block[] blocks = new Block[page.getBlockCount()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = page.getBlock(i);
            blocks[i].incRef();
        }
        return new Page(page.getPositionCount(), blocks);
    }

    private static Configuration readConfiguration(StreamInput in) throws IOException {
        return new Configuration(
            // TODO make Configuration Releasable
            new BlockStreamInput(
                in,
                BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker(CircuitBreaker.REQUEST)).build()
            )
        );
    }

    TrackedSessions trackedSessions() {
        return new TrackedSessions(this);
    }

    private final class BatchExchangeFetchClient implements RemoteFetchOperator.Client {
        private final CancellableTask parentTask;
        private final Executor executor = transportService.getThreadPool().executor(ThreadPool.Names.SEARCH);
        private final Map<TargetSession, TargetState> targets = new HashMap<>();
        private volatile boolean closed;

        private BatchExchangeFetchClient(CancellableTask parentTask) {
            this.parentTask = parentTask;
        }

        @Override
        public void fetchAsync(String nodeId, Request request, ActionListener<List<Page>> listener) {
            if (closed) {
                listener.onFailure(new IllegalStateException("remote fetch exchange client is closed"));
                return;
            }
            TargetSession target = new TargetSession(nodeId, request.sessionId());
            final TargetState state;
            synchronized (this) {
                state = targets.computeIfAbsent(
                    target,
                    t -> createTargetState(t, request.fields(), request.pushdownPlan(), request.configuration())
                );
            }
            ActionListener<List<Page>> cleanupListener = ActionListener.wrap(listener::onResponse, e -> {
                removeTargetState(target, state);
                listener.onFailure(e);
            });
            executor.execute(() -> state.fetch(request, cleanupListener));
        }

        @Override
        public void close() {
            Map<TargetSession, TargetState> states;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                states = new HashMap<>(targets);
                targets.clear();
            }
            for (TargetState state : states.values()) {
                state.close();
            }
        }

        private TargetState createTargetState(
            TargetSession target,
            List<FetchField> fields,
            PhysicalPlan pushdownPlan,
            Configuration configuration
        ) {
            DiscoveryNode node = clusterService.state().nodes().get(target.nodeId());
            if (node == null) {
                throw new IllegalStateException("remote fetch target node [" + target.nodeId() + "] not found");
            }
            BidirectionalBatchExchangeClient.ServerSetupCallback setupCallback = (
                serverNode,
                clientToServerId,
                serverToClientId,
                setupListener) -> {
                ExchangeSetupRequest setupRequest = new ExchangeSetupRequest(
                    target.sessionId(),
                    fields,
                    pushdownPlan,
                    configuration,
                    clientToServerId,
                    serverToClientId
                );
                transportService.sendChildRequest(
                    serverNode,
                    EXCHANGE_SETUP_ACTION_NAME,
                    setupRequest,
                    parentTask,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(
                        setupListener.map(ignored -> (String) null),
                        in -> ActionResponse.Empty.INSTANCE,
                        transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
                    )
                );
            };
            BidirectionalBatchExchangeClient client = new BidirectionalBatchExchangeClient(
                target.sessionId() + "/remote-fetch/" + exchangeIdGenerator.incrementAndGet(),
                exchangeService,
                transportService.getThreadPool().executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME),
                Math.max(1, fields.size() + 1),
                transportService,
                parentTask,
                ActionListener.noop(),
                clusterService.getSettings(),
                setupCallback,
                null,
                1,
                () -> node
            );
            return new TargetState(client, fields, pushdownPlan, configuration);
        }

        private void removeTargetState(TargetSession target, TargetState expectedState) {
            boolean removed;
            synchronized (this) {
                removed = targets.remove(target, expectedState);
            }
            if (removed) {
                expectedState.close();
            }
        }
    }

    private final class TargetState {
        private final BidirectionalBatchExchangeClient client;
        private final List<FetchField> fields;
        private final PhysicalPlan pushdownPlan;
        private final Configuration configuration;
        private final AtomicLong batchIdGenerator = new AtomicLong();
        private final Object lock = new Object();
        private boolean closed;

        private TargetState(
            BidirectionalBatchExchangeClient client,
            List<FetchField> fields,
            PhysicalPlan pushdownPlan,
            Configuration configuration
        ) {
            this.client = client;
            this.fields = List.copyOf(fields);
            this.pushdownPlan = pushdownPlan;
            this.configuration = configuration;
        }

        void fetch(Request request, ActionListener<List<Page>> listener) {
            synchronized (lock) {
                try {
                    if (closed) {
                        throw new IllegalStateException("remote fetch target state is closed");
                    }
                    if (fields.equals(request.fields()) == false) {
                        throw new IllegalStateException("remote fetch fields differ for reused target session channel");
                    }
                    if (Objects.equals(pushdownPlan, request.pushdownPlan()) == false) {
                        throw new IllegalStateException("remote fetch pushdown plan differs for reused target session channel");
                    }
                    if (configuration.equals(request.configuration()) == false) {
                        throw new IllegalStateException("remote fetch configuration differs for reused target session channel");
                    }
                    long batchId = batchIdGenerator.incrementAndGet();
                    Page handlesPage = buildHandlesPage(request.handles(), batchId);
                    client.sendPage(handlesPage);
                    listener.onResponse(collectFetchedPagesForBatch(client, batchId));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        }

        void close() {
            synchronized (lock) {
                if (closed) {
                    return;
                }
                closed = true;
                try {
                    client.finish();
                    client.finishCollectingResponseHeaders();
                    client.close();
                } catch (Exception e) {
                    logger.debug("failed to close remote fetch target state", e);
                }
            }
        }
    }

    private record TargetSession(String nodeId, String sessionId) {}

    private List<Page> collectFetchedPagesForBatch(BidirectionalBatchExchangeClient client, long batchId) throws Exception {
        List<Page> pages = new ArrayList<>();
        boolean completed = false;
        while (completed == false) {
            Page page = client.pollPage();
            if (page != null) {
                try {
                    BatchMetadata metadata = page.batchMetadata();
                    if (metadata == null || metadata.batchId() != batchId) {
                        throw new IllegalStateException("received unexpected batch metadata while awaiting batch [" + batchId + "]");
                    }
                    if (page.getPositionCount() > 0) {
                        pages.add(stripBatchMetadata(page));
                    }
                    if (metadata.isLastPageInBatch()) {
                        client.markBatchCompleted(batchId);
                        completed = true;
                    }
                } finally {
                    page.releaseBlocks();
                }
                continue;
            }
            Exception failure = client.getPrimaryFailure();
            if (failure != null) {
                throw failure;
            }
            IsBlockedResult blocked = client.waitUntilPageReady();
            if (blocked.listener().isDone() == false) {
                PlainActionFuture<Void> waitFuture = new PlainActionFuture<>();
                blocked.listener().addListener(waitFuture);
                FutureUtils.get(waitFuture);
            }
        }
        return pages;
    }

    int retainedSessions() {
        return retainedSearchContexts.retainedSessions();
    }

    boolean isRegistered(String sessionId) {
        return retainedSearchContexts.isRegistered(sessionId);
    }

    private void releaseSession(String sessionId) {
        retainedSearchContexts.closeRegistration(sessionId);
    }

    private void releaseBestEffort(DiscoveryNode targetNode, String sessionId) {
        releaseAsync(targetNode, sessionId, ActionListener.wrap(ignored -> {}, e -> {
            logger.debug("failed to release retained remote fetch session [{}] on node [{}]", sessionId, targetNode.getId(), e);
        }));
    }

    private void startExchangeFetchServer(ExchangeSetupRequest request, CancellableTask task, ActionListener<Void> listener) {
        final RetainedSearchContextsRegistry.Lease lease;
        try {
            lease = retainedSearchContexts.acquire(request.sessionId());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        final DiscoveryNode clientNode;
        try {
            clientNode = determineClientNode(task);
        } catch (Exception e) {
            lease.close();
            listener.onFailure(e);
            return;
        }
        final PlannerSettings settings = plannerSettings.get();
        final IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts = lease.searchContexts()
            .map(ComputeSearchContext::shardContext);
        final LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(
            blockFactory.breaker(),
            localBreakerSettings.overReservedBytes(),
            localBreakerSettings.maxOverReservedBytes()
        );
        final DriverContext driverContext = new DriverContext(
            bigArrays,
            blockFactory.newChildFactory(localBreaker),
            localBreakerSettings,
            "remote_fetch_exchange"
        );
        final BidirectionalBatchExchangeServer server = new BidirectionalBatchExchangeServer(
            request.clientToServerId(),
            request.clientToServerId(),
            request.serverToClientId(),
            exchangeService,
            transportService.getThreadPool().executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME),
            Math.max(1, request.fields().size()),
            transportService,
            task,
            clientNode,
            clusterService.getSettings()
        );
        final Releasable releasable = Releasables.wrap(server, lease, localBreaker);
        List<Operator> intermediate = List.of(
            new RemoteFetchBatchOperator(request.fields(), shardContexts, settings, request.pushdownPlan(), this)
        );
        server.startWithOperators(
            driverContext,
            transportService.getThreadPool().getThreadContext(),
            intermediate,
            clusterService.getClusterName().value(),
            releasable,
            listener
        );
    }

    private DiscoveryNode determineClientNode(CancellableTask task) {
        if (task == null) {
            throw new IllegalStateException("cannot determine remote fetch exchange client node: task is null");
        }
        if (task.getParentTaskId().isSet() == false) {
            throw new IllegalStateException("cannot determine remote fetch exchange client node: parent task is not set");
        }
        String nodeId = task.getParentTaskId().getNodeId();
        DiscoveryNode node = clusterService.state().nodes().get(nodeId);
        if (node == null) {
            throw new IllegalStateException("remote fetch exchange client node [" + nodeId + "] not found");
        }
        return node;
    }

    List<Page> executeFetchForExchange(
        List<RemoteFetchHandle> handles,
        List<FetchField> fields,
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        PlannerSettings settings
    ) {
        List<ValuesSourceReaderOperator.FieldInfo> fieldInfos = buildFieldInfos(fields, shardContexts, settings);
        IndexedByShardId<ValuesSourceReaderOperator.ShardContext> readerContexts = shardContexts.map(
            c -> new ValuesSourceReaderOperator.ShardContext(
                c.searcher().getIndexReader(),
                c::newSourceLoader,
                c.storedFieldsSequentialProportion()
            )
        );
        return executeFetch(handles, fieldInfos, readerContexts, bigArrays, blockFactory, localBreakerSettings, settings);
    }

    List<Page> executePushdownForExchange(
        List<Page> pages,
        PhysicalPlan pushdownPlan,
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts
    ) {
        if (pushdownPlan == null || pages.isEmpty()) {
            return pages;
        }
        List<Page> current = appendPositionColumn(pages);
        List<Operator.OperatorFactory> factories = new ArrayList<>();
        compilePushdownFactories(pushdownPlan, factories, shardContexts);
        for (Operator.OperatorFactory factory : factories) {
            List<Page> next = new ArrayList<>();
            Operator operator = factory.get(new DriverContext(bigArrays, blockFactory, localBreakerSettings, "remote_fetch_pushdown"));
            try {
                for (Page page : current) {
                    operator.addInput(page);
                    Page output;
                    while ((output = operator.getOutput()) != null) {
                        output.allowPassingToDifferentDriver();
                        next.add(output);
                    }
                }
                operator.finish();
                Page output;
                while ((output = operator.getOutput()) != null) {
                    output.allowPassingToDifferentDriver();
                    next.add(output);
                }
            } finally {
                operator.close();
            }
            current = next;
        }
        return current;
    }

    private List<Page> appendPositionColumn(List<Page> pages) {
        List<Page> withPosition = new ArrayList<>(pages.size());
        int position = 0;
        for (Page page : pages) {
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(page.getPositionCount())) {
                for (int row = 0; row < page.getPositionCount(); row++) {
                    builder.appendInt(position++);
                }
                withPosition.add(page.appendBlock(builder.build()));
            }
        }
        return withPosition;
    }

    private Layout compilePushdownFactories(
        PhysicalPlan plan,
        List<Operator.OperatorFactory> factories,
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts
    ) {
        if (plan instanceof RemoteFetchSourceExec sourceExec) {
            Layout.Builder builder = new Layout.Builder();
            builder.append(sourceExec.output());
            return builder.build();
        }
        if (plan instanceof EvalExec evalExec) {
            Layout childLayout = compilePushdownFactories(evalExec.child(), factories, shardContexts);
            Layout.Builder builder = childLayout.builder();
            for (Alias field : evalExec.fields()) {
                factories.add(
                    new EvalOperatorFactory(EvalMapper.toEvaluator(FoldContext.small(), field.child(), childLayout, shardContexts))
                );
                builder.append(field.toAttribute());
            }
            return builder.build();
        }
        if (plan instanceof FilterExec filterExec) {
            Layout childLayout = compilePushdownFactories(filterExec.child(), factories, shardContexts);
            factories.add(
                new FilterOperatorFactory(EvalMapper.toEvaluator(FoldContext.small(), filterExec.condition(), childLayout, shardContexts))
            );
            return childLayout;
        }
        if (plan instanceof ProjectExec projectExec) {
            Layout childLayout = compilePushdownFactories(projectExec.child(), factories, shardContexts);
            List<Integer> projectionList = new ArrayList<>(projectExec.projections().size());
            Layout.Builder builder = new Layout.Builder();
            for (NamedExpression projection : projectExec.projections()) {
                NameId inputId = projection instanceof Alias alias ? ((NamedExpression) alias.child()).id() : projection.id();
                Layout.ChannelAndType input = childLayout.get(inputId);
                if (input == null) {
                    throw new IllegalStateException("can't find pushdown input for [" + projection + "]");
                }
                projectionList.add(input.channel());
                builder.append(projection);
            }
            factories.add(new ProjectOperatorFactory(projectionList));
            return builder.build();
        }
        throw new IllegalStateException("unsupported remote fetch pushdown plan [" + plan.getClass().getSimpleName() + "]");
    }

    static List<ValuesSourceReaderOperator.FieldInfo> buildFieldInfos(
        List<FetchField> fields,
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        PlannerSettings plannerSettings
    ) {
        List<ValuesSourceReaderOperator.FieldInfo> fieldInfos = new ArrayList<>(fields.size());
        for (FetchField field : fields) {
            fieldInfos.add(
                new ValuesSourceReaderOperator.FieldInfo(
                    field.fieldName(),
                    PlannerUtils.toElementType(field.dataType()),
                    false,
                    (warningsMode, shardIdx) -> {
                        BlockLoader loader = shardContexts.get(shardIdx)
                            .blockLoader(
                                field.fieldName(),
                                field.dataType() == DataType.UNSUPPORTED,
                                MappedFieldType.FieldExtractPreference.NONE,
                                null,
                                null,
                                plannerSettings.blockLoaderSizeOrdinals(),
                                plannerSettings.blockLoaderSizeScript()
                            );
                        return ValuesSourceReaderOperator.load(loader);
                    }
                )
            );
        }
        return fieldInfos;
    }

    static List<Page> executeFetch(
        List<RemoteFetchHandle> handles,
        List<ValuesSourceReaderOperator.FieldInfo> fieldInfos,
        IndexedByShardId<ValuesSourceReaderOperator.ShardContext> shardContexts,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        LocalCircuitBreaker.SizeSettings localBreakerSettings,
        PlannerSettings plannerSettings
    ) {
        if (handles.isEmpty()) {
            return List.of();
        }
        if (fieldInfos.isEmpty()) {
            throw new IllegalArgumentException("remote fetch requires at least one field");
        }

        final LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(
            blockFactory.breaker(),
            localBreakerSettings.overReservedBytes(),
            localBreakerSettings.maxOverReservedBytes()
        );
        final DriverContext driverContext = new DriverContext(
            bigArrays,
            blockFactory.newChildFactory(localBreaker),
            localBreakerSettings,
            "remote_fetch"
        );
        final Operator operator = new ValuesSourceReaderOperator.Factory(
            plannerSettings.valuesLoadingJumboSize(),
            fieldInfos,
            shardContexts,
            fieldInfos.size() <= plannerSettings.reuseColumnLoadersThreshold(),
            0,
            plannerSettings.sourceReservationFactor(),
            plannerSettings.docSequenceBytesRefFieldThreshold()
        ).get(driverContext);
        final int[] projection = new int[fieldInfos.size()];
        for (int i = 0; i < projection.length; i++) {
            projection[i] = i + 1;
        }
        Page inputPage = inputPage(driverContext.blockFactory(), handles);
        boolean releaseInputPage = true;
        boolean success = false;
        List<Page> outputPages = new ArrayList<>();
        try {
            operator.addInput(inputPage);
            releaseInputPage = false;
            operator.finish();
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page == null) {
                    throw new IllegalStateException("remote fetch operator stalled without producing output");
                }
                Page projected = page.projectBlocks(projection);
                page.releaseBlocks();
                projected.allowPassingToDifferentDriver();
                outputPages.add(projected);
            }
            success = true;
            return outputPages;
        } finally {
            Releasables.closeExpectNoException(operator, localBreaker);
            if (releaseInputPage) {
                inputPage.releaseBlocks();
            }
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(outputPages.iterator(), page -> page::releaseBlocks)));
            }
        }
    }

    static Page inputPage(BlockFactory blockFactory, List<RemoteFetchHandle> handles) {
        try (DocVector.FixedBuilder builder = DocVector.newFixedBuilder(blockFactory, handles.size())) {
            for (RemoteFetchHandle handle : handles) {
                builder.append(handle.shard(), handle.segment(), handle.doc());
            }
            return new Page(builder.build(DocVector.config()).asBlock());
        }
    }

    public static final class FetchField implements Writeable {
        private final String fieldName;
        private final DataType dataType;

        public FetchField(String fieldName, DataType dataType) {
            this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
            this.dataType = Objects.requireNonNull(dataType, "dataType");
        }

        FetchField(StreamInput in) throws IOException {
            this(in.readString(), DataType.fromTypeName(in.readString()));
        }

        public String fieldName() {
            return fieldName;
        }

        public DataType dataType() {
            return dataType;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(fieldName);
            out.writeString(dataType.typeName());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof FetchField == false) {
                return false;
            }
            FetchField other = (FetchField) obj;
            return fieldName.equals(other.fieldName) && dataType == other.dataType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, dataType);
        }

        @Override
        public String toString() {
            return fieldName + ":" + dataType.typeName();
        }
    }

    public static final class Request extends AbstractTransportRequest {
        private final String sessionId;
        private final List<FetchField> fields;
        private final List<RemoteFetchHandle> handles;
        private final PhysicalPlan pushdownPlan;
        private final Configuration configuration;

        public Request(String sessionId, List<FetchField> fields, List<RemoteFetchHandle> handles, Configuration configuration) {
            this(sessionId, fields, handles, null, configuration);
        }

        public Request(
            String sessionId,
            List<FetchField> fields,
            List<RemoteFetchHandle> handles,
            PhysicalPlan pushdownPlan,
            Configuration configuration
        ) {
            this.sessionId = Objects.requireNonNull(sessionId, "sessionId");
            this.fields = List.copyOf(fields);
            this.handles = List.copyOf(handles);
            this.pushdownPlan = pushdownPlan;
            this.configuration = Objects.requireNonNull(configuration, "configuration");
        }

        Request(StreamInput in) throws IOException {
            super(in);
            this.sessionId = in.readString();
            this.fields = in.readCollectionAsList(FetchField::new);
            this.handles = in.readCollectionAsList(RemoteFetchHandle.READER);
            this.configuration = readConfiguration(in);
            PlanStreamInput pin = new PlanStreamInput(in, in.namedWriteableRegistry(), configuration);
            this.pushdownPlan = pin.readOptionalNamedWriteable(PhysicalPlan.class);
        }

        String sessionId() {
            return sessionId;
        }

        List<FetchField> fields() {
            return fields;
        }

        List<RemoteFetchHandle> handles() {
            return handles;
        }

        PhysicalPlan pushdownPlan() {
            return pushdownPlan;
        }

        Configuration configuration() {
            return configuration;
        }

        void validateForNode(String localNodeId) {
            if (fields.isEmpty()) {
                throw new IllegalArgumentException("remote fetch requires at least one field");
            }
            for (RemoteFetchHandle handle : handles) {
                if (sessionId.equals(handle.sessionId()) == false) {
                    throw new IllegalArgumentException(
                        "remote fetch request session [" + sessionId + "] does not match handle session [" + handle.sessionId() + "]"
                    );
                }
                if (localNodeId.equals(handle.nodeId()) == false) {
                    throw new IllegalArgumentException(
                        "remote fetch handle node [" + handle.nodeId() + "] does not match local node [" + localNodeId + "]"
                    );
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sessionId);
            out.writeCollection(fields);
            out.writeCollection(handles);
            configuration.writeTo(out);
            new PlanStreamOutput(out, configuration).writeOptionalNamedWriteable(pushdownPlan);
        }

        @Override
        public String toString() {
            return "RemoteFetchRequest[session=" + sessionId + ", fields=" + fields + ", handles=" + handles.size() + "]";
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof Request == false) {
                return false;
            }
            Request other = (Request) obj;
            return sessionId.equals(other.sessionId)
                && fields.equals(other.fields)
                && handles.equals(other.handles)
                && Objects.equals(pushdownPlan, other.pushdownPlan)
                && configuration.equals(other.configuration);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sessionId, fields, handles, pushdownPlan, configuration);
        }
    }

    static final class ReleaseRequest extends AbstractTransportRequest {
        private final String sessionId;

        ReleaseRequest(String sessionId) {
            this.sessionId = Objects.requireNonNull(sessionId, "sessionId");
        }

        ReleaseRequest(StreamInput in) throws IOException {
            super(in);
            this.sessionId = in.readString();
        }

        String sessionId() {
            return sessionId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sessionId);
        }
    }

    static final class ExchangeSetupRequest extends AbstractTransportRequest {
        private final String sessionId;
        private final List<FetchField> fields;
        private final PhysicalPlan pushdownPlan;
        private final Configuration configuration;
        private final String clientToServerId;
        private final String serverToClientId;

        ExchangeSetupRequest(
            String sessionId,
            List<FetchField> fields,
            PhysicalPlan pushdownPlan,
            Configuration configuration,
            String clientToServerId,
            String serverToClientId
        ) {
            this.sessionId = Objects.requireNonNull(sessionId);
            this.fields = List.copyOf(fields);
            this.pushdownPlan = pushdownPlan;
            this.configuration = Objects.requireNonNull(configuration);
            this.clientToServerId = Objects.requireNonNull(clientToServerId);
            this.serverToClientId = Objects.requireNonNull(serverToClientId);
        }

        ExchangeSetupRequest(StreamInput in) throws IOException {
            super(in);
            this.sessionId = in.readString();
            this.fields = in.readCollectionAsList(FetchField::new);
            this.configuration = readConfiguration(in);
            PlanStreamInput pin = new PlanStreamInput(in, in.namedWriteableRegistry(), configuration);
            this.pushdownPlan = pin.readOptionalNamedWriteable(PhysicalPlan.class);
            this.clientToServerId = in.readString();
            this.serverToClientId = in.readString();
        }

        String sessionId() {
            return sessionId;
        }

        List<FetchField> fields() {
            return fields;
        }

        PhysicalPlan pushdownPlan() {
            return pushdownPlan;
        }

        Configuration configuration() {
            return configuration;
        }

        String clientToServerId() {
            return clientToServerId;
        }

        String serverToClientId() {
            return serverToClientId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sessionId);
            out.writeCollection(fields);
            configuration.writeTo(out);
            new PlanStreamOutput(out, configuration).writeOptionalNamedWriteable(pushdownPlan);
            out.writeString(clientToServerId);
            out.writeString(serverToClientId);
        }
    }

    private class ReleaseTransportHandler implements TransportRequestHandler<ReleaseRequest> {
        @Override
        public void messageReceived(ReleaseRequest request, TransportChannel channel, Task task) throws Exception {
            releaseSession(request.sessionId());
            channel.sendResponse(ActionResponse.Empty.INSTANCE);
        }
    }

    private class ExchangeSetupTransportHandler implements TransportRequestHandler<ExchangeSetupRequest> {
        @Override
        public void messageReceived(ExchangeSetupRequest request, TransportChannel channel, Task task) {
            startExchangeFetchServer(
                request,
                (CancellableTask) task,
                new ChannelActionListener<>(channel).map(ignored -> ActionResponse.Empty.INSTANCE)
            );
        }
    }

    static final class TrackedSessions implements Releasable {
        private final RemoteFetchService remoteFetchService;
        private final List<TrackedSession> sessions = new ArrayList<>();
        private boolean closed;

        private TrackedSessions(RemoteFetchService remoteFetchService) {
            this.remoteFetchService = remoteFetchService;
        }

        synchronized void track(DiscoveryNode targetNode, String sessionId) {
            if (closed) {
                remoteFetchService.releaseBestEffort(targetNode, sessionId);
                return;
            }
            sessions.add(new TrackedSession(targetNode, sessionId));
        }

        @Override
        public void close() {
            List<TrackedSession> tracked;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                tracked = List.copyOf(sessions);
                sessions.clear();
            }
            for (TrackedSession session : tracked) {
                remoteFetchService.releaseBestEffort(session.targetNode(), session.sessionId());
            }
        }
    }

    private record TrackedSession(DiscoveryNode targetNode, String sessionId) {}
}
