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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BatchMetadata;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.ProjectOperator.ProjectOperatorFactory;
import org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeClient;
import org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeServer;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
    private static final TimeValue RETAINED_CONTEXTS_REAPER_INTERVAL = TimeValue.timeValueMinutes(1);

    private static final Logger logger = LogManager.getLogger(RemoteFetchService.class);
    private static final AtomicLong exchangeIdGenerator = new AtomicLong();

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final ExchangeService exchangeService;
    private final BigArrays bigArrays;
    private final BlockFactory blockFactory;
    private final PlannerSettings.Holder plannerSettings;
    private final LocalCircuitBreaker.SizeSettings localBreakerSettings;
    private final RemoteFetchPushdownOperatorBuilder pushdownOperatorBuilder;
    private final RetainedSearchContextsRegistry retainedSearchContexts;
    private final ExchangeServerFactory exchangeServerFactory;

    RemoteFetchService(TransportActionServices transportActionServices, BigArrays bigArrays, BlockFactory blockFactory) {
        this(
            transportActionServices,
            bigArrays,
            blockFactory,
            new RetainedSearchContextsRegistry(transportActionServices.transportService().getThreadPool()::relativeTimeInMillis)
        );
    }

    RemoteFetchService(
        TransportActionServices transportActionServices,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        RetainedSearchContextsRegistry retainedSearchContexts
    ) {
        this(transportActionServices, bigArrays, blockFactory, retainedSearchContexts, BidirectionalBatchExchangeServer::new);
    }

    RemoteFetchService(
        TransportActionServices transportActionServices,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        RetainedSearchContextsRegistry retainedSearchContexts,
        ExchangeServerFactory exchangeServerFactory
    ) {
        this.clusterService = transportActionServices.clusterService();
        this.transportService = transportActionServices.transportService();
        this.exchangeService = transportActionServices.exchangeService();
        this.bigArrays = bigArrays;
        this.blockFactory = blockFactory;
        this.plannerSettings = transportActionServices.plannerSettings();
        this.localBreakerSettings = new LocalCircuitBreaker.SizeSettings(clusterService.getSettings());
        this.pushdownOperatorBuilder = new RemoteFetchPushdownOperatorBuilder();
        this.retainedSearchContexts = Objects.requireNonNull(retainedSearchContexts);
        this.exchangeServerFactory = Objects.requireNonNull(exchangeServerFactory);
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
        transportService.getThreadPool()
            .scheduleWithFixedDelay(
                this::expireRetainedSearchContexts,
                RETAINED_CONTEXTS_REAPER_INTERVAL,
                transportService.getThreadPool().executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME)
            );
    }

    RetainedSearchContextsRegistry.Handle retainSearchContexts(String sessionId, AcquiredSearchContexts searchContexts) {
        return retainedSearchContexts.register(sessionId, searchContexts);
    }

    void releaseAsync(DiscoveryNode targetNode, String retainedSessionId, ActionListener<Void> listener) {
        transportService.sendRequest(
            targetNode,
            RELEASE_ACTION_NAME,
            new ReleaseRequest(retainedSessionId),
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

    /**
     * Creates a batch exchange client without automatic retained-session release.
     * <p>
     * Callers that use this overload are responsible for releasing remote retained search contexts themselves
     * (e.g. via {@link #releaseAsync}); otherwise the contexts will leak until they expire.
     */
    public Client newBatchExchangeClient(CancellableTask parentTask) {
        return newBatchExchangeClient(parentTask, RetainedSessionReleaser.NOOP);
    }

    Client newBatchExchangeClient(CancellableTask parentTask, RetainedSessionReleaser retainedSessionReleaser) {
        return new BatchExchangeFetchClient(parentTask, retainedSessionReleaser);
    }

    RetainedSessionReleaser newRetainedSessionReleaser() {
        return new RetainedSessionReleaser(
            (targetNode, retainedSessionId) -> releaseAsync(targetNode, retainedSessionId, ActionListener.wrap(ignored -> {}, e -> {
                logger.debug("failed to release retained remote fetch session [{}] on node [{}]", retainedSessionId, targetNode.getId(), e);
            }))
        );
    }

    private Page buildHandlesPage(List<RemoteFetchHandle> handles, long batchId) {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(handles.size())) {
            for (RemoteFetchHandle handle : handles) {
                builder.appendBytesRef(handle.toBytesRef());
            }
            return new Page(new BatchMetadata(batchId, 0, true), builder.build());
        }
    }

    private static Configuration readConfiguration(StreamInput in) throws IOException {
        return new Configuration(
            // The configuration is always serialized without tables (see ExchangeSetupRequest#writeTo), so no block
            // memory is attached to it and a non-recycling, non-breaking factory is safe here.
            new BlockStreamInput(
                in,
                BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker(CircuitBreaker.REQUEST)).build()
            )
        );
    }

    public interface Client extends Releasable {
        TargetExchange openTargetExchange(
            String nodeId,
            String retainedSessionId,
            List<FetchField> fields,
            PhysicalPlan pushdownPlan,
            Configuration configuration
        );

        @Override
        default void close() {}
    }

    /**
     * Coordinator-side handle to a single target-session exchange channel.
     * <p>
     * Each instance is bound to one target node plus retained session pair and validates that all handles routed
     * through it belong to that same target session.
     */
    public interface TargetExchange extends Releasable {
        void sendBatch(long batchId, List<RemoteFetchHandle> handles) throws Exception;

        Page pollPage();

        IsBlockedResult isBlocked();

        /**
         * Returns the first terminal failure observed by the underlying exchange, if any.
         * <p>
         * Implementations may release retained-session resources as soon as a failure becomes visible.
         */
        Exception getFailure();

        void markBatchCompleted(long batchId);

        void finish();

        /**
         * Returns {@code true} when all remote responses are consumed and the exchange has terminated.
         * <p>
         * Implementations may release retained-session resources when this transitions to {@code true}.
         */
        boolean isFinished();

        IsBlockedResult waitForCompletion();

        @Override
        default void close() {}
    }

    @FunctionalInterface
    public interface ClientFactory {
        Client create();
    }

    @FunctionalInterface
    interface ExchangeServerFactory {
        BidirectionalBatchExchangeServer create(
            String sessionId,
            String clientToServerId,
            String serverToClientId,
            ExchangeService exchangeService,
            Executor executor,
            int maxOutstandingResponses,
            TransportService transportService,
            CancellableTask task,
            DiscoveryNode clientNode,
            Settings settings
        );
    }

    private final class BatchExchangeFetchClient implements Client {
        private final CancellableTask parentTask;
        private final RetainedSessionReleaser retainedSessionReleaser;
        private final Map<TargetSession, TargetExchangeChannel> targetExchanges = new HashMap<>();
        private volatile boolean closed;

        private BatchExchangeFetchClient(CancellableTask parentTask, RetainedSessionReleaser retainedSessionReleaser) {
            this.parentTask = parentTask;
            this.retainedSessionReleaser = Objects.requireNonNull(retainedSessionReleaser);
        }

        @Override
        public TargetExchange openTargetExchange(
            String nodeId,
            String retainedSessionId,
            List<FetchField> fields,
            PhysicalPlan pushdownPlan,
            Configuration configuration
        ) {
            TargetSession target = new TargetSession(nodeId, retainedSessionId);
            synchronized (this) {
                if (closed) {
                    throw new IllegalStateException("remote fetch exchange client is closed");
                }
                TargetExchangeChannel existing = targetExchanges.get(target);
                if (existing != null) {
                    existing.validate(fields, pushdownPlan, configuration);
                    return existing;
                }
                TargetExchangeChannel created = createTargetExchange(target, fields, pushdownPlan, configuration);
                targetExchanges.put(target, created);
                return created;
            }
        }

        @Override
        public void close() {
            Map<TargetSession, TargetExchangeChannel> exchanges;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                exchanges = new HashMap<>(targetExchanges);
                targetExchanges.clear();
            }
            for (TargetExchangeChannel exchange : exchanges.values()) {
                exchange.close();
            }
            retainedSessionReleaser.close();
        }

        private TargetExchangeChannel createTargetExchange(
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
                    target.retainedSessionId(),
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
                        setupListener.map(ignored -> null),
                        in -> ActionResponse.Empty.INSTANCE,
                        transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
                    )
                );
            };
            BidirectionalBatchExchangeClient client = new BidirectionalBatchExchangeClient(
                target.retainedSessionId() + "/remote-fetch/" + exchangeIdGenerator.incrementAndGet(),
                exchangeService,
                transportService.getThreadPool().executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME),
                // Keep one extra response slot on the coordinator side so setup/metadata traffic does not
                // starve the field-value pages flowing back from the data node.
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
            retainedSessionReleaser.track(node, target.retainedSessionId());
            return new TargetExchangeChannel(target, node, retainedSessionReleaser, client, fields, pushdownPlan, configuration);
        }
    }

    private final class TargetExchangeChannel implements TargetExchange {
        private final TargetSession target;
        private final DiscoveryNode targetNode;
        private final RetainedSessionReleaser retainedSessionReleaser;
        private final BidirectionalBatchExchangeClient client;
        private final List<FetchField> fields;
        private final PhysicalPlan pushdownPlan;
        private final Configuration configuration;
        private final Object lock = new Object();
        private boolean finished;
        private boolean closed;
        private boolean released;

        private TargetExchangeChannel(
            TargetSession target,
            DiscoveryNode targetNode,
            RetainedSessionReleaser retainedSessionReleaser,
            BidirectionalBatchExchangeClient client,
            List<FetchField> fields,
            PhysicalPlan pushdownPlan,
            Configuration configuration
        ) {
            this.target = target;
            this.targetNode = targetNode;
            this.retainedSessionReleaser = retainedSessionReleaser;
            this.client = client;
            this.fields = List.copyOf(fields);
            this.pushdownPlan = pushdownPlan;
            this.configuration = configuration;
        }

        void validate(List<FetchField> fields, PhysicalPlan pushdownPlan, Configuration configuration) {
            if (this.fields.equals(fields) == false) {
                throw new IllegalStateException("remote fetch fields differ for reused target session channel");
            }
            if (Objects.equals(this.pushdownPlan, pushdownPlan) == false) {
                throw new IllegalStateException("remote fetch pushdown plan differs for reused target session channel");
            }
            if (this.configuration.equals(configuration) == false) {
                throw new IllegalStateException("remote fetch configuration differs for reused target session channel");
            }
        }

        @Override
        public void sendBatch(long batchId, List<RemoteFetchHandle> handles) throws Exception {
            synchronized (lock) {
                if (closed) {
                    throw new IllegalStateException("remote fetch target exchange is closed");
                }
                validateHandlesForTarget(handles);
                Page page = buildHandlesPage(handles, batchId);
                boolean accepted = false;
                try {
                    client.sendPage(page);
                    accepted = true;
                } catch (Exception e) {
                    client.markBatchCompleted(batchId);
                    throw e;
                } finally {
                    if (accepted == false) {
                        page.releaseBlocks();
                    }
                }
            }
        }

        @Override
        public Page pollPage() {
            return client.pollPage();
        }

        @Override
        public IsBlockedResult isBlocked() {
            return client.waitUntilPageReady();
        }

        @Override
        public Exception getFailure() {
            Exception failure = client.getPrimaryFailure();
            if (failure != null) {
                // Failures are terminal for this target exchange, so release the retained session eagerly.
                releaseTarget();
            }
            return failure;
        }

        @Override
        public void markBatchCompleted(long batchId) {
            client.markBatchCompleted(batchId);
        }

        @Override
        public void finish() {
            synchronized (lock) {
                if (finished) {
                    return;
                }
                finished = true;
                client.finish();
            }
        }

        @Override
        public boolean isFinished() {
            boolean done = client.isFinished();
            if (done) {
                // Once the exchange is drained, release the retained session exactly once.
                releaseTarget();
            }
            return done;
        }

        @Override
        public IsBlockedResult waitForCompletion() {
            return client.waitForServerResponse();
        }

        @Override
        public void close() {
            synchronized (lock) {
                if (closed) {
                    return;
                }
                closed = true;
                try {
                    finish();
                    client.finishCollectingResponseHeaders();
                    client.close();
                } catch (Exception e) {
                    logger.debug("failed to close remote fetch target exchange", e);
                } finally {
                    releaseTarget();
                }
            }
        }

        private void validateHandlesForTarget(List<RemoteFetchHandle> handles) {
            for (RemoteFetchHandle handle : handles) {
                if (target.nodeId().equals(handle.nodeId()) == false
                    || target.retainedSessionId().equals(handle.retainedSessionId()) == false) {
                    throw new IllegalStateException("remote fetch handle does not match target session [" + target + "]");
                }
            }
        }

        private void releaseTarget() {
            synchronized (lock) {
                if (released) {
                    return;
                }
                released = true;
            }
            retainedSessionReleaser.release(targetNode, target.retainedSessionId());
        }
    }

    private record TargetSession(String nodeId, String retainedSessionId) {}

    int retainedSessions() {
        return retainedSearchContexts.retainedSessions();
    }

    boolean isRetained(String sessionId) {
        return retainedSearchContexts.isRetained(sessionId);
    }

    void expireRetainedSearchContexts() {
        retainedSearchContexts.expire();
    }

    private void releaseSession(String sessionId) {
        retainedSearchContexts.closeRegistration(sessionId);
    }

    void startExchangeFetchServer(ExchangeSetupRequest request, CancellableTask task, ActionListener<Void> listener) {
        RetainedSearchContextsRegistry.Handle lease = null;
        LocalCircuitBreaker localBreaker = null;
        BidirectionalBatchExchangeServer server = null;
        Releasable releasable = null;
        boolean success = false;
        try {
            lease = retainedSearchContexts.acquire(request.retainedSessionId());
            final DiscoveryNode clientNode = determineClientNode(task);
            final PlannerSettings settings = plannerSettings.get();
            final IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts = lease.searchContexts()
                .map(ComputeSearchContext::newDetachedShardContext);
            localBreaker = new LocalCircuitBreaker(
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
            server = exchangeServerFactory.create(
                request.retainedSessionId(),
                request.clientToServerId(),
                request.serverToClientId(),
                exchangeService,
                transportService.getThreadPool().executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME),
                // Data-node server responses are bounded by requested fetch fields.
                Math.max(1, request.fields().size()),
                transportService,
                task,
                clientNode,
                clusterService.getSettings()
            );
            releasable = Releasables.wrap(server, lease, localBreaker);
            List<Operator> intermediate;
            try {
                intermediate = buildDataNodeOperators(request, shardContexts, settings, driverContext);
            } catch (Exception e) {
                throw new IllegalStateException(
                    "remote fetch exchange setup failed while building data-node operators for session ["
                        + request.retainedSessionId()
                        + "]",
                    e
                );
            }
            logger.debug(
                () -> Strings.format(
                    "starting remote fetch exchange setup [%s] with operator chain [%s]",
                    request.retainedSessionId(),
                    describeOperatorChain(intermediate)
                )
            );
            server.startWithOperators(
                driverContext,
                transportService.getThreadPool().getThreadContext(),
                intermediate,
                clusterService.getClusterName().value(),
                releasable,
                listener
            );
            success = true;
        } catch (Exception e) {
            listener.onFailure(e);
        } finally {
            if (success == false) {
                if (releasable != null) {
                    Releasables.closeExpectNoException(releasable);
                } else {
                    Releasables.closeExpectNoException(server, localBreaker, lease);
                }
            }
        }
    }

    private List<Operator> buildDataNodeOperators(
        ExchangeSetupRequest request,
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        PlannerSettings settings,
        DriverContext driverContext
    ) {
        boolean includePositionMapping = request.pushdownPlan() != null;
        List<Operator> operators = new ArrayList<>();
        boolean success = false;
        try {
            operators.add(new RemoteFetchHandleDecodeOperator(driverContext.blockFactory(), includePositionMapping));

            List<ValuesSourceReaderOperator.FieldInfo> fieldInfos = buildFieldInfos(request.fields(), shardContexts, settings);
            IndexedByShardId<ValuesSourceReaderOperator.ShardContext> readerContexts = shardContexts.map(
                c -> new ValuesSourceReaderOperator.ShardContext(
                    c.searcher().getIndexReader(),
                    c::newSourceLoader,
                    c.storedFieldsSequentialProportion()
                )
            );
            operators.add(
                new ValuesSourceReaderOperator.Factory(
                    settings.valuesLoadingJumboSize(),
                    fieldInfos,
                    readerContexts,
                    fieldInfos.size() <= settings.reuseColumnLoadersThreshold(),
                    0,
                    settings.sourceReservationFactor(),
                    settings.docSequenceBytesRefFieldThreshold(),
                    () -> 0L
                ).get(driverContext)
            );
            operators.add(
                new ProjectOperatorFactory(fetchedFieldsProjection(fieldInfos.size(), includePositionMapping)).get(driverContext)
            );
            if (request.pushdownPlan() != null) {
                try {
                    operators.addAll(
                        pushdownOperatorBuilder.buildOperators(
                            request.pushdownPlan(),
                            shardContexts,
                            request.configuration().newFoldContext(),
                            driverContext
                        )
                    );
                } catch (Exception e) {
                    throw new IllegalStateException(
                        "failed to build remote fetch pushdown operators for plan ["
                            + request.pushdownPlan().getClass().getSimpleName()
                            + "]",
                        e
                    );
                }
            }
            success = true;
            return operators;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(operators));
            }
        }
    }

    private static List<ValuesSourceReaderOperator.FieldInfo> buildFieldInfos(
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

    private static String describeOperatorChain(List<Operator> operators) {
        return operators.stream().map(op -> op.getClass().getSimpleName()).collect(Collectors.joining(" -> "));
    }

    private List<Integer> fetchedFieldsProjection(int fieldCount, boolean includePositionMapping) {
        List<Integer> projection = new ArrayList<>(fieldCount + (includePositionMapping ? 1 : 0));
        int firstFieldChannel = includePositionMapping ? 2 : 1;
        for (int field = 0; field < fieldCount; field++) {
            projection.add(firstFieldChannel + field);
        }
        if (includePositionMapping) {
            projection.add(1);
        }
        return projection;
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

    static final class ReleaseRequest extends AbstractTransportRequest {
        private final String retainedSessionId;

        ReleaseRequest(String retainedSessionId) {
            this.retainedSessionId = Objects.requireNonNull(retainedSessionId, "retainedSessionId");
        }

        ReleaseRequest(StreamInput in) throws IOException {
            super(in);
            this.retainedSessionId = in.readString();
        }

        String retainedSessionId() {
            return retainedSessionId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(retainedSessionId);
        }
    }

    static final class ExchangeSetupRequest extends AbstractTransportRequest {
        private final String retainedSessionId;
        private final List<FetchField> fields;
        private final PhysicalPlan pushdownPlan;
        private final Configuration configuration;
        private final String clientToServerId;
        private final String serverToClientId;

        ExchangeSetupRequest(
            String retainedSessionId,
            List<FetchField> fields,
            PhysicalPlan pushdownPlan,
            Configuration configuration,
            String clientToServerId,
            String serverToClientId
        ) {
            this.retainedSessionId = Objects.requireNonNull(retainedSessionId, "retainedSessionId");
            this.fields = List.copyOf(fields);
            if (this.fields.isEmpty()) {
                throw new IllegalArgumentException("remote fetch requires at least one request field");
            }
            this.pushdownPlan = validatePushdownPlan(pushdownPlan, "request_build");
            this.configuration = Objects.requireNonNull(configuration, "configuration");
            this.clientToServerId = requireNonBlank(clientToServerId, "clientToServerId");
            this.serverToClientId = requireNonBlank(serverToClientId, "serverToClientId");
        }

        ExchangeSetupRequest(StreamInput in) throws IOException {
            super(in);
            this.retainedSessionId = in.readString();
            this.fields = in.readCollectionAsList(FetchField::new);
            if (this.fields.isEmpty()) {
                throw new IllegalArgumentException("remote fetch requires at least one request field");
            }
            this.configuration = readConfiguration(in);
            PlanStreamInput pin = new PlanStreamInput(in, in.namedWriteableRegistry(), configuration);
            this.pushdownPlan = validatePushdownPlan(pin.readOptionalNamedWriteable(PhysicalPlan.class), "request_deserialize");
            this.clientToServerId = requireNonBlank(in.readString(), "clientToServerId");
            this.serverToClientId = requireNonBlank(in.readString(), "serverToClientId");
        }

        String retainedSessionId() {
            return retainedSessionId;
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
            out.writeString(retainedSessionId);
            out.writeCollection(fields);
            Configuration serializedConfiguration = configuration.withoutTables();
            serializedConfiguration.writeTo(out);
            new PlanStreamOutput(out, serializedConfiguration).writeOptionalNamedWriteable(
                validatePushdownPlan(pushdownPlan, "request_serialize")
            );
            out.writeString(clientToServerId);
            out.writeString(serverToClientId);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            if (parentTaskId.isSet() == false) {
                assert false : "remote fetch exchange setup must have a parent task";
                throw new IllegalStateException("remote fetch exchange setup must have a parent task");
            }
            return new CancellableTask(id, type, action, "", parentTaskId, headers) {
                @Override
                public String getDescription() {
                    return "remote fetch exchange setup [" + retainedSessionId + "]";
                }
            };
        }

        private static String requireNonBlank(String value, String fieldName) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("remote fetch exchange setup requires a non-empty [" + fieldName + "]");
            }
            return value;
        }

        /**
         * Validates the pushdown plan shape at each request lifecycle boundary and tags failures with that phase.
         */
        private static PhysicalPlan validatePushdownPlan(PhysicalPlan pushdownPlan, String phase) {
            try {
                RemoteFetchPushdownOperatorBuilder.validateSupportedPlan(pushdownPlan);
                return pushdownPlan;
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("remote fetch pushdown plan is invalid during [" + phase + "]: " + e.getMessage(), e);
            }
        }
    }

    private class ReleaseTransportHandler implements TransportRequestHandler<ReleaseRequest> {
        @Override
        public void messageReceived(ReleaseRequest request, TransportChannel channel, Task task) throws Exception {
            releaseSession(request.retainedSessionId());
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

    @FunctionalInterface
    interface ReleaseAction {
        void release(DiscoveryNode targetNode, String retainedSessionId);
    }

    static final class RetainedSessionReleaser implements Releasable {
        private static final RetainedSessionReleaser NOOP = new RetainedSessionReleaser((targetNode, retainedSessionId) -> {});

        private final ReleaseAction releaseAction;
        private final Map<TrackedSessionKey, DiscoveryNode> sessions = new LinkedHashMap<>();
        private boolean closed;

        RetainedSessionReleaser(ReleaseAction releaseAction) {
            this.releaseAction = releaseAction;
        }

        void track(DiscoveryNode targetNode, String retainedSessionId) {
            boolean releaseNow = false;
            synchronized (this) {
                if (closed) {
                    releaseNow = true;
                } else {
                    sessions.putIfAbsent(new TrackedSessionKey(targetNode.getId(), retainedSessionId), targetNode);
                }
            }
            if (releaseNow) {
                releaseBestEffort(targetNode, retainedSessionId);
            }
        }

        void release(DiscoveryNode targetNode, String retainedSessionId) {
            boolean releaseNow;
            synchronized (this) {
                if (closed) {
                    return;
                }
                releaseNow = sessions.remove(new TrackedSessionKey(targetNode.getId(), retainedSessionId)) != null;
            }
            if (releaseNow) {
                releaseBestEffort(targetNode, retainedSessionId);
            }
        }

        @Override
        public void close() {
            Map<TrackedSessionKey, DiscoveryNode> tracked;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                tracked = new LinkedHashMap<>(sessions);
                sessions.clear();
            }
            tracked.forEach((session, targetNode) -> releaseBestEffort(targetNode, session.retainedSessionId()));
        }

        private void releaseBestEffort(DiscoveryNode targetNode, String retainedSessionId) {
            try {
                releaseAction.release(targetNode, retainedSessionId);
            } catch (Exception e) {
                logger.debug("failed to release retained remote fetch session [{}] on node [{}]", retainedSessionId, targetNode.getId(), e);
            }
        }

        private record TrackedSessionKey(String nodeId, String retainedSessionId) {}
    }
}
