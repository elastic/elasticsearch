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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeClient;
import org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeServer;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
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
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
    private final RemoteFieldLoader fieldLoader;
    private final RemoteFetchPushdownCompiler pushdownCompiler;
    private final RetainedSearchContextsRegistry retainedSearchContexts;
    private final ExchangeServerFactory exchangeServerFactory;

    RemoteFetchService(TransportActionServices transportActionServices, BigArrays bigArrays, BlockFactory blockFactory) {
        this(transportActionServices, bigArrays, blockFactory, new RetainedSearchContextsRegistry());
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
        this.fieldLoader = new RemoteFieldLoader(bigArrays, localBreakerSettings);
        this.pushdownCompiler = new RemoteFetchPushdownCompiler(bigArrays, localBreakerSettings);
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
            // TODO make Configuration Releasable
            new BlockStreamInput(
                in,
                BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker(CircuitBreaker.REQUEST)).build()
            )
        );
    }

    public interface Client extends Releasable {
        Exchange openExchange(
            String nodeId,
            String retainedSessionId,
            List<FetchField> fields,
            PhysicalPlan pushdownPlan,
            Configuration configuration
        );

        @Override
        default void close() {}
    }

    public interface Exchange extends Releasable {
        void sendBatch(long batchId, List<RemoteFetchHandle> handles) throws Exception;

        Page pollPage();

        IsBlockedResult isBlocked();

        Exception getFailure();

        void markBatchCompleted(long batchId);

        void finish();

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
        private final Map<TargetSession, TargetState> targets = new HashMap<>();
        private volatile boolean closed;

        private BatchExchangeFetchClient(CancellableTask parentTask, RetainedSessionReleaser retainedSessionReleaser) {
            this.parentTask = parentTask;
            this.retainedSessionReleaser = Objects.requireNonNull(retainedSessionReleaser);
        }

        @Override
        public Exchange openExchange(
            String nodeId,
            String retainedSessionId,
            List<FetchField> fields,
            PhysicalPlan pushdownPlan,
            Configuration configuration
        ) {
            if (closed) {
                throw new IllegalStateException("remote fetch exchange client is closed");
            }
            TargetSession target = new TargetSession(nodeId, retainedSessionId);
            synchronized (this) {
                TargetState existing = targets.get(target);
                if (existing != null) {
                    existing.validate(fields, pushdownPlan, configuration);
                    return existing;
                }
                TargetState created = createTargetState(target, fields, pushdownPlan, configuration);
                targets.put(target, created);
                return created;
            }
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
            retainedSessionReleaser.close();
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
            return new TargetState(target, node, retainedSessionReleaser, client, fields, pushdownPlan, configuration);
        }
    }

    private final class TargetState implements Exchange {
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

        private TargetState(
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
                    throw new IllegalStateException("remote fetch target state is closed");
                }
                validateHandlesForTarget(handles);
                try {
                    client.sendPage(buildHandlesPage(handles, batchId));
                } catch (Exception e) {
                    client.markBatchCompleted(batchId);
                    throw e;
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
                    logger.debug("failed to close remote fetch target state", e);
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
                Math.max(1, request.fields().size()),
                transportService,
                task,
                clientNode,
                clusterService.getSettings()
            );
            releasable = Releasables.wrap(server, lease, localBreaker);
            List<Operator> intermediate = List.of(
                new RemoteFetchBatchOperator(buildRemoteFetcher(request, shardContexts, settings, driverContext.blockFactory()))
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

    private RemoteFetchBatchOperator.RemoteFetcher buildRemoteFetcher(
        ExchangeSetupRequest request,
        IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        PlannerSettings settings,
        BlockFactory exchangeBlockFactory
    ) {
        // Keep plan compilation in the service layer; the batch operator only needs to transform handle batches.
        return handles -> {
            List<Page> fetched = fieldLoader.execute(handles, request.fields(), shardContexts, settings, exchangeBlockFactory);
            return pushdownCompiler.execute(
                fetched,
                request.pushdownPlan(),
                shardContexts,
                exchangeBlockFactory,
                request.configuration().newFoldContext()
            );
        };
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
            RemoteFetchOperator.validatePushdownPlan(pushdownPlan);
            this.pushdownPlan = pushdownPlan;
            this.configuration = Objects.requireNonNull(configuration);
            this.clientToServerId = Objects.requireNonNull(clientToServerId);
            this.serverToClientId = Objects.requireNonNull(serverToClientId);
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
            this.pushdownPlan = pin.readOptionalNamedWriteable(PhysicalPlan.class);
            RemoteFetchOperator.validatePushdownPlan(pushdownPlan);
            this.clientToServerId = in.readString();
            this.serverToClientId = in.readString();
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
            new PlanStreamOutput(out, serializedConfiguration).writeOptionalNamedWriteable(pushdownPlan);
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
