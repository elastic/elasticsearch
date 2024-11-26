/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.lookup.EnrichQuerySourceOperator;
import org.elasticsearch.compute.operator.lookup.MergePositionsOperator;
import org.elasticsearch.compute.operator.lookup.QueryList;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * {@link AbstractLookupService} performs a single valued {@code LEFT JOIN} for a
 * given input page against another index. This is quite similar to a nested loop
 * join. It is restricted to indices with only a single shard.
 * <p>
 *     This registers a {@link TransportRequestHandler} so we can handle requests
 *     to join data that isn't local to the node, but it is much faster if the
 *     data is already local.
 * </p>
 * <p>
 *     The join process spawns a {@link Driver} per incoming page which runs in
 *     three stages:
 * </p>
 * <p>
 *     Stage 1: Finding matching document IDs for the input page. This stage is done
 *     by the {@link EnrichQuerySourceOperator}. The output page of this stage is
 *     represented as {@code [DocVector, IntBlock: positions of the input terms]}.
 * </p>
 * <p>
 *     Stage 2: Extracting field values for the matched document IDs. The output page
 *     is represented as
 *     {@code [DocVector, IntBlock: positions, Block: field1, Block: field2,...]}.
 * </p>
 * <p>
 *     Stage 3: Combining the extracted values based on positions and filling nulls for
 *     positions without matches. This is done by {@link MergePositionsOperator}. The output
 *     page is represented as {@code [Block: field1, Block: field2,...]}.
 * </p>
 * <p>
 *     The {@link Page#getPositionCount()} of the output {@link Page} is  equal to the
 *     {@link Page#getPositionCount()} of the input page. In other words - it returns
 *     the same number of rows that it was sent no matter how many documents match.
 * </p>
 */
abstract class AbstractLookupService<R extends AbstractLookupService.Request, T extends AbstractLookupService.TransportRequest> {
    private final String actionName;
    private final String privilegeName;
    private final ClusterService clusterService;
    private final SearchService searchService;
    private final TransportService transportService;
    private final Executor executor;
    private final BigArrays bigArrays;
    private final BlockFactory blockFactory;
    private final LocalCircuitBreaker.SizeSettings localBreakerSettings;

    AbstractLookupService(
        String actionName,
        String privilegeName,
        ClusterService clusterService,
        SearchService searchService,
        TransportService transportService,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        CheckedBiFunction<StreamInput, BlockFactory, T, IOException> readRequest
    ) {
        this.actionName = actionName;
        this.privilegeName = privilegeName;
        this.clusterService = clusterService;
        this.searchService = searchService;
        this.transportService = transportService;
        this.executor = transportService.getThreadPool().executor(ThreadPool.Names.SEARCH);
        this.bigArrays = bigArrays;
        this.blockFactory = blockFactory;
        this.localBreakerSettings = new LocalCircuitBreaker.SizeSettings(clusterService.getSettings());
        transportService.registerRequestHandler(
            actionName,
            transportService.getThreadPool().executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME),
            in -> readRequest.apply(in, blockFactory),
            new TransportHandler()
        );
    }

    public ThreadContext getThreadContext() {
        return transportService.getThreadPool().getThreadContext();
    }

    /**
     * Convert a request as sent to {@link #lookupAsync} into a transport request after
     * preflight checks have been performed.
     */
    protected abstract T transportRequest(R request, ShardId shardId);

    /**
     * Build a list of queries to perform inside the actual lookup.
     */
    protected abstract QueryList queryList(T request, SearchExecutionContext context, Block inputBlock, DataType inputDataType);

    protected static QueryList termQueryList(
        MappedFieldType field,
        SearchExecutionContext searchExecutionContext,
        Block block,
        DataType inputDataType
    ) {
        return switch (inputDataType) {
            case IP -> QueryList.ipTermQueryList(field, searchExecutionContext, (BytesRefBlock) block);
            case DATETIME -> QueryList.dateTermQueryList(field, searchExecutionContext, (LongBlock) block);
            case null, default -> QueryList.rawTermQueryList(field, searchExecutionContext, block);
        };
    }

    /**
     * Perform the actual lookup.
     */
    public final void lookupAsync(R request, CancellableTask parentTask, ActionListener<Page> outListener) {
        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        ActionListener<Page> listener = ContextPreservingActionListener.wrapPreservingContext(outListener, threadContext);
        hasPrivilege(listener.delegateFailureAndWrap((delegate, ignored) -> {
            ClusterState clusterState = clusterService.state();
            GroupShardsIterator<ShardIterator> shardIterators = clusterService.operationRouting()
                .searchShards(clusterState, new String[] { request.index }, Map.of(), "_local");
            if (shardIterators.size() != 1) {
                delegate.onFailure(new EsqlIllegalArgumentException("target index {} has more than one shard", request.index));
                return;
            }
            ShardIterator shardIt = shardIterators.get(0);
            ShardRouting shardRouting = shardIt.nextOrNull();
            ShardId shardId = shardIt.shardId();
            if (shardRouting == null) {
                delegate.onFailure(new UnavailableShardsException(shardId, "target index is not available"));
                return;
            }
            DiscoveryNode targetNode = clusterState.nodes().get(shardRouting.currentNodeId());
            T transportRequest = transportRequest(request, shardId);
            // TODO: handle retry and avoid forking for the local lookup
            try (ThreadContext.StoredContext unused = threadContext.stashWithOrigin(ClientHelper.ENRICH_ORIGIN)) {
                transportService.sendChildRequest(
                    targetNode,
                    actionName,
                    transportRequest,
                    parentTask,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(
                        delegate.map(LookupResponse::takePage),
                        in -> new LookupResponse(in, blockFactory),
                        executor
                    )
                );
            }
        }));
    }

    private void hasPrivilege(ActionListener<Void> outListener) {
        final Settings settings = clusterService.getSettings();
        if (settings.hasValue(XPackSettings.SECURITY_ENABLED.getKey()) == false || XPackSettings.SECURITY_ENABLED.get(settings) == false) {
            outListener.onResponse(null);
            return;
        }
        final ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        final User user = securityContext.getUser();
        if (user == null) {
            outListener.onFailure(new IllegalStateException("missing or unable to read authentication info on request"));
            return;
        }
        HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(user.principal());
        request.clusterPrivileges(privilegeName);
        request.indexPrivileges(new RoleDescriptor.IndicesPrivileges[0]);
        request.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        ActionListener<HasPrivilegesResponse> listener = outListener.delegateFailureAndWrap((l, resp) -> {
            if (resp.isCompleteMatch()) {
                l.onResponse(null);
                return;
            }
            String detailed = resp.getClusterPrivileges()
                .entrySet()
                .stream()
                .filter(e -> e.getValue() == false)
                .map(e -> "privilege [" + e.getKey() + "] is missing")
                .collect(Collectors.joining(", "));
            String message = "user ["
                + user.principal()
                + "] doesn't have "
                + "sufficient privileges to perform enrich lookup: "
                + detailed;
            l.onFailure(Exceptions.authorizationError(message));
        });
        transportService.sendRequest(
            transportService.getLocalNode(),
            HasPrivilegesAction.NAME,
            request,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, HasPrivilegesResponse::new, executor)
        );
    }

    private void doLookup(T request, CancellableTask task, ActionListener<Page> listener) {
        Block inputBlock = request.inputPage.getBlock(0);
        if (inputBlock.areAllValuesNull()) {
            listener.onResponse(createNullResponse(request.inputPage.getPositionCount(), request.extractFields));
            return;
        }
        final List<Releasable> releasables = new ArrayList<>(6);
        boolean started = false;
        try {
            final ShardSearchRequest shardSearchRequest = new ShardSearchRequest(request.shardId, 0, AliasFilter.EMPTY);
            final SearchContext searchContext = searchService.createSearchContext(shardSearchRequest, SearchService.NO_TIMEOUT);
            releasables.add(searchContext);
            final LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(
                blockFactory.breaker(),
                localBreakerSettings.overReservedBytes(),
                localBreakerSettings.maxOverReservedBytes()
            );
            releasables.add(localBreaker);
            final DriverContext driverContext = new DriverContext(bigArrays, blockFactory.newChildFactory(localBreaker));
            final ElementType[] mergingTypes = new ElementType[request.extractFields.size()];
            for (int i = 0; i < request.extractFields.size(); i++) {
                mergingTypes[i] = PlannerUtils.toElementType(request.extractFields.get(i).dataType());
            }
            final int[] mergingChannels = IntStream.range(0, request.extractFields.size()).map(i -> i + 2).toArray();
            final MergePositionsOperator mergePositionsOperator;
            final OrdinalBytesRefBlock ordinalsBytesRefBlock;
            if (inputBlock instanceof BytesRefBlock bytesRefBlock && (ordinalsBytesRefBlock = bytesRefBlock.asOrdinals()) != null) {
                inputBlock = ordinalsBytesRefBlock.getDictionaryVector().asBlock();
                var selectedPositions = ordinalsBytesRefBlock.getOrdinalsBlock();
                mergePositionsOperator = new MergePositionsOperator(
                    1,
                    mergingChannels,
                    mergingTypes,
                    selectedPositions,
                    driverContext.blockFactory()
                );

            } else {
                try (var selectedPositions = IntVector.range(0, inputBlock.getPositionCount(), blockFactory).asBlock()) {
                    mergePositionsOperator = new MergePositionsOperator(
                        1,
                        mergingChannels,
                        mergingTypes,
                        selectedPositions,
                        driverContext.blockFactory()
                    );
                }
            }
            releasables.add(mergePositionsOperator);
            SearchExecutionContext searchExecutionContext = searchContext.getSearchExecutionContext();
            QueryList queryList = queryList(request, searchExecutionContext, inputBlock, request.inputDataType);
            var warnings = Warnings.createWarnings(
                DriverContext.WarningsMode.COLLECT,
                request.source.source().getLineNumber(),
                request.source.source().getColumnNumber(),
                request.source.text()
            );
            var queryOperator = new EnrichQuerySourceOperator(
                driverContext.blockFactory(),
                EnrichQuerySourceOperator.DEFAULT_MAX_PAGE_SIZE,
                queryList,
                searchExecutionContext.getIndexReader(),
                warnings
            );
            releasables.add(queryOperator);
            var extractFieldsOperator = extractFieldsOperator(searchContext, driverContext, request.extractFields);
            releasables.add(extractFieldsOperator);

            AtomicReference<Page> result = new AtomicReference<>();
            OutputOperator outputOperator = new OutputOperator(List.of(), Function.identity(), result::set);
            releasables.add(outputOperator);
            Driver driver = new Driver(
                "enrich-lookup:" + request.sessionId,
                System.currentTimeMillis(),
                System.nanoTime(),
                driverContext,
                request::toString,
                queryOperator,
                List.of(extractFieldsOperator, mergePositionsOperator),
                outputOperator,
                Driver.DEFAULT_STATUS_INTERVAL,
                Releasables.wrap(searchContext, localBreaker)
            );
            task.addListener(() -> {
                String reason = Objects.requireNonNullElse(task.getReasonCancelled(), "task was cancelled");
                driver.cancel(reason);
            });
            var threadContext = transportService.getThreadPool().getThreadContext();
            Driver.start(threadContext, executor, driver, Driver.DEFAULT_MAX_ITERATIONS, listener.map(ignored -> {
                Page out = result.get();
                if (out == null) {
                    out = createNullResponse(request.inputPage.getPositionCount(), request.extractFields);
                }
                return out;
            }));
            started = true;
        } catch (Exception e) {
            listener.onFailure(e);
        } finally {
            if (started == false) {
                Releasables.close(releasables);
            }
        }
    }

    private static Operator extractFieldsOperator(
        SearchContext searchContext,
        DriverContext driverContext,
        List<NamedExpression> extractFields
    ) {
        EsPhysicalOperationProviders.ShardContext shardContext = new EsPhysicalOperationProviders.DefaultShardContext(
            0,
            searchContext.getSearchExecutionContext(),
            searchContext.request().getAliasFilter()
        );
        List<ValuesSourceReaderOperator.FieldInfo> fields = new ArrayList<>(extractFields.size());
        for (NamedExpression extractField : extractFields) {
            BlockLoader loader = shardContext.blockLoader(
                extractField instanceof Alias a ? ((NamedExpression) a.child()).name() : extractField.name(),
                extractField.dataType() == DataType.UNSUPPORTED,
                MappedFieldType.FieldExtractPreference.NONE
            );
            fields.add(
                new ValuesSourceReaderOperator.FieldInfo(
                    extractField.name(),
                    PlannerUtils.toElementType(extractField.dataType()),
                    shardIdx -> {
                        if (shardIdx != 0) {
                            throw new IllegalStateException("only one shard");
                        }
                        return loader;
                    }
                )
            );
        }
        return new ValuesSourceReaderOperator(
            driverContext.blockFactory(),
            fields,
            List.of(new ValuesSourceReaderOperator.ShardContext(searchContext.searcher().getIndexReader(), searchContext::newSourceLoader)),
            0
        );
    }

    private Page createNullResponse(int positionCount, List<NamedExpression> extractFields) {
        final Block[] blocks = new Block[extractFields.size()];
        try {
            for (int i = 0; i < extractFields.size(); i++) {
                blocks[i] = blockFactory.newConstantNullBlock(positionCount);
            }
            return new Page(blocks);
        } finally {
            if (blocks[blocks.length - 1] == null) {
                Releasables.close(blocks);
            }
        }
    }

    private class TransportHandler implements TransportRequestHandler<T> {
        @Override
        public void messageReceived(T request, TransportChannel channel, Task task) {
            request.incRef();
            ActionListener<LookupResponse> listener = ActionListener.runBefore(new ChannelActionListener<>(channel), request::decRef);
            doLookup(
                request,
                (CancellableTask) task,
                listener.delegateFailureAndWrap(
                    (l, outPage) -> ActionListener.respondAndRelease(l, new LookupResponse(outPage, blockFactory))
                )
            );
        }
    }

    abstract static class Request {
        final String sessionId;
        final String index;
        final DataType inputDataType;
        final Page inputPage;
        final List<NamedExpression> extractFields;
        final Source source;

        Request(
            String sessionId,
            String index,
            DataType inputDataType,
            Page inputPage,
            List<NamedExpression> extractFields,
            Source source
        ) {
            this.sessionId = sessionId;
            this.index = index;
            this.inputDataType = inputDataType;
            this.inputPage = inputPage;
            this.extractFields = extractFields;
            this.source = source;
        }
    }

    abstract static class TransportRequest extends org.elasticsearch.transport.TransportRequest implements IndicesRequest {
        final String sessionId;
        final ShardId shardId;
        /**
         * For mixed clusters with nodes &lt;8.14, this will be null.
         */
        @Nullable
        final DataType inputDataType;
        final Page inputPage;
        final List<NamedExpression> extractFields;
        final Source source;
        // TODO: Remove this workaround once we have Block RefCount
        final Page toRelease;
        final RefCounted refs = AbstractRefCounted.of(this::releasePage);

        TransportRequest(
            String sessionId,
            ShardId shardId,
            DataType inputDataType,
            Page inputPage,
            Page toRelease,
            List<NamedExpression> extractFields,
            Source source
        ) {
            this.sessionId = sessionId;
            this.shardId = shardId;
            this.inputDataType = inputDataType;
            this.inputPage = inputPage;
            this.toRelease = toRelease;
            this.extractFields = extractFields;
            this.source = source;
        }

        @Override
        public final String[] indices() {
            return new String[] { shardId.getIndexName() };
        }

        @Override
        public final IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        @Override
        public final Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers) {
                @Override
                public String getDescription() {
                    return this.toString();
                }
            };
        }

        private void releasePage() {
            if (toRelease != null) {
                Releasables.closeExpectNoException(toRelease::releaseBlocks);
            }
        }

        @Override
        public final void incRef() {
            refs.incRef();
        }

        @Override
        public final boolean tryIncRef() {
            return refs.tryIncRef();
        }

        @Override
        public final boolean decRef() {
            return refs.decRef();
        }

        @Override
        public final boolean hasReferences() {
            return refs.hasReferences();
        }

        @Override
        public final String toString() {
            return "LOOKUP("
                + " session="
                + sessionId
                + " ,shard="
                + shardId
                + " ,input_type="
                + inputDataType
                + " ,extract_fields="
                + extractFields
                + " ,positions="
                + inputPage.getPositionCount()
                + extraDescription()
                + ")";
        }

        protected abstract String extraDescription();
    }

    private static class LookupResponse extends TransportResponse {
        private final RefCounted refs = AbstractRefCounted.of(this::releasePage);
        private final BlockFactory blockFactory;
        private Page page;
        private long reservedBytes = 0;

        LookupResponse(Page page, BlockFactory blockFactory) {
            this.page = page;
            this.blockFactory = blockFactory;
        }

        LookupResponse(StreamInput in, BlockFactory blockFactory) throws IOException {
            try (BlockStreamInput bsi = new BlockStreamInput(in, blockFactory)) {
                this.page = new Page(bsi);
            }
            this.blockFactory = blockFactory;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            long bytes = page.ramBytesUsedByBlocks();
            blockFactory.breaker().addEstimateBytesAndMaybeBreak(bytes, "serialize enrich lookup response");
            reservedBytes += bytes;
            page.writeTo(out);
        }

        Page takePage() {
            var p = page;
            page = null;
            return p;
        }

        private void releasePage() {
            blockFactory.breaker().addWithoutBreaking(-reservedBytes);
            if (page != null) {
                Releasables.closeExpectNoException(page::releaseBlocks);
            }
        }

        @Override
        public void incRef() {
            refs.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return refs.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return refs.decRef();
        }

        @Override
        public boolean hasReferences() {
            return refs.hasReferences();
        }
    }
}
