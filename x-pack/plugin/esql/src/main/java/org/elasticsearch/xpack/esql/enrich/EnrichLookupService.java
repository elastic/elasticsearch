/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.TransportVersions;
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
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.core.AbstractRefCounted;
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
import org.elasticsearch.transport.TransportRequest;
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
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
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
 * {@link EnrichLookupService} performs enrich lookup for a given input page. The lookup process consists of three stages:
 * - Stage 1: Finding matching document IDs for the input page. This stage is done by the {@link EnrichQuerySourceOperator} or its variants.
 * The output page of this stage is represented as [DocVector, IntBlock: positions of the input terms].
 * <p>
 * - Stage 2: Extracting field values for the matched document IDs. The output page is represented as
 * [DocVector, IntBlock: positions, Block: field1, Block: field2,...].
 * <p>
 * - Stage 3: Combining the extracted values based on positions and filling nulls for positions without matches.
 * This is done by {@link MergePositionsOperator}. The output page is represented as [Block: field1, Block: field2,...].
 * <p>
 * The positionCount of the output page must be equal to the positionCount of the input page.
 */
public class EnrichLookupService {
    public static final String LOOKUP_ACTION_NAME = EsqlQueryAction.NAME + "/lookup";

    private final ClusterService clusterService;
    private final SearchService searchService;
    private final TransportService transportService;
    private final Executor executor;
    private final BigArrays bigArrays;
    private final BlockFactory blockFactory;
    private final LocalCircuitBreaker.SizeSettings localBreakerSettings;

    public EnrichLookupService(
        ClusterService clusterService,
        SearchService searchService,
        TransportService transportService,
        BigArrays bigArrays,
        BlockFactory blockFactory
    ) {
        this.clusterService = clusterService;
        this.searchService = searchService;
        this.transportService = transportService;
        this.executor = transportService.getThreadPool().executor(ThreadPool.Names.SEARCH);
        this.bigArrays = bigArrays;
        this.blockFactory = blockFactory;
        this.localBreakerSettings = new LocalCircuitBreaker.SizeSettings(clusterService.getSettings());
        transportService.registerRequestHandler(
            LOOKUP_ACTION_NAME,
            transportService.getThreadPool().executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME),
            in -> new LookupRequest(in, blockFactory),
            new TransportHandler()
        );
    }

    public void lookupAsync(
        String sessionId,
        CancellableTask parentTask,
        String index,
        DataType inputDataType,
        String matchType,
        String matchField,
        List<NamedExpression> extractFields,
        Page inputPage,
        ActionListener<Page> outListener
    ) {
        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        ActionListener<Page> listener = ContextPreservingActionListener.wrapPreservingContext(outListener, threadContext);
        hasEnrichPrivilege(listener.delegateFailureAndWrap((delegate, ignored) -> {
            ClusterState clusterState = clusterService.state();
            GroupShardsIterator<ShardIterator> shardIterators = clusterService.operationRouting()
                .searchShards(clusterState, new String[] { index }, Map.of(), "_local");
            if (shardIterators.size() != 1) {
                delegate.onFailure(new EsqlIllegalArgumentException("target index {} has more than one shard", index));
                return;
            }
            ShardIterator shardIt = shardIterators.get(0);
            ShardRouting shardRouting = shardIt.nextOrNull();
            ShardId shardId = shardIt.shardId();
            if (shardRouting == null) {
                delegate.onFailure(new UnavailableShardsException(shardId, "enrich index is not available"));
                return;
            }
            DiscoveryNode targetNode = clusterState.nodes().get(shardRouting.currentNodeId());
            var lookupRequest = new LookupRequest(sessionId, shardId, inputDataType, matchType, matchField, inputPage, extractFields);
            // TODO: handle retry and avoid forking for the local lookup
            try (ThreadContext.StoredContext unused = threadContext.stashWithOrigin(ClientHelper.ENRICH_ORIGIN)) {
                transportService.sendChildRequest(
                    targetNode,
                    LOOKUP_ACTION_NAME,
                    lookupRequest,
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

    private void hasEnrichPrivilege(ActionListener<Void> outListener) {
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
        request.clusterPrivileges(ClusterPrivilegeResolver.MONITOR_ENRICH.name());
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

    private void doLookup(
        String sessionId,
        CancellableTask task,
        ShardId shardId,
        DataType inputDataType,
        String matchType,
        String matchField,
        Page inputPage,
        List<NamedExpression> extractFields,
        ActionListener<Page> listener
    ) {
        Block inputBlock = inputPage.getBlock(0);
        if (inputBlock.areAllValuesNull()) {
            listener.onResponse(createNullResponse(inputPage.getPositionCount(), extractFields));
            return;
        }
        final List<Releasable> releasables = new ArrayList<>(6);
        boolean started = false;
        try {
            final ShardSearchRequest shardSearchRequest = new ShardSearchRequest(shardId, 0, AliasFilter.EMPTY);
            final SearchContext searchContext = searchService.createSearchContext(shardSearchRequest, SearchService.NO_TIMEOUT);
            releasables.add(searchContext);
            final LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(
                blockFactory.breaker(),
                localBreakerSettings.overReservedBytes(),
                localBreakerSettings.maxOverReservedBytes()
            );
            releasables.add(localBreaker);
            final DriverContext driverContext = new DriverContext(bigArrays, blockFactory.newChildFactory(localBreaker));
            final ElementType[] mergingTypes = new ElementType[extractFields.size()];
            for (int i = 0; i < extractFields.size(); i++) {
                mergingTypes[i] = PlannerUtils.toElementType(extractFields.get(i).dataType());
            }
            final int[] mergingChannels = IntStream.range(0, extractFields.size()).map(i -> i + 2).toArray();
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
            MappedFieldType fieldType = searchExecutionContext.getFieldType(matchField);
            var queryList = switch (matchType) {
                case "match", "range" -> QueryList.termQueryList(fieldType, searchExecutionContext, inputBlock, inputDataType);
                case "geo_match" -> QueryList.geoShapeQuery(fieldType, searchExecutionContext, inputBlock, inputDataType);
                default -> throw new EsqlIllegalArgumentException("illegal match type " + matchType);
            };
            var queryOperator = new EnrichQuerySourceOperator(
                driverContext.blockFactory(),
                EnrichQuerySourceOperator.DEFAULT_MAX_PAGE_SIZE,
                queryList,
                searchExecutionContext.getIndexReader()
            );
            releasables.add(queryOperator);
            var extractFieldsOperator = extractFieldsOperator(searchContext, driverContext, extractFields);
            releasables.add(extractFieldsOperator);

            AtomicReference<Page> result = new AtomicReference<>();
            OutputOperator outputOperator = new OutputOperator(List.of(), Function.identity(), result::set);
            releasables.add(outputOperator);
            Driver driver = new Driver(
                "enrich-lookup:" + sessionId,
                System.currentTimeMillis(),
                System.nanoTime(),
                driverContext,
                () -> lookupDescription(
                    sessionId,
                    shardId,
                    inputDataType,
                    matchType,
                    matchField,
                    extractFields,
                    inputPage.getPositionCount()
                ),
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
                    out = createNullResponse(inputPage.getPositionCount(), extractFields);
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

    private class TransportHandler implements TransportRequestHandler<LookupRequest> {
        @Override
        public void messageReceived(LookupRequest request, TransportChannel channel, Task task) {
            request.incRef();
            ActionListener<LookupResponse> listener = ActionListener.runBefore(new ChannelActionListener<>(channel), request::decRef);
            doLookup(
                request.sessionId,
                (CancellableTask) task,
                request.shardId,
                request.inputDataType,
                request.matchType,
                request.matchField,
                request.inputPage,
                request.extractFields,
                listener.delegateFailureAndWrap(
                    (l, outPage) -> ActionListener.respondAndRelease(l, new LookupResponse(outPage, blockFactory))
                )
            );
        }
    }

    private static class LookupRequest extends TransportRequest implements IndicesRequest {
        private final String sessionId;
        private final ShardId shardId;
        private final DataType inputDataType;
        private final String matchType;
        private final String matchField;
        private final Page inputPage;
        private final List<NamedExpression> extractFields;
        // TODO: Remove this workaround once we have Block RefCount
        private final Page toRelease;
        private final RefCounted refs = AbstractRefCounted.of(this::releasePage);

        LookupRequest(
            String sessionId,
            ShardId shardId,
            DataType inputDataType,
            String matchType,
            String matchField,
            Page inputPage,
            List<NamedExpression> extractFields
        ) {
            this.sessionId = sessionId;
            this.shardId = shardId;
            this.inputDataType = inputDataType;
            this.matchType = matchType;
            this.matchField = matchField;
            this.inputPage = inputPage;
            this.toRelease = null;
            this.extractFields = extractFields;
        }

        LookupRequest(StreamInput in, BlockFactory blockFactory) throws IOException {
            super(in);
            this.sessionId = in.readString();
            this.shardId = new ShardId(in);
            String inputDataType = (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) ? in.readString() : "unknown";
            this.inputDataType = DataType.fromTypeName(inputDataType);
            this.matchType = in.readString();
            this.matchField = in.readString();
            try (BlockStreamInput bsi = new BlockStreamInput(in, blockFactory)) {
                this.inputPage = new Page(bsi);
            }
            this.toRelease = inputPage;
            PlanStreamInput planIn = new PlanStreamInput(in, PlanNameRegistry.INSTANCE, in.namedWriteableRegistry(), null);
            this.extractFields = planIn.readNamedWriteableCollectionAsList(NamedExpression.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sessionId);
            out.writeWriteable(shardId);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                out.writeString(inputDataType.typeName());
            }
            out.writeString(matchType);
            out.writeString(matchField);
            out.writeWriteable(inputPage);
            PlanStreamOutput planOut = new PlanStreamOutput(out, PlanNameRegistry.INSTANCE, null);
            planOut.writeNamedWriteableCollection(extractFields);
        }

        @Override
        public String[] indices() {
            return new String[] { shardId.getIndexName() };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers) {
                @Override
                public String getDescription() {
                    return lookupDescription(
                        sessionId,
                        shardId,
                        inputDataType,
                        matchType,
                        matchField,
                        extractFields,
                        inputPage.getPositionCount()
                    );
                }
            };
        }

        private void releasePage() {
            if (toRelease != null) {
                Releasables.closeExpectNoException(toRelease::releaseBlocks);
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

    private static String lookupDescription(
        String sessionId,
        ShardId shardId,
        DataType inputDataType,
        String matchType,
        String matchField,
        List<NamedExpression> extractFields,
        int positionCount
    ) {
        return "ENRICH_LOOKUP("
            + " session="
            + sessionId
            + " ,shard="
            + shardId
            + " ,input_type="
            + inputDataType
            + " ,match_type="
            + matchType
            + " ,match_field="
            + matchField
            + " ,extract_fields="
            + extractFields
            + " ,positions="
            + positionCount
            + ")";
    }

    private static class LookupResponse extends TransportResponse {
        private Page page;
        private final RefCounted refs = AbstractRefCounted.of(this::releasePage);
        private final BlockFactory blockFactory;
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
