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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.SearchShardRouting;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.lookup.EnrichQuerySourceOperator;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
import org.elasticsearch.compute.operator.lookup.MergePositionsOperator;
import org.elasticsearch.compute.operator.lookup.QueryList;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupMergeDropExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.ParameterizedQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.stream.IntStream;

/**
 * {@link AbstractLookupService} performs a {@code LEFT JOIN} for a given input
 * page against another index that <strong>must</strong> have only a single
 * shard.
 * <p>
 * This registers a {@link TransportRequestHandler} so we can handle requests
 * to join data that isn't local to the node, but it is much faster if the
 * data is already local.
 * </p>
 * <p>
 * The join process spawns a {@link Driver} per incoming page which runs in
 * two or three stages:
 * </p>
 * <p>
 * Stage 1: Finding matching document IDs for the input page. This stage is done
 * by the {@link EnrichQuerySourceOperator}. The output page of this stage is
 * represented as {@code [DocVector, IntBlock: positions of the input terms]}.
 * </p>
 * <p>
 * Stage 2: Extracting field values for the matched document IDs. The output page
 * is represented as
 * {@code [DocVector, IntBlock: positions, Block: field1, Block: field2,...]}.
 * </p>
 * <p>
 * Stage 3: Optionally this combines the extracted values based on positions and filling
 * nulls for positions without matches. This is done by {@link MergePositionsOperator}.
 * The output page is represented as {@code [Block: field1, Block: field2,...]}.
 * </p>
 * <p>
 * The {@link Page#getPositionCount()} of the output {@link Page} is  equal to the
 * {@link Page#getPositionCount()} of the input page. In other words - it returns
 * the same number of rows that it was sent no matter how many documents match.
 * </p>
 */
public abstract class AbstractLookupService<R extends AbstractLookupService.Request, T extends AbstractLookupService.TransportRequest> {
    private final String actionName;
    protected final ClusterService clusterService;
    protected final IndicesService indicesService;
    private final LookupShardContextFactory lookupShardContextFactory;
    protected final TransportService transportService;
    IndexNameExpressionResolver indexNameExpressionResolver;
    protected final Executor executor;
    private final BlockFactory blockFactory;
    private final LocalCircuitBreaker.SizeSettings localBreakerSettings;
    private final ProjectResolver projectResolver;
    /**
     * Should output {@link Page pages} be combined into a single resulting page?
     * If this is {@code true} we'll run a {@link MergePositionsOperator} to merge
     * all output Pages into a single result, merging each found document into
     * one row per input row, squashing the fields into multivalued fields. If this
     * is {@code false} then we'll skip this step, and it's up to the caller to
     * figure out what to do with a {@link List} of resulting pages.
     */
    private final boolean mergePages;
    private final LookupExecutionMapper executionMapper;

    AbstractLookupService(
        String actionName,
        ClusterService clusterService,
        IndicesService indicesService,
        LookupShardContextFactory lookupShardContextFactory,
        TransportService transportService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        boolean mergePages,
        CheckedBiFunction<StreamInput, BlockFactory, T, IOException> readRequest,
        ProjectResolver projectResolver
    ) {
        this.actionName = actionName;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.lookupShardContextFactory = lookupShardContextFactory;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.executor = transportService.getThreadPool().executor(ThreadPool.Names.SEARCH);
        this.blockFactory = blockFactory;
        this.localBreakerSettings = new LocalCircuitBreaker.SizeSettings(clusterService.getSettings());
        this.mergePages = mergePages;
        this.projectResolver = projectResolver;
        this.executionMapper = new LookupExecutionMapper(blockFactory, bigArrays, localBreakerSettings, mergePages);
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
    protected abstract LookupEnrichQueryGenerator queryList(
        T request,
        SearchExecutionContext context,
        AliasFilter aliasFilter,
        Warnings warnings
    );

    /**
     * Build the response.
     */
    protected abstract LookupResponse createLookupResponse(List<Page> resultPages, BlockFactory blockFactory) throws IOException;

    /**
     * Read the response from a {@link StreamInput}.
     */
    protected abstract LookupResponse readLookupResponse(StreamInput in, BlockFactory blockFactory) throws IOException;

    protected static QueryList termQueryList(
        MappedFieldType field,
        SearchExecutionContext searchExecutionContext,
        AliasFilter aliasFilter,
        int channelOffset,
        DataType inputDataType
    ) {
        return switch (inputDataType) {
            case IP -> QueryList.ipTermQueryList(field, searchExecutionContext, aliasFilter, channelOffset);
            case DATETIME -> QueryList.dateTermQueryList(field, searchExecutionContext, aliasFilter, channelOffset);
            case DATE_NANOS -> QueryList.dateNanosTermQueryList(field, searchExecutionContext, aliasFilter, channelOffset);
            default -> QueryList.rawTermQueryList(
                field,
                searchExecutionContext,
                aliasFilter,
                channelOffset,
                PlannerUtils.toElementType(inputDataType)
            );
        };
    }

    /**
     * Perform the actual lookup.
     */
    public final void lookupAsync(R request, CancellableTask parentTask, ActionListener<List<Page>> outListener) {
        ClusterState clusterState = clusterService.state();
        var projectState = projectResolver.getProjectState(clusterState);
        List<SearchShardRouting> shardIterators = clusterService.operationRouting()
            .searchShards(projectState, new String[] { request.index }, Map.of(), "_local");
        if (shardIterators.size() != 1) {
            outListener.onFailure(new EsqlIllegalArgumentException("target index {} has more than one shard", request.index));
            return;
        }
        ShardIterator shardIt = shardIterators.get(0);
        ShardRouting shardRouting = shardIt.nextOrNull();
        ShardId shardId = shardIt.shardId();
        if (shardRouting == null) {
            outListener.onFailure(new UnavailableShardsException(shardId, "target index is not available"));
            return;
        }
        DiscoveryNode targetNode = clusterState.nodes().get(shardRouting.currentNodeId());
        T transportRequest = transportRequest(request, shardId);
        // TODO: handle retry and avoid forking for the local lookup
        sendChildRequest(parentTask, outListener, targetNode, transportRequest);
    }

    protected void sendChildRequest(
        CancellableTask parentTask,
        ActionListener<List<Page>> delegate,
        DiscoveryNode targetNode,
        T transportRequest
    ) {
        transportService.sendChildRequest(
            targetNode,
            actionName,
            transportRequest,
            parentTask,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(
                delegate.map(LookupResponse::takePages),
                in -> readLookupResponse(in, blockFactory),
                executor
            )
        );
    }

    private void doLookup(T request, CancellableTask task, ActionListener<List<Page>> listener) {
        final List<Releasable> releasables = new ArrayList<>(6);
        boolean started = false;
        try {

            // Create shard context once - used by both physical and execution planning
            LookupShardContext shardContext = lookupShardContextFactory.create(request.shardId);
            releasables.add(shardContext.release);

            PhysicalPlan physicalPlan = createLookupPhysicalPlan(request, shardContext);

            for (int j = 0; j < request.inputPage.getBlockCount(); j++) {
                Block inputBlock = request.inputPage.getBlock(j);
                if (inputBlock.areAllValuesNull()) {
                    List<Page> nullResponse = mergePages
                        ? List.of(createNullResponse(request.inputPage.getPositionCount(), request.extractFields))
                        : List.of();
                    listener.onResponse(nullResponse);
                    return;
                }
            }

            LookupQueryPlan lookupQueryPlan = executionMapper.map(request, physicalPlan, shardContext, releasables);

            startDriver(request, task, listener, lookupQueryPlan);
            started = true;
        } catch (Exception e) {
            listener.onFailure(e);
        } finally {
            if (started == false) {
                Releasables.close(releasables);
            }
        }
    }

    /**
     * Creates a PhysicalPlan tree representing the lookup operation structure.
     * This plan can be cached and reused across multiple calls with different input data.
     * Also precomputes mergingTypes, mergingChannels, and extractFieldInfos
     * which don't depend on driver context or input page.
     */
    private PhysicalPlan createLookupPhysicalPlan(T request, LookupShardContext shardContext) throws IOException {
        var projectState = projectResolver.getProjectState(clusterService.state());
        AliasFilter aliasFilter = indicesService.buildAliasFilter(
            projectState,
            request.shardId.getIndex().getName(),
            indexNameExpressionResolver.resolveExpressions(projectState.metadata(), request.indexPattern)
        );
        // Create output attributes: doc block
        FieldAttribute docAttribute = new FieldAttribute(
            request.source,
            null,
            null,
            EsQueryExec.DOC_ID_FIELD.getName(),
            EsQueryExec.DOC_ID_FIELD
        );
        List<Attribute> sourceOutput = List.of(docAttribute);

        // Create QueryList during physical plan creation - it will receive Block at runtime via getQuery()
        var warnings = Warnings.createWarnings(
            DriverContext.WarningsMode.COLLECT,
            request.source.source().getLineNumber(),
            request.source.source().getColumnNumber(),
            request.source.text()
        );
        // Create QueryList now - it doesn't need the Block until getQuery() is called
        LookupEnrichQueryGenerator queryList = queryList(request, shardContext.executionContext(), aliasFilter, warnings);

        ParameterizedQueryExec source = new ParameterizedQueryExec(request.source, sourceOutput, queryList);

        PhysicalPlan plan = source;

        final ElementType[] mergingTypes = new ElementType[request.extractFields.size()];
        for (int i = 0; i < request.extractFields.size(); i++) {
            mergingTypes[i] = PlannerUtils.toElementType(request.extractFields.get(i).dataType());
        }
        final int[] mergingChannels = IntStream.range(0, request.extractFields.size()).map(i -> i + 2).toArray();

        // Add FieldExtractExec if we have extract fields
        if (request.extractFields.isEmpty() == false) {
            List<Attribute> extractAttributes = new ArrayList<>();
            for (NamedExpression extractField : request.extractFields) {
                if (extractField instanceof Alias alias) {
                    // Extract the underlying attribute from the alias's child
                    NamedExpression child = (NamedExpression) alias.child();
                    extractAttributes.add(child.toAttribute());
                } else {
                    extractAttributes.add(extractField.toAttribute());
                }
            }
            plan = new FieldExtractExec(request.source, plan, extractAttributes, MappedFieldType.FieldExtractPreference.NONE);
        }

        // Add LookupMergeDropExec for merge/drop operations (with precomputed merging types and channels)
        // Note: mergePages decision is made dynamically when creating the operator, not here
        // because it depends on the block structure of each individual page
        // a different decision can be made for different pages
        plan = new LookupMergeDropExec(request.source, plan, request.extractFields, mergingTypes, mergingChannels);

        plan = new OutputExec(request.source, plan, null);

        return plan;
    }

    record LookupQueryPlan(
        LookupShardContext shardContext,
        LocalCircuitBreaker localBreaker,
        DriverContext driverContext,
        EnrichQuerySourceOperator queryOperator,
        List<Operator> operators,
        List<Page> collectedPages,
        OutputOperator outputOperator
    ) {}

    private void startDriver(T request, CancellableTask task, ActionListener<List<Page>> listener, LookupQueryPlan lookupQueryPlan) {
        Driver driver = new Driver(
            "enrich-lookup:" + request.sessionId,
            "enrich",
            clusterService.getClusterName().value(),
            clusterService.getNodeName(),
            System.currentTimeMillis(),
            System.nanoTime(),
            lookupQueryPlan.driverContext(),
            request::toString,
            lookupQueryPlan.queryOperator(),
            lookupQueryPlan.operators(),
            lookupQueryPlan.outputOperator(),
            Driver.DEFAULT_STATUS_INTERVAL,
            Releasables.wrap(lookupQueryPlan.shardContext().release, lookupQueryPlan.localBreaker())
        );
        task.addListener(() -> {
            String reason = Objects.requireNonNullElse(task.getReasonCancelled(), "task was cancelled");
            driver.cancel(reason);
        });
        var threadContext = transportService.getThreadPool().getThreadContext();
        Driver.start(threadContext, executor, driver, Driver.DEFAULT_MAX_ITERATIONS, new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                List<Page> out = lookupQueryPlan.collectedPages();
                if (mergePages && out.isEmpty()) {
                    out = List.of(createNullResponse(request.inputPage.getPositionCount(), request.extractFields));
                }
                listener.onResponse(out);
            }

            @Override
            public void onFailure(Exception e) {
                Releasables.closeExpectNoException(
                    Releasables.wrap(() -> Iterators.map(lookupQueryPlan.collectedPages().iterator(), p -> () -> {
                        p.allowPassingToDifferentDriver();
                        p.releaseBlocks();
                    }))
                );
                listener.onFailure(e);
            }
        });
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
                    (l, resultPages) -> ActionListener.respondAndRelease(l, createLookupResponse(resultPages, blockFactory))
                )
            );
        }
    }

    abstract static class Request {
        final String sessionId;
        final String index;
        final String indexPattern;
        final DataType inputDataType;
        final Page inputPage;
        final List<NamedExpression> extractFields;
        final Source source;

        Request(
            String sessionId,
            String index,
            String indexPattern,
            DataType inputDataType,
            Page inputPage,
            List<NamedExpression> extractFields,
            Source source
        ) {
            this.sessionId = sessionId;
            this.index = index;
            this.indexPattern = indexPattern;
            this.inputDataType = inputDataType;
            this.inputPage = inputPage;
            this.extractFields = extractFields;
            this.source = source;
        }
    }

    abstract static class TransportRequest extends AbstractTransportRequest implements IndicesRequest {
        final String sessionId;
        final ShardId shardId;
        final String indexPattern;
        final Page inputPage;
        final List<NamedExpression> extractFields;
        final Source source;
        // TODO: Remove this workaround once we have Block RefCount
        final Page toRelease;
        final RefCounted refs = AbstractRefCounted.of(this::releasePage);

        TransportRequest(
            String sessionId,
            ShardId shardId,
            String indexPattern,
            Page inputPage,
            Page toRelease,
            List<NamedExpression> extractFields,
            Source source
        ) {
            this.sessionId = sessionId;
            this.shardId = shardId;
            this.indexPattern = indexPattern;
            this.inputPage = inputPage;
            this.toRelease = toRelease;
            this.extractFields = extractFields;
            this.source = source;
        }

        public Page getInputPage() {
            return inputPage;
        }

        @Override
        public final String[] indices() {
            return new String[] { indexPattern };
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
                + " ,extract_fields="
                + extractFields
                + " ,positions="
                + inputPage.getPositionCount()
                + extraDescription()
                + ")";
        }

        protected abstract String extraDescription();
    }

    abstract static class LookupResponse extends TransportResponse {
        private final RefCounted refs = AbstractRefCounted.of(this::release);
        protected final BlockFactory blockFactory;
        protected long reservedBytes = 0;

        LookupResponse(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
        }

        protected abstract List<Page> takePages();

        private void release() {
            blockFactory.breaker().addWithoutBreaking(-reservedBytes);
            innerRelease();
        }

        protected abstract void innerRelease();

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

    /**
     * Create a {@link LookupShardContext} for a locally allocated {@link ShardId}.
     */
    public interface LookupShardContextFactory {
        LookupShardContext create(ShardId shardId) throws IOException;

        static LookupShardContextFactory fromSearchService(SearchService searchService) {
            return shardId -> {
                ShardSearchRequest shardSearchRequest = new ShardSearchRequest(shardId, 0, AliasFilter.EMPTY);
                return LookupShardContext.fromSearchContext(
                    searchService.createSearchContext(shardSearchRequest, SearchService.NO_TIMEOUT)
                );
            };
        }
    }

    /**
     * {@link AbstractLookupService} uses this to power the queries and field loading that
     * it needs to perform to actually do the lookup.
     */
    public record LookupShardContext(
        EsPhysicalOperationProviders.ShardContext context,
        SearchExecutionContext executionContext,
        Releasable release
    ) {
        public static LookupShardContext fromSearchContext(SearchContext searchContext) {
            return new LookupShardContext(
                new EsPhysicalOperationProviders.DefaultShardContext(
                    0,
                    searchContext,
                    searchContext.getSearchExecutionContext(),
                    searchContext.request().getAliasFilter()
                ),
                searchContext.getSearchExecutionContext(),
                searchContext
            );
        }
    }
}
