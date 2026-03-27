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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.ProjectOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.lookup.BlockOptimization;
import org.elasticsearch.compute.operator.lookup.EnrichQuerySourceOperator;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
import org.elasticsearch.compute.operator.lookup.MergePositionsOperator;
import org.elasticsearch.compute.operator.lookup.QueryList;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Function;
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

    /**
     * Represents an integer column that corresponds to the positions of the page that we perform the lookup for.
     */
    public static final ReferenceAttribute LOOKUP_POSITIONS_FIELD = new ReferenceAttribute(
        Source.EMPTY,
        null,
        "$$Positions$$",
        DataType.INTEGER
    );

    private final String actionName;
    protected final ClusterService clusterService;
    protected final IndicesService indicesService;
    protected final LookupShardContextFactory lookupShardContextFactory;
    protected final TransportService transportService;
    IndexNameExpressionResolver indexNameExpressionResolver;
    protected final Executor executor;
    protected final BlockFactory blockFactory;
    protected final BigArrays bigArrays;
    protected final LocalCircuitBreaker.SizeSettings localBreakerSettings;
    protected final ProjectResolver projectResolver;
    protected final PlannerSettings.Holder plannerSettings;
    /**
     * Should output {@link Page pages} be combined into a single resulting page?
     * If this is {@code true} we'll run a {@link MergePositionsOperator} to merge
     * all output Pages into a single result, merging each found document into
     * one row per input row, squashing the fields into multivalued fields. If this
     * is {@code false} then we'll skip this step, and it's up to the caller to
     * figure out what to do with a {@link List} of resulting pages.
     */
    protected final boolean mergePages;

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
        ProjectResolver projectResolver,
        PlannerSettings.Holder plannerSettings
    ) {
        this.actionName = actionName;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.lookupShardContextFactory = lookupShardContextFactory;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.executor = transportService.getThreadPool().executor(ThreadPool.Names.SEARCH);
        this.blockFactory = blockFactory;
        this.bigArrays = bigArrays;
        this.localBreakerSettings = new LocalCircuitBreaker.SizeSettings(clusterService.getSettings());
        this.mergePages = mergePages;
        this.projectResolver = projectResolver;
        this.plannerSettings = plannerSettings;
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

    protected static QueryList termQueryList(MappedFieldType field, AliasFilter aliasFilter, int channelOffset, DataType inputDataType) {
        return switch (inputDataType) {
            case IP -> QueryList.ipTermQueryList(field, aliasFilter, channelOffset);
            case DATETIME -> QueryList.dateTermQueryList(field, aliasFilter, channelOffset);
            case DATE_NANOS -> QueryList.dateNanosTermQueryList(field, aliasFilter, channelOffset);
            default -> QueryList.rawTermQueryList(field, aliasFilter, channelOffset, PlannerUtils.toElementType(inputDataType));
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

    protected void doLookup(T request, CancellableTask task, ActionListener<List<Page>> listener) {
        PlannerSettings plannerSettings = this.plannerSettings.get();
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
        final List<Releasable> releasables = new ArrayList<>(6);
        boolean started = false;
        try {
            var projectState = projectResolver.getProjectState(clusterService.state());
            AliasFilter aliasFilter = indicesService.buildAliasFilter(
                projectState,
                request.shardId.getIndex().getName(),
                indexNameExpressionResolver.resolveExpressionsIgnoringRemotes(projectState.metadata(), request.indexPattern)
            );

            LookupShardContext shardContext = lookupShardContextFactory.create(request.shardId);
            releasables.add(shardContext.release);
            final LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(
                blockFactory.breaker(),
                localBreakerSettings.overReservedBytes(),
                localBreakerSettings.maxOverReservedBytes()
            );
            releasables.add(localBreaker);
            final DriverContext driverContext = new DriverContext(
                bigArrays,
                blockFactory.newChildFactory(localBreaker),
                localBreakerSettings
            );
            final ElementType[] mergingTypes = new ElementType[request.extractFields.size()];
            for (int i = 0; i < request.extractFields.size(); i++) {
                mergingTypes[i] = PlannerUtils.toElementType(request.extractFields.get(i).dataType());
            }
            final int[] mergingChannels = IntStream.range(0, request.extractFields.size()).map(i -> i + 2).toArray();
            final Operator finishPages;
            // Determines the optimization applicable for the input page.
            // Returns the state that indicates what optimization can be applied:
            // - DICTIONARY: input block has ordinals, can use dictionary block and ordinals
            // - RANGE: need to create range block for selected positions
            // - NONE: no optimization needed
            BlockOptimization blockOptimization;
            if (mergePages == false) {
                blockOptimization = BlockOptimization.NONE;
            } else {
                Block inputBlock = request.inputPage.getBlock(0);
                if (inputBlock instanceof BytesRefBlock bytesRefBlock && bytesRefBlock.asOrdinals() != null) {
                    blockOptimization = BlockOptimization.DICTIONARY;
                } else {
                    blockOptimization = BlockOptimization.RANGE;
                }
            }
            if (mergePages) {
                finishPages = new MergePositionsOperator(
                    1,
                    mergingChannels,
                    mergingTypes,
                    blockOptimization,
                    request.inputPage,
                    driverContext.blockFactory()
                );
            } else {
                finishPages = dropDocBlockOperator(request.extractFields);
            }
            releasables.add(finishPages);
            var warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, request.source);
            LookupEnrichQueryGenerator queryList = queryList(request, shardContext.executionContext, aliasFilter, warnings);
            var queryOperator = new EnrichQuerySourceOperator(
                driverContext.blockFactory(),
                EnrichQuerySourceOperator.DEFAULT_MAX_PAGE_SIZE,
                queryList,
                request.inputPage,
                blockOptimization,
                new IndexedByShardIdFromSingleton<>(shardContext.context),
                0,
                shardContext.executionContext,
                warnings
            );
            releasables.add(queryOperator);

            List<Operator> operators = new ArrayList<>();
            if (request.extractFields.isEmpty() == false) {
                var extractFieldsOperator = extractFieldsOperator(
                    plannerSettings,
                    shardContext.context,
                    driverContext,
                    request.extractFields
                );
                releasables.add(extractFieldsOperator);
                operators.add(extractFieldsOperator);
            }
            operators.add(finishPages);

            /*
             * Collect all result Pages in a synchronizedList mostly out of paranoia. We'll
             * be collecting these results in the Driver thread and reading them in its
             * completion listener which absolutely happens-after the insertions. So,
             * technically, we don't need synchronization here. But we're doing it anyway
             * because the list will never grow mega large.
             */
            List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());
            OutputOperator outputOperator = new OutputOperator(List.of(), Function.identity(), collectedPages::add);
            releasables.add(outputOperator);
            Driver driver = new Driver(
                "enrich-lookup:" + request.sessionId,
                "enrich",
                clusterService.getClusterName().value(),
                clusterService.getNodeName(),
                System.currentTimeMillis(),
                System.nanoTime(),
                driverContext,
                request::toString,
                queryOperator,
                operators,
                outputOperator,
                Driver.DEFAULT_STATUS_INTERVAL,
                Releasables.wrap(shardContext.release, localBreaker)
            );
            task.addListener(() -> {
                String reason = Objects.requireNonNullElse(task.getReasonCancelled(), "task was cancelled");
                driver.cancel(reason);
            });
            var threadContext = transportService.getThreadPool().getThreadContext();
            Driver.start(threadContext, executor, driver, Driver.DEFAULT_MAX_ITERATIONS, new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    List<Page> out = collectedPages;
                    if (mergePages && out.isEmpty()) {
                        out = List.of(createNullResponse(request.inputPage.getPositionCount(), request.extractFields));
                    }
                    listener.onResponse(out);
                }

                @Override
                public void onFailure(Exception e) {
                    Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(collectedPages.iterator(), p -> () -> {
                        p.allowPassingToDifferentDriver();
                        p.releaseBlocks();
                    })));
                    listener.onFailure(e);
                }
            });
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
        PlannerSettings plannerSettings,
        EsPhysicalOperationProviders.ShardContext shardContext,
        DriverContext driverContext,
        List<NamedExpression> extractFields
    ) {
        List<ValuesSourceReaderOperator.FieldInfo> fields = new ArrayList<>(extractFields.size());
        for (NamedExpression extractField : extractFields) {
            // Cases for Alias and ReferenceAttribute: only required for ENRICH (Alias in case of ENRICH ... WITH x = field)
            // (LOOKUP JOIN uses FieldAttributes)
            String fieldName = extractFieldName(extractField);
            BlockLoader loader = shardContext.blockLoader(
                fieldName,
                extractField.dataType() == DataType.UNSUPPORTED,
                MappedFieldType.FieldExtractPreference.NONE,
                null,
                plannerSettings.blockLoaderSizeOrdinals(),
                plannerSettings.blockLoaderSizeScript()
            );
            fields.add(
                new ValuesSourceReaderOperator.FieldInfo(
                    fieldName,
                    PlannerUtils.toElementType(extractField.dataType()),
                    false,
                    shardIdx -> {
                        if (shardIdx != 0) {
                            throw new IllegalStateException("only one shard");
                        }
                        return ValuesSourceReaderOperator.load(loader);
                    }
                )
            );
        }
        return new ValuesSourceReaderOperator(
            driverContext,
            Long.MAX_VALUE,
            fields,
            new IndexedByShardIdFromSingleton<>(
                new ValuesSourceReaderOperator.ShardContext(
                    shardContext.searcher().getIndexReader(),
                    shardContext::newSourceLoader,
                    EsqlPlugin.STORED_FIELDS_SEQUENTIAL_PROPORTION.getDefault(Settings.EMPTY)
                )
            ),
            true,
            0
        );
    }

    /**
     * Extracts field name from a NamedExpression, handling FieldAttribute and Alias cases.
     * For Alias, recursively extracts the field name from the child expression.
     */
    public static String extractFieldName(NamedExpression extractField) {
        return extractField instanceof FieldAttribute fa ? fa.fieldName().string()
            : extractField instanceof Alias a ? extractFieldName((NamedExpression) a.child())
            : extractField.name();
    }

    /**
     * Drop just the first block, keeping the remaining.
     */
    private Operator dropDocBlockOperator(List<NamedExpression> extractFields) {
        int end = extractFields.size() + 1;
        List<Integer> projection = new ArrayList<>(end);
        for (int i = 1; i <= end; i++) {
            projection.add(i);
        }
        return new ProjectOperator(projection);
    }

    protected Page createNullResponse(int positionCount, List<NamedExpression> extractFields) {
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
