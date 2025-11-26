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
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.FilterOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.ProjectOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.lookup.EnrichQuerySourceOperator;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
    private final String actionName;
    protected final ClusterService clusterService;
    protected final IndicesService indicesService;
    private final LookupShardContextFactory lookupShardContextFactory;
    protected final TransportService transportService;
    IndexNameExpressionResolver indexNameExpressionResolver;
    protected final Executor executor;
    private final BigArrays bigArrays;
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
        this.bigArrays = bigArrays;
        this.blockFactory = blockFactory;
        this.localBreakerSettings = new LocalCircuitBreaker.SizeSettings(clusterService.getSettings());
        this.mergePages = mergePages;
        this.projectResolver = projectResolver;
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
        Block inputBlock,
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
        Block block,
        @Nullable DataType inputDataType
    ) {
        return switch (inputDataType) {
            case IP -> QueryList.ipTermQueryList(field, searchExecutionContext, aliasFilter, (BytesRefBlock) block);
            case DATETIME -> QueryList.dateTermQueryList(field, searchExecutionContext, aliasFilter, (LongBlock) block);
            case DATE_NANOS -> QueryList.dateNanosTermQueryList(field, searchExecutionContext, aliasFilter, (LongBlock) block);
            case null, default -> QueryList.rawTermQueryList(field, searchExecutionContext, aliasFilter, block);
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
                indexNameExpressionResolver.resolveExpressions(projectState.metadata(), request.indexPattern)
            );

            LookupShardContext shardContext = lookupShardContextFactory.create(request.shardId);
            releasables.add(shardContext.release);
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
            final Operator finishPages;
            final OrdinalBytesRefBlock ordinalsBytesRefBlock;
            Block inputBlock = request.inputPage.getBlock(0);
            if (mergePages  // TODO fix this optimization for Lookup.
                && inputBlock instanceof BytesRefBlock bytesRefBlock
                && (ordinalsBytesRefBlock = bytesRefBlock.asOrdinals()) != null) {

                inputBlock = ordinalsBytesRefBlock.getDictionaryVector().asBlock();
                var selectedPositions = ordinalsBytesRefBlock.getOrdinalsBlock();
                finishPages = new MergePositionsOperator(1, mergingChannels, mergingTypes, selectedPositions, driverContext.blockFactory());
            } else {
                if (mergePages) {
                    try (var selectedPositions = IntVector.range(0, inputBlock.getPositionCount(), blockFactory).asBlock()) {
                        finishPages = new MergePositionsOperator(
                            1,
                            mergingChannels,
                            mergingTypes,
                            selectedPositions,
                            driverContext.blockFactory()
                        );
                    }
                } else {
                    finishPages = dropDocBlockOperator(request.extractFields);
                }
            }
            releasables.add(finishPages);
            var warnings = Warnings.createWarnings(
                DriverContext.WarningsMode.COLLECT,
                request.source.source().getLineNumber(),
                request.source.source().getColumnNumber(),
                request.source.text()
            );
            // creates a layout builder and appends Docs and Positions fields
            Layout.Builder builder = createLookupLayoutBuilder();
            // add the main query operator (the Lucene search)
            LookupEnrichQueryGenerator queryList = queryList(request, shardContext.executionContext, aliasFilter, inputBlock, warnings);
            EnrichQuerySourceOperator queryOperator = createQueryOperator(
                queryList,
                shardContext,
                warnings,
                driverContext,
                builder,
                request.extractFields,
                releasables
            );
            List<Operator> operators = new ArrayList<>();

            // get the fields from the right side, as specified in extractFields
            // Also extract additional right-side fields that are referenced in post-join filters but not in extractFields
            extractRightFields(queryList, request, shardContext, driverContext, builder, releasables, operators);

            // get the left side fields that are needed for filter application
            // we read them from the input page and populate in the output page
            extractLeftFields(queryList, request, builder, driverContext, releasables, operators);

            // add the filters to be executed after the join
            // all fields needed have been populated above
            addPostJoinFilterOperator(queryList, shardContext, driverContext, builder, releasables, operators);

            // append the finish pages operator at the end
            // it should remove the fields that are not needed in the output (e.g. the left side fields extracted for filtering)
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

    /**
     * Field for DocID in lookup operations. Contains a DocVector.
     */
    private static final EsField LOOKUP_DOC_ID_FIELD = new EsField(
        "$$DocID$$",
        DataType.DOC_DATA_TYPE,
        Map.of(),
        false,
        EsField.TimeSeriesFieldType.NONE
    );

    /**
     * Field for Positions in lookup operations. Contains an IntBlock of positions.
     */
    private static final EsField LOOKUP_POSITIONS_FIELD = new EsField(
        "$$Positions$$",
        DataType.INTEGER,
        Map.of(),
        false,
        EsField.TimeSeriesFieldType.NONE
    );

    /**
     * Creates a Layout.Builder for lookup operations with Docs and Positions fields.
     */
    private static Layout.Builder createLookupLayoutBuilder() {
        Layout.Builder builder = new Layout.Builder();
        // append the docsIds and positions to the layout
        builder.append(new FieldAttribute(Source.EMPTY, null, null, LOOKUP_DOC_ID_FIELD.getName(), LOOKUP_DOC_ID_FIELD));
        builder.append(new FieldAttribute(Source.EMPTY, null, null, LOOKUP_POSITIONS_FIELD.getName(), LOOKUP_POSITIONS_FIELD));
        return builder;
    }

    /**
     * Creates the query operator for lookup operations and sets up the initial layout.
     * Adds the operator to releasables and appends extractFields to the builder.
     */
    private EnrichQuerySourceOperator createQueryOperator(
        LookupEnrichQueryGenerator queryList,
        LookupShardContext shardContext,
        Warnings warnings,
        DriverContext driverContext,
        Layout.Builder builder,
        List<NamedExpression> extractFields,
        List<Releasable> releasables
    ) {
        var queryOperator = new EnrichQuerySourceOperator(
            driverContext.blockFactory(),
            EnrichQuerySourceOperator.DEFAULT_MAX_PAGE_SIZE,
            queryList,
            new IndexedByShardIdFromSingleton<>(shardContext.context),
            0,
            warnings
        );
        releasables.add(queryOperator);
        builder.append(extractFields);
        return queryOperator;
    }

    /**
     * Extracts right-side fields from the lookup index and creates the extractFields operator.
     * Also extracts additional right-side fields that are referenced in post-join filters but not in extractFields.
     */
    private void extractRightFields(
        LookupEnrichQueryGenerator queryList,
        T request,
        LookupShardContext shardContext,
        DriverContext driverContext,
        Layout.Builder builder,
        List<Releasable> releasables,
        List<Operator> operators
    ) {
        // Start with the original extractFields
        List<NamedExpression> allExtractFields = new ArrayList<>(request.extractFields);

        // Collect additional right-side fields referenced in post-join filters but not in extractFields
        collectAdditionalRightFieldsForFilters(queryList, request, builder, allExtractFields);

        // Create a single operator for all extract fields
        if (allExtractFields.isEmpty() == false) {
            var extractFieldsOperator = extractFieldsOperator(shardContext.context, driverContext, allExtractFields);
            releasables.add(extractFieldsOperator);
            operators.add(extractFieldsOperator);
        }
    }

    /**
     * Collects additional right-side fields that are referenced in post-join filters but not in extractFields.
     * These fields are added to allExtractFields and the layout builder.
     */
    private void collectAdditionalRightFieldsForFilters(
        LookupEnrichQueryGenerator queryList,
        T request,
        Layout.Builder builder,
        List<NamedExpression> allExtractFields
    ) {
        if (queryList instanceof PostJoinFilterable postJoinFilterable) {
            List<Expression> postJoinFilterExpressions = postJoinFilterable.getPostJoinFilter();
            if (postJoinFilterExpressions.isEmpty() == false) {
                LookupFromIndexService.TransportRequest lookupRequest = (LookupFromIndexService.TransportRequest) request;
                // Build a set of extractFields NameIDs
                Set<NameId> extractFieldNameIds = new HashSet<>();
                for (NamedExpression extractField : request.extractFields) {
                    extractFieldNameIds.add(extractField.id());
                }
                // Collect right-side field NameIDs from EsRelation in rightPreJoinPlan
                Set<NameId> rightSideFieldNameIds = collectRightSideFieldNameIds(lookupRequest);

                // Collect right-side attributes referenced in post-join filters but not in extractFields
                Set<NameId> addedNameIds = new HashSet<>();
                for (Expression filterExpr : postJoinFilterExpressions) {
                    for (Attribute attr : filterExpr.references()) {
                        NameId nameId = attr.id();
                        // If it's a right-side field but not in extractFields, we need to extract it
                        if (rightSideFieldNameIds.contains(nameId) && extractFieldNameIds.contains(nameId) == false) {
                            if (addedNameIds.contains(nameId) == false) {
                                allExtractFields.add(attr);
                                builder.append(attr);
                                addedNameIds.add(nameId);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Collects right-side field NameIDs from EsRelation in the rightPreJoinPlan.
     * Similar to collectLeftSideFieldsToBroadcast, but collects right-side fields instead.
     */
    private static Set<NameId> collectRightSideFieldNameIds(LookupFromIndexService.TransportRequest request) {
        Set<NameId> rightSideFieldNameIds = new HashSet<>();
        if (request.getRightPreJoinPlan() instanceof FragmentExec fragmentExec) {
            fragmentExec.fragment().forEachDown(EsRelation.class, esRelation -> {
                for (Attribute attr : esRelation.output()) {
                    rightSideFieldNameIds.add(attr.id());
                }
            });
        }
        return rightSideFieldNameIds;
    }

    /**
     * Extracts left-side fields that need to be broadcast and creates the matchFields operator.
     * Collects left-side fields from post-join filter expressions and broadcasts them.
     */
    private void extractLeftFields(
        LookupEnrichQueryGenerator queryList,
        T request,
        Layout.Builder builder,
        DriverContext driverContext,
        List<Releasable> releasables,
        List<Operator> operators
    ) {
        // Collect all left-side fields that will be added to the layout so we can broadcast them
        List<MatchConfig> allLeftFieldsToBroadcast = collectLeftSideFieldsToBroadcast(queryList, request, builder);
        // Add all left-side fields to the Page so they're available when evaluating post-join filters
        // We broadcast them using the Positions block (channel 1), similar to RightChunkedLeftJoin
        // The order must match the layout order
        if (allLeftFieldsToBroadcast.isEmpty() == false) {
            Operator matchFieldsOperator = broadcastMatchFieldsOperator(driverContext, request.inputPage, allLeftFieldsToBroadcast);
            releasables.add(matchFieldsOperator);
            operators.add(matchFieldsOperator);
        }
    }

    /**
     * Adds a post-join filter operator if the queryList has post-join filter expressions.
     */
    private void addPostJoinFilterOperator(
        LookupEnrichQueryGenerator queryList,
        LookupShardContext shardContext,
        DriverContext driverContext,
        Layout.Builder builder,
        List<Releasable> releasables,
        List<Operator> operators
    ) {
        if (queryList instanceof PostJoinFilterable postJoinFilterable) {
            List<Expression> postJoinFilterExpressions = postJoinFilterable.getPostJoinFilter();
            if (postJoinFilterExpressions.isEmpty() == false) {
                Expression combinedFilter = Predicates.combineAnd(postJoinFilterExpressions);
                Operator postJoinFilter = filterExecOperator(combinedFilter, shardContext.context, driverContext, builder);
                if (postJoinFilter != null) {
                    releasables.add(postJoinFilter);
                    operators.add(postJoinFilter);
                }
            }
        }
    }

    private Operator filterExecOperator(
        Expression filterExpression,
        EsPhysicalOperationProviders.ShardContext shardContext,
        DriverContext driverContext,
        Layout.Builder builder
    ) {
        if (filterExpression == null) {
            return null;
        }

        var evaluatorFactory = EvalMapper.toEvaluator(
            FoldContext.small()/*is this correct*/,
            filterExpression,
            builder.build(),
            new IndexedByShardIdFromSingleton<>(shardContext)
        );
        var filterOperatorFactory = new FilterOperator.FilterOperatorFactory(evaluatorFactory);
        return filterOperatorFactory.get(driverContext);
    }

    /**
     * Collects left-side fields from post-join filter expressions that need to be broadcast.
     * Adds these fields to the layout builder and returns the list of MatchConfigs to broadcast.
     */
    private List<MatchConfig> collectLeftSideFieldsToBroadcast(LookupEnrichQueryGenerator queryList, T request, Layout.Builder builder) {
        List<MatchConfig> allLeftFieldsToBroadcast = new ArrayList<>();
        // Extract left-side fields from post-join filter expressions to determine what needs to be broadcast
        if (queryList instanceof PostJoinFilterable postJoinFilterable) {
            List<Expression> postJoinFilterExpressions = postJoinFilterable.getPostJoinFilter();
            if (postJoinFilterExpressions.isEmpty() == false) {
                LookupFromIndexService.TransportRequest lookupRequest = (LookupFromIndexService.TransportRequest) request;
                // Build a set of extractFields NameIDs to avoid adding duplicates
                Set<NameId> extractFieldNameIds = new HashSet<>();
                for (NamedExpression extractField : request.extractFields) {
                    extractFieldNameIds.add(extractField.id());
                }
                // Collect right-side field NameIDs from EsRelation in rightPreJoinPlan
                Set<NameId> rightSideFieldNameIds = new HashSet<>();
                if (lookupRequest.getRightPreJoinPlan() instanceof FragmentExec fragmentExec) {
                    fragmentExec.fragment().forEachDown(EsRelation.class, esRelation -> {
                        for (Attribute attr : esRelation.output()) {
                            rightSideFieldNameIds.add(attr.id());
                        }
                    });
                }

                // Track which NameIDs we've already added to avoid duplicates
                Set<NameId> addedNameIds = new HashSet<>();
                // Traverse filter expressions and match attributes to MatchConfigs by NameID
                // Exclude right-side fields and fields already in extractFields
                for (Expression filterExpr : postJoinFilterExpressions) {
                    for (Attribute attr : filterExpr.references()) {
                        NameId nameId = attr.id();
                        // Skip if right-side field, already in extractFields, or already found a match for
                        if (rightSideFieldNameIds.contains(nameId)
                            || extractFieldNameIds.contains(nameId)
                            || addedNameIds.contains(nameId)) {
                            continue;
                        }
                        // Find the corresponding MatchConfig for this attribute
                        // we do match by just name
                        // we made sure the same attribute is not on the right side with the checks above
                        for (MatchConfig matchField : lookupRequest.getMatchFields()) {
                            if (attr.equals(matchField.fieldName())) {
                                builder.append(attr);
                                allLeftFieldsToBroadcast.add(matchField);
                                addedNameIds.add(nameId);
                                break;
                            }
                        }

                    }
                }
            }
        }
        return allLeftFieldsToBroadcast;
    }

    /**
     * Creates an operator that broadcasts matchFields from the inputPage to each output Page
     * using the Positions block at the specified channel to determine which left-hand row each right-hand row corresponds to.
     * This is similar to how RightChunkedLeftJoin broadcasts left-hand blocks.
     */
    private Operator broadcastMatchFieldsOperator(DriverContext driverContext, Page inputPage, List<MatchConfig> matchFields) {
        return new BroadcastMatchFieldsOperator(driverContext, inputPage, matchFields, 1);
    }

    private static Operator extractFieldsOperator(
        EsPhysicalOperationProviders.ShardContext shardContext,
        DriverContext driverContext,
        List<NamedExpression> extractFields
    ) {
        List<ValuesSourceReaderOperator.FieldInfo> fields = new ArrayList<>(extractFields.size());
        for (NamedExpression extractField : extractFields) {
            String fieldName = extractField instanceof FieldAttribute fa ? fa.fieldName().string()
                // Cases for Alias and ReferenceAttribute: only required for ENRICH (Alias in case of ENRICH ... WITH x = field)
                // (LOOKUP JOIN uses FieldAttributes)
                : extractField instanceof Alias a ? ((NamedExpression) a.child()).name()
                : extractField.name();
            BlockLoader loader = shardContext.blockLoader(
                fieldName,
                extractField.dataType() == DataType.UNSUPPORTED,
                MappedFieldType.FieldExtractPreference.NONE,
                null
            );
            fields.add(
                new ValuesSourceReaderOperator.FieldInfo(
                    extractField.name(),
                    PlannerUtils.toElementType(extractField.dataType()),
                    false,
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
            Long.MAX_VALUE,
            fields,
            new IndexedByShardIdFromSingleton<>(
                new ValuesSourceReaderOperator.ShardContext(
                    shardContext.searcher().getIndexReader(),
                    shardContext::newSourceLoader,
                    EsqlPlugin.STORED_FIELDS_SEQUENTIAL_PROPORTION.getDefault(Settings.EMPTY)
                )
            ),
            0
        );
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
