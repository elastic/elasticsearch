/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeServer;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.compute.operator.lookup.BlockOptimization;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
import org.elasticsearch.compute.operator.lookup.QueryList;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.ParameterizedQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * {@link LookupFromIndexService} performs lookup against a Lookup index for
 * a given input page. See {@link AbstractLookupService} for how it works
 * where it refers to this process as a {@code LEFT JOIN}. Which is mostly is.
 */
public class LookupFromIndexService extends AbstractLookupService<LookupFromIndexService.Request, LookupFromIndexService.TransportRequest> {
    public static final String LOOKUP_ACTION_NAME = EsqlQueryAction.NAME + "/lookup_from_index";
    private static final Logger logger = LogManager.getLogger(LookupFromIndexService.class);

    private static final TransportVersion ESQL_LOOKUP_JOIN_SOURCE_TEXT = TransportVersion.fromName("esql_lookup_join_source_text");
    private static final TransportVersion ESQL_LOOKUP_JOIN_PRE_JOIN_FILTER = TransportVersion.fromName("esql_lookup_join_pre_join_filter");

    private final LookupExecutionPlanner executionPlanner;
    protected final ExchangeService exchangeService;

    public LookupFromIndexService(
        ClusterService clusterService,
        IndicesService indicesService,
        LookupShardContextFactory lookupShardContextFactory,
        TransportService transportService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        ProjectResolver projectResolver,
        ExchangeService exchangeService
    ) {
        super(
            LOOKUP_ACTION_NAME,
            clusterService,
            indicesService,
            lookupShardContextFactory,
            transportService,
            indexNameExpressionResolver,
            bigArrays,
            blockFactory,
            false,// merge pages is NOT implemented for Lookup Join
            TransportRequest::readFrom,
            projectResolver
        );
        this.executionPlanner = new LookupExecutionPlanner(blockFactory, bigArrays, localBreakerSettings);
        this.exchangeService = exchangeService;
    }

    public ExchangeService getExchangeService() {
        return exchangeService;
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public Settings getSettings() {
        return clusterService.getSettings();
    }

    public TransportService getTransportService() {
        return transportService;
    }

    public ProjectResolver getProjectResolver() {
        return projectResolver;
    }

    @Override
    protected TransportRequest transportRequest(LookupFromIndexService.Request request, ShardId shardId) {
        return new TransportRequest(
            request.sessionId,
            shardId,
            request.indexPattern,
            request.inputPage,
            null,
            request.extractFields,
            request.matchFields,
            request.source,
            request.rightPreJoinPlan,
            request.joinOnConditions,
            request.clientToServerId,
            request.serverToClientId,
            request.profile
        );
    }

    @Override
    protected LookupEnrichQueryGenerator queryList(
        TransportRequest request,
        SearchExecutionContext context,
        AliasFilter aliasFilter,
        Warnings warnings
    ) {
        PhysicalPlan lookupNodePlan = localLookupNodePlanning(request.rightPreJoinPlan);
        if (request.joinOnConditions == null) {
            // this is a field based join
            List<QueryList> queryLists = new ArrayList<>();
            for (int i = 0; i < request.matchFields.size(); i++) {
                MatchConfig matchField = request.matchFields.get(i);
                int channelOffset = matchField.channel();
                QueryList q = termQueryList(context.getFieldType(matchField.fieldName()), aliasFilter, channelOffset, matchField.type());
                queryLists.add(q.onlySingleValues(warnings, "LOOKUP JOIN encountered multi-value"));
            }
            if (queryLists.size() == 1 && lookupNodePlan instanceof FilterExec == false) {
                return queryLists.getFirst();
            }
            return ExpressionQueryList.fieldBasedJoin(queryLists, context, lookupNodePlan, clusterService, aliasFilter);
        } else {
            // this is an expression based join
            return ExpressionQueryList.expressionBasedJoin(context, lookupNodePlan, clusterService, request, aliasFilter, warnings);
        }

    }

    /**
     * This function will perform any planning needed on the local node
     * For now, we will just do mapping of the logical plan to physical plan
     * In the future we can also do local physical and logical optimizations.
     * We only support a FragmentExec node containing a logical plan or a null plan
     * If any other plan is sent we will just return null. This can happen in cases
     * where the coordinator is running an older version that does not support
     * keeping the plan as Logical Plan inside FragmentExec yet
     * In those cases, it is safe to ignore the plan sent and return null
     */
    private static PhysicalPlan localLookupNodePlanning(PhysicalPlan physicalPlan) {
        return physicalPlan instanceof FragmentExec fragmentExec ? LocalMapper.INSTANCE.map(fragmentExec.fragment()) : null;
    }

    @Override
    protected LookupResponse createLookupResponse(List<Page> pages, BlockFactory blockFactory) throws IOException {
        return new LookupResponse(pages, blockFactory);
    }

    @Override
    protected AbstractLookupService.LookupResponse readLookupResponse(StreamInput in, BlockFactory blockFactory) throws IOException {
        return new LookupResponse(in, blockFactory);
    }

    public static class Request extends AbstractLookupService.Request {
        private final List<MatchConfig> matchFields;
        private final PhysicalPlan rightPreJoinPlan;
        private final Expression joinOnConditions;
        private final String clientToServerId;
        private final String serverToClientId;
        private final boolean profile;

        Request(
            String sessionId,
            String index,
            String indexPattern,
            List<MatchConfig> matchFields,
            Page inputPage,
            List<NamedExpression> extractFields,
            Source source,
            PhysicalPlan rightPreJoinPlan,
            Expression joinOnConditions,
            String clientToServerId,
            String serverToClientId,
            boolean profile
        ) {
            super(sessionId, index, indexPattern, matchFields.get(0).type(), inputPage, extractFields, source);
            this.matchFields = matchFields;
            this.rightPreJoinPlan = rightPreJoinPlan;
            this.joinOnConditions = joinOnConditions;
            this.clientToServerId = clientToServerId;
            this.serverToClientId = serverToClientId;
            this.profile = profile;
        }
    }

    protected static class TransportRequest extends AbstractLookupService.TransportRequest {

        private static final TransportVersion JOIN_ON_ALIASES = TransportVersion.fromName("join_on_aliases");
        private static final TransportVersion ESQL_LOOKUP_JOIN_ON_MANY_FIELDS = TransportVersion.fromName(
            "esql_lookup_join_on_many_fields"
        );
        private static final TransportVersion ESQL_LOOKUP_JOIN_ON_EXPRESSION = TransportVersion.fromName("esql_lookup_join_on_expression");
        private static final TransportVersion ESQL_STREAMING_LOOKUP_JOIN = TransportVersion.fromName("esql_streaming_lookup_join");

        private final List<MatchConfig> matchFields;
        private final PhysicalPlan rightPreJoinPlan;
        private final Expression joinOnConditions;
        private final String clientToServerId;
        private final String serverToClientId;
        private final boolean profile;

        // Right now we assume that the page contains the same number of blocks as matchFields and that the blocks are in the same order
        // The channel information inside the MatchConfig, should say the same thing
        TransportRequest(
            String sessionId,
            ShardId shardId,
            String indexPattern,
            Page inputPage,
            Page toRelease,
            List<NamedExpression> extractFields,
            List<MatchConfig> matchFields,
            Source source,
            PhysicalPlan rightPreJoinPlan,
            Expression joinOnConditions,
            String clientToServerId,
            String serverToClientId,
            boolean profile
        ) {
            super(sessionId, shardId, indexPattern, inputPage, toRelease, extractFields, source);
            this.matchFields = matchFields;
            this.rightPreJoinPlan = rightPreJoinPlan;
            this.joinOnConditions = joinOnConditions;
            this.clientToServerId = clientToServerId;
            this.serverToClientId = serverToClientId;
            this.profile = profile;
        }

        static TransportRequest readFrom(StreamInput in, BlockFactory blockFactory) throws IOException {
            TaskId parentTaskId = TaskId.readFromStream(in);
            String sessionId = in.readString();
            ShardId shardId = new ShardId(in);

            String indexPattern;
            if (in.getTransportVersion().supports(JOIN_ON_ALIASES)) {
                indexPattern = in.readString();
            } else {
                indexPattern = shardId.getIndexName();
            }

            DataType inputDataType = null;
            if (in.getTransportVersion().supports(ESQL_LOOKUP_JOIN_ON_MANY_FIELDS) == false) {
                inputDataType = DataType.fromTypeName(in.readString());
            }

            Page inputPage;
            try (BlockStreamInput bsi = new BlockStreamInput(in, blockFactory)) {
                inputPage = new Page(bsi);
            }
            PlanStreamInput planIn = new PlanStreamInput(in, in.namedWriteableRegistry(), null);
            List<NamedExpression> extractFields = planIn.readNamedWriteableCollectionAsList(NamedExpression.class);
            List<MatchConfig> matchFields = null;
            if (in.getTransportVersion().supports(ESQL_LOOKUP_JOIN_ON_MANY_FIELDS)) {
                matchFields = planIn.readCollectionAsList(MatchConfig::new);
            } else {
                String matchField = in.readString();
                // For older versions, we only support a single match field.
                matchFields = new ArrayList<>(1);
                matchFields.add(new MatchConfig(matchField, 0, inputDataType));
            }
            var source = Source.readFrom(planIn);
            // Source.readFrom() requires the query from the Configuration passed to PlanStreamInput.
            // As we don't have the Configuration here, and it may be heavy to serialize, we directly pass the Source text.
            if (in.getTransportVersion().supports(ESQL_LOOKUP_JOIN_SOURCE_TEXT)) {
                String sourceText = in.readString();
                source = new Source(source.source(), sourceText);
            }
            PhysicalPlan rightPreJoinPlan = null;
            if (in.getTransportVersion().supports(ESQL_LOOKUP_JOIN_PRE_JOIN_FILTER)) {
                rightPreJoinPlan = planIn.readOptionalNamedWriteable(PhysicalPlan.class);
            }
            Expression joinOnConditions = null;
            if (in.getTransportVersion().supports(ESQL_LOOKUP_JOIN_ON_EXPRESSION)) {
                joinOnConditions = planIn.readOptionalNamedWriteable(Expression.class);
            }
            String clientToServerId = null;
            if (in.getTransportVersion().supports(ESQL_STREAMING_LOOKUP_JOIN)) {
                clientToServerId = in.readOptionalString();
            }
            String serverToClientId = null;
            if (in.getTransportVersion().supports(ESQL_STREAMING_LOOKUP_JOIN)) {
                serverToClientId = in.readOptionalString();
            }
            boolean profile = false;
            if (in.getTransportVersion().supports(ESQL_STREAMING_LOOKUP_JOIN)) {
                profile = in.readBoolean();
            }
            TransportRequest result = new TransportRequest(
                sessionId,
                shardId,
                indexPattern,
                inputPage,
                inputPage,
                extractFields,
                matchFields,
                source,
                rightPreJoinPlan,
                joinOnConditions,
                clientToServerId,
                serverToClientId,
                profile
            );
            result.setParentTask(parentTaskId);
            return result;
        }

        public Expression getJoinOnConditions() {
            return joinOnConditions;
        }

        public List<MatchConfig> getMatchFields() {
            return matchFields;
        }

        public String getClientToServerId() {
            return clientToServerId;
        }

        public String getServerToClientId() {
            return serverToClientId;
        }

        public boolean isProfile() {
            return profile;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sessionId);
            out.writeWriteable(shardId);

            if (out.getTransportVersion().supports(JOIN_ON_ALIASES)) {
                out.writeString(indexPattern);
            } else if (indexPattern.equals(shardId.getIndexName()) == false) {
                throw new EsqlIllegalArgumentException("Aliases and index patterns are not allowed for LOOKUP JOIN [{}]", indexPattern);
            }
            if (out.getTransportVersion().supports(ESQL_LOOKUP_JOIN_ON_MANY_FIELDS) == false) {
                // only write this for old versions
                // older versions only support a single match field
                if (matchFields.size() > 1) {
                    throw new EsqlIllegalArgumentException("LOOKUP JOIN on multiple fields is not supported on remote node");
                }
                out.writeString(matchFields.get(0).type().typeName());
            }
            out.writeWriteable(inputPage);
            PlanStreamOutput planOut = new PlanStreamOutput(out, null);
            planOut.writeNamedWriteableCollection(extractFields);
            if (out.getTransportVersion().supports(ESQL_LOOKUP_JOIN_ON_MANY_FIELDS)) {
                // serialize all match fields for new versions
                planOut.writeCollection(matchFields, (o, matchConfig) -> matchConfig.writeTo(o));
            } else {
                // older versions only support a single match field, we already checked this above when writing the datatype
                // send the field name of the first and only match field here
                out.writeString(matchFields.get(0).fieldName());
            }
            source.writeTo(planOut);
            if (out.getTransportVersion().supports(ESQL_LOOKUP_JOIN_SOURCE_TEXT)) {
                out.writeString(source.text());
            }
            if (out.getTransportVersion().supports(ESQL_LOOKUP_JOIN_PRE_JOIN_FILTER)) {
                planOut.writeOptionalNamedWriteable(rightPreJoinPlan);
            }
            if (out.getTransportVersion().supports(ESQL_LOOKUP_JOIN_ON_EXPRESSION)) {
                planOut.writeOptionalNamedWriteable(joinOnConditions);
            } else {
                if (joinOnConditions != null) {
                    throw new IllegalArgumentException("LOOKUP JOIN with ON conditions is not supported on remote node");
                }
            }
            if (out.getTransportVersion().supports(ESQL_STREAMING_LOOKUP_JOIN)) {
                out.writeOptionalString(clientToServerId);
            }
            if (out.getTransportVersion().supports(ESQL_STREAMING_LOOKUP_JOIN)) {
                out.writeOptionalString(serverToClientId);
            }
            if (out.getTransportVersion().supports(ESQL_STREAMING_LOOKUP_JOIN)) {
                out.writeBoolean(profile);
            }
        }

        @Override
        protected String extraDescription() {
            return " ,match_fields="
                + matchFields.stream().map(MatchConfig::fieldName).collect(Collectors.joining(", "))
                + ", right_pre_join_plan="
                + (rightPreJoinPlan == null ? "null" : rightPreJoinPlan.toString());
        }
    }

    protected static class LookupResponse extends AbstractLookupService.LookupResponse {
        // Reuse the streaming session ID version since streaming is not in production yet
        private static final TransportVersion ESQL_LOOKUP_PLAN_STRING = TransportVersion.fromName("esql_streaming_lookup_join");

        private List<Page> pages;
        @Nullable
        private final String planString;

        LookupResponse(List<Page> pages, BlockFactory blockFactory) {
            this(pages, blockFactory, null);
        }

        LookupResponse(List<Page> pages, BlockFactory blockFactory, @Nullable String planString) {
            super(blockFactory);
            this.pages = pages;
            this.planString = planString;
        }

        LookupResponse(StreamInput in, BlockFactory blockFactory) throws IOException {
            super(blockFactory);
            try (BlockStreamInput bsi = new BlockStreamInput(in, blockFactory)) {
                this.pages = bsi.readCollectionAsList(Page::new);
            }
            if (in.getTransportVersion().supports(ESQL_LOOKUP_PLAN_STRING)) {
                this.planString = in.readOptionalString();
            } else {
                this.planString = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            long bytes = pages.stream().mapToLong(Page::ramBytesUsedByBlocks).sum();
            blockFactory.breaker().addEstimateBytesAndMaybeBreak(bytes, "serialize lookup join response");
            reservedBytes += bytes;
            out.writeCollection(pages);
            if (out.getTransportVersion().supports(ESQL_LOOKUP_PLAN_STRING)) {
                out.writeOptionalString(planString);
            }
        }

        @Nullable
        public String planString() {
            return planString;
        }

        @Override
        protected List<Page> takePages() {
            var p = pages;
            pages = null;
            return p;
        }

        List<Page> pages() {
            return pages;
        }

        @Override
        protected void innerRelease() {
            if (pages != null) {
                Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(pages.iterator(), page -> page::releaseBlocks)));
            }
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LookupResponse that = (LookupResponse) o;
            return Objects.equals(pages, that.pages) && Objects.equals(planString, that.planString);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pages, planString);
        }

        @Override
        public String toString() {
            return "LookupResponse{pages=" + pages + ", planString=" + planString + '}';
        }
    }

    @Override
    protected void doLookup(TransportRequest request, CancellableTask task, ActionListener<AbstractLookupService.LookupResponse> listener) {
        if (request.getClientToServerId() != null) {
            doLookupStreaming(request, task, listener);
        } else {
            super.doLookup(request, task, listener);
        }
    }

    protected void doLookupStreaming(
        LookupFromIndexService.TransportRequest request,
        CancellableTask task,
        ActionListener<AbstractLookupService.LookupResponse> listener
    ) {
        // Streaming lookup is always a setup request - check that input page is empty
        if (request.inputPage.getPositionCount() != 0) {
            listener.onFailure(
                new IllegalStateException("Streaming lookup setup request must have 0 rows, got " + request.inputPage.getPositionCount())
            );
            return;
        }
        final List<Releasable> releasables = new ArrayList<>(6);
        boolean started = false;
        try {
            LookupShardContext shardContext = lookupShardContextFactory.create(request.shardId);
            releasables.add(shardContext.release());

            // Create aliasFilter here before building operators
            var projectState = projectResolver.getProjectState(clusterService.state());
            AliasFilter aliasFilter = indicesService.buildAliasFilter(
                projectState,
                request.shardId.getIndex().getName(),
                indexNameExpressionResolver.resolveExpressionsIgnoringRemotes(projectState.metadata(), request.indexPattern)
            );

            // Determine client node (from task origin or use local node as fallback)
            DiscoveryNode clientNode = determineClientNode(request, task);

            // Stage 1: Create BidirectionalBatchExchangeServer (creates source handler)
            // Use explicit exchange IDs: clientToServerId is per-server unique,
            // serverToClientId is shared across all servers
            BidirectionalBatchExchangeServer server = new BidirectionalBatchExchangeServer(
                request.getClientToServerId(),
                request.getClientToServerId(),
                request.getServerToClientId(),
                exchangeService,
                executor,
                QueryPragmas.EXCHANGE_BUFFER_SIZE.getDefault(Settings.EMPTY),
                transportService,
                task,
                clientNode,
                getSettings()
            );
            releasables.add(server);

            // Get source factory from server for planning
            ExchangeSourceOperator.ExchangeSourceOperatorFactory sourceFactory = server.getSourceOperatorFactory();

            PhysicalPlan physicalPlan = createLookupPhysicalPlan(request);
            String planString = request.isProfile() ? physicalPlan.toString() : null;

            // Build operators using the planning system with the actual source factory
            LocalExecutionPlanner.PhysicalOperation physicalOperation = executionPlanner.buildOperatorFactories(
                request,
                physicalPlan,
                BlockOptimization.NONE,
                dc -> this,
                sourceFactory
            );

            LookupQueryPlan lookupQueryPlan = executionPlanner.buildOperators(
                physicalOperation,
                shardContext,
                releasables,
                request,
                aliasFilter,
                (req, context, filter, warn) -> queryList((TransportRequest) req, context, filter, warn)
            );

            // Wrap releasables (shardContext and localBreaker) - they're already in releasables list
            // If started == false, releasables will be closed in finally block
            Releasable serverReleasables = Releasables.wrap(shardContext.release(), lookupQueryPlan.localBreaker());

            List<Operator> intermediateOperators = lookupQueryPlan.operators();

            // Stage 2: Start batch processing with the operators
            startServerWithOperators(server, lookupQueryPlan, intermediateOperators, serverReleasables);

            // Server is ready - send response with optional plan string
            // Response will be released by the transport layer after sending
            listener.onResponse(new LookupResponse(List.of(), blockFactory, planString));
            started = true;
        } catch (Exception e) {
            listener.onFailure(e);
        } finally {
            if (started == false) {
                Releasables.close(releasables);
            }
        }
    }

    protected DiscoveryNode determineClientNode(TransportRequest request, CancellableTask task) {
        // Get the client node from the parent task's node ID
        // The parent task is running on the client node that initiated this lookup request
        if (task == null) {
            throw new IllegalStateException(
                "Cannot determine client node for streaming lookup: task is null. "
                    + "This indicates the lookup request was not properly initiated."
            );
        }
        TaskId parentTaskId = task.getParentTaskId();
        if (parentTaskId == null || parentTaskId.isSet() == false) {
            throw new IllegalStateException(
                "Cannot determine client node for streaming lookup: parent task ID is not set. "
                    + "This indicates the lookup request was not properly initiated from a parent task."
            );
        }
        String nodeId = parentTaskId.getNodeId();
        DiscoveryNode clientNode = clusterService.state().nodes().get(nodeId);
        if (clientNode == null) {
            throw new IllegalStateException(
                "Cannot determine client node for streaming lookup: node ["
                    + nodeId
                    + "] from parent task not found in cluster state. "
                    + "The client node may have left the cluster."
            );
        }
        return clientNode;
    }

    /**
     * Starts the exchange server with the generated operators.
     * This method can be overridden in tests to capture the plan without actually starting the server.
     */
    protected void startServerWithOperators(
        BidirectionalBatchExchangeServer server,
        LookupQueryPlan lookupQueryPlan,
        List<Operator> intermediateOperators,
        Releasable releasables
    ) throws Exception {
        server.startWithOperators(
            lookupQueryPlan.driverContext(),
            transportService.getThreadPool().getThreadContext(),
            intermediateOperators,
            clusterService.getClusterName().value(),
            releasables
        );
    }

    /**
     * Creates a PhysicalPlan tree representing the lookup operation structure.
     * This plan can be cached and reused across multiple calls with different input data.
     */
    protected PhysicalPlan createLookupPhysicalPlan(TransportRequest request) throws IOException {
        // Create output attributes: doc block
        FieldAttribute docAttribute = new FieldAttribute(
            request.source,
            null,
            null,
            EsQueryExec.DOC_ID_FIELD.getName(),
            EsQueryExec.DOC_ID_FIELD
        );
        List<Attribute> sourceOutput = new ArrayList<>();
        sourceOutput.add(docAttribute);

        // Use the reference attribute directly
        sourceOutput.add(AbstractLookupService.LOOKUP_POSITIONS_FIELD);

        ParameterizedQueryExec source = new ParameterizedQueryExec(request.source, sourceOutput);

        PhysicalPlan plan = source;

        // Add FieldExtractExec if we have extract fields
        if (request.extractFields.isEmpty() == false) {
            List<Attribute> extractAttributes = request.extractFields.stream()
                .<Attribute>map(LookupFromIndexService::getExtractFieldAttribute)
                .toList();
            plan = new FieldExtractExec(request.source, plan, extractAttributes, MappedFieldType.FieldExtractPreference.NONE);
        }

        List<Attribute> childOutput = plan.output();
        List<NamedExpression> projections = new ArrayList<>(childOutput.size() - 1);
        // Skip index 0 (doc), keep indices 1+ (positions + extract fields)
        for (int i = 1; i < childOutput.size(); i++) {
            projections.add(childOutput.get(i));
        }
        plan = new ProjectExec(request.source, plan, projections);

        plan = new OutputExec(request.source, plan, null);

        return plan;
    }

    record LookupQueryPlan(
        LookupShardContext shardContext,
        LocalCircuitBreaker localBreaker,
        DriverContext driverContext,
        List<Operator> operators,
        List<Page> collectedPages
    ) {}

    protected void startDriver(
        TransportRequest request,
        CancellableTask task,
        ActionListener<List<Page>> listener,
        LookupQueryPlan lookupQueryPlan
    ) {
        Driver driver = new Driver(
            "lookup-join:" + request.sessionId,
            "lookup-join",
            clusterService.getClusterName().value(),
            clusterService.getNodeName(),
            System.currentTimeMillis(),
            System.nanoTime(),
            lookupQueryPlan.driverContext(),
            request::toString,
            null, // sourceOperator - not used in streaming mode
            lookupQueryPlan.operators(),
            null, // outputOperator - not used in streaming mode
            Driver.DEFAULT_STATUS_INTERVAL,
            Releasables.wrap(lookupQueryPlan.shardContext().release(), lookupQueryPlan.localBreaker())
        );
        task.addListener(() -> {
            String reason = Objects.requireNonNullElse(task.getReasonCancelled(), "task was cancelled");
            driver.cancel(reason);
        });
        var threadContext = transportService.getThreadPool().getThreadContext();
        Driver.start(threadContext, executor, driver, Driver.DEFAULT_MAX_ITERATIONS, new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                listener.onResponse(lookupQueryPlan.collectedPages());
            }

            @Override
            public void onFailure(Exception e) {
                Releasables.closeExpectNoException(
                    Releasables.wrap(() -> Iterators.map(lookupQueryPlan.collectedPages().iterator(), p -> () -> {
                        p.releaseBlocks();
                    }))
                );
                listener.onFailure(e);
            }
        });
    }

    /**
     * Extracts a FieldAttribute from a NamedExpression, throwing an exception if it's not a FieldAttribute.
     */
    private static FieldAttribute getExtractFieldAttribute(NamedExpression extractField) {
        if (extractField instanceof FieldAttribute fieldAttribute) {
            return fieldAttribute;
        }
        throw new EsqlIllegalArgumentException(
            "Expected extract field to be a FieldAttribute but found [{}]",
            extractField.getClass().getSimpleName()
        );
    }
}
