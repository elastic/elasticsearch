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
import org.elasticsearch.index.query.QueryBuilder;
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
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LookupLogicalOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LookupPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.ParameterizedQuery;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.ParameterizedQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchContextStats;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link LookupFromIndexService} performs lookup against a Lookup index for
 * a given input page. See {@link AbstractLookupService} for how it works
 * where it refers to this process as a {@code LEFT JOIN}. Which is mostly is.
 */
public class LookupFromIndexService extends AbstractLookupService<LookupFromIndexService.Request, LookupFromIndexService.LookupRequest> {
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
        PlannerSettings.Holder plannerSettings,
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
            LookupRequest::readFrom,
            projectResolver,
            plannerSettings
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
    protected LookupRequest transportRequest(LookupFromIndexService.Request request, ShardId shardId) {
        return new LookupRequest(
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
            request.profile,
            request.configuration
        );
    }

    @Override
    protected LookupEnrichQueryGenerator queryList(
        LookupRequest request,
        SearchExecutionContext context,
        AliasFilter aliasFilter,
        Warnings warnings
    ) {
        PhysicalPlan lookupNodePlan = mapFragmentToPhysical(request.rightPreJoinPlan);
        Expression rightOnlyFilter = lookupNodePlan instanceof FilterExec filterExec ? filterExec.condition() : null;
        return buildQueryGenerator(request.matchFields, request.joinOnConditions, rightOnlyFilter, null, context, aliasFilter, warnings);
    }

    private LookupEnrichQueryGenerator buildQueryGenerator(
        List<MatchConfig> matchFields,
        @Nullable Expression joinOnConditions,
        @Nullable Expression rightOnlyFilter,
        @Nullable QueryBuilder pushedQuery,
        SearchExecutionContext context,
        AliasFilter aliasFilter,
        Warnings warnings
    ) {
        if (joinOnConditions == null) {
            List<QueryList> queryLists = new ArrayList<>();
            for (int i = 0; i < matchFields.size(); i++) {
                MatchConfig matchField = matchFields.get(i);
                int channelOffset = matchField.channel();
                QueryList q = termQueryList(context.getFieldType(matchField.fieldName()), aliasFilter, channelOffset, matchField.type());
                queryLists.add(q.onlySingleValues(warnings, "LOOKUP JOIN encountered multi-value"));
            }
            if (queryLists.size() == 1 && rightOnlyFilter == null && pushedQuery == null) {
                return queryLists.getFirst();
            }
            return ExpressionQueryList.fieldBasedJoin(queryLists, context, rightOnlyFilter, pushedQuery, clusterService, aliasFilter);
        } else {
            return ExpressionQueryList.expressionBasedJoin(
                context,
                rightOnlyFilter,
                pushedQuery,
                clusterService,
                matchFields,
                joinOnConditions,
                aliasFilter,
                warnings
            );
        }
    }

    /**
     * Builds a query list for the streaming lookup path using data from the physical plan tree
     * rather than from the transport request.
     */
    protected LookupEnrichQueryGenerator queryListFromPlan(
        List<MatchConfig> matchFields,
        @Nullable Expression joinOnConditions,
        @Nullable QueryBuilder pushedQuery,
        SearchExecutionContext context,
        AliasFilter aliasFilter,
        Warnings warnings
    ) {
        return buildQueryGenerator(matchFields, joinOnConditions, null, pushedQuery, context, aliasFilter, warnings);
    }

    /**
     * Maps the logical plan inside a {@link FragmentExec} to a physical plan.
     * Returns {@code null} if the input is not a {@link FragmentExec} (e.g. older coordinator
     * that does not wrap the plan in a {@link FragmentExec}).
     */
    private static PhysicalPlan mapFragmentToPhysical(@Nullable PhysicalPlan physicalPlan) {
        return physicalPlan instanceof FragmentExec fragmentExec ? LocalMapper.INSTANCE.map(fragmentExec.fragment()) : null;
    }

    @Override
    protected LookupResponse createLookupResponse(List<Page> pages, BlockFactory blockFactory) {
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
        @Nullable
        private final Configuration configuration;

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
            boolean profile,
            @Nullable Configuration configuration
        ) {
            super(sessionId, index, indexPattern, matchFields.get(0).type(), inputPage, extractFields, source);
            this.matchFields = matchFields;
            this.rightPreJoinPlan = rightPreJoinPlan;
            this.joinOnConditions = joinOnConditions;
            this.clientToServerId = clientToServerId;
            this.serverToClientId = serverToClientId;
            this.profile = profile;
            this.configuration = configuration;
        }
    }

    protected static class LookupRequest extends AbstractLookupService.TransportRequest {

        private static final TransportVersion JOIN_ON_ALIASES = TransportVersion.fromName("join_on_aliases");
        private static final TransportVersion ESQL_LOOKUP_JOIN_ON_MANY_FIELDS = TransportVersion.fromName(
            "esql_lookup_join_on_many_fields"
        );
        private static final TransportVersion ESQL_LOOKUP_JOIN_ON_EXPRESSION = TransportVersion.fromName("esql_lookup_join_on_expression");
        private static final TransportVersion ESQL_STREAMING_LOOKUP_JOIN = TransportVersion.fromName("esql_streaming_lookup_join");
        private static final TransportVersion ESQL_LOOKUP_PLANNING = TransportVersion.fromName("esql_lookup_planning");

        private final List<MatchConfig> matchFields;
        private final PhysicalPlan rightPreJoinPlan;
        private final Expression joinOnConditions;
        private final String clientToServerId;
        private final String serverToClientId;
        private final boolean profile;
        @Nullable
        private final Configuration configuration;

        LookupRequest(
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
            boolean profile,
            @Nullable Configuration configuration
        ) {
            super(sessionId, shardId, indexPattern, inputPage, toRelease, extractFields, source);
            this.matchFields = matchFields;
            this.rightPreJoinPlan = rightPreJoinPlan;
            this.joinOnConditions = joinOnConditions;
            this.clientToServerId = clientToServerId;
            this.serverToClientId = serverToClientId;
            this.profile = profile;
            this.configuration = configuration;
        }

        static LookupRequest readFrom(StreamInput in, BlockFactory blockFactory) throws IOException {
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
            // configuration is needed for the PlanStreamInput, read it here first
            Configuration configuration = null;
            if (in.getTransportVersion().supports(ESQL_LOOKUP_PLANNING)) {
                configuration = in.readOptionalWriteable(Configuration::readWithoutTables);
            }
            PlanStreamInput planIn = new PlanStreamInput(in, in.namedWriteableRegistry(), configuration);
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
            String serverToClientId = null;
            boolean profile = false;
            if (in.getTransportVersion().supports(ESQL_STREAMING_LOOKUP_JOIN)) {
                clientToServerId = in.readOptionalString();
                serverToClientId = in.readOptionalString();
                profile = in.readBoolean();
            }
            LookupRequest result = new LookupRequest(
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
                profile,
                configuration
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

        @Nullable
        public Configuration getConfiguration() {
            return configuration;
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
            // configuration is needed for the PlanStreamOutput, write it here early
            if (out.getTransportVersion().supports(ESQL_LOOKUP_PLANNING)) {
                out.writeOptionalWriteable(configuration != null ? configuration.withoutTables() : null);
            }
            PlanStreamOutput planOut = new PlanStreamOutput(out, configuration);
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
                out.writeOptionalString(serverToClientId);
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
    protected void doLookup(LookupRequest request, CancellableTask task, ActionListener<AbstractLookupService.LookupResponse> listener) {
        if (request.getClientToServerId() != null) {
            doLookupStreaming(request, task, listener);
        } else {
            super.doLookup(request, task, listener);
        }
    }

    protected void doLookupStreaming(
        LookupFromIndexService.LookupRequest request,
        CancellableTask task,
        ActionListener<AbstractLookupService.LookupResponse> listener
    ) {
        PlannerSettings plannerSettings = this.plannerSettings.get();
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

            // Create BidirectionalBatchExchangeServer (creates source handler)
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

            Configuration configuration = request.configuration;
            FoldContext foldCtx = configuration != null ? configuration.newFoldContext() : FoldContext.small();
            SearchStats searchStats = SearchContextStats.from(List.of(shardContext.executionContext()));
            EsqlFlags flags = new EsqlFlags(clusterService.getClusterSettings());

            PhysicalPlan physicalPlan;
            LookupExecutionPlanner.QueryListFromPlanFactory queryListFactory;
            if (configuration != null) {
                LogicalPlan logicalPlan = extractOrBuildLogicalPlan(request);
                physicalPlan = createLookupPhysicalPlan(logicalPlan, configuration, plannerSettings, foldCtx, searchStats, flags);
                queryListFactory = this::queryListFromPlan;
            } else {
                // BWC: old data node without Configuration
                // Do not do logical and physical planning (It will fail without configuration)
                // We just build the physical plan directly using the legacy code
                physicalPlan = createLegacyLookupPhysicalPlan(request);
                queryListFactory = (mf, joc, pq, ctx, af, w) -> queryList(request, ctx, af, w);
            }
            String planString = request.isProfile() ? physicalPlan.toString() : null;

            // Build operators using the planning system with the actual source factory.
            LocalExecutionPlanner.PhysicalOperation physicalOperation = executionPlanner.buildOperatorFactories(
                plannerSettings,
                physicalPlan,
                BlockOptimization.NONE,
                sourceFactory,
                foldCtx,
                queryListFactory,
                request.source
            );

            LookupQueryPlan lookupQueryPlan = executionPlanner.buildOperators(
                physicalOperation,
                shardContext,
                releasables,
                request.inputPage,
                aliasFilter
            );

            // Wrap releasables (shardContext and localBreaker) - they're already in releasables list
            // If started == false, releasables will be closed in finally block
            Releasable serverReleasables = Releasables.wrap(shardContext.release(), lookupQueryPlan.localBreaker());

            List<Operator> intermediateOperators = lookupQueryPlan.operators();

            // The response will be sent after the remote sink connection is ready
            // This ensures the client doesn't send pages before the server can receive them
            startServerWithOperators(server, lookupQueryPlan, intermediateOperators, serverReleasables, listener, planString);
            started = true;
        } catch (Exception e) {
            listener.onFailure(e);
        } finally {
            if (started == false) {
                Releasables.close(releasables);
            }
        }
    }

    protected DiscoveryNode determineClientNode(LookupRequest request, CancellableTask task) {
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
        Releasable releasables,
        ActionListener<? super LookupResponse> responseListener,
        @Nullable String planString
    ) {
        server.startWithOperators(
            lookupQueryPlan.driverContext(),
            transportService.getThreadPool().getThreadContext(),
            intermediateOperators,
            clusterService.getClusterName().value(),
            releasables,
            ActionListener.wrap(
                ignored -> responseListener.onResponse(new LookupResponse(List.of(), blockFactory, planString)),
                responseListener::onFailure
            )
        );
    }

    /**
     * If the data node already called {@link #buildLocalLogicalPlan} (new path), the logical plan
     * inside {@code rightPreJoinPlan} will contain a {@link ParameterizedQuery} and can be used directly.
     * Otherwise (BWC with older data nodes), we build it here on the lookup node.
     */
    private static LogicalPlan extractOrBuildLogicalPlan(LookupRequest request) {
        if (request.rightPreJoinPlan instanceof FragmentExec fragmentExec) {
            LogicalPlan fragment = fragmentExec.fragment();
            if (fragment.anyMatch(p -> p instanceof ParameterizedQuery)) {
                return fragment;
            }
        }
        // bwc path, make sure rolling updates still work
        return buildLocalLogicalPlan(
            request.source,
            request.matchFields,
            request.joinOnConditions,
            request.rightPreJoinPlan,
            request.extractFields
        );
    }

    /**
     * Builds a logical plan for the lookup node from the request.
     * Walks the logical plan tree inside {@code rightPreJoinPlan}, preserving all nodes (Filter, etc.),
     * and replaces the {@link EsRelation} leaf with a {@link ParameterizedQuery}.
     * Then adds a {@link Project} on top for [{@code _positions}, {@code extractFields}].
     */
    public static LogicalPlan buildLocalLogicalPlan(
        Source source,
        List<MatchConfig> matchFields,
        @Nullable Expression joinOnConditions,
        @Nullable PhysicalPlan rightPreJoinPlan,
        List<NamedExpression> extractFields
    ) {
        FieldAttribute docAttribute = new FieldAttribute(source, null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);

        List<Expression> leftRightParts = new ArrayList<>();
        List<Expression> rightOnlyFromConditions = new ArrayList<>();
        if (joinOnConditions != null) {
            Set<String> leftFieldNames = matchFields.stream().map(MatchConfig::fieldName).collect(Collectors.toSet());
            for (Expression expr : Predicates.splitAnd(joinOnConditions)) {
                boolean referencesLeft = expr.references().stream().anyMatch(attr -> leftFieldNames.contains(attr.name()));
                if (referencesLeft) {
                    leftRightParts.add(expr);
                } else {
                    rightOnlyFromConditions.add(expr);
                }
            }
        }
        Expression finalJoinOnConditions = Predicates.combineAnd(leftRightParts);

        LogicalPlan plan;
        if (rightPreJoinPlan == null) {
            List<Attribute> paramQueryOutput = buildParameterizedQueryOutput(docAttribute, extractFields, rightOnlyFromConditions, null);
            plan = new ParameterizedQuery(source, paramQueryOutput, matchFields, finalJoinOnConditions);
        } else if (rightPreJoinPlan instanceof FragmentExec fragmentExec) {
            plan = fragmentExec.fragment();
            plan = plan.transformUp(EsRelation.class, esRelation -> {
                List<Attribute> paramQueryOutput = buildParameterizedQueryOutput(
                    docAttribute,
                    extractFields,
                    rightOnlyFromConditions,
                    esRelation.output()
                );
                return new ParameterizedQuery(source, paramQueryOutput, matchFields, finalJoinOnConditions);
            });
        } else {
            throw new EsqlIllegalArgumentException(
                "Expected FragmentExec or null but got [{}]",
                rightPreJoinPlan.getClass().getSimpleName()
            );
        }

        if (rightOnlyFromConditions.isEmpty() == false) {
            if (plan instanceof Filter existingFilter) {
                rightOnlyFromConditions.add(existingFilter.condition());
                plan = existingFilter.child();
            }
            plan = new Filter(source, plan, Predicates.combineAnd(rightOnlyFromConditions));
        }

        List<NamedExpression> projections = new ArrayList<>(extractFields.size() + 1);
        projections.add(AbstractLookupService.LOOKUP_POSITIONS_FIELD);
        projections.addAll(extractFields);
        return new Project(source, plan, projections);
    }

    /**
     * Builds the output attributes for a {@link ParameterizedQuery}, mirroring how {@link EsRelation}
     * exposes all index fields. This ensures the logical verifier can validate that all field references
     * in the plan are satisfied. At the physical level,
     * {@link org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceSourceAttributes ReplaceSourceAttributes}
     * strips the output back down to just {@code [_doc, _positions]}, and {@code InsertFieldExtraction}
     * adds the needed fields back — the same pattern used for {@code EsRelation}.
     */
    private static List<Attribute> buildParameterizedQueryOutput(
        FieldAttribute docAttribute,
        List<NamedExpression> extractFields,
        List<Expression> rightOnlyConditions,
        @Nullable List<Attribute> esRelationOutput
    ) {
        LinkedHashSet<Attribute> output = new LinkedHashSet<>();
        output.add(docAttribute);
        output.add(AbstractLookupService.LOOKUP_POSITIONS_FIELD);
        if (esRelationOutput != null) {
            output.addAll(esRelationOutput);
        } else {
            for (NamedExpression field : extractFields) {
                if (field instanceof Attribute attr) {
                    output.add(attr);
                }
            }
            for (Expression condition : rightOnlyConditions) {
                condition.forEachDown(FieldAttribute.class, output::add);
            }
        }
        return new ArrayList<>(output);
    }

    /**
     * Builds the physical plan for the lookup node by running:
     * LookupLogicalOptimizer -> LocalMapper.map -> LookupPhysicalPlanOptimizer.
     * The caller is responsible for building the logical plan via {@link #buildLocalLogicalPlan}.
     */
    public static PhysicalPlan createLookupPhysicalPlan(
        LogicalPlan logicalPlan,
        @Nullable Configuration configuration,
        PlannerSettings plannerSettings,
        FoldContext foldCtx,
        SearchStats searchStats,
        EsqlFlags flags
    ) {
        LogicalPlan optimizedLogical = new LookupLogicalOptimizer(new LocalLogicalOptimizerContext(configuration, foldCtx, searchStats))
            .localOptimize(logicalPlan);
        PhysicalPlan physicalPlan = LocalMapper.INSTANCE.map(optimizedLogical);
        LocalPhysicalOptimizerContext context = new LocalPhysicalOptimizerContext(
            plannerSettings,
            flags,
            configuration,
            foldCtx,
            searchStats
        );
        return new LookupPhysicalPlanOptimizer(context).optimize(physicalPlan);
    }

    /**
     * BWC: builds a flat physical plan directly (no logical planning or optimization).
     * Used when the data node does not send a {@link Configuration}.
     */
    private static PhysicalPlan createLegacyLookupPhysicalPlan(LookupRequest request) {
        FieldAttribute docAttribute = new FieldAttribute(
            request.source,
            null,
            null,
            EsQueryExec.DOC_ID_FIELD.getName(),
            EsQueryExec.DOC_ID_FIELD
        );
        List<Attribute> sourceOutput = List.of(docAttribute, AbstractLookupService.LOOKUP_POSITIONS_FIELD);

        PhysicalPlan plan = new ParameterizedQueryExec(request.source, sourceOutput, request.matchFields, request.joinOnConditions, null);

        if (request.extractFields.isEmpty() == false) {
            List<Attribute> extractAttributes = request.extractFields.stream()
                .<Attribute>map(LookupFromIndexService::getExtractFieldAttribute)
                .toList();
            plan = new FieldExtractExec(request.source, plan, extractAttributes, MappedFieldType.FieldExtractPreference.NONE);
        }

        List<Attribute> childOutput = plan.output();
        List<NamedExpression> projections = new ArrayList<>(childOutput.size() - 1);
        for (int i = 1; i < childOutput.size(); i++) {
            projections.add(childOutput.get(i));
        }
        plan = new ProjectExec(request.source, plan, projections);
        return new OutputExec(request.source, plan, null);
    }

    private static FieldAttribute getExtractFieldAttribute(NamedExpression extractField) {
        if (extractField instanceof FieldAttribute fieldAttribute) {
            return fieldAttribute;
        }
        throw new EsqlIllegalArgumentException(
            "Expected extract field to be a FieldAttribute but found [{}]",
            extractField.getClass().getSimpleName()
        );
    }

    record LookupQueryPlan(
        LookupShardContext shardContext,
        LocalCircuitBreaker localBreaker,
        DriverContext driverContext,
        List<Operator> operators
    ) {}
}
