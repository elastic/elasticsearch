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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
import org.elasticsearch.compute.operator.lookup.QueryList;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;

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

    public LookupFromIndexService(
        ClusterService clusterService,
        IndicesService indicesService,
        LookupShardContextFactory lookupShardContextFactory,
        TransportService transportService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        ProjectResolver projectResolver
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
            false,
            TransportRequest::readFrom,
            projectResolver
        );
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
            request.joinOnConditions
        );
    }

    @Override
    protected LookupEnrichQueryGenerator queryList(
        TransportRequest request,
        SearchExecutionContext context,
        AliasFilter aliasFilter,
        Block inputBlock,
        Warnings warnings
    ) {
        PhysicalPlan lookupNodePlan = localLookupNodePlanning(request.rightPreJoinPlan);
        if (request.joinOnConditions == null) {
            // this is a field based join
            List<QueryList> queryLists = new ArrayList<>();
            for (int i = 0; i < request.matchFields.size(); i++) {
                MatchConfig matchField = request.matchFields.get(i);
                QueryList q = termQueryList(
                    context.getFieldType(matchField.fieldName()),
                    context,
                    aliasFilter,
                    request.inputPage.getBlock(matchField.channel()),
                    matchField.type()
                ).onlySingleValues(warnings, "LOOKUP JOIN encountered multi-value");
                queryLists.add(q);
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

        Request(
            String sessionId,
            String index,
            String indexPattern,
            List<MatchConfig> matchFields,
            Page inputPage,
            List<NamedExpression> extractFields,
            Source source,
            PhysicalPlan rightPreJoinPlan,
            Expression joinOnConditions
        ) {
            super(sessionId, index, indexPattern, matchFields.get(0).type(), inputPage, extractFields, source);
            this.matchFields = matchFields;
            this.rightPreJoinPlan = rightPreJoinPlan;
            this.joinOnConditions = joinOnConditions;
        }
    }

    protected static class TransportRequest extends AbstractLookupService.TransportRequest {

        private static final TransportVersion JOIN_ON_ALIASES = TransportVersion.fromName("join_on_aliases");
        private static final TransportVersion ESQL_LOOKUP_JOIN_ON_MANY_FIELDS = TransportVersion.fromName(
            "esql_lookup_join_on_many_fields"
        );
        private static final TransportVersion ESQL_LOOKUP_JOIN_ON_EXPRESSION = TransportVersion.fromName("esql_lookup_join_on_expression");

        private final List<MatchConfig> matchFields;
        private final PhysicalPlan rightPreJoinPlan;
        private final Expression joinOnConditions;

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
            Expression joinOnConditions
        ) {
            super(sessionId, shardId, indexPattern, inputPage, toRelease, extractFields, source);
            this.matchFields = matchFields;
            this.rightPreJoinPlan = rightPreJoinPlan;
            this.joinOnConditions = joinOnConditions;
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
                joinOnConditions
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
        private List<Page> pages;

        LookupResponse(List<Page> pages, BlockFactory blockFactory) {
            super(blockFactory);
            this.pages = pages;
        }

        LookupResponse(StreamInput in, BlockFactory blockFactory) throws IOException {
            super(blockFactory);
            try (BlockStreamInput bsi = new BlockStreamInput(in, blockFactory)) {
                this.pages = bsi.readCollectionAsList(Page::new);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            long bytes = pages.stream().mapToLong(Page::ramBytesUsedByBlocks).sum();
            blockFactory.breaker().addEstimateBytesAndMaybeBreak(bytes, "serialize lookup join response");
            reservedBytes += bytes;
            out.writeCollection(pages);
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
            return Objects.equals(pages, that.pages);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(pages);
        }

        @Override
        public String toString() {
            return "LookupResponse{pages=" + pages + '}';
        }
    }
}
