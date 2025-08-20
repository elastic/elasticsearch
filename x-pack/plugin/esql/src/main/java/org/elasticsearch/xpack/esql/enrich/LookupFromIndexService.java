/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.TransportVersions;
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
import org.elasticsearch.compute.operator.lookup.ExpressionQueryList;
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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

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
            request.source
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
        List<QueryList> queryLists = new ArrayList<>();
        for (int i = 0; i < request.matchFields.size(); i++) {
            MatchConfig matchField = request.matchFields.get(i);
            QueryList q = termQueryList(
                context.getFieldType(matchField.fieldName().string()),
                context,
                aliasFilter,
                request.inputPage.getBlock(matchField.channel()),
                matchField.type()
            ).onlySingleValues(warnings, "LOOKUP JOIN encountered multi-value");
            queryLists.add(q);
        }
        if (queryLists.size() == 1) {
            return queryLists.getFirst();
        }
        return new ExpressionQueryList(queryLists);
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

        Request(
            String sessionId,
            String index,
            String indexPattern,
            List<MatchConfig> matchFields,
            Page inputPage,
            List<NamedExpression> extractFields,
            Source source
        ) {
            super(sessionId, index, indexPattern, matchFields.get(0).type(), inputPage, extractFields, source);
            this.matchFields = matchFields;
        }
    }

    protected static class TransportRequest extends AbstractLookupService.TransportRequest {
        private final List<MatchConfig> matchFields;

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
            Source source
        ) {
            super(sessionId, shardId, indexPattern, inputPage, toRelease, extractFields, source);
            this.matchFields = matchFields;
        }

        static TransportRequest readFrom(StreamInput in, BlockFactory blockFactory) throws IOException {
            TaskId parentTaskId = TaskId.readFromStream(in);
            String sessionId = in.readString();
            ShardId shardId = new ShardId(in);

            String indexPattern;
            if (in.getTransportVersion().onOrAfter(TransportVersions.JOIN_ON_ALIASES)
                || in.getTransportVersion().isPatchFrom(TransportVersions.JOIN_ON_ALIASES_8_19)) {
                indexPattern = in.readString();
            } else {
                indexPattern = shardId.getIndexName();
            }

            DataType inputDataType = null;
            if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_LOOKUP_JOIN_ON_MANY_FIELDS) == false) {
                inputDataType = DataType.fromTypeName(in.readString());
            }

            Page inputPage;
            try (BlockStreamInput bsi = new BlockStreamInput(in, blockFactory)) {
                inputPage = new Page(bsi);
            }
            PlanStreamInput planIn = new PlanStreamInput(in, in.namedWriteableRegistry(), null);
            List<NamedExpression> extractFields = planIn.readNamedWriteableCollectionAsList(NamedExpression.class);
            List<MatchConfig> matchFields = null;
            if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_LOOKUP_JOIN_ON_MANY_FIELDS)) {
                matchFields = planIn.readCollectionAsList(MatchConfig::new);
            } else {
                String matchField = in.readString();
                // For older versions, we only support a single match field.
                matchFields = new ArrayList<>(1);
                matchFields.add(new MatchConfig(new FieldAttribute.FieldName(matchField), 0, inputDataType));
            }
            var source = Source.EMPTY;
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_17_0)) {
                source = Source.readFrom(planIn);
            }
            // Source.readFrom() requires the query from the Configuration passed to PlanStreamInput.
            // As we don't have the Configuration here, and it may be heavy to serialize, we directly pass the Source text.
            if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_LOOKUP_JOIN_SOURCE_TEXT)) {
                String sourceText = in.readString();
                source = new Source(source.source(), sourceText);
            }
            TransportRequest result = new TransportRequest(
                sessionId,
                shardId,
                indexPattern,
                inputPage,
                inputPage,
                extractFields,
                matchFields,
                source
            );
            result.setParentTask(parentTaskId);
            return result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sessionId);
            out.writeWriteable(shardId);

            if (out.getTransportVersion().onOrAfter(TransportVersions.JOIN_ON_ALIASES)
                || out.getTransportVersion().isPatchFrom(TransportVersions.JOIN_ON_ALIASES_8_19)) {
                out.writeString(indexPattern);
            } else if (indexPattern.equals(shardId.getIndexName()) == false) {
                throw new EsqlIllegalArgumentException("Aliases and index patterns are not allowed for LOOKUP JOIN [{}]", indexPattern);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_LOOKUP_JOIN_ON_MANY_FIELDS) == false) {
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
            if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_LOOKUP_JOIN_ON_MANY_FIELDS)) {
                // serialize all match fields for new versions
                planOut.writeCollection(matchFields, (o, matchConfig) -> matchConfig.writeTo(o));
            } else {
                // older versions only support a single match field, we already checked this above when writing the datatype
                // send the field name of the first and only match field here
                out.writeString(matchFields.get(0).fieldName().string());
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_17_0)) {
                source.writeTo(planOut);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_LOOKUP_JOIN_SOURCE_TEXT)) {
                out.writeString(source.text());
            }
        }

        @Override
        protected String extraDescription() {
            return " ,match_fields=" + matchFields.stream().map(x -> x.fieldName().string()).collect(Collectors.joining(", "));
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
