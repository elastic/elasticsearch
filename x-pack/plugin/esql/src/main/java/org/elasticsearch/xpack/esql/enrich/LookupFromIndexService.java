/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.TransportVersions;
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
import org.elasticsearch.compute.operator.lookup.QueryList;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * {@link LookupFromIndexService} performs lookup against a Lookup index for
 * a given input page. See {@link AbstractLookupService} for how it works
 * where it refers to this process as a {@code LEFT JOIN}. Which is mostly is.
 */
public class LookupFromIndexService extends AbstractLookupService<LookupFromIndexService.Request, LookupFromIndexService.TransportRequest> {
    public static final String LOOKUP_ACTION_NAME = EsqlQueryAction.NAME + "/lookup_from_index";

    public LookupFromIndexService(
        ClusterService clusterService,
        LookupShardContextFactory lookupShardContextFactory,
        TransportService transportService,
        BigArrays bigArrays,
        BlockFactory blockFactory
    ) {
        super(
            LOOKUP_ACTION_NAME,
            clusterService,
            lookupShardContextFactory,
            transportService,
            bigArrays,
            blockFactory,
            false,
            TransportRequest::readFrom
        );
    }

    @Override
    protected TransportRequest transportRequest(LookupFromIndexService.Request request, ShardId shardId) {
        return new TransportRequest(
            request.sessionId,
            shardId,
            request.indexPattern,
            request.inputDataType,
            request.inputPage,
            null,
            request.extractFields,
            request.matchField,
            request.source
        );
    }

    @Override
    protected QueryList queryList(
        TransportRequest request,
        SearchExecutionContext context,
        Block inputBlock,
        @Nullable DataType inputDataType,
        Warnings warnings
    ) {
        return termQueryList(context.getFieldType(request.matchField), context, inputBlock, inputDataType).onlySingleValues(
            warnings,
            "LOOKUP JOIN encountered multi-value"
        );
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
        private final String matchField;

        Request(
            String sessionId,
            String index,
            String indexPattern,
            DataType inputDataType,
            String matchField,
            Page inputPage,
            List<NamedExpression> extractFields,
            Source source
        ) {
            super(sessionId, index, indexPattern, inputDataType, inputPage, extractFields, source);
            this.matchField = matchField;
        }
    }

    protected static class TransportRequest extends AbstractLookupService.TransportRequest {
        private final String matchField;

        TransportRequest(
            String sessionId,
            ShardId shardId,
            String indexPattern,
            DataType inputDataType,
            Page inputPage,
            Page toRelease,
            List<NamedExpression> extractFields,
            String matchField,
            Source source
        ) {
            super(sessionId, shardId, indexPattern, inputDataType, inputPage, toRelease, extractFields, source);
            this.matchField = matchField;
        }

        static TransportRequest readFrom(StreamInput in, BlockFactory blockFactory) throws IOException {
            TaskId parentTaskId = TaskId.readFromStream(in);
            String sessionId = in.readString();
            ShardId shardId = new ShardId(in);

            String indexPattern;
            if (in.getTransportVersion().onOrAfter(TransportVersions.JOIN_ON_ALIASES)) {
                indexPattern = in.readString();
            } else {
                indexPattern = shardId.getIndexName();
            }

            DataType inputDataType = DataType.fromTypeName(in.readString());
            Page inputPage;
            try (BlockStreamInput bsi = new BlockStreamInput(in, blockFactory)) {
                inputPage = new Page(bsi);
            }
            PlanStreamInput planIn = new PlanStreamInput(in, in.namedWriteableRegistry(), null);
            List<NamedExpression> extractFields = planIn.readNamedWriteableCollectionAsList(NamedExpression.class);
            String matchField = in.readString();
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
                inputDataType,
                inputPage,
                inputPage,
                extractFields,
                matchField,
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

            if (indexPattern.equals(shardId.getIndexName()) == false
                && out.getTransportVersion().before(TransportVersions.JOIN_ON_ALIASES)) {
                // TODO can we throw exceptions here?
                throw new VerificationException("Aliases and index patterns are not allowed for LOOKUP JOIN []", indexPattern);
            }
            out.writeString(indexPattern);

            out.writeString(inputDataType.typeName());
            out.writeWriteable(inputPage);
            PlanStreamOutput planOut = new PlanStreamOutput(out, null);
            planOut.writeNamedWriteableCollection(extractFields);
            out.writeString(matchField);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_17_0)) {
                source.writeTo(planOut);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_LOOKUP_JOIN_SOURCE_TEXT)) {
                out.writeString(source.text());
            }
        }

        @Override
        protected String extraDescription() {
            return " ,match_field=" + matchField;
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
