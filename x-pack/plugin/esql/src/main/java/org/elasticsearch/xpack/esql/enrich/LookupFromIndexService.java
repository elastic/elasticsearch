/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.lookup.QueryList;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * {@link LookupFromIndexService} performs lookup against a Lookup index for
 * a given input page. See {@link AbstractLookupService} for how it works
 * where it refers to this process as a {@code LEFT JOIN}. Which is mostly is.
 */
public class LookupFromIndexService extends AbstractLookupService<LookupFromIndexService.Request, LookupFromIndexService.TransportRequest> {
    public static final String LOOKUP_ACTION_NAME = EsqlQueryAction.NAME + "/lookup_from_index";

    public LookupFromIndexService(
        ClusterService clusterService,
        SearchService searchService,
        TransportService transportService,
        BigArrays bigArrays,
        BlockFactory blockFactory
    ) {
        super(
            LOOKUP_ACTION_NAME,
            ClusterPrivilegeResolver.MONITOR_ENRICH.name(), // TODO some other privilege
            clusterService,
            searchService,
            transportService,
            bigArrays,
            blockFactory,
            TransportRequest::readFrom
        );
    }

    @Override
    protected TransportRequest transportRequest(LookupFromIndexService.Request request, ShardId shardId) {
        return new TransportRequest(
            request.sessionId,
            shardId,
            request.inputDataType,
            request.inputPage,
            null,
            request.extractFields,
            request.matchField,
            request.source
        );
    }

    @Override
    protected QueryList queryList(TransportRequest request, SearchExecutionContext context, Block inputBlock, DataType inputDataType) {
        MappedFieldType fieldType = context.getFieldType(request.matchField);
        validateTypes(request.inputDataType, fieldType);
        return termQueryList(fieldType, context, inputBlock, inputDataType);
    }

    private static void validateTypes(DataType inputDataType, MappedFieldType fieldType) {
        // TODO: consider supporting implicit type conversion as done in ENRICH for some types
        if (fieldType.typeName().equals(inputDataType.typeName()) == false) {
            throw new EsqlIllegalArgumentException(
                "LOOKUP JOIN match and input types are incompatible: match[" + fieldType.typeName() + "], input[" + inputDataType + "]"
            );
        }
    }

    public static class Request extends AbstractLookupService.Request {
        private final String matchField;

        Request(
            String sessionId,
            String index,
            DataType inputDataType,
            String matchField,
            Page inputPage,
            List<NamedExpression> extractFields,
            Source source
        ) {
            super(sessionId, index, inputDataType, inputPage, extractFields, source);
            this.matchField = matchField;
        }
    }

    protected static class TransportRequest extends AbstractLookupService.TransportRequest {
        private final String matchField;

        TransportRequest(
            String sessionId,
            ShardId shardId,
            DataType inputDataType,
            Page inputPage,
            Page toRelease,
            List<NamedExpression> extractFields,
            String matchField,
            Source source
        ) {
            super(sessionId, shardId, inputDataType, inputPage, toRelease, extractFields, source);
            this.matchField = matchField;
        }

        static TransportRequest readFrom(StreamInput in, BlockFactory blockFactory) throws IOException {
            TaskId parentTaskId = TaskId.readFromStream(in);
            String sessionId = in.readString();
            ShardId shardId = new ShardId(in);
            DataType inputDataType = DataType.fromTypeName(in.readString());
            Page inputPage;
            try (BlockStreamInput bsi = new BlockStreamInput(in, blockFactory)) {
                inputPage = new Page(bsi);
            }
            PlanStreamInput planIn = new PlanStreamInput(in, in.namedWriteableRegistry(), null);
            List<NamedExpression> extractFields = planIn.readNamedWriteableCollectionAsList(NamedExpression.class);
            String matchField = in.readString();
            var source = Source.EMPTY;
            if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_ENRICH_RUNTIME_WARNINGS)) {
                source = Source.readFrom(planIn);
            }
            TransportRequest result = new TransportRequest(
                sessionId,
                shardId,
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
            out.writeString(inputDataType.typeName());
            out.writeWriteable(inputPage);
            PlanStreamOutput planOut = new PlanStreamOutput(out, null);
            planOut.writeNamedWriteableCollection(extractFields);
            out.writeString(matchField);
            if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_ENRICH_RUNTIME_WARNINGS)) {
                source.writeTo(planOut);
            }
        }

        @Override
        protected String extraDescription() {
            return " ,match_field=" + matchField;
        }
    }
}
