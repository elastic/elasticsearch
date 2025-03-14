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
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.lookup.QueryList;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequestOptions;
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
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link EnrichLookupService} performs enrich lookup for a given input page.
 * See {@link AbstractLookupService} for how it works where it refers to this
 * process as a {@code LEFT JOIN}. Which is mostly is.
 */
public class EnrichLookupService extends AbstractLookupService<EnrichLookupService.Request, EnrichLookupService.TransportRequest> {
    public static final String LOOKUP_ACTION_NAME = EsqlQueryAction.NAME + "/lookup";

    public EnrichLookupService(
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
            true,
            TransportRequest::readFrom
        );
    }

    @Override
    protected TransportRequest transportRequest(EnrichLookupService.Request request, ShardId shardId) {
        return new TransportRequest(
            request.sessionId,
            shardId,
            request.inputDataType,
            request.matchType,
            request.matchField,
            request.inputPage,
            null,
            request.extractFields,
            request.source
        );
    }

    @Override
    protected QueryList queryList(
        TransportRequest request,
        SearchExecutionContext context,
        Block inputBlock,
        DataType inputDataType,
        Warnings warnings
    ) {
        MappedFieldType fieldType = context.getFieldType(request.matchField);
        validateTypes(inputDataType, fieldType);
        return switch (request.matchType) {
            case "match", "range" -> termQueryList(fieldType, context, inputBlock, inputDataType);
            case "geo_match" -> QueryList.geoShapeQueryList(fieldType, context, inputBlock);
            default -> throw new EsqlIllegalArgumentException("illegal match type " + request.matchType);
        };
    }

    @Override
    protected LookupResponse createLookupResponse(List<Page> pages, BlockFactory blockFactory) throws IOException {
        if (pages.size() != 1) {
            throw new UnsupportedOperationException("ENRICH always makes a single page of output");
        }
        return new LookupResponse(pages.getFirst(), blockFactory);
    }

    @Override
    protected LookupResponse readLookupResponse(StreamInput in, BlockFactory blockFactory) throws IOException {
        return new LookupResponse(in, blockFactory);
    }

    private static void validateTypes(DataType inputDataType, MappedFieldType fieldType) {
        if (fieldType instanceof RangeFieldMapper.RangeFieldType rangeType) {
            // For range policy types, the ENRICH index field type will be one of a list of supported range types,
            // which need to match the input data type (eg. ip-range -> ip, date-range -> date, etc.)
            if (rangeTypesCompatible(rangeType.rangeType(), inputDataType) == false) {
                throw new EsqlIllegalArgumentException(
                    "ENRICH range and input types are incompatible: range[" + rangeType.rangeType() + "], input[" + inputDataType + "]"
                );
            }
        }
        // For match policies, the ENRICH index field will always be KEYWORD, and input type will be converted to KEYWORD.
        // For geo_match, type validation is done earlier, in the Analyzer.
    }

    private static boolean rangeTypesCompatible(RangeType rangeType, DataType inputDataType) {
        if (inputDataType.noText() == DataType.KEYWORD) {
            // We allow runtime parsing of string types to numeric types
            return true;
        }
        return switch (rangeType) {
            case INTEGER, LONG -> inputDataType.isWholeNumber();
            case IP -> inputDataType == DataType.IP;
            case DATE -> inputDataType.isDate();
            default -> rangeType.isNumeric() == inputDataType.isNumeric();
        };
    }

    public static class Request extends AbstractLookupService.Request {
        private final String matchType;
        private final String matchField;

        Request(
            String sessionId,
            String index,
            DataType inputDataType,
            String matchType,
            String matchField,
            Page inputPage,
            List<NamedExpression> extractFields,
            Source source
        ) {
            super(sessionId, index, inputDataType, inputPage, extractFields, source);
            this.matchType = matchType;
            this.matchField = matchField;
        }
    }

    protected static class TransportRequest extends AbstractLookupService.TransportRequest {
        private final String matchType;
        private final String matchField;

        TransportRequest(
            String sessionId,
            ShardId shardId,
            DataType inputDataType,
            String matchType,
            String matchField,
            Page inputPage,
            Page toRelease,
            List<NamedExpression> extractFields,
            Source source
        ) {
            super(sessionId, shardId, inputDataType, inputPage, toRelease, extractFields, source);
            this.matchType = matchType;
            this.matchField = matchField;
        }

        static TransportRequest readFrom(StreamInput in, BlockFactory blockFactory) throws IOException {
            TaskId parentTaskId = TaskId.readFromStream(in);
            String sessionId = in.readString();
            ShardId shardId = new ShardId(in);
            DataType inputDataType = (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0))
                ? DataType.fromTypeName(in.readString())
                : null;
            String matchType = in.readString();
            String matchField = in.readString();
            Page inputPage;
            try (BlockStreamInput bsi = new BlockStreamInput(in, blockFactory)) {
                inputPage = new Page(bsi);
            }
            PlanStreamInput planIn = new PlanStreamInput(in, in.namedWriteableRegistry(), null);
            List<NamedExpression> extractFields = planIn.readNamedWriteableCollectionAsList(NamedExpression.class);
            var source = Source.EMPTY;
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_17_0)) {
                source = Source.readFrom(planIn);
            }
            TransportRequest result = new TransportRequest(
                sessionId,
                shardId,
                inputDataType,
                matchType,
                matchField,
                inputPage,
                inputPage,
                extractFields,
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
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                out.writeString(inputDataType.typeName());
            }
            out.writeString(matchType);
            out.writeString(matchField);
            out.writeWriteable(inputPage);
            PlanStreamOutput planOut = new PlanStreamOutput(out, null);
            planOut.writeNamedWriteableCollection(extractFields);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_17_0)) {
                source.writeTo(planOut);
            }
        }

        @Override
        protected String extraDescription() {
            return " ,match_type=" + matchType + " ,match_field=" + matchField;
        }
    }

    private static class LookupResponse extends AbstractLookupService.LookupResponse {
        private Page page;

        private LookupResponse(Page page, BlockFactory blockFactory) {
            super(blockFactory);
            this.page = page;
        }

        private LookupResponse(StreamInput in, BlockFactory blockFactory) throws IOException {
            super(blockFactory);
            try (BlockStreamInput bsi = new BlockStreamInput(in, blockFactory)) {
                this.page = new Page(bsi);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            long bytes = page.ramBytesUsedByBlocks();
            blockFactory.breaker().addEstimateBytesAndMaybeBreak(bytes, "serialize enrich lookup response");
            reservedBytes += bytes;
            page.writeTo(out);
        }

        @Override
        protected List<Page> takePages() {
            var p = List.of(page);
            page = null;
            return p;
        }

        @Override
        protected void innerRelease() {
            if (page != null) {
                Releasables.closeExpectNoException(page::releaseBlocks);
            }
        }
    }

    @Override
    protected void sendChildRequest(
        CancellableTask parentTask,
        ActionListener<List<Page>> delegate,
        DiscoveryNode targetNode,
        TransportRequest transportRequest
    ) {
        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        ActionListener<List<Page>> listener = ContextPreservingActionListener.wrapPreservingContext(delegate, threadContext);
        hasEnrichPrivilege(listener.delegateFailureAndWrap((l, ignored) -> {
            // Since we just checked the needed privileges
            // we can access the index regardless of the user/role that is executing the query
            try (ThreadContext.StoredContext unused = threadContext.stashWithOrigin(ClientHelper.ENRICH_ORIGIN)) {
                super.sendChildRequest(parentTask, l, targetNode, transportRequest);
            }
        }));
    }

    protected void hasEnrichPrivilege(ActionListener<Void> outListener) {
        final Settings settings = clusterService.getSettings();
        String privilegeName = ClusterPrivilegeResolver.MONITOR_ENRICH.name();
        if (privilegeName == null
            || settings.hasValue(XPackSettings.SECURITY_ENABLED.getKey()) == false
            || XPackSettings.SECURITY_ENABLED.get(settings) == false) {
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
        request.clusterPrivileges(privilegeName);
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
}
