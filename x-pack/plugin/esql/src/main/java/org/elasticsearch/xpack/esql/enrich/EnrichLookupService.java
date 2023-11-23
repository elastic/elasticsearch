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
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.BlockReaderFactories;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.ProjectOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
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
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.NamedExpression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry.PlanReader.readerFromPlanReader;
import static org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry.PlanWriter.writerFromPlanWriter;

/**
 * {@link EnrichLookupService} performs enrich lookup for a given input page. The lookup process consists of three stages:
 * - Stage 1: Finding matching document IDs for the input page. This stage is done by the {@link EnrichQuerySourceOperator} or its variants.
 * The output page of this stage is represented as [DocVector, IntBlock: positions of the input terms].
 * <p>
 * - Stage 2: Extracting field values for the matched document IDs. The output page is represented as
 * [DocVector, IntBlock: positions, Block: field1, Block: field2,...].
 * <p>
 * - Stage 3: Combining the extracted values based on positions and filling nulls for positions without matches.
 * This is done by {@link MergePositionsOperator}. The output page is represented as [Block: field1, Block: field2,...].
 * <p>
 * The positionCount of the output page must be equal to the positionCount of the input page.
 */
public class EnrichLookupService {
    public static final String LOOKUP_ACTION_NAME = EsqlQueryAction.NAME + "/lookup";

    private final ClusterService clusterService;
    private final SearchService searchService;
    private final TransportService transportService;
    private final Executor executor;
    private final BigArrays bigArrays;
    private final BlockFactory blockFactory;

    public EnrichLookupService(
        ClusterService clusterService,
        SearchService searchService,
        TransportService transportService,
        BigArrays bigArrays,
        BlockFactory blockFactory
    ) {
        this.clusterService = clusterService;
        this.searchService = searchService;
        this.transportService = transportService;
        this.executor = transportService.getThreadPool().executor(EsqlPlugin.ESQL_THREAD_POOL_NAME);
        this.bigArrays = bigArrays;
        this.blockFactory = blockFactory;
        transportService.registerRequestHandler(
            LOOKUP_ACTION_NAME,
            this.executor,
            in -> new LookupRequest(in, blockFactory),
            new TransportHandler()
        );
    }

    public void lookupAsync(
        String sessionId,
        CancellableTask parentTask,
        String index,
        String matchType,
        String matchField,
        List<NamedExpression> extractFields,
        Page inputPage,
        ActionListener<Page> outListener
    ) {
        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        ActionListener<Page> listener = ContextPreservingActionListener.wrapPreservingContext(outListener, threadContext);
        hasEnrichPrivilege(ActionListener.wrap(ignored -> {
            ClusterState clusterState = clusterService.state();
            GroupShardsIterator<ShardIterator> shardIterators = clusterService.operationRouting()
                .searchShards(clusterState, new String[] { index }, Map.of(), "_local");
            if (shardIterators.size() != 1) {
                listener.onFailure(new EsqlIllegalArgumentException("target index {} has more than one shard", index));
                return;
            }
            ShardIterator shardIt = shardIterators.get(0);
            ShardRouting shardRouting = shardIt.nextOrNull();
            if (shardRouting == null) {
                listener.onFailure(new UnavailableShardsException(shardIt.shardId(), "enrich index is not available"));
                return;
            }
            DiscoveryNode targetNode = clusterState.nodes().get(shardRouting.currentNodeId());
            LookupRequest lookupRequest = new LookupRequest(sessionId, shardIt.shardId(), matchType, matchField, inputPage, extractFields);
            // TODO: handle retry and avoid forking for the local lookup
            try (ThreadContext.StoredContext unused = threadContext.stashWithOrigin(ClientHelper.ENRICH_ORIGIN)) {
                transportService.sendChildRequest(
                    targetNode,
                    LOOKUP_ACTION_NAME,
                    lookupRequest,
                    parentTask,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(
                        listener.map(LookupResponse::takePage),
                        in -> new LookupResponse(in, blockFactory),
                        executor
                    )
                );
            }
        }, listener::onFailure));
    }

    private void hasEnrichPrivilege(ActionListener<Void> outListener) {
        final Settings settings = clusterService.getSettings();
        if (settings.hasValue(XPackSettings.SECURITY_ENABLED.getKey()) == false || XPackSettings.SECURITY_ENABLED.get(settings) == false) {
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
        request.clusterPrivileges(ClusterPrivilegeResolver.MONITOR_ENRICH.name());
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

    private void doLookup(
        String sessionId,
        CancellableTask task,
        ShardId shardId,
        String matchType,
        String matchField,
        Page inputPage,
        List<NamedExpression> extractFields,
        ActionListener<Page> listener
    ) {
        Block inputBlock = inputPage.getBlock(0);
        try {
            if (inputBlock.areAllValuesNull()) {
                listener.onResponse(createNullResponse(inputPage.getPositionCount(), extractFields));
                return;
            }
            ShardSearchRequest shardSearchRequest = new ShardSearchRequest(shardId, 0, AliasFilter.EMPTY);
            SearchContext searchContext = searchService.createSearchContext(shardSearchRequest, SearchService.NO_TIMEOUT);
            listener = ActionListener.runBefore(listener, searchContext::close);
            SearchExecutionContext searchExecutionContext = searchContext.getSearchExecutionContext();
            MappedFieldType fieldType = searchExecutionContext.getFieldType(matchField);
            final SourceOperator queryOperator = switch (matchType) {
                case "match", "range" -> {
                    QueryList queryList = QueryList.termQueryList(fieldType, searchExecutionContext, inputBlock);
                    yield new EnrichQuerySourceOperator(blockFactory, queryList, searchExecutionContext.getIndexReader());
                }
                default -> throw new EsqlIllegalArgumentException("illegal match type " + matchType);
            };
            List<Operator> intermediateOperators = new ArrayList<>(extractFields.size() + 2);
            final ElementType[] mergingTypes = new ElementType[extractFields.size()];

            // load the fields
            List<ValuesSourceReaderOperator.FieldInfo> fields = new ArrayList<>(extractFields.size());
            for (int i = 0; i < extractFields.size(); i++) {
                NamedExpression extractField = extractFields.get(i);
                final ElementType elementType = LocalExecutionPlanner.toElementType(extractField.dataType());
                mergingTypes[i] = elementType;
                var loaders = BlockReaderFactories.loaders(
                    List.of(searchContext),
                    extractField instanceof Alias a ? ((NamedExpression) a.child()).name() : extractField.name(),
                    EsqlDataTypes.isUnsupported(extractField.dataType())
                );
                fields.add(new ValuesSourceReaderOperator.FieldInfo(extractField.name(), loaders));
            }
            intermediateOperators.add(
                new ValuesSourceReaderOperator(
                    blockFactory,
                    fields,
                    List.of(new ValuesSourceReaderOperator.ShardContext(searchContext.searcher().getIndexReader(), () -> {
                        throw new UnsupportedOperationException("can't load _source as part of enrich");
                    })),
                    0
                )
            );

            // drop docs block
            intermediateOperators.add(droppingBlockOperator(extractFields.size() + 2, 0));
            boolean singleLeaf = searchContext.searcher().getLeafContexts().size() == 1;

            // merging field-values by position
            final int[] mergingChannels = IntStream.range(0, extractFields.size()).map(i -> i + 1).toArray();
            intermediateOperators.add(
                new MergePositionsOperator(singleLeaf, inputPage.getPositionCount(), 0, mergingChannels, mergingTypes, blockFactory)
            );
            AtomicReference<Page> result = new AtomicReference<>();
            OutputOperator outputOperator = new OutputOperator(List.of(), Function.identity(), result::set);
            Driver driver = new Driver(
                "enrich-lookup:" + sessionId,
                new DriverContext(bigArrays, blockFactory),
                () -> lookupDescription(sessionId, shardId, matchType, matchField, extractFields, inputPage.getPositionCount()),
                queryOperator,
                intermediateOperators,
                outputOperator,
                Driver.DEFAULT_STATUS_INTERVAL,
                searchContext
            );
            task.addListener(() -> {
                String reason = Objects.requireNonNullElse(task.getReasonCancelled(), "task was cancelled");
                driver.cancel(reason);
            });

            var threadContext = transportService.getThreadPool().getThreadContext();
            Driver.start(threadContext, executor, driver, Driver.DEFAULT_MAX_ITERATIONS, listener.map(ignored -> {
                Page out = result.get();
                if (out == null) {
                    out = createNullResponse(inputPage.getPositionCount(), extractFields);
                }
                return out;
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
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

    private static Operator droppingBlockOperator(int totalBlocks, int droppingPosition) {
        var size = totalBlocks - 1;
        var projection = new ArrayList<Integer>(size);
        for (int i = 0; i < totalBlocks; i++) {
            if (i != droppingPosition) {
                projection.add(i);
            }
        }
        return new ProjectOperator(projection);
    }

    private class TransportHandler implements TransportRequestHandler<LookupRequest> {
        @Override
        public void messageReceived(LookupRequest request, TransportChannel channel, Task task) {
            request.incRef();
            ActionListener<LookupResponse> listener = ActionListener.runBefore(new ChannelActionListener<>(channel), request::decRef);
            doLookup(
                request.sessionId,
                (CancellableTask) task,
                request.shardId,
                request.matchType,
                request.matchField,
                request.inputPage,
                request.extractFields,
                listener.map(LookupResponse::new)
            );
        }
    }

    private static class LookupRequest extends TransportRequest implements IndicesRequest {
        private final String sessionId;
        private final ShardId shardId;
        private final String matchType;
        private final String matchField;
        private final Page inputPage;
        private final List<NamedExpression> extractFields;
        // TODO: Remove this workaround once we have Block RefCount
        private final Page toRelease;
        private final RefCounted refs = AbstractRefCounted.of(this::releasePage);

        LookupRequest(
            String sessionId,
            ShardId shardId,
            String matchType,
            String matchField,
            Page inputPage,
            List<NamedExpression> extractFields
        ) {
            this.sessionId = sessionId;
            this.shardId = shardId;
            this.matchType = matchType;
            this.matchField = matchField;
            this.inputPage = inputPage;
            this.toRelease = null;
            this.extractFields = extractFields;
        }

        LookupRequest(StreamInput in, BlockFactory blockFactory) throws IOException {
            super(in);
            this.sessionId = in.readString();
            this.shardId = new ShardId(in);
            this.matchType = in.readString();
            this.matchField = in.readString();
            this.inputPage = new Page(new BlockStreamInput(in, blockFactory));
            this.toRelease = inputPage;
            PlanStreamInput planIn = new PlanStreamInput(in, PlanNameRegistry.INSTANCE, in.namedWriteableRegistry(), null);
            this.extractFields = planIn.readCollectionAsList(readerFromPlanReader(PlanStreamInput::readNamedExpression));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sessionId);
            out.writeWriteable(shardId);
            out.writeString(matchType);
            out.writeString(matchField);
            out.writeWriteable(inputPage);
            PlanStreamOutput planOut = new PlanStreamOutput(out, PlanNameRegistry.INSTANCE);
            planOut.writeCollection(extractFields, writerFromPlanWriter(PlanStreamOutput::writeNamedExpression));
        }

        @Override
        public String[] indices() {
            return new String[] { shardId.getIndexName() };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers) {
                @Override
                public String getDescription() {
                    return lookupDescription(sessionId, shardId, matchType, matchField, extractFields, inputPage.getPositionCount());
                }
            };
        }

        private void releasePage() {
            if (toRelease != null) {
                Releasables.closeExpectNoException(toRelease::releaseBlocks);
            }
        }

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

    private static String lookupDescription(
        String sessionId,
        ShardId shardId,
        String matchType,
        String matchField,
        List<NamedExpression> extractFields,
        int positionCount
    ) {
        return "ENRICH_LOOKUP("
            + " session="
            + sessionId
            + " ,shard="
            + shardId
            + " ,match_type="
            + matchType
            + " ,match_field="
            + matchField
            + " ,extract_fields="
            + extractFields
            + " ,positions="
            + positionCount
            + ")";
    }

    private static class LookupResponse extends TransportResponse {
        private Page page;
        private final RefCounted refs = AbstractRefCounted.of(this::releasePage);

        LookupResponse(Page page) {
            this.page = page;
        }

        LookupResponse(StreamInput in, BlockFactory blockFactory) throws IOException {
            this.page = new Page(new BlockStreamInput(in, blockFactory));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            page.writeTo(out);
        }

        Page takePage() {
            var p = page;
            page = null;
            return p;
        }

        private void releasePage() {
            if (page != null) {
                Releasables.closeExpectNoException(page::releaseBlocks);
            }
        }

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
}
