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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.ValueSources;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.ProjectOperator;
import org.elasticsearch.compute.operator.SourceOperator;
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
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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

    public EnrichLookupService(ClusterService clusterService, SearchService searchService, TransportService transportService) {
        this.clusterService = clusterService;
        this.searchService = searchService;
        this.transportService = transportService;
        this.executor = transportService.getThreadPool().executor(EsqlPlugin.ESQL_THREAD_POOL_NAME);
        transportService.registerRequestHandler(
            LOOKUP_ACTION_NAME,
            EsqlPlugin.ESQL_THREAD_POOL_NAME,
            LookupRequest::new,
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
        ActionListener<Page> listener
    ) {
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
        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        listener = ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
        try (ThreadContext.StoredContext ignored = threadContext.stashWithOrigin(ClientHelper.ENRICH_ORIGIN)) {
            // TODO: handle retry and avoid forking for the local lookup
            transportService.sendChildRequest(
                targetNode,
                LOOKUP_ACTION_NAME,
                lookupRequest,
                parentTask,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener.map(r -> r.page), LookupResponse::new)
            );
        }
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
        if (inputBlock.areAllValuesNull()) {
            listener.onResponse(createNullResponse(inputPage.getPositionCount(), extractFields));
            return;
        }
        try {
            ShardSearchRequest shardSearchRequest = new ShardSearchRequest(shardId, 0, AliasFilter.EMPTY);
            SearchContext searchContext = searchService.createSearchContext(shardSearchRequest, SearchService.NO_TIMEOUT);
            listener = ActionListener.runBefore(listener, searchContext::close);
            SearchExecutionContext searchExecutionContext = searchContext.getSearchExecutionContext();
            MappedFieldType fieldType = searchExecutionContext.getFieldType(matchField);
            final SourceOperator queryOperator = switch (matchType) {
                case "match", "range" -> {
                    QueryList queryList = QueryList.termQueryList(fieldType, searchExecutionContext, inputBlock);
                    yield new EnrichQuerySourceOperator(queryList, searchExecutionContext.getIndexReader());
                }
                default -> throw new UnsupportedOperationException("unsupported match type " + matchType);
            };
            List<Operator> intermediateOperators = new ArrayList<>(extractFields.size() + 2);
            final ElementType[] mergingTypes = new ElementType[extractFields.size()];
            // extract-field operators
            for (int i = 0; i < extractFields.size(); i++) {
                NamedExpression extractField = extractFields.get(i);
                final ElementType elementType = LocalExecutionPlanner.toElementType(extractField.dataType());
                mergingTypes[i] = elementType;
                var sources = ValueSources.sources(
                    List.of(searchContext),
                    extractField instanceof Alias a ? ((NamedExpression) a.child()).name() : extractField.name(),
                    EsqlDataTypes.isUnsupported(extractField.dataType()),
                    elementType
                );
                intermediateOperators.add(new ValuesSourceReaderOperator(sources, 0, extractField.name()));
            }
            // drop docs block
            intermediateOperators.add(droppingBlockOperator(extractFields.size() + 2, 0));
            boolean singleLeaf = searchContext.searcher().getLeafContexts().size() == 1;
            // merging field-values by position
            final int[] mergingChannels = IntStream.range(0, extractFields.size()).map(i -> i + 1).toArray();
            intermediateOperators.add(
                new MergePositionsOperator(singleLeaf, inputPage.getPositionCount(), 0, mergingChannels, mergingTypes)
            );
            AtomicReference<Page> result = new AtomicReference<>();
            OutputOperator outputOperator = new OutputOperator(List.of(), Function.identity(), result::set);
            Driver driver = new Driver(
                "enrich-lookup:" + sessionId,
                new DriverContext(),
                () -> lookupDescription(sessionId, shardId, matchType, matchField, extractFields, inputPage.getPositionCount()),
                queryOperator,
                intermediateOperators,
                outputOperator,
                searchContext
            );
            task.addListener(() -> {
                String reason = Objects.requireNonNullElse(task.getReasonCancelled(), "task was cancelled");
                driver.cancel(reason);
            });
            Driver.start(executor, driver, Driver.DEFAULT_MAX_ITERATIONS, listener.map(ignored -> {
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

    private static Page createNullResponse(int positionCount, List<NamedExpression> extractFields) {
        final Block[] blocks = new Block[extractFields.size()];
        for (int i = 0; i < extractFields.size(); i++) {
            blocks[i] = Block.constantNullBlock(positionCount);
        }
        return new Page(blocks);
    }

    private static Operator droppingBlockOperator(int totalBlocks, int droppingPosition) {
        BitSet bitSet = new BitSet(totalBlocks);
        bitSet.set(0, totalBlocks);
        bitSet.clear(droppingPosition);
        return new ProjectOperator(bitSet);
    }

    private class TransportHandler implements TransportRequestHandler<LookupRequest> {
        @Override
        public void messageReceived(LookupRequest request, TransportChannel channel, Task task) {
            ActionListener<LookupResponse> listener = new ChannelActionListener<>(channel);
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
            this.extractFields = extractFields;
        }

        LookupRequest(StreamInput in) throws IOException {
            super(in);
            this.sessionId = in.readString();
            this.shardId = new ShardId(in);
            this.matchType = in.readString();
            this.matchField = in.readString();
            this.inputPage = new Page(in);
            PlanStreamInput planIn = new PlanStreamInput(in, PlanNameRegistry.INSTANCE, in.namedWriteableRegistry(), null);
            this.extractFields = planIn.readList(readerFromPlanReader(PlanStreamInput::readNamedExpression));
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
        private final Page page;

        LookupResponse(Page page) {
            this.page = page;
        }

        LookupResponse(StreamInput in) throws IOException {
            this.page = new Page(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            page.writeTo(out);
        }
    }
}
