/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.RuntimeField;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.action.support.TransportActions.isShardNotAvailableException;

public class TransportFieldCapabilitiesAction extends HandledTransportAction<FieldCapabilitiesRequest, FieldCapabilitiesResponse> {

    private static final String ACTION_SHARD_NAME = FieldCapabilitiesAction.NAME + "[index][s]";

    private static final Logger logger = LogManager.getLogger(TransportFieldCapabilitiesAction.class);

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Predicate<String> metadataFieldPred;
    private final IndicesService indicesService;

    @Inject
    public TransportFieldCapabilitiesAction(TransportService transportService,
                                            ClusterService clusterService,
                                            ThreadPool threadPool,
                                            ActionFilters actionFilters,
                                            IndicesService indicesService,
                                            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(FieldCapabilitiesAction.NAME, transportService, actionFilters, FieldCapabilitiesRequest::new);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indicesService = indicesService;
        final Set<String> metadataFields = indicesService.getAllMetadataFields();
        this.metadataFieldPred = metadataFields::contains;
        transportService.registerRequestHandler(ACTION_SHARD_NAME, ThreadPool.Names.SAME,
                FieldCapabilitiesIndexRequest::new, new ShardTransportHandler());
    }

    @Override
    protected void doExecute(Task task, FieldCapabilitiesRequest request, final ActionListener<FieldCapabilitiesResponse> listener) {
        // retrieve the initial timestamp in case the action is a cross cluster search
        long nowInMillis = request.nowInMillis() == null ? System.currentTimeMillis() : request.nowInMillis();
        final ClusterState clusterState = clusterService.state();
        final Map<String, OriginalIndices> remoteClusterIndices =
            transportService.getRemoteClusterService().groupIndices(request.indicesOptions(), request.indices());
        final OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        final String[] concreteIndices;
        if (localIndices == null) {
            // in the case we have one or more remote indices but no local we don't expand to all local indices and just do remote indices
            concreteIndices = Strings.EMPTY_ARRAY;
        } else {
            concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, localIndices);
        }
        final int totalNumRequest = concreteIndices.length + remoteClusterIndices.size();
        if (totalNumRequest == 0) {
            listener.onResponse(new FieldCapabilitiesResponse(new String[0], Collections.emptyMap()));
            return;
        }

        final CountDown completionCounter = new CountDown(totalNumRequest);
        final List<FieldCapabilitiesIndexResponse> indexResponses = Collections.synchronizedList(new ArrayList<>());
        final FailureCollector indexFailures = new FailureCollector();

        final Runnable countDown = () -> {
            if (completionCounter.countDown()) {
                List<FieldCapabilitiesFailure> failures = indexFailures.values();
                if (indexResponses.size() > 0) {
                    if (request.isMergeResults()) {
                        // fork off to the management pool for merging the responses as the operation can run for longer than is acceptable
                        // on a transport thread in case of large numbers of indices and/or fields
                        threadPool.executor(ThreadPool.Names.MANAGEMENT).submit(
                            ActionRunnable.supply(
                                listener,
                                () -> merge(indexResponses, request.includeUnmapped(), new ArrayList<>(failures)))
                        );
                    } else {
                        listener.onResponse(new FieldCapabilitiesResponse(indexResponses, new ArrayList<>(failures)));
                    }
                } else {
                    // we have no responses at all, maybe because of errors
                    if (indexFailures.size() > 0) {
                        // throw back the first exception
                        listener.onFailure(failures.iterator().next().getException());
                    } else {
                        listener.onResponse(new FieldCapabilitiesResponse(Collections.emptyList(), Collections.emptyList()));
                    }
                }
            }
        };

        if (concreteIndices.length > 0) {
            // fork this action to the management pool as it can fan out to a large number of child requests that get handled on SAME and
            // thus would all run on the current transport thread and block it for an unacceptable amount of time
            // (particularly with security enabled)
            threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(ActionRunnable.wrap(listener, l -> {
                for (String index : concreteIndices) {
                    new AsyncShardsAction(
                        transportService,
                        clusterService,
                        new FieldCapabilitiesIndexRequest(
                            request.fields(),
                            index,
                            localIndices,
                            request.indexFilter(),
                            nowInMillis,
                            request.runtimeFields()
                        ),
                        new ActionListener<>() {
                            @Override
                            public void onResponse(FieldCapabilitiesIndexResponse result) {
                                if (result.canMatch()) {
                                    indexResponses.add(result);
                                }
                                countDown.run();
                            }

                            @Override
                            public void onFailure(Exception e) {
                                indexFailures.collect(e, index);
                                countDown.run();
                            }
                        }
                    ).start();
                }
            }));
        }

        // this is the cross cluster part of this API - we force the other cluster to not merge the results but instead
        // send us back all individual index results.
        for (Map.Entry<String, OriginalIndices> remoteIndices : remoteClusterIndices.entrySet()) {
            String clusterAlias = remoteIndices.getKey();
            OriginalIndices originalIndices = remoteIndices.getValue();
            Client remoteClusterClient = transportService.getRemoteClusterService().getRemoteClusterClient(threadPool, clusterAlias);
            FieldCapabilitiesRequest remoteRequest = new FieldCapabilitiesRequest();
            remoteRequest.setMergeResults(false); // we need to merge on this node
            remoteRequest.indicesOptions(originalIndices.indicesOptions());
            remoteRequest.indices(originalIndices.indices());
            remoteRequest.fields(request.fields());
            remoteRequest.runtimeFields(request.runtimeFields());
            remoteRequest.indexFilter(request.indexFilter());
            remoteRequest.nowInMillis(nowInMillis);
            remoteClusterClient.fieldCaps(remoteRequest, ActionListener.wrap(response -> {
                for (FieldCapabilitiesIndexResponse resp : response.getIndexResponses()) {
                    indexResponses.add(
                        new FieldCapabilitiesIndexResponse(
                            RemoteClusterAware.buildRemoteIndexName(clusterAlias, resp.getIndexName()),
                            resp.get(),
                            resp.canMatch()
                        )
                    );
                }
                for (FieldCapabilitiesFailure failure : response.getFailures()) {
                    Exception ex = failure.getException();
                    indexFailures.collectRemoteException(ex, clusterAlias, failure.getIndices());
                }
                countDown.run();
            }, ex -> {
                indexFailures.collectRemoteException(ex, clusterAlias, originalIndices.indices());
                countDown.run();
                }
            ));
        }
    }

    private FieldCapabilitiesResponse merge(
        List<FieldCapabilitiesIndexResponse> indexResponses,
        boolean includeUnmapped,
        List<FieldCapabilitiesFailure> failures
    ) {
        String[] indices = indexResponses.stream().map(FieldCapabilitiesIndexResponse::getIndexName).sorted().toArray(String[]::new);
        final Map<String, Map<String, FieldCapabilities.Builder>> responseMapBuilder = new HashMap<>();
        for (FieldCapabilitiesIndexResponse response : indexResponses) {
            innerMerge(responseMapBuilder, response);
        }
        final Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();
        for (Map.Entry<String, Map<String, FieldCapabilities.Builder>> entry : responseMapBuilder.entrySet()) {
            final Map<String, FieldCapabilities.Builder> typeMapBuilder = entry.getValue();
            if (includeUnmapped) {
                addUnmappedFields(indices, entry.getKey(), typeMapBuilder);
            }
            boolean multiTypes = typeMapBuilder.size() > 1;
            final Map<String, FieldCapabilities> typeMap = new HashMap<>();
            for (Map.Entry<String, FieldCapabilities.Builder> fieldEntry : typeMapBuilder.entrySet()) {
                typeMap.put(fieldEntry.getKey(), fieldEntry.getValue().build(multiTypes));
            }
            responseMap.put(entry.getKey(), Collections.unmodifiableMap(typeMap));
        }
        // de-dup failures
        return new FieldCapabilitiesResponse(indices, Collections.unmodifiableMap(responseMap), failures);
    }

    private void addUnmappedFields(String[] indices, String field, Map<String, FieldCapabilities.Builder> typeMap) {
        Set<String> unmappedIndices = new HashSet<>(Arrays.asList(indices));
        typeMap.values().forEach((b) -> b.getIndices().forEach(unmappedIndices::remove));
        if (unmappedIndices.isEmpty() == false) {
            FieldCapabilities.Builder unmapped = new FieldCapabilities.Builder(field, "unmapped");
            typeMap.put("unmapped", unmapped);
            for (String index : unmappedIndices) {
                unmapped.add(index, false, false, false, Collections.emptyMap());
            }
        }
    }

    private void innerMerge(Map<String, Map<String, FieldCapabilities.Builder>> responseMapBuilder,
                            FieldCapabilitiesIndexResponse response) {
        for (Map.Entry<String, IndexFieldCapabilities> entry : response.get().entrySet()) {
            final String field = entry.getKey();
            // best effort to detect metadata field coming from older nodes
            final boolean isMetadataField = response.getOriginVersion().onOrAfter(Version.V_7_13_0) ?
                entry.getValue().isMetadatafield() : metadataFieldPred.test(field);
            final IndexFieldCapabilities fieldCap = entry.getValue();
            Map<String, FieldCapabilities.Builder> typeMap = responseMapBuilder.computeIfAbsent(field, f -> new HashMap<>());
            FieldCapabilities.Builder builder = typeMap.computeIfAbsent(fieldCap.getType(),
                key -> new FieldCapabilities.Builder(field, key));
            builder.add(response.getIndexName(), isMetadataField, fieldCap.isSearchable(), fieldCap.isAggregatable(), fieldCap.meta());
        }
    }

    private static final class FailureCollector {
        final Map<Tuple<String, String>, FieldCapabilitiesFailure> indexFailures = Collections.synchronizedMap(
            new HashMap<>()
        );

        List<FieldCapabilitiesFailure> values() {
            return new ArrayList<>(indexFailures.values());
        }

        void collect(Exception e, String index) {
            // we deduplicate exceptions on the underlying causes message and classname
            // we unwrap the cause to e.g. group RemoteTransportexceptions coming from different nodes if the cause is the same
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            Tuple<String, String> groupingKey = new Tuple<String, String>(cause.getMessage(), cause.getClass().getName());
            indexFailures.compute(
                groupingKey,
                (k, v) -> v == null ? new FieldCapabilitiesFailure(new String[] {index}, e) : v.addIndex(index)
            );
        }

        void collectRemoteException(Exception ex, String clusterAlias, String[] remoteIndices) {
            for (String failedIndex : remoteIndices) {
                collect(ex, RemoteClusterAware.buildRemoteIndexName(clusterAlias, failedIndex));
            }
        }

        int size() {
            return this.indexFailures.size();
        }
    }

    private static ClusterBlockException checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    private static ClusterBlockException checkRequestBlock(ClusterState state, String concreteIndex) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, concreteIndex);
    }

    private FieldCapabilitiesIndexResponse shardOperation(final FieldCapabilitiesIndexRequest request) throws IOException {
        final ShardId shardId = request.shardId();
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexShard indexShard = indexService.getShard(request.shardId().getId());
        try (Engine.Searcher searcher = indexShard.acquireSearcher(Engine.CAN_MATCH_SEARCH_SOURCE)) {

            final SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(shardId.id(), 0,
                    searcher, request::nowInMillis, null, request.runtimeFields());

            if (canMatchShard(request, searchExecutionContext) == false) {
                return new FieldCapabilitiesIndexResponse(request.index(), Collections.emptyMap(), false);
            }

            Set<String> fieldNames = new HashSet<>();
            for (String pattern : request.fields()) {
                fieldNames.addAll(searchExecutionContext.getMatchingFieldNames(pattern));
            }

            Predicate<String> fieldPredicate = indicesService.getFieldFilter().apply(shardId.getIndexName());
            Map<String, IndexFieldCapabilities> responseMap = new HashMap<>();
            for (String field : fieldNames) {
                MappedFieldType ft = searchExecutionContext.getFieldType(field);
                boolean isMetadataField = searchExecutionContext.isMetadataField(field);
                if (isMetadataField || fieldPredicate.test(ft.name())) {
                    IndexFieldCapabilities fieldCap = new IndexFieldCapabilities(field,
                            ft.familyTypeName(), isMetadataField, ft.isSearchable(), ft.isAggregatable(), ft.meta());
                    responseMap.put(field, fieldCap);
                } else {
                    continue;
                }

                // Check the ancestor of the field to find nested and object fields.
                // Runtime fields are excluded since they can override any path.
                //TODO find a way to do this that does not require an instanceof check
                if (ft instanceof RuntimeField == false) {
                    int dotIndex = ft.name().lastIndexOf('.');
                    while (dotIndex > -1) {
                        String parentField = ft.name().substring(0, dotIndex);
                        if (responseMap.containsKey(parentField)) {
                            // we added this path on another field already
                            break;
                        }
                        // checks if the parent field contains sub-fields
                        if (searchExecutionContext.getFieldType(parentField) == null) {
                            // no field type, it must be an object field
                            ObjectMapper mapper = searchExecutionContext.getObjectMapper(parentField);
                            // Composite runtime fields do not have a mapped type for the root - check for null
                            if (mapper != null) {
                                String type = mapper.isNested() ? "nested" : "object";
                                IndexFieldCapabilities fieldCap = new IndexFieldCapabilities(parentField, type,
                                    false, false, false, Collections.emptyMap());
                                responseMap.put(parentField, fieldCap);
                            }
                        }
                        dotIndex = parentField.lastIndexOf('.');
                    }
                }
            }
            return new FieldCapabilitiesIndexResponse(request.index(), responseMap, true);
        }
    }

    private boolean canMatchShard(FieldCapabilitiesIndexRequest req, SearchExecutionContext searchExecutionContext) throws IOException {
        if (req.indexFilter() == null || req.indexFilter() instanceof MatchAllQueryBuilder) {
            return true;
        }
        assert req.nowInMillis() != 0L;
        ShardSearchRequest searchRequest = new ShardSearchRequest(req.shardId(), req.nowInMillis(), AliasFilter.EMPTY);
        searchRequest.source(new SearchSourceBuilder().query(req.indexFilter()));
        return SearchService.queryStillMatchesAfterRewrite(searchRequest, searchExecutionContext);
    }

    /**
     * An action that executes on each shard sequentially until it finds one that can match the provided
     * {@link FieldCapabilitiesIndexRequest#indexFilter()}. In which case the shard is used
     * to create the final {@link FieldCapabilitiesIndexResponse}.
     */
    public static class AsyncShardsAction {
        private final FieldCapabilitiesIndexRequest request;
        private final TransportService transportService;
        private final DiscoveryNodes nodes;
        private final ActionListener<FieldCapabilitiesIndexResponse> listener;
        private final GroupShardsIterator<ShardIterator> shardsIt;

        private volatile int shardIndex = 0;

        public AsyncShardsAction(TransportService transportService,
                                 ClusterService clusterService,
                                 FieldCapabilitiesIndexRequest request,
                                 ActionListener<FieldCapabilitiesIndexResponse> listener) {
            this.listener = listener;
            this.transportService = transportService;

            ClusterState clusterState = clusterService.state();
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}] based on cluster state version [{}]", request, clusterState.version());
            }
            nodes = clusterState.nodes();
            ClusterBlockException blockException = checkGlobalBlock(clusterState);
            if (blockException != null) {
                throw blockException;
            }

            this.request = request;
            blockException = checkRequestBlock(clusterState, request.index());
            if (blockException != null) {
                throw blockException;
            }

            shardsIt = clusterService.operationRouting().searchShards(clusterService.state(),
                new String[]{request.index()}, null, null, null, null);
        }

        public void start() {
            tryNext(null, true);
        }

        private void onFailure(ShardRouting shardRouting, Exception e) {
            if (e != null) {
                logger.trace(() -> new ParameterizedMessage("{}: failed to execute [{}]", shardRouting, request), e);
            }
            tryNext(e, false);
        }

        private ShardRouting nextRoutingOrNull()  {
            if (shardsIt.size() == 0 || shardIndex >= shardsIt.size()) {
                return null;
            }
            ShardRouting next = shardsIt.get(shardIndex).nextOrNull();
            if (next != null) {
                return next;
            }
            moveToNextShard();
            return nextRoutingOrNull();
        }

        private void moveToNextShard() {
            ++ shardIndex;
        }

        private void tryNext(@Nullable final Exception lastFailure, boolean canMatchShard) {
            ShardRouting shardRouting = nextRoutingOrNull();
            if (shardRouting == null) {
                if (canMatchShard == false) {
                    if (lastFailure == null) {
                        listener.onResponse(new FieldCapabilitiesIndexResponse(request.index(), Collections.emptyMap(), false));
                    } else {
                        logger.debug(() -> new ParameterizedMessage("{}: failed to execute [{}]", null, request), lastFailure);
                        listener.onFailure(lastFailure);
                    }
                } else {
                    if (lastFailure == null || isShardNotAvailableException(lastFailure)) {
                        listener.onFailure(new NoShardAvailableActionException(null,
                            LoggerMessageFormat.format("No shard available for [{}]", request), lastFailure));
                    } else {
                        logger.debug(() -> new ParameterizedMessage("{}: failed to execute [{}]", null, request), lastFailure);
                        listener.onFailure(lastFailure);
                    }
                }
                return;
            }
            DiscoveryNode node = nodes.get(shardRouting.currentNodeId());
            if (node == null) {
                onFailure(shardRouting, new NoShardAvailableActionException(shardRouting.shardId()));
            } else {
                request.shardId(shardRouting.shardId());
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "sending request [{}] on node [{}]",
                        request,
                        node
                    );
                }
                transportService.sendRequest(node, ACTION_SHARD_NAME, request,
                    new TransportResponseHandler<FieldCapabilitiesIndexResponse>() {

                        @Override
                        public FieldCapabilitiesIndexResponse read(StreamInput in) throws IOException {
                            return new FieldCapabilitiesIndexResponse(in);
                        }

                        @Override
                        public void handleResponse(final FieldCapabilitiesIndexResponse response) {
                            if (response.canMatch()) {
                                listener.onResponse(response);
                            } else {
                                moveToNextShard();
                                tryNext(null, false);
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            onFailure(shardRouting, exp);
                        }
                    });
            }
        }
    }

    private class ShardTransportHandler implements TransportRequestHandler<FieldCapabilitiesIndexRequest> {
        @Override
        public void messageReceived(final FieldCapabilitiesIndexRequest request,
                                    final TransportChannel channel,
                                    Task task) throws Exception {
            if (logger.isTraceEnabled()) {
                logger.trace("executing [{}]", request);
            }
            ActionListener<FieldCapabilitiesIndexResponse> listener = new ChannelActionListener<>(channel, ACTION_SHARD_NAME, request);
            final FieldCapabilitiesIndexResponse resp;
            try {
                resp = shardOperation(request);
            } catch (Exception exc) {
                listener.onFailure(exc);
                return;
            }
            listener.onResponse(resp);
        }
    }
}
