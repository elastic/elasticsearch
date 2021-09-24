/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public class TransportFieldCapabilitiesAction extends HandledTransportAction<FieldCapabilitiesRequest, FieldCapabilitiesResponse> {
    public static final String ACTION_NODE_NAME = FieldCapabilitiesAction.NAME + "[n]";

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private final FieldCapabilitiesFetcher fieldCapabilitiesFetcher;
    private final Predicate<String> metadataFieldPred;

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

        this.fieldCapabilitiesFetcher = new FieldCapabilitiesFetcher(indicesService);
        final Set<String> metadataFields = indicesService.getAllMetadataFields();
        this.metadataFieldPred = metadataFields::contains;

        transportService.registerRequestHandler(ACTION_NODE_NAME, ThreadPool.Names.SAME,
            FieldCapabilitiesNodeRequest::new, new NodeTransportHandler());
    }

    @Override
    protected void doExecute(Task task, FieldCapabilitiesRequest request, final ActionListener<FieldCapabilitiesResponse> listener) {
        // retrieve the initial timestamp in case the action is a cross cluster search
        long nowInMillis = request.nowInMillis() == null ? System.currentTimeMillis() : request.nowInMillis();
        final ClusterState clusterState = clusterService.state();
        final Map<String, OriginalIndices> remoteClusterIndices =
            transportService.getRemoteClusterService().groupIndices(request.indicesOptions(),
                    request.indices(), idx -> indexNameExpressionResolver.hasIndexAbstraction(idx, clusterState));
        final OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        final String[] concreteIndices;
        if (localIndices == null) {
            // in the case we have one or more remote indices but no local we don't expand to all local indices and just do remote indices
            concreteIndices = Strings.EMPTY_ARRAY;
        } else {
            concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, localIndices);
        }

        if (concreteIndices.length == 0 && remoteClusterIndices.isEmpty()) {
            listener.onResponse(new FieldCapabilitiesResponse(new String[0], Collections.emptyMap()));
            return;
        }

        checkIndexBlocks(clusterState, concreteIndices);

        final Map<String, FieldCapabilitiesIndexResponse> indexResponses = Collections.synchronizedMap(new HashMap<>());
        final FailureCollector indexFailures = new FailureCollector();

        // If all nodes are on version 7.16 or higher, then we group the shard requests and send a single request per node.
        // Otherwise, for backwards compatibility we follow the old strategy of sending a separate request per shard.
        final Map<String, List<ShardId>> shardsByNode;
        final CountDown completionCounter;
        if (clusterState.getNodes().getMinNodeVersion().onOrAfter(Version.V_7_16_0)) {
            shardsByNode = groupShardsByNode(request, clusterState, concreteIndices, indexFailures);
            completionCounter = new CountDown(shardsByNode.size() + remoteClusterIndices.size());
        } else {
            shardsByNode = null;
            completionCounter = new CountDown(concreteIndices.length + remoteClusterIndices.size());
        }

        final Runnable countDown = createResponseMerger(request, completionCounter, indexResponses, indexFailures, listener);
        if (concreteIndices.length > 0) {
            // fork this action to the management pool as it can fan out to a large number of child requests that get handled on SAME and
            // thus would all run on the current transport thread and block it for an unacceptable amount of time
            // (particularly with security enabled)
            threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(ActionRunnable.wrap(listener, l -> {
                if (shardsByNode != null) {
                    for (Map.Entry<String, List<ShardId>> entry : shardsByNode.entrySet()) {
                        String nodeId = entry.getKey();
                        List<ShardId> shardIds = entry.getValue();

                        ActionListener<FieldCapabilitiesNodeResponse> nodeListener = new ActionListener<FieldCapabilitiesNodeResponse>() {
                            @Override
                            public void onResponse(FieldCapabilitiesNodeResponse response) {
                                for (FieldCapabilitiesIndexResponse indexResponse : response.getIndexResponses()) {
                                    if (indexResponse.canMatch()) {
                                        indexResponses.putIfAbsent(indexResponse.getIndexName(), indexResponse);
                                    }
                                }
                                for (FieldCapabilitiesFailure indexFailure : response.getFailures()) {
                                    indexFailures.collect(indexFailure.getException(), indexFailure.getIndices());
                                }
                                countDown.run();
                            }

                            @Override
                            public void onFailure(Exception e) {
                                for (ShardId shardId : shardIds) {
                                    indexFailures.collect(e, shardId.getIndexName());
                                }
                                countDown.run();
                            }
                        };

                        DiscoveryNode node = clusterState.getNodes().get(nodeId);
                        if (node == null) {
                            nodeListener.onFailure(new NoNodeAvailableException("node [" + nodeId + "] is not available"));
                        } else {
                            FieldCapabilitiesNodeRequest nodeRequest = prepareLocalNodeRequest(
                                request, shardIds, localIndices, nowInMillis);
                            transportService.sendRequest(node, ACTION_NODE_NAME, nodeRequest, new ActionListenerResponseHandler<>(
                                nodeListener, FieldCapabilitiesNodeResponse::new));
                        }
                    }
                } else {
                    for (String index : concreteIndices) {
                        new TransportFieldCapabilitiesIndexAction.AsyncShardsAction(
                            transportService,
                            clusterService,
                            prepareLocalIndexRequest(request, index, localIndices, nowInMillis),
                            new ActionListener<FieldCapabilitiesIndexResponse>() {
                                @Override
                                public void onResponse(FieldCapabilitiesIndexResponse result) {
                                    if (result.canMatch()) {
                                        indexResponses.putIfAbsent(result.getIndexName(), result);
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
                }
            }));
        }

        // this is the cross cluster part of this API - we force the other cluster to not merge the results but instead
        // send us back all individual index results.
        for (Map.Entry<String, OriginalIndices> remoteIndices : remoteClusterIndices.entrySet()) {
            String clusterAlias = remoteIndices.getKey();
            OriginalIndices originalIndices = remoteIndices.getValue();
            Client remoteClusterClient = transportService.getRemoteClusterService().getRemoteClusterClient(threadPool, clusterAlias);
            FieldCapabilitiesRequest remoteRequest = prepareRemoteRequest(request, originalIndices, nowInMillis);
            remoteClusterClient.fieldCaps(remoteRequest, ActionListener.wrap(response -> {
                for (FieldCapabilitiesIndexResponse resp : response.getIndexResponses()) {
                    String indexName = RemoteClusterAware.buildRemoteIndexName(clusterAlias, resp.getIndexName());
                    indexResponses.putIfAbsent(indexName,
                        new FieldCapabilitiesIndexResponse(indexName, resp.get(), resp.canMatch()));
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

    private void checkIndexBlocks(ClusterState clusterState, String[] concreteIndices) {
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        for (String index : concreteIndices) {
            clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index);
        }
    }

    private Runnable createResponseMerger(FieldCapabilitiesRequest request,
                                          CountDown completionCounter,
                                          Map<String, FieldCapabilitiesIndexResponse> indexResponses,
                                          FailureCollector indexFailures,
                                          ActionListener<FieldCapabilitiesResponse> listener) {
        return () -> {
            if (completionCounter.countDown()) {
                List<FieldCapabilitiesFailure> failures = indexFailures.build(indexResponses.keySet());
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
                        listener.onResponse(new FieldCapabilitiesResponse(
                            new ArrayList<>(indexResponses.values()),
                            new ArrayList<>(failures)));
                    }
                } else {
                    // we have no responses at all, maybe because of errors
                    if (indexFailures.isEmpty() == false) {
                        // throw back the first exception
                        listener.onFailure(failures.iterator().next().getException());
                    } else {
                        listener.onResponse(new FieldCapabilitiesResponse(Collections.emptyList(), Collections.emptyList()));
                    }
                }
            }
        };
    }

    private static FieldCapabilitiesNodeRequest prepareLocalNodeRequest(FieldCapabilitiesRequest request,
                                                                        List<ShardId> shardIds,
                                                                        OriginalIndices originalIndices,
                                                                        long nowInMillis) {
        ShardId[] shardIdArray = shardIds.toArray(new ShardId[0]);
        return new FieldCapabilitiesNodeRequest(shardIdArray, request.fields(), originalIndices,
            request.indexFilter(), nowInMillis, request.runtimeFields());
    }

    private static FieldCapabilitiesIndexRequest prepareLocalIndexRequest(FieldCapabilitiesRequest request,
                                                                          String index,
                                                                          OriginalIndices originalIndices,
                                                                          long nowInMillis) {
        return new FieldCapabilitiesIndexRequest(request.fields(), index, originalIndices,
            request.indexFilter(), nowInMillis, request.runtimeFields());
    }

    private static FieldCapabilitiesRequest prepareRemoteRequest(FieldCapabilitiesRequest request,
                                                                 OriginalIndices originalIndices,
                                                                 long nowInMillis) {
        FieldCapabilitiesRequest remoteRequest = new FieldCapabilitiesRequest();
        remoteRequest.setMergeResults(false); // we need to merge on this node
        remoteRequest.indicesOptions(originalIndices.indicesOptions());
        remoteRequest.indices(originalIndices.indices());
        remoteRequest.fields(request.fields());
        remoteRequest.runtimeFields(request.runtimeFields());
        remoteRequest.indexFilter(request.indexFilter());
        remoteRequest.nowInMillis(nowInMillis);
        return remoteRequest;
    }

    private Map<String, List<ShardId>> groupShardsByNode(FieldCapabilitiesRequest request,
                                                         ClusterState clusterState,
                                                         String[] concreteIndices,
                                                         FailureCollector indexFailures) {
        // If an index filter is specified, then we must reach out to all of an index's shards to check
        // if one of them can match. Otherwise, for efficiency we just reach out to one of its shards.
        boolean includeAllShards = request.indexFilter() != null;
        Map<String, List<ShardId>> shardsByNodeId = new HashMap<>();
        for (String indexName : concreteIndices) {
            GroupShardsIterator<ShardIterator> shards = clusterService.operationRouting()
                .searchShards(clusterState, new String[]{ indexName }, null, null);

            ShardRouting selectedCopy = null;
            for (ShardIterator shardCopies : shards) {
                for (ShardRouting copy : shardCopies) {
                    if (copy.active() && copy.assignedToNode()) {
                        selectedCopy = copy;
                        break;
                    }
                }

                if (selectedCopy != null) {
                    String nodeId = selectedCopy.currentNodeId();
                    List<ShardId> shardGroup = shardsByNodeId.computeIfAbsent(nodeId, key -> new ArrayList<>());
                    shardGroup.add(selectedCopy.shardId());
                    if (includeAllShards == false) {
                        // We only need one shard for this index, so stop early
                        break;
                    }
                } else if (includeAllShards) {
                    // We need all index shards but couldn't find a shard copy
                    Exception e = new NoShardAvailableActionException(shardCopies.shardId(),
                        LoggerMessageFormat.format("No shard available for index [{}]", indexName));
                    indexFailures.collect(e, indexName);
                }
            }

            if (includeAllShards == false && selectedCopy == null) {
                // We only needed one shard for the index but couldn't find any shard copy
                Exception e = new NoShardAvailableActionException(null,
                    LoggerMessageFormat.format("No shard available for index [{}]", indexName));
                indexFailures.collect(e, indexName);
            }
        }
        return shardsByNodeId;
    }

    private FieldCapabilitiesResponse merge(
        Map<String, FieldCapabilitiesIndexResponse> indexResponses,
        boolean includeUnmapped,
        List<FieldCapabilitiesFailure> failures
    ) {
        String[] indices = indexResponses.keySet().stream().sorted().toArray(String[]::new);
        final Map<String, Map<String, FieldCapabilities.Builder>> responseMapBuilder = new HashMap<>();
        for (FieldCapabilitiesIndexResponse response : indexResponses.values()) {
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

    /**
     * Collects failures from all the individual index requests, then builds a failure list grouped by the underlying cause.
     *
     * This collector can contain a failure for an index even if one of its shards was successful. When building the final
     * list, these failures will be skipped because they have no affect on the final response.
     */
    private static final class FailureCollector {
        private final Map<String, Exception> failuresByIndex = Collections.synchronizedMap(new HashMap<>());

        List<FieldCapabilitiesFailure> build(Set<String> successfulIndices) {
            Map<Tuple<String, String>, FieldCapabilitiesFailure> indexFailures = Collections.synchronizedMap(new HashMap<>());
            for (Map.Entry<String, Exception> failure : failuresByIndex.entrySet()) {
                String index = failure.getKey();
                Exception e = failure.getValue();

                if (successfulIndices.contains(index) == false) {
                    // we deduplicate exceptions on the underlying causes message and classname
                    // we unwrap the cause to e.g. group RemoteTransportExceptions coming from different nodes if the cause is the same
                    Throwable cause = ExceptionsHelper.unwrapCause(e);
                    Tuple<String, String> groupingKey = new Tuple<>(cause.getMessage(), cause.getClass().getName());
                    indexFailures.compute(groupingKey,
                        (k, v) -> v == null ? new FieldCapabilitiesFailure(new String[]{index}, e) : v.addIndex(index));
                }
            }
            return new ArrayList<>(indexFailures.values());
        }

        void collect(Exception e, String index) {
            failuresByIndex.putIfAbsent(index, e);
        }

        void collect(Exception e, String[] indices) {
            for (String index : indices) {
                collect(e, index);
            }
        }

        void collectRemoteException(Exception ex, String clusterAlias, String[] remoteIndices) {
            for (String failedIndex : remoteIndices) {
                collect(ex, RemoteClusterAware.buildRemoteIndexName(clusterAlias, failedIndex));
            }
        }

        boolean isEmpty() {
            return failuresByIndex.isEmpty();
        }
    }

    private class NodeTransportHandler implements TransportRequestHandler<FieldCapabilitiesNodeRequest> {
        @Override
        public void messageReceived(final FieldCapabilitiesNodeRequest request,
                                    final TransportChannel channel,
                                    Task task) throws Exception {
            ActionListener<FieldCapabilitiesNodeResponse> listener = new ChannelActionListener<>(channel, ACTION_NODE_NAME, request);

            List<FieldCapabilitiesIndexResponse> indexResponses = new ArrayList<>();
            List<FieldCapabilitiesFailure> indexFailures = new ArrayList<>();

            for (ShardId shardId : request.shardIds()) {
                FieldCapabilitiesIndexRequest indexRequest = new FieldCapabilitiesIndexRequest(request.fields(), shardId.getIndexName(),
                    request.originalIndices(), request.indexFilter(), request.nowInMillis(), request.runtimeFields());
                indexRequest.shardId(shardId);
                try {
                    FieldCapabilitiesIndexResponse indexResponse = fieldCapabilitiesFetcher.fetch(indexRequest);
                    indexResponses.add(indexResponse);
                } catch (Exception e) {
                    FieldCapabilitiesFailure failure = new FieldCapabilitiesFailure(new String[] {shardId.getIndexName()}, e);
                    indexFailures.add(failure);
                }
            }

            FieldCapabilitiesNodeResponse response = new FieldCapabilitiesNodeResponse(request.indices(),
                indexResponses, indexFailures);
            listener.onResponse(response);
        }
    }
}
