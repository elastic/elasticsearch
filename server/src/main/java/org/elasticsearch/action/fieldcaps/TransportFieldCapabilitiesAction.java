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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TransportFieldCapabilitiesAction extends HandledTransportAction<FieldCapabilitiesRequest, FieldCapabilitiesResponse> {
    public static final String ACTION_NODE_NAME = FieldCapabilitiesAction.NAME + "[n]";
    public static final Logger LOGGER = LogManager.getLogger(TransportFieldCapabilitiesAction.class);

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

        transportService.registerRequestHandler(ACTION_NODE_NAME, ThreadPool.Names.MANAGEMENT,
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

        final AsyncResponseMerger merger = new AsyncResponseMerger(threadPool, concreteIndices.length + remoteClusterIndices.size(),
            request.isMergeResults(), request.includeUnmapped(), metadataFieldPred, listener);
        if (clusterState.nodes().getMinNodeVersion().onOrAfter(Version.V_7_16_0)) {
            NodeRequestDispatcher dispatcher = new NodeRequestDispatcher(
                clusterState, task, request, localIndices, nowInMillis, concreteIndices, merger);
            dispatcher.execute();
        } else {
            // fork this action to the management pool as it can fan out to a large number of child requests that get handled on SAME and
            // thus would all run on the current transport thread and block it for an unacceptable amount of time
            // (particularly with security enabled)
            if (concreteIndices.length > 0) {
                threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(ActionRunnable.wrap(listener, l -> {
                    for (String index : concreteIndices) {
                        final FieldCapabilitiesIndexRequest indexRequest = new FieldCapabilitiesIndexRequest(
                            request.fields(), index, localIndices, request.indexFilter(), nowInMillis, request.runtimeFields());
                        new TransportFieldCapabilitiesIndexAction.AsyncShardsAction(
                            transportService,
                            clusterService,
                            indexRequest,
                            ActionListener.wrap(merger::addIndexResponse, e -> merger.addIndexFailure(index, e))
                        ).start();
                    }
                }));
            }
        }

        // this is the cross cluster part of this API - we force the other cluster to not merge the results but instead
        // send us back all individual index results.
        for (Map.Entry<String, OriginalIndices> remoteIndices : remoteClusterIndices.entrySet()) {
            String clusterAlias = remoteIndices.getKey();
            OriginalIndices originalIndices = remoteIndices.getValue();
            Client remoteClusterClient = transportService.getRemoteClusterService().getRemoteClusterClient(threadPool, clusterAlias);
            FieldCapabilitiesRequest remoteRequest = prepareRemoteRequest(request, originalIndices, nowInMillis);
            remoteClusterClient.fieldCaps(remoteRequest, ActionListener.wrap(
                resp -> merger.addRemoteResponse(clusterAlias, resp),
                e -> merger.addRemoteFailure(clusterAlias, originalIndices.indices(), e)));
        }
    }

    private void checkIndexBlocks(ClusterState clusterState, String[] concreteIndices) {
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        for (String index : concreteIndices) {
            clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index);
        }
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

    private class NodeRequestDispatcher {
        private final Map<String, NodeSelector> indices;
        private final ClusterState clusterState;
        private final Task parentTask;
        private final FieldCapabilitiesRequest fieldCapsRequest;
        private final OriginalIndices originalIndices;
        private final long nowInMillis;
        private final boolean hasFilter;
        private final AsyncResponseMerger responseMerger;
        private int pendingRequests = 0;
        private int executionRound = 0;

        NodeRequestDispatcher(ClusterState clusterState, Task parentTask, FieldCapabilitiesRequest fieldCapsRequest,
                              OriginalIndices originalIndices, long nowInMillis, String[] indices,
                              AsyncResponseMerger responseMerger) {
            this.clusterState = clusterState;
            this.parentTask = parentTask;
            this.fieldCapsRequest = fieldCapsRequest;
            this.originalIndices = originalIndices;
            this.nowInMillis = nowInMillis;
            this.indices = new HashMap<>(indices.length);
            for (String index : indices) {
                final GroupShardsIterator<ShardIterator> shardIts =
                    clusterService.operationRouting().searchShards(clusterState, new String[]{index}, null, null, null, null);
                this.indices.put(index, new NodeSelector(shardIts, clusterState.nodes()));
            }
            this.hasFilter =
                fieldCapsRequest.indexFilter() != null && fieldCapsRequest.indexFilter() instanceof MatchAllQueryBuilder == false;
            this.responseMerger = responseMerger;
        }

        void execute() {
            assert Thread.holdsLock(this) == false;
            final Map<DiscoveryNode, List<ShardId>> nodeToShards = new HashMap<>();
            final List<Map.Entry<String, NodeSelector>> failedIndices = new ArrayList<>();
            synchronized (this) {
                final Iterator<Map.Entry<String, NodeSelector>> it = indices.entrySet().iterator();
                while (it.hasNext()) {
                    final Map.Entry<String, NodeSelector> e = it.next();
                    final NodeSelector nodeSelector = e.getValue();
                    final List<ShardRouting> shards;
                    if (executionRound == 0 || nodeSelector.lastFailure != null) {
                        shards = nodeSelector.nextTarget(hasFilter);
                    } else {
                        // If all previous shards don't match the filter, then we don't need
                        // to retry other copies as they won't match the filter anyway.
                        assert hasFilter;
                        assert nodeSelector.lastResponse != null;
                        shards = Collections.emptyList();
                    }
                    if (shards.isEmpty()) {
                        failedIndices.add(e);
                        it.remove(); // remove a failed index from the pending list
                    } else {
                        for (ShardRouting shard : shards) {
                            final DiscoveryNode node = clusterState.nodes().get(shard.currentNodeId());
                            assert node != null : "Must be filtered by NodeSelector";
                            nodeToShards.computeIfAbsent(node, n -> new ArrayList<>()).add(shard.shardId());
                        }
                    }
                }
                pendingRequests = nodeToShards.size();
            }
            // send requests
            for (Map.Entry<DiscoveryNode, List<ShardId>> e : nodeToShards.entrySet()) {
                final DiscoveryNode node = e.getKey();
                final List<ShardId> shardIds = e.getValue();
                final ActionListener<FieldCapabilitiesNodeResponse> listener =
                    ActionListener.wrap(this::onNodeResponse, failure -> onNodeFailure(shardIds, failure));
                final FieldCapabilitiesNodeRequest nodeRequest = new FieldCapabilitiesNodeRequest(
                    shardIds,
                    fieldCapsRequest.fields(),
                    originalIndices,
                    fieldCapsRequest.indexFilter(),
                    nowInMillis,
                    fieldCapsRequest.runtimeFields());
                LOGGER.debug("round {} sends field caps request to node {} for shardIds {}", executionRound, node, shardIds);
                transportService.sendChildRequest(node, ACTION_NODE_NAME, nodeRequest, parentTask, TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(listener, FieldCapabilitiesNodeResponse::new));
            }
            // report failed indices
            for (Map.Entry<String, NodeSelector> e : failedIndices) {
                final String index = e.getKey();
                final NodeSelector nodeSelector = e.getValue();
                if (nodeSelector.lastResponse != null) {
                    assert hasFilter;
                    assert nodeSelector.lastResponse.canMatch() == false;
                    responseMerger.addIndexResponse(nodeSelector.lastResponse);
                } else if (nodeSelector.lastFailure != null) {
                    responseMerger.addIndexFailure(index, nodeSelector.lastFailure);
                } else {
                    responseMerger.addIndexFailure(index, new ElasticsearchException("index [{}] has no active shard copy", index));
                }
            }
            executionRound++;
        }

        void onNodeResponse(FieldCapabilitiesNodeResponse nodeResponse) {
            assert Thread.holdsLock(this) == false;
            final boolean executeNext;
            final List<FieldCapabilitiesIndexResponse> matchedResponses = new ArrayList<>();
            synchronized (this) {
                for (FieldCapabilitiesIndexResponse indexResponse : nodeResponse.getIndexResponses()) {
                    if (indexResponse.canMatch()) {
                        final NodeSelector nodeSelector = indices.remove(indexResponse.getIndexName());
                        if (nodeSelector != null) {
                            matchedResponses.add(indexResponse);
                        }
                    } else {
                        final NodeSelector nodeSelector = indices.get(indexResponse.getIndexName());
                        if (nodeSelector != null) {
                            nodeSelector.lastResponse = indexResponse;
                        }
                    }
                }
                for (FieldCapabilitiesFailure failure : nodeResponse.getFailures()) {
                    for (String index : failure.getIndices()) {
                        final NodeSelector nodeSelector = indices.get(index);
                        if (nodeSelector != null) {
                            nodeSelector.lastFailure = failure.getException();
                        }
                    }
                }
                // Here we only retry after all pending requests have responded to avoid exploding network requests
                // when the cluster is unstable or overloaded as an eager retry approach can add more load to the cluster.
                pendingRequests--;
                executeNext = pendingRequests == 0;
            }
            for (FieldCapabilitiesIndexResponse indexResponse : matchedResponses) {
                responseMerger.addIndexResponse(indexResponse);
            }
            if (executeNext) {
                execute();
            }
        }

        void onNodeFailure(List<ShardId> shardIds, Exception e) {
            assert Thread.holdsLock(this) == false;
            final boolean executeNext;
            synchronized (this) {
                for (String index : shardIds.stream().map(ShardId::getIndexName).collect(Collectors.toSet())) {
                    final NodeSelector nodeSelector = indices.get(index);
                    if (nodeSelector != null) {
                        nodeSelector.lastFailure = ExceptionsHelper.useOrSuppress(nodeSelector.lastFailure, e);
                    }
                }
                pendingRequests--;
                executeNext = pendingRequests == 0;
            }
            if (executeNext) {
                execute();
            }
        }
    }

    private static class NodeSelector {
        private final Map<DiscoveryNode, List<ShardRouting>> nodeToShards = new HashMap<>();
        private Exception lastFailure;
        private FieldCapabilitiesIndexResponse lastResponse;

        NodeSelector(GroupShardsIterator<ShardIterator> shardsIt, DiscoveryNodes discoveryNodes) {
            for (ShardIterator shards : shardsIt) {
                for (ShardRouting shard : shards) {
                    final DiscoveryNode node = discoveryNodes.get(shard.currentNodeId());
                    if (node != null) {
                        nodeToShards.computeIfAbsent(node, n -> new ArrayList<>()).add(shard);
                    }
                }
            }
        }

        List<ShardRouting> nextTarget(boolean hasFilter) {
            if (nodeToShards.isEmpty()) {
                return Collections.emptyList();
            }
            final Iterator<Map.Entry<DiscoveryNode, List<ShardRouting>>> nodeIt = nodeToShards.entrySet().iterator();
            // If an index filter is specified, then we must reach out to all of an index's shards to check
            // if one of them can match. Otherwise, for efficiency we just reach out to one of its shards.
            if (hasFilter == false) {
                final Map.Entry<DiscoveryNode, List<ShardRouting>> node = nodeIt.next();
                assert node.getValue().isEmpty() == false : "Entry without shards should have been removed";
                nodeIt.remove();
                return node.getValue();
            }
            final Set<ShardId> selectedShardIds = new HashSet<>();
            final List<ShardRouting> selectedShards = new ArrayList<>();
            while (nodeIt.hasNext()) {
                final Map.Entry<DiscoveryNode, List<ShardRouting>> node = nodeIt.next();
                final Iterator<ShardRouting> shardIt = node.getValue().iterator();
                while (shardIt.hasNext()) {
                    final ShardRouting shardRouting = shardIt.next();
                    if (selectedShardIds.add(shardRouting.shardId())) {
                        selectedShards.add(shardRouting);
                        shardIt.remove();
                    }
                }
                if (node.getValue().isEmpty()) {
                    nodeIt.remove();
                }
            }
            return selectedShards;
        }
    }

    private class NodeTransportHandler implements TransportRequestHandler<FieldCapabilitiesNodeRequest> {
        @Override
        public void messageReceived(final FieldCapabilitiesNodeRequest request,
                                    final TransportChannel channel,
                                    Task task) throws Exception {
            final Map<String, FieldCapabilitiesIndexResponse> responses = new HashMap<>();
            final Map<String, Exception> failures = new HashMap<>();
            for (ShardId shardId : request.shardIds()) {
                final FieldCapabilitiesIndexResponse resp = responses.get(shardId.getIndexName());
                // If the request has an index filter, it may contain several shards belonging to the same
                // index. We make sure to skip over a shard if we already found a match for that index.
                if (resp == null || resp.canMatch() == false) {
                    FieldCapabilitiesIndexRequest indexRequest = new FieldCapabilitiesIndexRequest(request.fields(), shardId.getIndexName(),
                        request.originalIndices(), request.indexFilter(), request.nowInMillis(), request.runtimeFields());
                    indexRequest.shardId(shardId);
                    try {
                        FieldCapabilitiesIndexResponse indexResponse = fieldCapabilitiesFetcher.fetch(indexRequest);
                        if (indexResponse.canMatch()) {
                            failures.remove(shardId.getIndexName()); // do not report failures for a matched index
                        }
                        responses.put(shardId.getIndexName(), indexResponse);
                    } catch (Exception e) {
                        failures.compute(shardId.getIndexName(), (k, curr) -> ExceptionsHelper.useOrSuppress(curr, e));
                    }
                }
            }
            final ActionListener<FieldCapabilitiesNodeResponse> listener = new ChannelActionListener<>(channel, ACTION_NODE_NAME, request);
            final List<FieldCapabilitiesIndexResponse> indexResponses = new ArrayList<>(responses.values());
            final List<FieldCapabilitiesFailure> indexFailures = failures.entrySet().stream()
                .map(e -> new FieldCapabilitiesFailure(new String[]{e.getKey()}, e.getValue()))
                .collect(Collectors.toList());
            listener.onResponse(new FieldCapabilitiesNodeResponse(request.indices(), indexResponses, indexFailures));
        }
    }

    static final class AsyncResponseMerger {
        private final ThreadPool threadPool;
        private final CountDown counter;
        private final boolean mergeResults;
        private final boolean includeUnmapped;
        private final Predicate<String> metadataFieldPred;
        private final ActionListener<FieldCapabilitiesResponse> listener;
        private final Map<String, FieldCapabilitiesIndexResponse> indexResponses = ConcurrentCollections.newConcurrentMap();
        private final Map<String, Exception> indexFailures = ConcurrentCollections.newConcurrentMap();

        AsyncResponseMerger(ThreadPool threadPool, int expectedCount, boolean mergeResult, boolean includeUnmapped,
                            Predicate<String> metadataFieldPred, ActionListener<FieldCapabilitiesResponse> listener) {
            this.threadPool = threadPool;
            this.counter = new CountDown(expectedCount);
            this.mergeResults = mergeResult;
            this.includeUnmapped = includeUnmapped;
            this.metadataFieldPred = metadataFieldPred;
            this.listener = listener;
        }

        void addIndexResponse(FieldCapabilitiesIndexResponse indexResponse) {
            if (indexResponse.canMatch()) {
                indexResponses.put(indexResponse.getIndexName(), indexResponse);
            }
            countDown();
        }

        void addIndexFailure(String index, Exception e) {
            indexFailures.put(index, e);
            countDown();
        }

        void addRemoteResponse(String clusterAlias, FieldCapabilitiesResponse response) {
            for (FieldCapabilitiesIndexResponse resp : response.getIndexResponses()) {
                final String indexName = RemoteClusterAware.buildRemoteIndexName(clusterAlias, resp.getIndexName());
                final FieldCapabilitiesIndexResponse current =
                    indexResponses.put(indexName, new FieldCapabilitiesIndexResponse(indexName, resp.get(), resp.canMatch()));
                assert current == null : "index " + indexName + " already got response";
            }
            for (FieldCapabilitiesFailure failure : response.getFailures()) {
                for (String index : failure.getIndices()) {
                    final String indexName = RemoteClusterAware.buildRemoteIndexName(clusterAlias, index);
                    indexFailures.put(indexName, failure.getException());
                }
            }
            countDown();
        }

        void addRemoteFailure(String clusterAlias, String[] indices, Exception e) {
            for (String index : indices) {
                final String indexName = RemoteClusterAware.buildRemoteIndexName(clusterAlias, index);
                indexFailures.put(indexName, e);
            }
            countDown();
        }

        private void countDown() {
            if (counter.countDown()) {
                final List<FieldCapabilitiesFailure> failures = groupFailures();
                if (indexResponses.size() > 0) {
                    if (mergeResults) {
                        // fork off to the management pool for merging the responses as the operation can run for longer than is acceptable
                        // on a transport thread in case of large numbers of indices and/or fields
                        threadPool.executor(ThreadPool.Names.MANAGEMENT).submit(
                            ActionRunnable.supply(listener, () -> merge(indexResponses, new ArrayList<>(failures)))
                        );
                    } else {
                        listener.onResponse(new FieldCapabilitiesResponse(
                            new ArrayList<>(indexResponses.values()), new ArrayList<>(failures)));
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
        }

        private List<FieldCapabilitiesFailure> groupFailures() {
            Map<Tuple<String, String>, FieldCapabilitiesFailure> groupedFailures = new HashMap<>();
            for (Map.Entry<String, Exception> failure : indexFailures.entrySet()) {
                String index = failure.getKey();
                Exception e = failure.getValue();
                if (indexResponses.containsKey(index) == false) {
                    // we deduplicate exceptions on the underlying causes message and classname
                    // we unwrap the cause to e.g. group RemoteTransportExceptions coming from different nodes if the cause is the same
                    Throwable cause = ExceptionsHelper.unwrapCause(e);
                    Tuple<String, String> groupingKey = new Tuple<>(cause.getMessage(), cause.getClass().getName());
                    groupedFailures.compute(groupingKey,
                        (k, v) -> v == null ? new FieldCapabilitiesFailure(new String[]{index}, e) : v.addIndex(index));
                }
            }
            return new ArrayList<>(groupedFailures.values());
        }

        private FieldCapabilitiesResponse merge(Map<String, FieldCapabilitiesIndexResponse> indexResponses,
                                                List<FieldCapabilitiesFailure> failures) {
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
    }
}
