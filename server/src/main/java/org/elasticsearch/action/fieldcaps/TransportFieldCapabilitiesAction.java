/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.action.search.TransportSearchHelper.checkCCSVersionCompatibility;

public class TransportFieldCapabilitiesAction extends HandledTransportAction<FieldCapabilitiesRequest, FieldCapabilitiesResponse> {
    public static final String ACTION_NODE_NAME = FieldCapabilitiesAction.NAME + "[n]";

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private final Predicate<String> metadataFieldPred;
    private final IndicesService indicesService;
    private final boolean ccsCheckCompatibility;

    @Inject
    public TransportFieldCapabilitiesAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndicesService indicesService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(FieldCapabilitiesAction.NAME, transportService, actionFilters, FieldCapabilitiesRequest::new);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indicesService = indicesService;
        final Set<String> metadataFields = indicesService.getAllMetadataFields();
        this.metadataFieldPred = metadataFields::contains;
        transportService.registerRequestHandler(
            ACTION_NODE_NAME,
            ThreadPool.Names.SEARCH_COORDINATION,
            FieldCapabilitiesNodeRequest::new,
            new NodeTransportHandler()
        );
        this.ccsCheckCompatibility = SearchService.CCS_VERSION_CHECK_SETTING.get(clusterService.getSettings());
    }

    @Override
    protected void doExecute(Task task, FieldCapabilitiesRequest request, final ActionListener<FieldCapabilitiesResponse> listener) {
        if (ccsCheckCompatibility) {
            checkCCSVersionCompatibility(request);
        }
        // retrieve the initial timestamp in case the action is a cross cluster search
        long nowInMillis = request.nowInMillis() == null ? System.currentTimeMillis() : request.nowInMillis();
        final ClusterState clusterState = clusterService.state();
        final Map<String, OriginalIndices> remoteClusterIndices = transportService.getRemoteClusterService()
            .groupIndices(request.indicesOptions(), request.indices());
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
        // This map is used to share the index response for indices which have the same index mapping hash to reduce the memory usage.
        final Map<String, Map<String, IndexFieldCapabilities>> indexMappingHashToResponses = Collections.synchronizedMap(new HashMap<>());
        final Consumer<FieldCapabilitiesIndexResponse> handleIndexResponse = resp -> {
            if (resp.canMatch() && resp.getIndexMappingHash() != null) {
                Map<String, IndexFieldCapabilities> curr = indexMappingHashToResponses.putIfAbsent(resp.getIndexMappingHash(), resp.get());
                if (curr != null) {
                    resp = new FieldCapabilitiesIndexResponse(resp.getIndexName(), resp.getIndexMappingHash(), curr, true);
                }
            }
            indexResponses.putIfAbsent(resp.getIndexName(), resp);
        };
        final FailureCollector indexFailures = new FailureCollector();
        // One for each cluster including the local cluster
        final CountDown completionCounter = new CountDown(1 + remoteClusterIndices.size());
        final Runnable countDown = createResponseMerger(request, completionCounter, indexResponses, indexFailures, listener);
        final RequestDispatcher requestDispatcher = new RequestDispatcher(
            clusterService,
            transportService,
            task,
            request,
            localIndices,
            nowInMillis,
            concreteIndices,
            threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
            handleIndexResponse,
            indexFailures::collect,
            countDown
        );
        requestDispatcher.execute();

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
                    handleIndexResponse.accept(
                        new FieldCapabilitiesIndexResponse(indexName, resp.getIndexMappingHash(), resp.get(), resp.canMatch())
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
            }));
        }
    }

    private static void checkIndexBlocks(ClusterState clusterState, String[] concreteIndices) {
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        for (String index : concreteIndices) {
            clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index);
        }
    }

    private Runnable createResponseMerger(
        FieldCapabilitiesRequest request,
        CountDown completionCounter,
        Map<String, FieldCapabilitiesIndexResponse> indexResponses,
        FailureCollector indexFailures,
        ActionListener<FieldCapabilitiesResponse> listener
    ) {
        return () -> {
            if (completionCounter.countDown()) {
                List<FieldCapabilitiesFailure> failures = indexFailures.build(indexResponses.keySet());
                if (indexResponses.size() > 0) {
                    if (request.isMergeResults()) {
                        // fork off to the management pool for merging the responses as the operation can run for longer than is acceptable
                        // on a transport thread in case of large numbers of indices and/or fields
                        threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION)
                            .submit(ActionRunnable.supply(listener, () -> merge(indexResponses, request, new ArrayList<>(failures))));
                    } else {
                        listener.onResponse(
                            new FieldCapabilitiesResponse(new ArrayList<>(indexResponses.values()), new ArrayList<>(failures))
                        );
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

    private static FieldCapabilitiesRequest prepareRemoteRequest(
        FieldCapabilitiesRequest request,
        OriginalIndices originalIndices,
        long nowInMillis
    ) {
        FieldCapabilitiesRequest remoteRequest = new FieldCapabilitiesRequest();
        remoteRequest.setMergeResults(false); // we need to merge on this node
        remoteRequest.indicesOptions(originalIndices.indicesOptions());
        remoteRequest.indices(originalIndices.indices());
        remoteRequest.fields(request.fields());
        remoteRequest.filters(request.filters());
        remoteRequest.types(request.types());
        remoteRequest.runtimeFields(request.runtimeFields());
        remoteRequest.indexFilter(request.indexFilter());
        remoteRequest.nowInMillis(nowInMillis);
        return remoteRequest;
    }

    private FieldCapabilitiesResponse merge(
        Map<String, FieldCapabilitiesIndexResponse> indexResponsesMap,
        FieldCapabilitiesRequest request,
        List<FieldCapabilitiesFailure> failures
    ) {
        final List<FieldCapabilitiesIndexResponse> indexResponses = indexResponsesMap.values()
            .stream()
            .sorted(Comparator.comparing(FieldCapabilitiesIndexResponse::getIndexName))
            .toList();
        final String[] indices = indexResponses.stream().map(FieldCapabilitiesIndexResponse::getIndexName).toArray(String[]::new);
        final Map<String, Map<String, FieldCapabilities.Builder>> responseMapBuilder = new HashMap<>();
        for (FieldCapabilitiesIndexResponse response : indexResponses) {
            innerMerge(responseMapBuilder, request, response);
        }
        final Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();
        for (Map.Entry<String, Map<String, FieldCapabilities.Builder>> entry : responseMapBuilder.entrySet()) {
            final Map<String, FieldCapabilities.Builder> typeMapBuilder = entry.getValue();
            if (request.includeUnmapped()) {
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

    private static void addUnmappedFields(String[] indices, String field, Map<String, FieldCapabilities.Builder> typeMap) {
        final Set<String> mappedIndices = new HashSet<>();
        typeMap.values().forEach(t -> t.getIndices(mappedIndices));
        if (mappedIndices.size() != indices.length) {
            final FieldCapabilities.Builder unmapped = new FieldCapabilities.Builder(field, "unmapped");
            for (String index : indices) {
                if (mappedIndices.contains(index) == false) {
                    unmapped.add(index, false, false, false, false, null, Collections.emptyMap());
                }
            }
            typeMap.put("unmapped", unmapped);
        }
    }

    private void innerMerge(
        Map<String, Map<String, FieldCapabilities.Builder>> responseMapBuilder,
        FieldCapabilitiesRequest request,
        FieldCapabilitiesIndexResponse response
    ) {
        Map<String, IndexFieldCapabilities> fields = ResponseRewriter.rewriteOldResponses(
            response.getOriginVersion(),
            response.get(),
            request.filters(),
            request.types(),
            metadataFieldPred
        );
        for (Map.Entry<String, IndexFieldCapabilities> entry : fields.entrySet()) {
            final String field = entry.getKey();
            final IndexFieldCapabilities fieldCap = entry.getValue();
            Map<String, FieldCapabilities.Builder> typeMap = responseMapBuilder.computeIfAbsent(field, f -> new HashMap<>());
            FieldCapabilities.Builder builder = typeMap.computeIfAbsent(
                fieldCap.getType(),
                key -> new FieldCapabilities.Builder(field, key)
            );
            builder.add(
                response.getIndexName(),
                fieldCap.isMetadatafield(),
                fieldCap.isSearchable(),
                fieldCap.isAggregatable(),
                fieldCap.isDimension(),
                fieldCap.getMetricType(),
                fieldCap.meta()
            );
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
                    indexFailures.compute(
                        groupingKey,
                        (k, v) -> v == null ? new FieldCapabilitiesFailure(new String[] { index }, e) : v.addIndex(index)
                    );
                }
            }
            return new ArrayList<>(indexFailures.values());
        }

        void collect(String index, Exception e) {
            failuresByIndex.putIfAbsent(index, e);
        }

        void collectRemoteException(Exception ex, String clusterAlias, String[] remoteIndices) {
            for (String failedIndex : remoteIndices) {
                collect(RemoteClusterAware.buildRemoteIndexName(clusterAlias, failedIndex), ex);
            }
        }

        boolean isEmpty() {
            return failuresByIndex.isEmpty();
        }
    }

    private class NodeTransportHandler implements TransportRequestHandler<FieldCapabilitiesNodeRequest> {
        @Override
        public void messageReceived(FieldCapabilitiesNodeRequest request, TransportChannel channel, Task task) throws Exception {
            final ActionListener<FieldCapabilitiesNodeResponse> listener = new ChannelActionListener<>(channel, ACTION_NODE_NAME, request);
            ActionListener.completeWith(listener, () -> {
                final List<FieldCapabilitiesIndexResponse> allResponses = new ArrayList<>();
                final Map<ShardId, Exception> allFailures = new HashMap<>();
                final Set<ShardId> allUnmatchedShardIds = new HashSet<>();
                // If the request has an index filter, it may contain several shards belonging to the same
                // index. We make sure to skip over a shard if we already found a match for that index.
                final Map<String, List<ShardId>> groupedShardIds = request.shardIds()
                    .stream()
                    .collect(Collectors.groupingBy(ShardId::getIndexName));
                final FieldCapabilitiesFetcher fetcher = new FieldCapabilitiesFetcher(indicesService);
                for (List<ShardId> shardIds : groupedShardIds.values()) {
                    final Map<ShardId, Exception> failures = new HashMap<>();
                    final Set<ShardId> unmatched = new HashSet<>();
                    for (ShardId shardId : shardIds) {
                        try {
                            final FieldCapabilitiesIndexResponse response = fetcher.fetch(
                                shardId,
                                request.fields(),
                                request.filters(),
                                request.allowedTypes(),
                                request.indexFilter(),
                                request.nowInMillis(),
                                request.runtimeFields()
                            );
                            if (response.canMatch()) {
                                unmatched.clear();
                                failures.clear();
                                allResponses.add(response);
                                break;
                            } else {
                                unmatched.add(shardId);
                            }
                        } catch (Exception e) {
                            failures.put(shardId, e);
                        }
                    }
                    allUnmatchedShardIds.addAll(unmatched);
                    allFailures.putAll(failures);
                }
                return new FieldCapabilitiesNodeResponse(allResponses, allFailures, allUnmatchedShardIds);
            });
        }
    }
}
