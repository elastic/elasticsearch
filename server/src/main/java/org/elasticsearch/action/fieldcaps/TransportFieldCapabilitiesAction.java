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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Tuple;
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
    static final String ACTION_SHARD_NAME = FieldCapabilitiesAction.NAME + "[index][s]";
    private static final Logger logger = LogManager.getLogger(TransportFieldCapabilitiesAction.class);

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
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
        final Set<String> metadataFields = indicesService.getAllMetadataFields();
        this.metadataFieldPred = metadataFields::contains;

        transportService.registerRequestHandler(ACTION_SHARD_NAME, ThreadPool.Names.SAME,
                FieldCapabilitiesIndexRequest::new, new ShardTransportHandler(indicesService));
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

        checkIndexBlocks(clusterState, concreteIndices);

        final int totalNumRequest = concreteIndices.length + remoteClusterIndices.size();
        if (totalNumRequest == 0) {
            listener.onResponse(new FieldCapabilitiesResponse(new String[0], Collections.emptyMap()));
            return;
        }

        final List<FieldCapabilitiesIndexResponse> indexResponses = Collections.synchronizedList(new ArrayList<>());
        final FailureCollector indexFailures = new FailureCollector();

        final Runnable countDown = createResponseMerger(request, totalNumRequest, indexResponses, indexFailures, listener);

        if (concreteIndices.length > 0) {
            // fork this action to the management pool as it can fan out to a large number of child requests that get handled on SAME and
            // thus would all run on the current transport thread and block it for an unacceptable amount of time
            // (particularly with security enabled)
            threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(ActionRunnable.wrap(listener, l -> {
                for (String index : concreteIndices) {
                    new AsyncFieldCapabilitiesShardsAction(
                        transportService,
                        clusterService,
                        prepareLocalIndexRequest(request, index, localIndices, nowInMillis),
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
            FieldCapabilitiesRequest remoteRequest = prepareRemoteRequest(request, originalIndices, nowInMillis);
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

    private void checkIndexBlocks(ClusterState clusterState, String[] concreteIndices) {
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        for (String index : concreteIndices) {
            clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index);
        }
    }

    private Runnable createResponseMerger(FieldCapabilitiesRequest request,
                                          int totalNumRequests,
                                          List<FieldCapabilitiesIndexResponse> indexResponses,
                                          FailureCollector indexFailures,
                                          ActionListener<FieldCapabilitiesResponse> listener) {
        final CountDown completionCounter = new CountDown(totalNumRequests);
        return () -> {
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
    }

    private FieldCapabilitiesIndexRequest prepareLocalIndexRequest(FieldCapabilitiesRequest request,
                                                                   String index,
                                                                   OriginalIndices originalIndices,
                                                                   long nowInMillis) {
        return new FieldCapabilitiesIndexRequest(request.fields(), index, originalIndices,
            request.indexFilter(), nowInMillis, request.runtimeFields());
    }

    private FieldCapabilitiesRequest prepareRemoteRequest(FieldCapabilitiesRequest request,
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

    private class ShardTransportHandler implements TransportRequestHandler<FieldCapabilitiesIndexRequest> {
        private final FieldCapabilitiesFetcher fetcher;

        private ShardTransportHandler(IndicesService indicesService) {
            this.fetcher = new FieldCapabilitiesFetcher(indicesService);
        }

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
                resp = fetcher.fetch(request);
            } catch (Exception exc) {
                listener.onFailure(exc);
                return;
            }
            listener.onResponse(resp);
        }
    }
}
