/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.support.AbstractThreadedActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.action.search.TransportSearchHelper.checkCCSVersionCompatibility;

public class TransportFieldCapabilitiesAction extends HandledTransportAction<FieldCapabilitiesRequest, FieldCapabilitiesResponse> {
    public static final String NAME = "indices:data/read/field_caps";
    public static final ActionType<FieldCapabilitiesResponse> TYPE = new ActionType<>(NAME);
    public static final RemoteClusterActionType<FieldCapabilitiesResponse> REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        FieldCapabilitiesResponse::new
    );
    public static final String ACTION_NODE_NAME = NAME + "[n]";
    public static final Logger LOGGER = LogManager.getLogger(TransportFieldCapabilitiesAction.class);

    private final Executor searchCoordinationExecutor;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private final IndicesService indicesService;
    private final boolean ccsCheckCompatibility;

    @Inject
    public TransportFieldCapabilitiesAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndicesService indicesService,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(NAME, transportService, actionFilters, FieldCapabilitiesRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.searchCoordinationExecutor = threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indicesService = indicesService;
        transportService.registerRequestHandler(
            ACTION_NODE_NAME,
            this.searchCoordinationExecutor,
            FieldCapabilitiesNodeRequest::new,
            new NodeTransportHandler()
        );
        this.ccsCheckCompatibility = SearchService.CCS_VERSION_CHECK_SETTING.get(clusterService.getSettings());
    }

    @Override
    protected void doExecute(Task task, FieldCapabilitiesRequest request, final ActionListener<FieldCapabilitiesResponse> listener) {
        executeRequest(
            task,
            request,
            (remoteClient, remoteRequest, remoteListener) -> remoteClient.execute(REMOTE_TYPE, remoteRequest, remoteListener),
            listener
        );
    }

    public void executeRequest(
        Task task,
        FieldCapabilitiesRequest request,
        RemoteRequestExecutor remoteRequestExecutor,
        ActionListener<FieldCapabilitiesResponse> listener
    ) {
        // workaround for https://github.com/elastic/elasticsearch/issues/97916 - TODO remove this when we can
        searchCoordinationExecutor.execute(ActionRunnable.wrap(listener, l -> doExecuteForked(task, request, remoteRequestExecutor, l)));
    }

    private void doExecuteForked(
        Task task,
        FieldCapabilitiesRequest request,
        RemoteRequestExecutor remoteRequestExecutor,
        ActionListener<FieldCapabilitiesResponse> listener
    ) {
        if (ccsCheckCompatibility) {
            checkCCSVersionCompatibility(request);
        }
        final Executor singleThreadedExecutor = buildSingleThreadedExecutor();
        assert task instanceof CancellableTask;
        final CancellableTask fieldCapTask = (CancellableTask) task;
        // retrieve the initial timestamp in case the action is a cross cluster search
        long nowInMillis = request.nowInMillis() == null ? System.currentTimeMillis() : request.nowInMillis();
        final ProjectState projectState = projectResolver.getProjectState(clusterService.state());
        final Map<String, OriginalIndices> remoteClusterIndices = transportService.getRemoteClusterService()
            .groupIndices(request.indicesOptions(), request.indices());
        final OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        final String[] concreteIndices;
        if (localIndices == null) {
            // in the case we have one or more remote indices but no local we don't expand to all local indices and just do remote indices
            concreteIndices = Strings.EMPTY_ARRAY;
        } else {
            concreteIndices = indexNameExpressionResolver.concreteIndexNames(projectState.metadata(), localIndices);
        }

        if (concreteIndices.length == 0 && remoteClusterIndices.isEmpty()) {
            listener.onResponse(new FieldCapabilitiesResponse(new String[0], Collections.emptyMap()));
            return;
        }

        checkIndexBlocks(projectState, concreteIndices);
        final FailureCollector indexFailures = new FailureCollector();
        final Map<String, FieldCapabilitiesIndexResponse> indexResponses = new HashMap<>();
        // This map is used to share the index response for indices which have the same index mapping hash to reduce the memory usage.
        final Map<String, FieldCapabilitiesIndexResponse> indexMappingHashToResponses = new HashMap<>();
        final Runnable releaseResourcesOnCancel = () -> {
            LOGGER.trace("clear index responses on cancellation");
            indexFailures.clear();
            indexResponses.clear();
            indexMappingHashToResponses.clear();
        };
        final Consumer<FieldCapabilitiesIndexResponse> handleIndexResponse = resp -> {
            if (fieldCapTask.isCancelled()) {
                releaseResourcesOnCancel.run();
                return;
            }
            if (resp.canMatch() && resp.getIndexMappingHash() != null) {
                FieldCapabilitiesIndexResponse curr = indexMappingHashToResponses.putIfAbsent(resp.getIndexMappingHash(), resp);
                if (curr != null) {
                    resp = new FieldCapabilitiesIndexResponse(
                        resp.getIndexName(),
                        curr.getIndexMappingHash(),
                        curr.get(),
                        true,
                        curr.getIndexMode()
                    );
                }
            }
            if (request.includeEmptyFields()) {
                indexResponses.putIfAbsent(resp.getIndexName(), resp);
            } else {
                indexResponses.merge(resp.getIndexName(), resp, (a, b) -> {
                    if (a.get().equals(b.get())) {
                        return a;
                    }
                    Map<String, IndexFieldCapabilities> mergedCaps = new HashMap<>(a.get());
                    mergedCaps.putAll(b.get());
                    return new FieldCapabilitiesIndexResponse(
                        a.getIndexName(),
                        a.getIndexMappingHash(),
                        mergedCaps,
                        true,
                        a.getIndexMode()
                    );
                });
            }
            if (fieldCapTask.isCancelled()) {
                releaseResourcesOnCancel.run();
            }
        };
        final BiConsumer<String, Exception> handleIndexFailure = (index, error) -> {
            if (fieldCapTask.isCancelled()) {
                releaseResourcesOnCancel.run();
                return;
            }
            indexFailures.collect(index, error);
            if (fieldCapTask.isCancelled()) {
                releaseResourcesOnCancel.run();
            }
        };
        final var finishedOrCancelled = new AtomicBoolean();
        fieldCapTask.addListener(() -> {
            if (finishedOrCancelled.compareAndSet(false, true)) {
                singleThreadedExecutor.execute(releaseResourcesOnCancel);
                LOGGER.trace("clear index responses on cancellation submitted");
            }
        });
        try (RefCountingRunnable refs = new RefCountingRunnable(() -> {
            finishedOrCancelled.set(true);
            if (fieldCapTask.notifyIfCancelled(listener)) {
                releaseResourcesOnCancel.run();
            } else {
                mergeIndexResponses(request, fieldCapTask, indexResponses, indexFailures, listener);
            }
        })) {
            // local cluster
            final RequestDispatcher requestDispatcher = new RequestDispatcher(
                clusterService,
                transportService,
                projectResolver,
                indicesService.getCoordinatorRewriteContextProvider(() -> nowInMillis),
                task,
                request,
                localIndices,
                nowInMillis,
                concreteIndices,
                singleThreadedExecutor,
                handleIndexResponse,
                handleIndexFailure,
                refs.acquire()::close
            );
            requestDispatcher.execute();

            // this is the cross cluster part of this API - we force the other cluster to not merge the results but instead
            // send us back all individual index results.
            for (Map.Entry<String, OriginalIndices> remoteIndices : remoteClusterIndices.entrySet()) {
                String clusterAlias = remoteIndices.getKey();
                OriginalIndices originalIndices = remoteIndices.getValue();
                var remoteClusterClient = transportService.getRemoteClusterService()
                    .getRemoteClusterClient(
                        clusterAlias,
                        singleThreadedExecutor,
                        RemoteClusterService.DisconnectedStrategy.RECONNECT_UNLESS_SKIP_UNAVAILABLE
                    );
                FieldCapabilitiesRequest remoteRequest = prepareRemoteRequest(clusterAlias, request, originalIndices, nowInMillis);
                ActionListener<FieldCapabilitiesResponse> remoteListener = ActionListener.wrap(response -> {
                    for (FieldCapabilitiesIndexResponse resp : response.getIndexResponses()) {
                        String indexName = RemoteClusterAware.buildRemoteIndexName(clusterAlias, resp.getIndexName());
                        handleIndexResponse.accept(
                            new FieldCapabilitiesIndexResponse(
                                indexName,
                                resp.getIndexMappingHash(),
                                resp.get(),
                                resp.canMatch(),
                                resp.getIndexMode()
                            )
                        );
                    }
                    for (FieldCapabilitiesFailure failure : response.getFailures()) {
                        Exception ex = failure.getException();
                        for (String index : failure.getIndices()) {
                            handleIndexFailure.accept(RemoteClusterAware.buildRemoteIndexName(clusterAlias, index), ex);
                        }
                    }
                }, ex -> {
                    for (String index : originalIndices.indices()) {
                        handleIndexFailure.accept(RemoteClusterAware.buildRemoteIndexName(clusterAlias, index), ex);
                    }
                });
                remoteRequestExecutor.executeRemoteRequest(
                    remoteClusterClient,
                    remoteRequest,
                    // The underlying transport service may call onFailure with a thread pool other than search_coordinator.
                    // This fork is a workaround to ensure that the merging of field-caps always occurs on the search_coordinator.
                    // TODO: remove this workaround after we fixed https://github.com/elastic/elasticsearch/issues/107439
                    new ForkingOnFailureActionListener<>(
                        singleThreadedExecutor,
                        true,
                        ActionListener.releaseAfter(remoteListener, refs.acquire())
                    )
                );
            }
        }
    }

    private Executor buildSingleThreadedExecutor() {
        final ThrottledTaskRunner throttledTaskRunner = new ThrottledTaskRunner("field_caps", 1, searchCoordinationExecutor);
        return r -> throttledTaskRunner.enqueueTask(new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                try (releasable) {
                    r.run();
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (r instanceof AbstractRunnable abstractRunnable) {
                    abstractRunnable.onFailure(e);
                } else {
                    // should be impossible, we should always submit an AbstractRunnable
                    logger.error("unexpected failure running " + r, e);
                    assert false : new AssertionError("unexpected failure running " + r, e);
                }
            }
        });
    }

    public interface RemoteRequestExecutor {
        void executeRemoteRequest(
            RemoteClusterClient remoteClient,
            FieldCapabilitiesRequest remoteRequest,
            ActionListener<FieldCapabilitiesResponse> remoteListener
        );
    }

    private static void checkIndexBlocks(ProjectState projectState, String[] concreteIndices) {
        var blocks = projectState.blocks();
        if (blocks.global().isEmpty() && blocks.indices(projectState.projectId()).isEmpty()) {
            // short circuit optimization because block check below is relatively expensive for many indices
            return;
        }
        blocks.globalBlockedRaiseException(ClusterBlockLevel.READ);
        for (String index : concreteIndices) {
            blocks.indexBlockedRaiseException(projectState.projectId(), ClusterBlockLevel.READ, index);
        }
    }

    private static void mergeIndexResponses(
        FieldCapabilitiesRequest request,
        CancellableTask task,
        Map<String, FieldCapabilitiesIndexResponse> indexResponses,
        FailureCollector indexFailures,
        ActionListener<FieldCapabilitiesResponse> listener
    ) {
        List<FieldCapabilitiesFailure> failures = indexFailures.build(indexResponses.keySet());
        if (indexResponses.size() > 0) {
            if (request.isMergeResults()) {
                ActionListener.completeWith(listener, () -> merge(indexResponses, task, request, failures));
            } else {
                listener.onResponse(new FieldCapabilitiesResponse(new ArrayList<>(indexResponses.values()), failures));
            }
        } else {
            // we have no responses at all, maybe because of errors
            if (indexFailures.isEmpty() == false) {
                // throw back the first exception
                listener.onFailure(failures.get(0).getException());
            } else {
                listener.onResponse(new FieldCapabilitiesResponse(Collections.emptyList(), Collections.emptyList()));
            }
        }
    }

    private static FieldCapabilitiesRequest prepareRemoteRequest(
        String clusterAlias,
        FieldCapabilitiesRequest request,
        OriginalIndices originalIndices,
        long nowInMillis
    ) {
        FieldCapabilitiesRequest remoteRequest = new FieldCapabilitiesRequest();
        remoteRequest.clusterAlias(clusterAlias);
        remoteRequest.setMergeResults(false); // we need to merge on this node
        remoteRequest.indicesOptions(originalIndices.indicesOptions());
        remoteRequest.indices(originalIndices.indices());
        remoteRequest.fields(request.fields());
        remoteRequest.filters(request.filters());
        remoteRequest.types(request.types());
        remoteRequest.runtimeFields(request.runtimeFields());
        remoteRequest.indexFilter(request.indexFilter());
        remoteRequest.nowInMillis(nowInMillis);
        remoteRequest.includeEmptyFields(request.includeEmptyFields());
        return remoteRequest;
    }

    private static boolean hasSameMappingHash(FieldCapabilitiesIndexResponse r1, FieldCapabilitiesIndexResponse r2) {
        return r1.getIndexMappingHash() != null
            && r2.getIndexMappingHash() != null
            && r1.getIndexMappingHash().equals(r2.getIndexMappingHash());
    }

    private static FieldCapabilitiesResponse merge(
        Map<String, FieldCapabilitiesIndexResponse> indexResponsesMap,
        CancellableTask task,
        FieldCapabilitiesRequest request,
        List<FieldCapabilitiesFailure> failures
    ) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH_COORDINATION); // too expensive to run this on a transport worker
        task.ensureNotCancelled();
        final FieldCapabilitiesIndexResponse[] indexResponses = indexResponsesMap.values().toArray(new FieldCapabilitiesIndexResponse[0]);
        Arrays.sort(indexResponses, Comparator.comparing(FieldCapabilitiesIndexResponse::getIndexName));
        final String[] indices = Arrays.stream(indexResponses).map(FieldCapabilitiesIndexResponse::getIndexName).toArray(String[]::new);
        final Map<String, Map<String, FieldCapabilities.Builder>> responseMapBuilder = new HashMap<>();
        int lastPendingIndex = 0;
        for (int i = 1; i <= indexResponses.length; i++) {
            if (i == indexResponses.length || hasSameMappingHash(indexResponses[lastPendingIndex], indexResponses[i]) == false) {
                final String[] subIndices;
                if (lastPendingIndex == 0 && i == indexResponses.length) {
                    subIndices = indices;
                } else {
                    subIndices = ArrayUtil.copyOfSubArray(indices, lastPendingIndex, i);
                }
                innerMerge(subIndices, responseMapBuilder, request, indexResponses[lastPendingIndex]);
                lastPendingIndex = i;
            }
        }

        task.ensureNotCancelled();
        Map<String, Map<String, FieldCapabilities>> responseMap = Maps.newMapWithExpectedSize(responseMapBuilder.size());
        if (request.includeUnmapped()) {
            collectResponseMapIncludingUnmapped(indices, responseMapBuilder, responseMap);
        } else {
            collectResponseMap(responseMapBuilder, responseMap);
        }

        // The merge method is only called on the primary coordinator for cross-cluster field caps, so we
        // log relevant "5xx" errors that occurred in this 2xx response to ensure they are only logged once.
        // These failures have already been deduplicated, before this method was called.
        for (FieldCapabilitiesFailure failure : failures) {
            if (shouldLogException(failure.getException())) {
                LOGGER.warn(
                    "Field caps partial-results Exception for indices " + Arrays.toString(failure.getIndices()),
                    failure.getException()
                );
            }
        }
        return new FieldCapabilitiesResponse(indices, Collections.unmodifiableMap(responseMap), failures);
    }

    private static boolean shouldLogException(Exception e) {
        // ConnectTransportExceptions are thrown when a cluster marked with skip_unavailable=false are not available for searching
        // (Clusters marked with skip_unavailable=false return a different error that is considered a 4xx error.)
        // In such a case, the field-caps endpoint returns a 200 (unless all clusters failed).
        // To keep the logs from being too noisy, we choose not to log the ConnectTransportException here.
        return e instanceof org.elasticsearch.transport.ConnectTransportException == false
            && ExceptionsHelper.status(e).getStatus() >= 500
            && ExceptionsHelper.isNodeOrShardUnavailableTypeException(e) == false;
    }

    private static void collectResponseMapIncludingUnmapped(
        String[] indices,
        Map<String, Map<String, FieldCapabilities.Builder>> responseMapBuilder,
        Map<String, Map<String, FieldCapabilities>> responseMap
    ) {
        final Set<String> mappedScratch = new HashSet<>();
        for (Map.Entry<String, Map<String, FieldCapabilities.Builder>> entry : responseMapBuilder.entrySet()) {
            var typeMapBuilder = entry.getValue().entrySet();

            // do this directly, rather than using the builder, to save creating a whole lot of objects we don't need
            mappedScratch.clear();
            for (Map.Entry<String, FieldCapabilities.Builder> b : typeMapBuilder) {
                b.getValue().getIndices(mappedScratch);
            }
            var unmapped = getUnmappedFields(indices, entry.getKey(), mappedScratch);

            final int resSize = typeMapBuilder.size() + (unmapped == null ? 0 : 1);
            final Map<String, FieldCapabilities> res = capabilities(resSize, typeMapBuilder);
            if (unmapped != null) {
                res.put("unmapped", unmapped.apply(resSize > 1));
            }
            responseMap.put(entry.getKey(), Collections.unmodifiableMap(res));
        }
    }

    private static void collectResponseMap(
        Map<String, Map<String, FieldCapabilities.Builder>> responseMapBuilder,
        Map<String, Map<String, FieldCapabilities>> responseMap
    ) {
        for (Map.Entry<String, Map<String, FieldCapabilities.Builder>> entry : responseMapBuilder.entrySet()) {
            var typeMapBuilder = entry.getValue().entrySet();
            responseMap.put(entry.getKey(), Collections.unmodifiableMap(capabilities(typeMapBuilder.size(), typeMapBuilder)));
        }
    }

    private static Map<String, FieldCapabilities> capabilities(int resSize, Set<Map.Entry<String, FieldCapabilities.Builder>> builders) {
        boolean multiTypes = resSize > 1;
        final Map<String, FieldCapabilities> res = Maps.newHashMapWithExpectedSize(resSize);
        for (Map.Entry<String, FieldCapabilities.Builder> e : builders) {
            res.put(e.getKey(), e.getValue().build(multiTypes));
        }
        return res;
    }

    @Nullable
    private static Function<Boolean, FieldCapabilities> getUnmappedFields(String[] indices, String field, Set<String> mappedIndices) {
        if (mappedIndices.size() != indices.length) {
            return mt -> {
                final String[] diff;
                if (mt) {
                    diff = new String[indices.length - mappedIndices.size()];
                    Iterator<String> indicesIter = Iterators.forArray(indices);
                    for (int i = 0; i < diff.length; i++) {
                        diff[i] = nextIndex(indicesIter, mappedIndices);
                    }
                } else {
                    diff = null;
                }
                return new FieldCapabilities(field, "unmapped", false, false, false, false, null, diff, null, null, null, null, Map.of());
            };
        }
        return null;
    }

    private static String nextIndex(Iterator<String> iter, Set<String> filtered) {
        while (true) {
            String index = iter.next();
            if (filtered.contains(index) == false) {
                return index;
            }
        }
    }

    private static void innerMerge(
        String[] indices,
        Map<String, Map<String, FieldCapabilities.Builder>> responseMapBuilder,
        FieldCapabilitiesRequest request,
        FieldCapabilitiesIndexResponse response
    ) {
        Map<String, IndexFieldCapabilities> fields = ResponseRewriter.rewriteOldResponses(
            response.getOriginVersion(),
            response.get(),
            request.filters(),
            request.types()
        );
        for (Map.Entry<String, IndexFieldCapabilities> entry : fields.entrySet()) {
            final String field = entry.getKey();
            final IndexFieldCapabilities fieldCap = entry.getValue();
            Map<String, FieldCapabilities.Builder> typeMap = responseMapBuilder.computeIfAbsent(field, f -> new HashMap<>());
            FieldCapabilities.Builder builder = typeMap.computeIfAbsent(fieldCap.type(), key -> new FieldCapabilities.Builder(field, key));
            builder.add(
                indices,
                fieldCap.isMetadatafield(),
                fieldCap.isSearchable(),
                fieldCap.isAggregatable(),
                fieldCap.isDimension(),
                fieldCap.metricType(),
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
        private final Map<String, Exception> failuresByIndex = new HashMap<>();

        List<FieldCapabilitiesFailure> build(Set<String> successfulIndices) {
            Map<Tuple<String, String>, FieldCapabilitiesFailure> indexFailures = new HashMap<>();
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

        void clear() {
            failuresByIndex.clear();
        }

        boolean isEmpty() {
            return failuresByIndex.isEmpty();
        }
    }

    private class NodeTransportHandler implements TransportRequestHandler<FieldCapabilitiesNodeRequest> {
        @Override
        public void messageReceived(FieldCapabilitiesNodeRequest request, TransportChannel channel, Task task) {
            assert task instanceof CancellableTask;
            final ActionListener<FieldCapabilitiesNodeResponse> listener = new ChannelActionListener<>(channel);
            ActionListener.completeWith(listener, () -> {
                final List<FieldCapabilitiesIndexResponse> allResponses = new ArrayList<>();
                final Map<ShardId, Exception> allFailures = new HashMap<>();
                final Set<ShardId> allUnmatchedShardIds = new HashSet<>();
                // If the request has an index filter, it may contain several shards belonging to the same
                // index. We make sure to skip over a shard if we already found a match for that index.
                final Map<String, List<ShardId>> groupedShardIds = request.shardIds()
                    .stream()
                    .collect(Collectors.groupingBy(ShardId::getIndexName));
                final FieldCapabilitiesFetcher fetcher = new FieldCapabilitiesFetcher(indicesService, request.includeEmptyFields());
                Predicate<String> fieldNameFilter;
                try {
                    fieldNameFilter = Regex.simpleMatcher(request.fields());
                } catch (TooComplexToDeterminizeException e) {
                    throw new IllegalArgumentException("The field names are too complex to process. " + e.getMessage());
                }
                for (List<ShardId> shardIds : groupedShardIds.values()) {
                    final Map<ShardId, Exception> failures = new HashMap<>();
                    final Set<ShardId> unmatched = new HashSet<>();
                    for (ShardId shardId : shardIds) {
                        try {
                            final FieldCapabilitiesIndexResponse response = fetcher.fetch(
                                (CancellableTask) task,
                                shardId,
                                fieldNameFilter,
                                request.filters(),
                                request.allowedTypes(),
                                request.indexFilter(),
                                request.nowInMillis(),
                                request.runtimeFields()
                            );
                            if (response.canMatch()) {
                                allResponses.add(response);
                                if (request.includeEmptyFields()) {
                                    unmatched.clear();
                                    failures.clear();
                                    break;
                                }
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

    private static class ForkingOnFailureActionListener<Response> extends AbstractThreadedActionListener<Response> {
        ForkingOnFailureActionListener(Executor executor, boolean forceExecution, ActionListener<Response> delegate) {
            super(executor, forceExecution, delegate);
        }

        @Override
        public void onResponse(Response response) {
            delegate.onResponse(response);
        }
    }
}
