/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportService;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

/** Compares each candidate's top-k with the baseline's, using one PIT and sequential passes to keep changes and contention out. */
public class TransportKnnEvalAction extends HandledTransportAction<KnnEvalRequest, KnnEvalResponse> {

    private static final Logger logger = LogManager.getLogger(TransportKnnEvalAction.class);

    /** Idle gap between searches, not the sweep length: each PIT search renews it. */
    static final TimeValue POINT_IN_TIME_KEEP_ALIVE = TimeValue.timeValueMinutes(5);
    static final long MAX_EXACT_VECTOR_COMPARISONS = 100_000_000L;

    /** Cluster or evaluation failures, not one query's: every remaining search would fail alike. */
    private static final Class<?>[] EVALUATION_FAILURES = {
        SearchContextMissingException.class,
        NoShardAvailableActionException.class,
        NodeClosedException.class,
        ConnectTransportException.class,
        EsRejectedExecutionException.class,
        CircuitBreakingException.class,
        TaskCancelledException.class,
        ElasticsearchSecurityException.class,
        IndexNotFoundException.class,
        ClusterBlockException.class };

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportKnnEvalAction(
        ActionFilters actionFilters,
        Client client,
        TransportService transportService,
        ClusterService clusterService
    ) {
        super(
            KnnEvalPlugin.KNN_EVAL_ACTION.name(),
            transportService,
            actionFilters,
            KnnEvalRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, KnnEvalRequest request, ActionListener<KnnEvalResponse> listener) {
        if (checkCancelled(task, listener)) {
            return;
        }
        if (clusterService.getClusterSettings().get(KnnEvalPlugin.ENABLED) == false) {
            listener.onFailure(
                new IllegalArgumentException("[" + RestKnnEvalAction.ENDPOINT + "] is disabled by [" + KnnEvalPlugin.ENABLED.getKey() + "]")
            );
            return;
        }
        if (request.getKnnEvalSpec().getBaseline().isExact()
            && clusterService.getClusterSettings().get(SearchService.ALLOW_EXPENSIVE_QUERIES) == false) {
            // a full scan is what that setting guards; approximate baselines are ordinary kNN searches
            listener.onFailure(
                new IllegalArgumentException(
                    "["
                        + KnnEvalSettings.EXACT_FIELD.getPreferredName()
                        + "] baseline requires ["
                        + SearchService.ALLOW_EXPENSIVE_QUERIES.getKey()
                        + "] to be true; set it or pass a non-exact baseline such as "
                        + "{visit_percentage: 100, rescore_vector: {oversample: 50}}"
                )
            );
            return;
        }
        resolveField(
            task,
            request,
            listener.delegateFailureAndWrap((delegate, rescore) -> rejectNestedField(task, request, rescore, delegate))
        );
    }

    /** Nested vectors need nested sampling, exact queries and parent-level recall; refused until supported. */
    private void rejectNestedField(Task task, KnnEvalRequest request, KnnEvalRescore rescore, ActionListener<KnnEvalResponse> listener) {
        String field = request.getKnnEvalSpec().getField();
        FieldCapabilitiesRequest capabilitiesRequest = new FieldCapabilitiesRequest().indices(request.indices())
            .indicesOptions(request.indicesOptions())
            .fields(field);
        setParentTask(task, capabilitiesRequest);
        client.execute(TransportFieldCapabilitiesAction.TYPE, capabilitiesRequest, listener.delegateFailureAndWrap((delegate, response) -> {
            String nestedPath = nestedAncestor(field, response.get());
            if (nestedPath != null) {
                throw new IllegalArgumentException(
                    "field ["
                        + field
                        + "] is inside the [nested] object ["
                        + nestedPath
                        + "], which ["
                        + RestKnnEvalAction.ENDPOINT
                        + "] does not support yet"
                );
            }
            openPointInTime(task, request, rescore, delegate);
        }));
    }

    /** Field caps reports a nested ancestor as [nested] under a prefix of the path. */
    @Nullable
    static String nestedAncestor(String field, Map<String, Map<String, FieldCapabilities>> capabilities) {
        for (int dot = field.lastIndexOf('.'); dot > 0; dot = field.lastIndexOf('.', dot - 1)) {
            String ancestor = field.substring(0, dot);
            if (capabilities.getOrDefault(ancestor, Map.of()).containsKey("nested")) {
                return ancestor;
            }
        }
        return null;
    }

    /** Checks the field maps to one supported DiskBBQ config across all indices. */
    private void resolveField(Task task, KnnEvalRequest request, ActionListener<KnnEvalRescore> listener) {
        KnnEvalSpec spec = request.getKnnEvalSpec();
        String field = spec.getField();
        GetFieldMappingsRequest mappingsRequest = new GetFieldMappingsRequest().indices(request.indices())
            .indicesOptions(request.indicesOptions())
            .fields(field);
        setParentTask(task, mappingsRequest);
        client.execute(
            GetFieldMappingsAction.INSTANCE,
            mappingsRequest,
            listener.<GetFieldMappingsResponse>map(response -> rescoreOf(spec, response))
                .delegateResponse((delegate, e) -> delegate.onFailure(mappingLookupFailure(field, e)))
        );
    }

    /** A [read]-only caller is refused by an action they never invoked; name this one. */
    private static Exception mappingLookupFailure(String field, Exception e) {
        if (ExceptionsHelper.unwrap(e, ElasticsearchSecurityException.class) instanceof ElasticsearchSecurityException security
            && security.status() == RestStatus.FORBIDDEN) {
            return new ElasticsearchSecurityException(
                "[{}] reads the mapping of field [{}] to validate it, which needs the [view_index_metadata] index privilege in "
                    + "addition to [read]",
                RestStatus.FORBIDDEN,
                e,
                RestKnnEvalAction.ENDPOINT,
                field
            );
        }
        return e;
    }

    private static KnnEvalRescore rescoreOf(KnnEvalSpec spec, GetFieldMappingsResponse response) {
        String field = spec.getField();
        Map<String, Object> fieldMapping = null;
        FieldResolution firstResolution = null;
        for (Map<String, FieldMappingMetadata> indexMappings : response.mappings().values()) {
            FieldMappingMetadata metadata = indexMappings.get(field);
            FieldResolution resolution = FieldResolution.UNMAPPED;
            // keyed by leaf name (emb for obj.emb): take the sole entry
            Map<String, Object> source = metadata == null ? Map.of() : metadata.sourceAsMap();
            if (source.size() == 1 && source.values().iterator().next() instanceof Map<?, ?> mapping) {
                @SuppressWarnings("unchecked") // a field mapping body is always a string-keyed object
                Map<String, Object> typed = (Map<String, Object>) mapping;
                fieldMapping = fieldMapping == null ? typed : fieldMapping;
                resolution = FieldResolution.of(typed);
            }
            if (firstResolution == null) {
                firstResolution = resolution;
            } else if (firstResolution.equals(resolution) == false) {
                throw new IllegalArgumentException(
                    "[" + field + "] resolves differently across indices; evaluate one vector space at a time"
                );
            }
        }
        KnnEvalRescore rescore = KnnEvalRescore.fromFieldMapping(field, fieldMapping);
        validateQueryDimensions(field, firstResolution.dims(), spec.getQueries());
        return rescore;
    }

    /** Fails once up front, since every search would fail alike; encoded vectors can't be checked here. */
    static void validateQueryDimensions(String field, @Nullable Integer dims, @Nullable List<KnnEvalQuery> queries) {
        if (dims == null || queries == null) {
            return;
        }
        for (KnnEvalQuery query : queries) {
            VectorData vector = query.getQueryVector();
            if (vector.isStringVector() == false && vector.size() != dims) {
                throw new IllegalArgumentException(
                    "query vector ["
                        + query.getId()
                        + "] has ["
                        + vector.size()
                        + "] dimensions but field ["
                        + field
                        + "] has ["
                        + dims
                        + "]"
                );
            }
        }
    }

    private void openPointInTime(Task task, KnnEvalRequest request, KnnEvalRescore rescore, ActionListener<KnnEvalResponse> listener) {
        OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest(request.indices()).indicesOptions(request.indicesOptions())
            .keepAlive(POINT_IN_TIME_KEEP_ALIVE);
        setParentTask(task, openRequest);
        client.execute(TransportOpenPointInTimeAction.TYPE, openRequest, listener.delegateFailureAndWrap((delegate, openResponse) -> {
            BytesReference pointInTimeId = openResponse.getPointInTimeId();
            // runAfter always fires and run() routes throws to onFailure, so the PIT always closes
            ActionListener<KnnEvalResponse> closingListener = ActionListener.runAfter(delegate, () -> closePointInTime(pointInTimeId));
            ActionListener.run(closingListener, l -> countVectors(task, request, rescore, pointInTimeId, l));
        }));
    }

    private void countVectors(
        Task task,
        KnnEvalRequest request,
        KnnEvalRescore rescore,
        BytesReference pointInTimeId,
        ActionListener<KnnEvalResponse> listener
    ) {
        KnnEvalSpec spec = request.getKnnEvalSpec();
        if (spec.getBaseline().isExact() == false) {
            resolveQueries(task, request, rescore, pointInTimeId, listener);
            return;
        }
        SearchRequest countRequest = KnnEvalSearches.buildVectorCountRequest(spec, pointInTimeId);
        setParentTask(task, countRequest);
        client.search(countRequest, listener.delegateFailureAndWrap((delegate, searchResponse) -> {
            validateExactWorkload(spec, searchResponse.getHits().getTotalHits().value());
            resolveQueries(task, request, rescore, pointInTimeId, delegate);
        }));
    }

    static void validateExactWorkload(KnnEvalSpec spec, long vectorCount) {
        if (spec.getBaseline().isExact() == false) {
            return;
        }
        long queryCount = switch (spec.getQuerySource()) {
            case KnnEvalQuerySource.VectorsSource vs -> vs.vectors().size();
            case KnnEvalQuerySource.DocsSource ds -> ds.sample().getSize();
        };
        long comparisons = vectorCount > Long.MAX_VALUE / queryCount ? Long.MAX_VALUE : vectorCount * queryCount;
        if (comparisons > MAX_EXACT_VECTOR_COMPARISONS) {
            throw new IllegalArgumentException(
                "exact baseline would perform approximately ["
                    + comparisons
                    + "] full-precision vector comparisons, exceeding the ["
                    + MAX_EXACT_VECTOR_COMPARISONS
                    + "] limit; reduce the query count or use a bounded approximate baseline"
            );
        }
    }

    private void closePointInTime(BytesReference pointInTimeId) {
        client.execute(
            TransportClosePointInTimeAction.TYPE,
            new ClosePointInTimeRequest(pointInTimeId),
            ActionListener.wrap(ignored -> {}, e -> {
                // the keep-alive expires anyway; costs only search context memory
                logger.warn("failed to close the point in time opened for kNN evaluation", e);
            })
        );
    }

    private void resolveQueries(
        Task task,
        KnnEvalRequest request,
        KnnEvalRescore rescore,
        BytesReference pointInTimeId,
        ActionListener<KnnEvalResponse> listener
    ) {
        KnnEvalSpec spec = request.getKnnEvalSpec();
        switch (spec.getQuerySource()) {
            case KnnEvalQuerySource.VectorsSource vs -> evaluate(task, spec, vs.vectors(), false, rescore, pointInTimeId, listener);
            case KnnEvalQuerySource.DocsSource ds -> {
                var sampleRequest = KnnEvalSearches.buildSampleRequest(spec, ds.sample(), pointInTimeId);
                setParentTask(task, sampleRequest);
                client.search(sampleRequest, listener.delegateFailureAndWrap((delegate, searchResponse) -> {
                    List<KnnEvalQuery> sampledQueries = KnnEvalSearches.extractSampledQueries(searchResponse, spec.getField());
                    if (sampledQueries.isEmpty()) {
                        // zero recall would look like a terrible candidate, not an empty index or wrong field
                        throw new IllegalArgumentException(
                            "sampling query vectors from field ["
                                + spec.getField()
                                + "] returned no documents; check that the indices contain documents with that dense_vector field"
                        );
                    }
                    evaluate(task, spec, sampledQueries, true, rescore, pointInTimeId, delegate);
                }));
            }
        }
    }

    private void evaluate(
        Task task,
        KnnEvalSpec spec,
        List<KnnEvalQuery> queries,
        boolean queryIsSampledFromDocuments,
        KnnEvalRescore rescore,
        BytesReference pointInTimeId,
        ActionListener<KnnEvalResponse> listener
    ) {
        new EvaluationRunner(task, new KnnEvalState(spec, queryIsSampledFromDocuments, queries, rescore), pointInTimeId, listener).run();
    }

    private final class EvaluationRunner {

        /** Set once the listener fails (cancellation or evaluation-level failure): nothing further starts. */
        private volatile boolean stopped;
        private final Task task;
        private final KnnEvalState state;
        private final BytesReference pointInTimeId;
        private final ActionListener<KnnEvalResponse> listener;
        private final Client client;

        private EvaluationRunner(Task task, KnnEvalState state, BytesReference pointInTimeId, ActionListener<KnnEvalResponse> listener) {
            this.task = task;
            this.state = state;
            this.pointInTimeId = pointInTimeId;
            this.listener = listener;
            this.client = task == null
                ? TransportKnnEvalAction.this.client
                : new ParentTaskAssigningClient(TransportKnnEvalAction.this.client, clusterService.localNode(), task);
        }

        private void run() {
            runQueries(state.queries, state.spec.getBaseline(), state::addBaseline, () -> runCandidatePass(0));
        }

        private void runCandidatePass(int candidateIndex) {
            if (candidateIndex >= state.spec.getKnnSettings().size()) {
                listener.onResponse(state.buildResponse());
                return;
            }
            runQueries(
                state.evaluableQueries(),
                state.spec.getKnnSettings().get(candidateIndex),
                (query, response) -> state.addCandidate(candidateIndex, query, response),
                () -> runCandidatePass(candidateIndex + 1)
            );
        }

        /**
         * One search at a time, so each took is shard time without sibling contention. ThrottledIterator loops instead of
         * recursing, so a full sweep can't overflow the stack.
         */
        private void runQueries(
            List<KnnEvalQuery> queries,
            KnnEvalSettings knnSettings,
            BiConsumer<KnnEvalQuery, SearchResponse> consumer,
            Runnable onComplete
        ) {
            Iterator<KnnEvalQuery> remaining = queries.iterator();
            Iterator<KnnEvalQuery> untilCancelled = new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return stopped == false && remaining.hasNext();
                }

                @Override
                public KnnEvalQuery next() {
                    return remaining.next();
                }
            };
            ThrottledIterator.run(untilCancelled, (ref, query) -> {
                if (checkCancelled(task, listener)) {
                    stopped = true;
                    ref.close();
                    return;
                }
                SearchRequest request = KnnEvalSearches.buildSearch(state.spec, query, knnSettings, state.searchSize, pointInTimeId);
                client.search(request, ActionListener.releaseAfter(ActionListener.wrap(response -> consumer.accept(query, response), e -> {
                    if (failsTheEvaluation(e)) {
                        stop(
                            new ElasticsearchException(
                                "["
                                    + RestKnnEvalAction.ENDPOINT
                                    + "] stopped at query ["
                                    + query.getId()
                                    + "]: the failure is not specific to it",
                                e
                            )
                        );
                    } else {
                        // a query's own failure is reported against it; the sweep continues
                        state.addFailure(query, e);
                    }
                }), ref));
            }, 1, () -> {
                if (stopped == false) {
                    onComplete.run();
                }
            });
        }

        private void stop(Exception e) {
            stopped = true;
            listener.onFailure(e);
        }
    }

    /** Whether a failure is not specific to its query, so per-query reporting would hide it. */
    static boolean failsTheEvaluation(Exception e) {
        if (ExceptionsHelper.unwrap(e, EVALUATION_FAILURES) != null) {
            return true;
        }
        if (ExceptionsHelper.unwrapCause(e) instanceof SearchPhaseExecutionException searchFailure) {
            for (ShardSearchFailure shardFailure : searchFailure.shardFailures()) {
                if (shardFailure.getCause() != null && ExceptionsHelper.unwrap(shardFailure.getCause(), EVALUATION_FAILURES) != null) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean checkCancelled(Task task, ActionListener<KnnEvalResponse> listener) {
        return task instanceof CancellableTask cancellableTask && cancellableTask.notifyIfCancelled(listener);
    }

    private void setParentTask(Task task, ActionRequest childRequest) {
        if (task != null) {
            childRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        }
    }

    private record FieldResolution(
        boolean mapped,
        @Nullable Integer dims,
        String similarity,
        String elementType,
        Map<String, Object> indexOptions
    ) {
        private static final FieldResolution UNMAPPED = new FieldResolution(false, null, "", "", Map.of());
        private static final String DIMS_FIELD = "dims";
        private static final String SIMILARITY_FIELD = "similarity";
        private static final String ELEMENT_TYPE_FIELD = "element_type";

        @SuppressWarnings("unchecked")
        private static FieldResolution of(Map<String, Object> mapping) {
            Object dims = mapping.get(DIMS_FIELD);
            Object similarity = mapping.get(SIMILARITY_FIELD);
            Object elementType = mapping.get(ELEMENT_TYPE_FIELD);
            Object indexOptions = mapping.get(KnnEvalRescore.INDEX_OPTIONS_FIELD);
            return new FieldResolution(
                true,
                dims instanceof Number number ? number.intValue() : null,
                Objects.toString(similarity, "cosine"),
                Objects.toString(elementType, "float"),
                indexOptions instanceof Map<?, ?> map ? Map.copyOf((Map<String, Object>) map) : Map.of()
            );
        }
    }
}
