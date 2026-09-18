/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Compares each candidate's top-k with an exact or approximate baseline. All passes share one point-in-time and run sequentially so
 * index changes and inter-setting contention do not distort results.
 */
public class TransportKnnEvalAction extends HandledTransportAction<KnnEvalRequest, KnnEvalResponse> {

    private static final Logger logger = LogManager.getLogger(TransportKnnEvalAction.class);

    /** Held for the whole sweep -- every batch of every pass -- and never refreshed. */
    static final TimeValue POINT_IN_TIME_KEEP_ALIVE = TimeValue.timeValueMinutes(5);
    static final long MAX_EXACT_VECTOR_COMPARISONS = 100_000_000L;

    private final Client client;
    private final ClusterService clusterService;

    /** Creates the action with services used to validate mappings and execute child searches. */
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
        if (request.getKnnEvalSpec().getBaseline().isExact()
            && clusterService.getClusterSettings().get(SearchService.ALLOW_EXPENSIVE_QUERIES) == false) {
            // a full scan is what that setting exists to keep off a cluster; an approximate baseline is an ordinary kNN search
            listener.onFailure(
                new IllegalArgumentException(
                    "["
                        + KnnEvalKnobs.EXACT_FIELD.getPreferredName()
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
            listener.delegateFailureAndWrap((delegate, rescore) -> openPointInTime(task, request, rescore, delegate))
        );
    }

    /** Validates that the field resolves consistently to a supported DiskBBQ mapping across all target indices. */
    private void resolveField(Task task, KnnEvalRequest request, ActionListener<KnnEvalRescore> listener) {
        KnnEvalSpec spec = request.getKnnEvalSpec();
        String field = spec.getField();
        GetFieldMappingsRequest mappingsRequest = new GetFieldMappingsRequest().indices(request.indices())
            .indicesOptions(request.indicesOptions())
            .fields(field);
        setParentTask(task, mappingsRequest);
        client.execute(GetFieldMappingsAction.INSTANCE, mappingsRequest, listener.map(response -> rescoreOf(field, response)));
    }

    private static KnnEvalRescore rescoreOf(String field, GetFieldMappingsResponse response) {
        Map<String, Object> fieldMapping = null;
        FieldResolution firstResolution = null;
        for (Map<String, FieldMappingMetadata> indexMappings : response.mappings().values()) {
            FieldMappingMetadata metadata = indexMappings.get(field);
            FieldResolution resolution = FieldResolution.UNMAPPED;
            if (metadata != null && metadata.sourceAsMap().get(field) instanceof Map<?, ?> mapping) {
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
        return KnnEvalRescore.fromFieldMapping(field, fieldMapping);
    }

    private void openPointInTime(Task task, KnnEvalRequest request, KnnEvalRescore rescore, ActionListener<KnnEvalResponse> listener) {
        OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest(request.indices()).indicesOptions(request.indicesOptions())
            .keepAlive(POINT_IN_TIME_KEEP_ALIVE);
        setParentTask(task, openRequest);
        client.execute(TransportOpenPointInTimeAction.TYPE, openRequest, listener.delegateFailureAndWrap((delegate, openResponse) -> {
            BytesReference pointInTimeId = openResponse.getPointInTimeId();
            // runAfter fires either way and ActionListener.run funnels throws into onFailure: no path leaves the PIT open
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
        long queryCount = spec.getQueries() == null ? spec.getSample().getSize() : spec.getQueries().size();
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
                // the keep-alive expires anyway, so this costs search context memory and nothing else
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
        KnnEvalSample sample = spec.getSample();
        if (sample == null) {
            evaluate(task, spec, spec.getQueries(), false, rescore, pointInTimeId, listener);
            return;
        }
        var sampleRequest = KnnEvalSearches.buildSampleRequest(spec, sample, pointInTimeId);
        setParentTask(task, sampleRequest);
        client.search(sampleRequest, listener.delegateFailureAndWrap((delegate, searchResponse) -> {
            List<KnnEvalQuery> sampledQueries = KnnEvalSearches.extractSampledQueries(searchResponse, spec.getField());
            if (sampledQueries.isEmpty()) {
                // a recall of zero would read as a catastrophic candidate rather than an empty index or a wrong field name
                throw new IllegalArgumentException(
                    "sampling query vectors from field ["
                        + spec.getField()
                        + "] returned no documents; check that the indices contain documents with that dense_vector field"
                );
            }
            evaluate(task, spec, sampledQueries, true, rescore, pointInTimeId, delegate);
        }));
    }

    private void evaluate(
        Task task,
        KnnEvalSpec spec,
        List<KnnEvalQuery> queries,
        boolean excludeQueryDocument,
        KnnEvalRescore rescore,
        BytesReference pointInTimeId,
        ActionListener<KnnEvalResponse> listener
    ) {
        new EvaluationRunner(task, new KnnEvalState(spec, excludeQueryDocument, queries, rescore), pointInTimeId, listener).run();
    }

    private final class EvaluationRunner {
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
            runBatches(state.queries, state.spec.getBaseline(), 0, state::addBaselineBatch, () -> runCandidatePass(0));
        }

        private void runCandidatePass(int candidateIndex) {
            if (candidateIndex >= state.spec.getKnnSettings().size()) {
                listener.onResponse(state.buildResponse());
                return;
            }
            runBatches(
                state.evaluableQueries(),
                state.spec.getKnnSettings().get(candidateIndex),
                0,
                (response, batch) -> state.addCandidateBatch(candidateIndex, response, batch),
                () -> runCandidatePass(candidateIndex + 1)
            );
        }

        private void runBatches(List<KnnEvalQuery> queries, KnnEvalKnobs knobs, int from, BatchConsumer consumer, Runnable onComplete) {
            if (checkCancelled(task, listener)) {
                return;
            }
            if (from >= queries.size()) {
                onComplete.run();
                return;
            }
            int to = Math.min(from + state.spec.getMaxQueriesPerBatch(), queries.size());
            List<KnnEvalQuery> batch = queries.subList(from, to);
            MultiSearchRequest request = KnnEvalSearches.newMultiSearchRequest();
            for (KnnEvalQuery query : batch) {
                request.add(KnnEvalSearches.buildSearch(state.spec, query, knobs, state.searchSize, pointInTimeId));
            }
            client.multiSearch(request, listener.delegateFailureAndWrap((delegate, response) -> {
                consumer.accept(response, batch);
                runBatches(queries, knobs, to, consumer, onComplete);
            }));
        }

        @FunctionalInterface
        private interface BatchConsumer {
            void accept(MultiSearchResponse response, List<KnnEvalQuery> batch);
        }
    }

    private static boolean checkCancelled(Task task, ActionListener<KnnEvalResponse> listener) {
        if (task instanceof CancellableTask cancellableTask && cancellableTask.isCancelled()) {
            listener.onFailure(new TaskCancelledException("task cancelled"));
            return true;
        }
        return false;
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
