/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.knneval;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
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

/**
 * Estimates ANN recall without brute-force ground truth: each query runs under the baseline knobs and then under each candidate, and
 * the overlap of the candidate's top-k with the baseline's top-k is the recall.
 * <p>
 * All passes share one point-in-time, so a concurrent refresh cannot masquerade as a recall difference, and each pass is homogeneous so
 * that an expensive baseline search cannot steal search threads from whichever candidate was scheduled beside it. {@code took_ms}
 * therefore reflects an index the baseline pass has already warmed; {@code vector_ops} is the cache-independent axis.
 */
public class TransportKnnEvalAction extends HandledTransportAction<KnnEvalRequest, KnnEvalResponse> {

    private static final Logger logger = LogManager.getLogger(TransportKnnEvalAction.class);

    /** Held for the whole sweep -- every batch of every pass -- and never refreshed. */
    static final TimeValue POINT_IN_TIME_KEEP_ALIVE = TimeValue.timeValueMinutes(5);

    private final Client client;
    private final ClusterService clusterService;
    private final KnnEvalEnvironmentResolver environmentResolver;

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
        this.environmentResolver = new KnnEvalEnvironmentResolver(client, clusterService);
    }

    @Override
    protected void doExecute(Task task, KnnEvalRequest request, ActionListener<KnnEvalResponse> listener) {
        if (request.getKnnEvalSpec().getBaseline().isExact()
            && clusterService.getClusterSettings().get(SearchService.ALLOW_EXPENSIVE_QUERIES) == false) {
            // a full scan is what that setting exists to keep off a cluster; an approximate baseline is an ordinary kNN search
            listener.onFailure(
                new IllegalArgumentException(
                    "["
                        + KnnEvalKnobs.EXACT_FIELD.getPreferredName()
                        + "] baseline requires ["
                        + SearchService.ALLOW_EXPENSIVE_QUERIES.getKey()
                        + "] to be true; set it or pass a non-exact baseline such as {visit_percentage: 100, oversample: 50}"
                )
            );
            return;
        }
        resolveField(
            request,
            listener.delegateFailureAndWrap((withField, fieldContext) -> openPointInTime(task, request, fieldContext, withField))
        );
    }

    /**
     * Reads the field's mapping, which the similarity-based metrics, the resolved candidate windows and the knob compatibility checks
     * all need. It requires {@code view_index_metadata}, so a failed lookup drops those rather than failing the request.
     */
    private void resolveField(KnnEvalRequest request, ActionListener<KnnEvalFieldContext> listener) {
        KnnEvalSpec spec = request.getKnnEvalSpec();
        String field = spec.getField();
        GetFieldMappingsRequest mappingsRequest = new GetFieldMappingsRequest().indices(request.indices())
            .indicesOptions(request.indicesOptions())
            .fields(field);
        client.execute(GetFieldMappingsAction.INSTANCE, mappingsRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetFieldMappingsResponse response) {
                // ActionListener.run is what turns a validation failure below into a response
                ActionListener.run(listener, l -> l.onResponse(fieldContextOf(response)));
            }

            private KnnEvalFieldContext fieldContextOf(GetFieldMappingsResponse response) {
                Map<String, Object> fieldMapping = null;
                for (Map<String, FieldMappingMetadata> indexMappings : response.mappings().values()) {
                    FieldMappingMetadata metadata = indexMappings.get(field);
                    if (metadata != null && metadata.sourceAsMap().get(field) instanceof Map<?, ?> mapping) {
                        @SuppressWarnings("unchecked") // a field mapping body is always a string-keyed object
                        Map<String, Object> typed = (Map<String, Object>) mapping;
                        fieldMapping = typed;
                        break;
                    }
                }
                KnnEvalRescore rescore = fieldMapping == null ? null : KnnEvalRescore.fromFieldMapping(fieldMapping);
                if (rescore != null) {
                    // a knob the field would ignore yields a sweep in which nothing varied, which reads as good news
                    rescore.validateSupportedKnobs(spec.getBaseline());
                    for (KnnEvalKnobs knobs : spec.getKnnSettings()) {
                        rescore.validateSupportedKnobs(knobs);
                    }
                }
                // a mapping that is present but wrong is a caller error, unlike one we could not read
                KnnEvalFidelity fidelity = spec.isIncludeFidelity()
                    ? KnnEvalFidelity.fromFieldMapping(field, fieldMapping, rescore, KnnEvalSearches.baselineOversample(spec.getBaseline()))
                    : null;
                return new KnnEvalFieldContext(fidelity, rescore, fieldSummary(fieldMapping), null);
            }

            @SuppressWarnings("unchecked") // a field mapping body and its index_options are string-keyed objects
            private KnnEvalEnvironment.FieldSummary fieldSummary(@Nullable Map<String, Object> fieldMapping) {
                if (fieldMapping == null) {
                    return null;
                }
                Object indexOptions = fieldMapping.get(KnnEvalFidelity.INDEX_OPTIONS_FIELD);
                Object dims = fieldMapping.get(KnnEvalFidelity.DIMS_FIELD);
                Object elementType = fieldMapping.get(KnnEvalFidelity.ELEMENT_TYPE_FIELD);
                Object similarity = fieldMapping.get(KnnEvalFidelity.SIMILARITY_FIELD);
                return new KnnEvalEnvironment.FieldSummary(
                    String.valueOf(fieldMapping.get(KnnEvalFidelity.TYPE_FIELD)),
                    dims instanceof Number number ? number.intValue() : null,
                    elementType == null ? "float" : elementType.toString(),
                    similarity == null ? null : similarity.toString(),
                    indexOptions instanceof Map<?, ?> map ? (Map<String, Object>) map : Map.of()
                );
            }

            @Override
            public void onFailure(Exception e) {
                if (spec.isIncludeFidelity()) {
                    // asked for by name, and not computable without the mapping
                    listener.onResponse(
                        new KnnEvalFieldContext(
                            KnnEvalFidelity.unavailable("field mapping unavailable: " + e.getMessage()),
                            null,
                            null,
                            null
                        )
                    );
                    return;
                }
                logger.debug(() -> "could not read the mapping of field [" + field + "]; omitting the derived knob fields", e);
                listener.onResponse(KnnEvalFieldContext.EMPTY);
            }
        });
    }

    private void openPointInTime(
        Task task,
        KnnEvalRequest request,
        KnnEvalFieldContext fieldContext,
        ActionListener<KnnEvalResponse> listener
    ) {
        OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest(request.indices()).indicesOptions(request.indicesOptions())
            .keepAlive(POINT_IN_TIME_KEEP_ALIVE);
        client.execute(TransportOpenPointInTimeAction.TYPE, openRequest, listener.delegateFailureAndWrap((delegate, openResponse) -> {
            BytesReference pointInTimeId = openResponse.getPointInTimeId();
            // runAfter fires either way and ActionListener.run funnels throws into onFailure: no path leaves the PIT open
            ActionListener<KnnEvalResponse> closingListener = ActionListener.runAfter(delegate, () -> closePointInTime(pointInTimeId));
            ActionListener.run(
                closingListener,
                l -> environmentResolver.resolve(
                    request,
                    fieldContext,
                    l.delegateFailureAndWrap(
                        (withEnvironment, withContext) -> resolveQueries(task, request, withContext, pointInTimeId, withEnvironment)
                    )
                )
            );
        }));
    }

    private void closePointInTime(BytesReference pointInTimeId) {
        client.execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pointInTimeId), new ActionListener<>() {
            @Override
            public void onResponse(ClosePointInTimeResponse response) {}

            @Override
            public void onFailure(Exception e) {
                // the keep-alive expires anyway, so this costs search context memory and nothing else
                logger.warn("failed to close the point in time opened for kNN evaluation", e);
            }
        });
    }

    private void resolveQueries(
        Task task,
        KnnEvalRequest request,
        KnnEvalFieldContext fieldContext,
        BytesReference pointInTimeId,
        ActionListener<KnnEvalResponse> listener
    ) {
        KnnEvalSpec spec = request.getKnnEvalSpec();
        KnnEvalSample sample = spec.getSample();
        if (sample == null) {
            evaluate(task, spec, spec.getQueries(), false, fieldContext, pointInTimeId, listener);
            return;
        }
        client.search(
            KnnEvalSearches.buildSampleRequest(spec, sample, pointInTimeId),
            listener.delegateFailureAndWrap((delegate, searchResponse) -> {
                List<KnnEvalQuery> sampledQueries = KnnEvalSearches.extractSampledQueries(searchResponse, spec.getField());
                if (sampledQueries.isEmpty()) {
                    // a recall of zero would read as a catastrophic candidate rather than an empty index or a wrong field name
                    throw new IllegalArgumentException(
                        "sampling query vectors from field ["
                            + spec.getField()
                            + "] returned no documents; check that the indices contain documents with that dense_vector field"
                    );
                }
                evaluate(task, spec, sampledQueries, true, fieldContext, pointInTimeId, delegate);
            })
        );
    }

    private void evaluate(
        Task task,
        KnnEvalSpec spec,
        List<KnnEvalQuery> queries,
        boolean excludeQueryDocument,
        KnnEvalFieldContext fieldContext,
        BytesReference pointInTimeId,
        ActionListener<KnnEvalResponse> listener
    ) {
        runBaselineBatch(task, new KnnEvalState(spec, excludeQueryDocument, queries, fieldContext), pointInTimeId, 0, listener);
    }

    /**
     * Phase 1: baseline searches only, one batch at a time. Batching, not concurrency, is what bounds heap -- a coordinator holds every
     * sub-search response of one msearch until the last arrives. Recursing inside the callback is safe because the state copies out
     * what it needs synchronously, so each batch's response is released before the next callback runs.
     */
    private void runBaselineBatch(
        Task task,
        KnnEvalState state,
        BytesReference pointInTimeId,
        int from,
        ActionListener<KnnEvalResponse> listener
    ) {
        if (checkCancelled(task, listener)) {
            return;
        }
        List<KnnEvalQuery> queries = state.queries;
        if (from >= queries.size()) {
            runCandidatePass(task, state, pointInTimeId, 0, listener);
            return;
        }
        int to = Math.min(from + state.spec.getMaxQueriesPerBatch(), queries.size());
        List<KnnEvalQuery> batch = queries.subList(from, to);
        MultiSearchRequest msearchRequest = KnnEvalSearches.newMultiSearchRequest(state.spec);
        for (KnnEvalQuery query : batch) {
            msearchRequest.add(KnnEvalSearches.buildSearch(state.spec, query, state.spec.getBaseline(), state.searchSize, pointInTimeId));
        }
        client.multiSearch(msearchRequest, listener.delegateFailureAndWrap((delegate, msearchResponse) -> {
            state.addBaselineBatch(msearchResponse, batch);
            runBaselineBatch(task, state, pointInTimeId, to, delegate);
        }));
    }

    /** Phase 2: one homogeneous pass per knn_settings entry, in the order the caller listed them. */
    private void runCandidatePass(
        Task task,
        KnnEvalState state,
        BytesReference pointInTimeId,
        int candidateIndex,
        ActionListener<KnnEvalResponse> listener
    ) {
        if (checkCancelled(task, listener)) {
            return;
        }
        if (candidateIndex >= state.spec.getKnnSettings().size()) {
            listener.onResponse(state.buildResponse());
            return;
        }
        runCandidateBatch(task, state, pointInTimeId, candidateIndex, 0, listener);
    }

    private void runCandidateBatch(
        Task task,
        KnnEvalState state,
        BytesReference pointInTimeId,
        int candidateIndex,
        int from,
        ActionListener<KnnEvalResponse> listener
    ) {
        if (checkCancelled(task, listener)) {
            return;
        }
        List<KnnEvalQuery> queries = state.evaluableQueries();
        if (from >= queries.size()) {
            runCandidatePass(task, state, pointInTimeId, candidateIndex + 1, listener);
            return;
        }
        int to = Math.min(from + state.spec.getMaxQueriesPerBatch(), queries.size());
        List<KnnEvalQuery> batch = queries.subList(from, to);
        KnnEvalKnobs candidate = state.spec.getKnnSettings().get(candidateIndex);
        MultiSearchRequest msearchRequest = KnnEvalSearches.newMultiSearchRequest(state.spec);
        for (KnnEvalQuery query : batch) {
            msearchRequest.add(KnnEvalSearches.buildSearch(state.spec, query, candidate, state.searchSize, pointInTimeId));
        }
        client.multiSearch(msearchRequest, listener.delegateFailureAndWrap((delegate, msearchResponse) -> {
            state.addCandidateBatch(candidateIndex, msearchResponse, batch);
            runCandidateBatch(task, state, pointInTimeId, candidateIndex, to, delegate);
        }));
    }

    private static boolean checkCancelled(Task task, ActionListener<KnnEvalResponse> listener) {
        if (task instanceof CancellableTask cancellableTask && cancellableTask.isCancelled()) {
            listener.onFailure(new TaskCancelledException("task cancelled"));
            return true;
        }
        return false;
    }

}
