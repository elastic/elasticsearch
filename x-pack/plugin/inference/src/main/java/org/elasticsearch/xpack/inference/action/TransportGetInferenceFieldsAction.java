/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportGetInferenceFieldsAction extends HandledTransportAction<
    GetInferenceFieldsAction.Request,
    GetInferenceFieldsAction.Response> {

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Client client;

    @Inject
    public TransportGetInferenceFieldsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(
            GetInferenceFieldsAction.NAME,
            transportService,
            actionFilters,
            GetInferenceFieldsAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.client = client;
    }

    @Override
    protected void doExecute(
        Task task,
        GetInferenceFieldsAction.Request request,
        ActionListener<GetInferenceFieldsAction.Response> listener
    ) {
        final Set<String> indices = request.getIndices();
        final Map<String, Float> fields = request.getFields();
        final boolean resolveWildcards = request.resolveWildcards();
        final boolean useDefaultFields = request.useDefaultFields();
        final String query = request.getQuery();
        final IndicesOptions indicesOptions = request.getIndicesOptions();

        try {
            Map<String, OriginalIndices> groupedIndices = transportService.getRemoteClusterService()
                .groupIndices(indicesOptions, indices.toArray(new String[0]), true);
            OriginalIndices localIndices = groupedIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            if (groupedIndices.isEmpty() == false) {
                throw new IllegalArgumentException("GetInferenceFieldsAction does not support remote indices");
            }

            ProjectState projectState = projectResolver.getProjectState(clusterService.state());
            String[] concreteLocalIndices = indexNameExpressionResolver.concreteIndexNames(projectState.metadata(), localIndices);

            Map<String, List<GetInferenceFieldsAction.ExtendedInferenceFieldMetadata>> inferenceFieldsMap = new HashMap<>(
                concreteLocalIndices.length
            );
            Arrays.stream(concreteLocalIndices).forEach(index -> {
                List<GetInferenceFieldsAction.ExtendedInferenceFieldMetadata> inferenceFieldMetadataList = getInferenceFieldMetadata(
                    index,
                    fields,
                    resolveWildcards,
                    useDefaultFields
                );
                inferenceFieldsMap.put(index, inferenceFieldMetadataList);
            });

            if (query != null && query.isBlank() == false) {
                Set<String> inferenceIds = inferenceFieldsMap.values()
                    .stream()
                    .flatMap(List::stream)
                    .map(eifm -> eifm.inferenceFieldMetadata().getSearchInferenceId())
                    .collect(Collectors.toSet());

                getInferenceResults(query, inferenceIds, inferenceFieldsMap, listener);
            } else {
                listener.onResponse(new GetInferenceFieldsAction.Response(inferenceFieldsMap, Map.of()));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private List<GetInferenceFieldsAction.ExtendedInferenceFieldMetadata> getInferenceFieldMetadata(
        String index,
        Map<String, Float> fields,
        boolean resolveWildcards,
        boolean useDefaultFields
    ) {
        IndexMetadata indexMetadata = projectResolver.getProjectMetadata(clusterService.state()).indices().get(index);
        if (indexMetadata == null) {
            throw new IndexNotFoundException(index);
        }

        Map<InferenceFieldMetadata, Float> matchingInferenceFieldMap = indexMetadata.getMatchingInferenceFields(
            fields,
            resolveWildcards,
            useDefaultFields
        );
        return matchingInferenceFieldMap.entrySet()
            .stream()
            .map(e -> new GetInferenceFieldsAction.ExtendedInferenceFieldMetadata(e.getKey(), e.getValue()))
            .toList();
    }

    private void getInferenceResults(
        String query,
        Set<String> inferenceIds,
        Map<String, List<GetInferenceFieldsAction.ExtendedInferenceFieldMetadata>> inferenceFieldsMap,
        ActionListener<GetInferenceFieldsAction.Response> listener
    ) {
        if (inferenceIds.isEmpty()) {
            listener.onResponse(new GetInferenceFieldsAction.Response(inferenceFieldsMap, Map.of()));
            return;
        }

        GroupedActionListener<Tuple<String, InferenceResults>> gal = new GroupedActionListener<>(
            inferenceIds.size(),
            listener.delegateFailureAndWrap((l, c) -> {
                Map<String, InferenceResults> inferenceResultsMap = new HashMap<>(inferenceIds.size());
                c.forEach(t -> inferenceResultsMap.put(t.v1(), t.v2()));

                GetInferenceFieldsAction.Response response = new GetInferenceFieldsAction.Response(inferenceFieldsMap, inferenceResultsMap);
                l.onResponse(response);
            })
        );

        List<InferenceAction.Request> inferenceRequests = inferenceIds.stream()
            .map(
                i -> new InferenceAction.Request(
                    TaskType.ANY,
                    i,
                    null,
                    null,
                    null,
                    List.of(query),
                    Map.of(),
                    InputType.INTERNAL_SEARCH,
                    null,
                    false
                )
            )
            .toList();

        inferenceRequests.forEach(
            request -> executeAsyncWithOrigin(client, ML_ORIGIN, InferenceAction.INSTANCE, request, gal.delegateFailureAndWrap((l, r) -> {
                String inferenceId = request.getInferenceEntityId();
                InferenceResults inferenceResults = validateAndConvertInferenceResults(r.getResults(), inferenceId);
                l.onResponse(Tuple.tuple(inferenceId, inferenceResults));
            }))
        );
    }

    private static InferenceResults validateAndConvertInferenceResults(
        InferenceServiceResults inferenceServiceResults,
        String inferenceId
    ) {
        List<? extends InferenceResults> inferenceResultsList = inferenceServiceResults.transformToCoordinationFormat();
        if (inferenceResultsList.isEmpty()) {
            return new ErrorInferenceResults(
                new IllegalArgumentException("No inference results retrieved for inference ID [" + inferenceId + "]")
            );
        } else if (inferenceResultsList.size() > 1) {
            // We don't chunk queries, so there should always be one inference result.
            // Thus, if we receive more than one inference result, it is a server-side error.
            return new ErrorInferenceResults(
                new IllegalStateException(
                    inferenceResultsList.size() + " inference results retrieved for inference ID [" + inferenceId + "]"
                )
            );
        }

        return inferenceResultsList.getFirst();
    }
}
