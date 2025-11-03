/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.index.IndexSettings.DEFAULT_FIELD_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

// TODO: Handle multi-project

public class TransportGetInferenceFieldsAction extends HandledTransportAction<
    GetInferenceFieldsAction.Request,
    GetInferenceFieldsAction.Response> {

    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public TransportGetInferenceFieldsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client
    ) {
        super(
            GetInferenceFieldsAction.NAME,
            transportService,
            actionFilters,
            GetInferenceFieldsAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void doExecute(
        Task task,
        GetInferenceFieldsAction.Request request,
        ActionListener<GetInferenceFieldsAction.Response> listener
    ) {
        final Set<String> indices = request.getIndices();
        final Set<String> fields = request.getFields();
        final boolean resolveWildcards = request.resolveWildcards();
        final boolean useDefaultFields = request.useDefaultFields();
        final String query = request.getQuery();

        Map<String, List<InferenceFieldMetadata>> inferenceFieldsMap = new HashMap<>(indices.size());
        indices.forEach(index -> {
            List<InferenceFieldMetadata> inferenceFieldMetadataList = getInferenceFieldMetadata(
                index,
                fields,
                resolveWildcards,
                useDefaultFields
            );
            if (inferenceFieldMetadataList != null) {
                inferenceFieldsMap.put(index, inferenceFieldMetadataList);
            }
        });

        if (query != null && query.isBlank() == false) {
            Set<String> inferenceIds = inferenceFieldsMap.values()
                .stream()
                .flatMap(List::stream)
                .map(InferenceFieldMetadata::getSearchInferenceId)
                .collect(Collectors.toSet());

            getInferenceResults(query, inferenceIds, inferenceFieldsMap, listener);
        } else {
            listener.onResponse(new GetInferenceFieldsAction.Response(inferenceFieldsMap, Map.of()));
        }
    }

    private List<InferenceFieldMetadata> getInferenceFieldMetadata(
        String index,
        Set<String> fields,
        boolean resolveWildcards,
        boolean useDefaultFields
    ) {
        ClusterState clusterState = clusterService.state();
        IndexMetadata indexMetadata = clusterState.getMetadata().getProject().indices().get(index);
        if (indexMetadata == null) {
            return null;
        }

        Map<String, InferenceFieldMetadata> inferenceFieldsMap = indexMetadata.getInferenceFields();
        List<InferenceFieldMetadata> inferenceFieldMetadataList = new ArrayList<>();
        Set<String> effectiveFields = fields.isEmpty() && useDefaultFields ? getDefaultFields(indexMetadata.getSettings()) : fields;
        for (String field : effectiveFields) {
            if (inferenceFieldsMap.containsKey(field)) {
                // No wildcards in field name
                inferenceFieldMetadataList.add(inferenceFieldsMap.get(field));
            } else if (resolveWildcards) {
                if (Regex.isMatchAllPattern(field)) {
                    inferenceFieldMetadataList.addAll(inferenceFieldsMap.values());
                } else if (Regex.isSimpleMatchPattern(field)) {
                    inferenceFieldsMap.values()
                        .stream()
                        .filter(ifm -> Regex.simpleMatch(field, ifm.getName()))
                        .forEach(inferenceFieldMetadataList::add);
                }
            }
        }

        return inferenceFieldMetadataList;
    }

    private void getInferenceResults(
        String query,
        Set<String> inferenceIds,
        Map<String, List<InferenceFieldMetadata>> inferenceFieldsMap,
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

    private static Set<String> getDefaultFields(Settings settings) {
        List<String> defaultFieldsWithWeights = settings.getAsList(
            DEFAULT_FIELD_SETTING.getKey(),
            DEFAULT_FIELD_SETTING.getDefault(settings)
        );
        return QueryParserHelper.parseFieldsAndWeights(defaultFieldsWithWeights).keySet();
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
