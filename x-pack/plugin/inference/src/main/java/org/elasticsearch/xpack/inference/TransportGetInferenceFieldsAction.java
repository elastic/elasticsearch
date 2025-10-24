/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.IndexSettings.DEFAULT_FIELD_SETTING;

// TODO: Handle multi-project
// TODO: Don't hard-code useDefaultFields

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
        final List<String> indices = request.getIndices();
        final List<String> fields = request.getFields();
        final boolean resolveWildcards = request.resolveWildcards();

        Map<String, List<InferenceFieldMetadata>> inferenceFieldsMap = new HashMap<>(indices.size());
        indices.forEach(index -> {
            List<InferenceFieldMetadata> inferenceFieldMetadataList = getInferenceFieldMetadata(index, fields, resolveWildcards, false);
            if (inferenceFieldMetadataList != null) {
                inferenceFieldsMap.put(index, inferenceFieldMetadataList);
            }
        });

        listener.onResponse(new GetInferenceFieldsAction.Response(inferenceFieldsMap, Map.of()));
    }

    private List<InferenceFieldMetadata> getInferenceFieldMetadata(
        String index,
        List<String> fields,
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
        List<String> effectiveFields = fields.isEmpty() && useDefaultFields ? getDefaultFields(indexMetadata.getSettings()) : fields;
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

    private static List<String> getDefaultFields(Settings settings) {
        return settings.getAsList(DEFAULT_FIELD_SETTING.getKey(), DEFAULT_FIELD_SETTING.getDefault(settings));
    }
}
