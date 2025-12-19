/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction.GET_INFERENCE_FIELDS_ACTION_TV;
import static org.mockito.Mockito.when;

public class MockInferenceRemoteClusterClient implements RemoteClusterClient {
    public record RemoteIndexConfig(String indexName, Map<String, String> inferenceFields) {}

    public record RemoteClusterConfig(
        Map<String, MinimalServiceSettings> inferenceEndpoints,
        List<RemoteIndexConfig> indexConfigs,
        TransportVersion transportVersion
    ) {}

    private final Transport.Connection connection;
    private final Map<String, Map<String, InferenceFieldMetadata>> clusterInferenceFieldsMap;
    private final MockInferenceGenerator inferenceGenerator;

    public MockInferenceRemoteClusterClient(RemoteClusterConfig remoteClusterConfig) {
        this.connection = createMockConnection(remoteClusterConfig.transportVersion());
        this.clusterInferenceFieldsMap = createClusterInferenceFieldsMap(remoteClusterConfig.indexConfigs());
        this.inferenceGenerator = new MockInferenceGenerator(remoteClusterConfig.inferenceEndpoints());
    }

    @Override
    public <Request extends ActionRequest, Response extends TransportResponse> void execute(
        Transport.Connection connection,
        RemoteClusterActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        if (action.equals(GetInferenceFieldsAction.REMOTE_TYPE)
            && request instanceof GetInferenceFieldsAction.Request getInferenceFieldsRequest) {

            @SuppressWarnings("unchecked")
            ActionListener<GetInferenceFieldsAction.Response> actionListener = (ActionListener<GetInferenceFieldsAction.Response>) listener;

            if (connection.getTransportVersion().supports(GET_INFERENCE_FIELDS_ACTION_TV) == false) {
                actionListener.onFailure(new IllegalStateException("Mock remote cluster does not support GetInferenceFieldsAction"));
                return;
            }

            final Set<String> indices = getInferenceFieldsRequest.getIndices();
            final Map<String, Float> fields = getInferenceFieldsRequest.getFields();
            final boolean resolveWildcards = getInferenceFieldsRequest.resolveWildcards();
            final boolean useDefaultFields = getInferenceFieldsRequest.useDefaultFields();
            final String query = getInferenceFieldsRequest.getQuery();

            try {
                var inferenceFieldsMap = getInferenceFieldsMap(indices, fields, resolveWildcards, useDefaultFields);
                var inferenceResultsMap = getInferenceResultsMap(inferenceFieldsMap, query);
                actionListener.onResponse(new GetInferenceFieldsAction.Response(inferenceFieldsMap, inferenceResultsMap));
            } catch (Exception e) {
                actionListener.onFailure(e);
            }
        } else {
            listener.onFailure(new IllegalStateException("Unsupported action type"));
        }
    }

    @Override
    public <Request extends ActionRequest> void getConnection(Request request, ActionListener<Transport.Connection> listener) {
        listener.onResponse(connection);
    }

    private Map<String, List<GetInferenceFieldsAction.ExtendedInferenceFieldMetadata>> getInferenceFieldsMap(
        Set<String> indices,
        Map<String, Float> fields,
        boolean resolveWildcards,
        boolean useDefaultFields
    ) {
        Map<String, Float> effectiveFields = fields;
        if (effectiveFields.isEmpty() && useDefaultFields) {
            effectiveFields = Map.of("*", 1.0f);
        }

        Map<String, List<GetInferenceFieldsAction.ExtendedInferenceFieldMetadata>> inferenceFieldsMap = new HashMap<>(indices.size());
        for (String index : indices) {
            var inferenceFieldsMetadataMap = clusterInferenceFieldsMap.get(index);
            if (inferenceFieldsMetadataMap == null) {
                continue;
            }

            var matchingInferenceFieldsMap = IndexMetadata.getMatchingInferenceFields(
                inferenceFieldsMetadataMap,
                effectiveFields,
                resolveWildcards
            );
            List<GetInferenceFieldsAction.ExtendedInferenceFieldMetadata> eifm = matchingInferenceFieldsMap.entrySet()
                .stream()
                .map(e -> new GetInferenceFieldsAction.ExtendedInferenceFieldMetadata(e.getKey(), e.getValue()))
                .toList();

            inferenceFieldsMap.put(index, eifm);
        }

        return inferenceFieldsMap;
    }

    private Map<String, InferenceResults> getInferenceResultsMap(
        Map<String, List<GetInferenceFieldsAction.ExtendedInferenceFieldMetadata>> inferenceFieldsMap,
        @Nullable String query
    ) {
        if (query == null || query.isBlank()) {
            return Map.of();
        }

        Set<String> inferenceIds = inferenceFieldsMap.values()
            .stream()
            .flatMap(List::stream)
            .map(eifm -> eifm.inferenceFieldMetadata().getSearchInferenceId())
            .collect(Collectors.toSet());

        Map<String, InferenceResults> inferenceResultsMap = new HashMap<>(inferenceIds.size());
        inferenceIds.forEach(inferenceId -> {
            InferenceResults inferenceResults = inferenceGenerator.generate(inferenceId, query);
            inferenceResultsMap.put(inferenceId, inferenceResults);
        });

        return inferenceResultsMap;
    }

    private static Transport.Connection createMockConnection(TransportVersion transportVersion) {
        Transport.Connection connection = Mockito.mock(Transport.Connection.class);
        when(connection.getTransportVersion()).thenReturn(transportVersion);
        return connection;
    }

    private static Map<String, Map<String, InferenceFieldMetadata>> createClusterInferenceFieldsMap(List<RemoteIndexConfig> indexConfigs) {
        Map<String, Map<String, InferenceFieldMetadata>> clusterInferenceFieldsMap = new HashMap<>();
        for (RemoteIndexConfig indexConfig : indexConfigs) {
            Map<String, InferenceFieldMetadata> inferenceFieldMetadataMap = new HashMap<>();
            indexConfig.inferenceFields().forEach((fieldName, inferenceId) -> {
                InferenceFieldMetadata inferenceFieldMetadata = new InferenceFieldMetadata(
                    fieldName,
                    inferenceId,
                    new String[] { fieldName },
                    null
                );
                inferenceFieldMetadataMap.put(fieldName, inferenceFieldMetadata);
            });
            clusterInferenceFieldsMap.put(indexConfig.indexName(), inferenceFieldMetadataMap);
        }

        return clusterInferenceFieldsMap;
    }
}
