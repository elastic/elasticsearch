/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.application.analytics.action.DeleteAnalyticsCollectionAction;
import org.elasticsearch.xpack.application.analytics.action.GetAnalyticsCollectionAction;
import org.elasticsearch.xpack.application.analytics.action.PutAnalyticsCollectionAction;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

/**
 * A service that allows the manipulation of persistent {@link AnalyticsCollection} model.
 * Until we have more specific need the {@link AnalyticsCollection} is just another representation
 * of a {@link org.elasticsearch.cluster.metadata.IndexAbstraction.DataStream}.
 * As a consequence, this service is mostly a facade for the data stream API.
 */
public class AnalyticsCollectionService {

    private final Client clientWithOrigin;

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public AnalyticsCollectionService(Client client, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    /**
     * Retrieve an analytics collection by name {@link AnalyticsCollection}
     *
     * @param state    Cluster state ({@link ClusterState}).
     * @param request  {@link PutAnalyticsCollectionAction.Request} The request.
     * @param listener The action listener to invoke on response/failure.
     */
    public void getAnalyticsCollection(
        ClusterState state,
        GetAnalyticsCollectionAction.Request request,
        ActionListener<GetAnalyticsCollectionAction.Response> listener
    ) {
        List<AnalyticsCollection> collections = analyticsCollections(state, request.getCollectionName());

        if (collections.isEmpty()) {
            listener.onFailure(new ResourceNotFoundException(request.getCollectionName()));
            return;
        }

        listener.onResponse(new GetAnalyticsCollectionAction.Response(collections));
    }

    /**
     * Create a new {@link AnalyticsCollection}
     *
     * @param state    Cluster state ({@link ClusterState}).
     * @param request  {@link PutAnalyticsCollectionAction.Request} The request.
     * @param listener The action listener to invoke on response/failure.
     */
    public void putAnalyticsCollection(
        ClusterState state,
        PutAnalyticsCollectionAction.Request request,
        ActionListener<PutAnalyticsCollectionAction.Response> listener
    ) {
        if (analyticsCollections(state, request.getName()).isEmpty() == false) {
            listener.onFailure(new ResourceAlreadyExistsException(request.getName()));
            return;
        }

        AnalyticsCollection analyticsCollection = new AnalyticsCollection(request.getName());
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            analyticsCollection.getEventDataStream()
        );

        ActionListener<AcknowledgedResponse> createDataStreamListener = ActionListener.wrap(
            resp -> listener.onResponse(new PutAnalyticsCollectionAction.Response(resp.isAcknowledged(), request.getName())),
            listener::onFailure
        );

        clientWithOrigin.execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest, createDataStreamListener);
    }

    /**
     * Delete an analytics collection by name {@link AnalyticsCollection}
     *
     * @param state    Cluster state ({@link ClusterState}).
     * @param request  {@link AnalyticsCollection} name.
     * @param listener The action listener to invoke on response/failure.
     */
    public void deleteAnalyticsCollection(
        ClusterState state,
        DeleteAnalyticsCollectionAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        List<AnalyticsCollection> collections = analyticsCollections(state, request.getCollectionName());

        if (collections.isEmpty()) {
            listener.onFailure(new ResourceNotFoundException(request.getCollectionName()));
            return;
        }

        DeleteDataStreamAction.Request deleteDataStreamRequest = new DeleteDataStreamAction.Request(
            collections.get(0).getEventDataStream()
        );

        clientWithOrigin.execute(DeleteDataStreamAction.INSTANCE, deleteDataStreamRequest, listener);
    }

    private List<AnalyticsCollection> analyticsCollections(ClusterState state, String... expressions) {

        Set<String> collectionNames = Arrays.stream(expressions).collect(Collectors.toSet());

        // Listing data streams that are matching the analytics collection pattern.
        List<String> dataStreams = indexNameExpressionResolver.dataStreamNames(
            state,
            IndicesOptions.lenientExpandOpen(),
            AnalyticsTemplateRegistry.EVENT_DATA_STREAM_INDEX_PATTERN
        );

        // Init an AnalyticsCollection instance from each matching data stream.
        return dataStreams.stream()
            .map(AnalyticsCollection::fromDataStreamName)
            .filter(analyticsCollection -> collectionNames.contains(analyticsCollection.getName()))
            .collect(Collectors.toList());
    }
}
