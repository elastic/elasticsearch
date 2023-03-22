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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.application.analytics.action.DeleteAnalyticsCollectionAction;
import org.elasticsearch.xpack.application.analytics.action.GetAnalyticsCollectionAction;
import org.elasticsearch.xpack.application.analytics.action.PutAnalyticsCollectionAction;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

/**
 * A service that allows the manipulation of persistent {@link AnalyticsCollection} model.
 * Until we have more specific need the {@link AnalyticsCollection} is just another representation
 * of a {@link org.elasticsearch.cluster.metadata.DataStream}.
 * As a consequence, this service is mostly a facade for the data stream API.
 */
public class AnalyticsCollectionService {

    private final Client clientWithOrigin;

    private final AnalyticsCollectionResolver analyticsCollectionResolver;

    @Inject
    public AnalyticsCollectionService(Client client, AnalyticsCollectionResolver analyticsCollectionResolver) {
        this.clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
        this.analyticsCollectionResolver = analyticsCollectionResolver;
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
        // This operation is supposed to be executed on the master node only.
        assert (state.nodes().isLocalNodeElectedMaster());

        listener.onResponse(new GetAnalyticsCollectionAction.Response(analyticsCollectionResolver.collections(state, request.getNames())));
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
        // This operation is supposed to be executed on the master node only.
        assert (state.nodes().isLocalNodeElectedMaster());

        try {
            analyticsCollectionResolver.collection(state, request.getName());
            listener.onFailure(new ResourceAlreadyExistsException("analytics collection {} already exists", request.getName()));
        } catch (ResourceNotFoundException e) {
            AnalyticsCollection collection = new AnalyticsCollection(request.getName());
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(collection.getEventDataStream());

            ActionListener<AcknowledgedResponse> createDataStreamListener = ActionListener.wrap(
                resp -> listener.onResponse(new PutAnalyticsCollectionAction.Response(resp.isAcknowledged(), request.getName())),
                listener::onFailure
            );

            clientWithOrigin.execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest, createDataStreamListener);
        }
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
        // This operation is supposed to be executed on the master node.
        assert (state.nodes().isLocalNodeElectedMaster());

        try {
            AnalyticsCollection collection = analyticsCollectionResolver.collection(state, request.getCollectionName());
            DeleteDataStreamAction.Request deleteDataStreamRequest = new DeleteDataStreamAction.Request(collection.getEventDataStream());
            clientWithOrigin.execute(DeleteDataStreamAction.INSTANCE, deleteDataStreamRequest, listener);
        } catch (ResourceNotFoundException e) {
            listener.onFailure(e);
        }
    }
}
