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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.application.analytics.action.DeleteAnalyticsCollectionAction;
import org.elasticsearch.xpack.application.analytics.action.GetAnalyticsCollectionAction;
import org.elasticsearch.xpack.application.analytics.action.PutAnalyticsCollectionAction;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

/**
 * A service that allows the manipulation of persistent {@link AnalyticsCollection} model.
 *
 * Until we have more specific need the {@link AnalyticsCollection} is just another representation
 * of a {@link org.elasticsearch.cluster.metadata.IndexAbstraction.DataStream}.
 *
 * As a consequence, this service is mostly a facade for the data stream API.
 */
public class AnalyticsCollectionService implements ClusterStateListener {

    private final Client clientWithOrigin;

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private volatile Map<String, AnalyticsCollection> analyticsCollections = Collections.emptyMap();

    public AnalyticsCollectionService(
        Client client,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        this.clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        clusterService.addListener(this);
    }

    /**
     * Retrieve an analytics collection by name {@link AnalyticsCollection}
     *
     * @param request  {@link PutAnalyticsCollectionAction.Request} The request.
     * @param listener The action listener to invoke on response/failure.
     */
    public void getAnalyticsCollection(
        GetAnalyticsCollectionAction.Request request,
        ActionListener<GetAnalyticsCollectionAction.Response> listener
    ) {
        Map<String, AnalyticsCollection> collections = analyticsCollections;

        if (collections.containsKey(request.getCollectionName()) == false) {
            listener.onFailure(new ResourceNotFoundException(request.getCollectionName()));
            return;
        }

        listener.onResponse(
            new GetAnalyticsCollectionAction.Response(Collections.singletonList(collections.get(request.getCollectionName())))
        );
    }

    /**
     * Create a new {@link AnalyticsCollection}
     *
     * @param request  {@link PutAnalyticsCollectionAction.Request} The request.
     * @param listener The action listener to invoke on response/failure.
     */
    public void putAnalyticsCollection(
        PutAnalyticsCollectionAction.Request request,
        ActionListener<PutAnalyticsCollectionAction.Response> listener
    ) {
        if (analyticsCollections.containsKey(request.getName())) {
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
     * @param request  {@link AnalyticsCollection} name.
     * @param listener The action listener to invoke on response/failure.
     */
    public void deleteAnalyticsCollection(DeleteAnalyticsCollectionAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        Map<String, AnalyticsCollection> collections = analyticsCollections;

        if (collections.containsKey(request.getCollectionName()) == false) {
            listener.onFailure(new ResourceNotFoundException(request.getCollectionName()));
            return;
        }

        DeleteDataStreamAction.Request deleteDataStreamRequest = new DeleteDataStreamAction.Request(
            collections.get(request.getCollectionName()).getEventDataStream()
        );

        clientWithOrigin.execute(DeleteDataStreamAction.INSTANCE, deleteDataStreamRequest, listener);
    }

    /**
     * We refresh the local cache of the collections when cluster is updated.
     *
     * @param event {@link ClusterChangedEvent} event.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.metadataChanged() == false) {
            // Skipping the update if cluster metadata did not change.
            return;
        }

        // Listing data streams that are matching the analytics collection pattern.
        List<String> dataStreams = indexNameExpressionResolver.dataStreamNames(
            event.state(),
            IndicesOptions.lenientExpandOpen(),
            AnalyticsTemplateRegistry.EVENT_DATA_STREAM_INDEX_PATTERN
        );

        // Init an AnalyticsCollection instance from each matching data stream.
        analyticsCollections = dataStreams.stream()
            .map(AnalyticsCollection::fromDataStreamName)
            .collect(Collectors.toMap(AnalyticsCollection::getName, Function.identity()));
    }
}
