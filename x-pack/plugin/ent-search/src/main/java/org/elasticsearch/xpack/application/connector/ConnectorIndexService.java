/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.connector.action.PostConnectorAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorConfigurationAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorErrorAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorFilteringAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorLastSeenAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorLastSyncStatsAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorNameAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorPipelineAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorSchedulingAction;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.CONNECTORS_ORIGIN;

/**
 * A service that manages persistent {@link Connector} configurations.
 */
public class ConnectorIndexService {

    private final Client clientWithOrigin;

    public static final String CONNECTOR_INDEX_NAME = ConnectorTemplateRegistry.CONNECTOR_INDEX_NAME_PATTERN;

    /**
     * @param client A client for executing actions on the connector index
     */
    public ConnectorIndexService(Client client) {
        this.clientWithOrigin = new OriginSettingClient(client, CONNECTORS_ORIGIN);
    }

    /**
     * Creates or updates the {@link Connector} in the underlying index.
     *
     * @param docId     The ID of the connector.
     * @param connector The connector object.
     * @param listener  The action listener to invoke on response/failure.
     */
    public void putConnector(String docId, Connector connector, ActionListener<DocWriteResponse> listener) {
        try {
            final IndexRequest indexRequest = new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                .id(docId)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(connector.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS));
            clientWithOrigin.index(indexRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Creates or updates the {@link Connector} in the underlying index.
     * Assigns connector an auto-generated doc ID.
     *
     * @param connector The connector object.
     * @param listener  The action listener to invoke on response/failure.
     */
    public void postConnector(Connector connector, ActionListener<PostConnectorAction.Response> listener) {
        try {
            final IndexRequest indexRequest = new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(connector.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS));

            clientWithOrigin.index(
                indexRequest,
                listener.delegateFailureAndWrap((l, indexResponse) -> l.onResponse(new PostConnectorAction.Response(indexResponse.getId())))
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Gets the {@link Connector} from the underlying index.
     *
     * @param connectorId The id of the connector object.
     * @param listener    The action listener to invoke on response/failure.
     */
    public void getConnector(String connectorId, ActionListener<Connector> listener) {
        try {
            final GetRequest getRequest = new GetRequest(CONNECTOR_INDEX_NAME).id(connectorId).realtime(true);

            clientWithOrigin.get(getRequest, new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, getResponse) -> {
                if (getResponse.isExists() == false) {
                    l.onFailure(new ResourceNotFoundException(connectorId));
                    return;
                }
                try {
                    final Connector connector = Connector.fromXContentBytes(
                        getResponse.getSourceAsBytesRef(),
                        connectorId,
                        XContentType.JSON
                    );
                    l.onResponse(connector);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Deletes the {@link Connector} in the underlying index.
     *
     * @param connectorId The id of the connector object.
     * @param listener    The action listener to invoke on response/failure.
     */
    public void deleteConnector(String connectorId, ActionListener<DeleteResponse> listener) {

        final DeleteRequest deleteRequest = new DeleteRequest(CONNECTOR_INDEX_NAME).id(connectorId)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        try {
            clientWithOrigin.delete(
                deleteRequest,
                new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, deleteResponse) -> {
                    if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                        l.onFailure(new ResourceNotFoundException(connectorId));
                        return;
                    }
                    l.onResponse(deleteResponse);
                })
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }

    }

    /**
     * List the {@link Connector} in ascending order of their index names.
     *
     * @param from From index to start the search from.
     * @param size The maximum number of {@link Connector}s to return.
     * @param listener The action listener to invoke on response/failure.
     */
    public void listConnectors(int from, int size, ActionListener<ConnectorIndexService.ConnectorResult> listener) {
        try {
            final SearchSourceBuilder source = new SearchSourceBuilder().from(from)
                .size(size)
                .query(new MatchAllQueryBuilder())
                .fetchSource(true)
                .sort(Connector.INDEX_NAME_FIELD.getPreferredName(), SortOrder.ASC);
            final SearchRequest req = new SearchRequest(CONNECTOR_INDEX_NAME).source(source);
            clientWithOrigin.search(req, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    try {
                        listener.onResponse(mapSearchResponseToConnectorList(searchResponse));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof IndexNotFoundException) {
                        listener.onResponse(new ConnectorIndexService.ConnectorResult(Collections.emptyList(), 0L));
                        return;
                    }
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Updates the {@link ConnectorConfiguration} property of a {@link Connector}.
     *
     * @param request   Request for updating connector configuration property.
     * @param listener  Listener to respond to a successful response or an error.
     */
    public void updateConnectorConfiguration(UpdateConnectorConfigurationAction.Request request, ActionListener<UpdateResponse> listener) {
        try {
            String connectorId = request.getConnectorId();
            final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                    .id(connectorId)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(
                        Map.of(
                            Connector.CONFIGURATION_FIELD.getPreferredName(),
                            request.getConfiguration(),
                            Connector.STATUS_FIELD.getPreferredName(),
                            ConnectorStatus.CONFIGURED.toString()
                        )
                    )
            );
            clientWithOrigin.update(
                updateRequest,
                new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        l.onFailure(new ResourceNotFoundException(connectorId));
                        return;
                    }
                    l.onResponse(updateResponse);
                })
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Updates the error property of a {@link Connector}.
     *
     * @param request  The request for updating the connector's error.
     * @param listener The listener for handling responses, including successful updates or errors.
     */
    public void updateConnectorError(UpdateConnectorErrorAction.Request request, ActionListener<UpdateResponse> listener) {
        try {
            String connectorId = request.getConnectorId();
            final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                    .id(connectorId)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(request.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
            );
            clientWithOrigin.update(
                updateRequest,
                new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        l.onFailure(new ResourceNotFoundException(connectorId));
                        return;
                    }
                    l.onResponse(updateResponse);
                })
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Updates the name and/or description property of a {@link Connector}.
     *
     * @param request  The request for updating the connector's name and/or description.
     * @param listener The listener for handling responses, including successful updates or errors.
     */
    public void updateConnectorNameOrDescription(UpdateConnectorNameAction.Request request, ActionListener<UpdateResponse> listener) {
        try {
            String connectorId = request.getConnectorId();
            final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                    .id(connectorId)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(request.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
            );
            clientWithOrigin.update(
                updateRequest,
                new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        l.onFailure(new ResourceNotFoundException(connectorId));
                        return;
                    }
                    l.onResponse(updateResponse);
                })
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Updates the {@link ConnectorFiltering} property of a {@link Connector}.
     *
     * @param request   Request for updating connector filtering property.
     * @param listener  Listener to respond to a successful response or an error.
     */
    public void updateConnectorFiltering(UpdateConnectorFilteringAction.Request request, ActionListener<UpdateResponse> listener) {
        try {
            String connectorId = request.getConnectorId();
            final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                    .id(connectorId)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(request.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
            );
            clientWithOrigin.update(
                updateRequest,
                new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        l.onFailure(new ResourceNotFoundException(connectorId));
                        return;
                    }
                    l.onResponse(updateResponse);
                })
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Updates the lastSeen property of a {@link Connector}.
     *
     * @param request  The request for updating the connector's lastSeen status.
     * @param listener The listener for handling responses, including successful updates or errors.
     */
    public void updateConnectorLastSeen(UpdateConnectorLastSeenAction.Request request, ActionListener<UpdateResponse> listener) {
        try {
            String connectorId = request.getConnectorId();
            final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                    .id(connectorId)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(request.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
            );
            clientWithOrigin.update(
                updateRequest,
                new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        l.onFailure(new ResourceNotFoundException(connectorId));
                        return;
                    }
                    l.onResponse(updateResponse);
                })
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Updates the {@link ConnectorSyncInfo} properties in a {@link Connector}.
     *
     * @param request   Request for updating connector last sync stats properties.
     * @param listener  Listener to respond to a successful response or an error.
     */
    public void updateConnectorLastSyncStats(UpdateConnectorLastSyncStatsAction.Request request, ActionListener<UpdateResponse> listener) {
        try {
            String connectorId = request.getConnectorId();
            final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                    .id(connectorId)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(request.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
            );
            clientWithOrigin.update(
                updateRequest,
                new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        l.onFailure(new ResourceNotFoundException(connectorId));
                        return;
                    }
                    l.onResponse(updateResponse);
                })
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Updates the {@link ConnectorIngestPipeline} property of a {@link Connector}.
     *
     * @param request   Request for updating connector ingest pipeline property.
     * @param listener  Listener to respond to a successful response or an error.
     */
    public void updateConnectorPipeline(UpdateConnectorPipelineAction.Request request, ActionListener<UpdateResponse> listener) {
        try {
            String connectorId = request.getConnectorId();
            final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                    .id(connectorId)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(request.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
            );
            clientWithOrigin.update(
                updateRequest,
                new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        l.onFailure(new ResourceNotFoundException(connectorId));
                        return;
                    }
                    l.onResponse(updateResponse);
                })
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Updates the {@link ConnectorScheduling} property of a {@link Connector}.
     *
     * @param request  The request for updating the connector's scheduling.
     * @param listener The listener for handling responses, including successful updates or errors.
     */
    public void updateConnectorScheduling(UpdateConnectorSchedulingAction.Request request, ActionListener<UpdateResponse> listener) {
        try {
            String connectorId = request.getConnectorId();
            final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                    .id(connectorId)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(request.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
            );
            clientWithOrigin.update(
                updateRequest,
                new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        l.onFailure(new ResourceNotFoundException(connectorId));
                        return;
                    }
                    l.onResponse(updateResponse);
                })
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static ConnectorIndexService.ConnectorResult mapSearchResponseToConnectorList(SearchResponse response) {
        final List<Connector> connectorResults = Arrays.stream(response.getHits().getHits())
            .map(ConnectorIndexService::hitToConnector)
            .toList();
        return new ConnectorIndexService.ConnectorResult(connectorResults, (int) response.getHits().getTotalHits().value);
    }

    private static Connector hitToConnector(SearchHit searchHit) {

        // todo: don't return sensitive data from configuration in list endpoint

        return Connector.fromXContentBytes(searchHit.getSourceRef(), searchHit.getId(), XContentType.JSON);
    }

    public record ConnectorResult(List<Connector> connectors, long totalResults) {}

    /**
     * Listeners that checks failures for IndexNotFoundException, and transforms them in ResourceNotFoundException,
     * invoking onFailure on the delegate listener
     */
    static class DelegatingIndexNotFoundActionListener<T, R> extends DelegatingActionListener<T, R> {

        private final BiConsumer<ActionListener<R>, T> bc;
        private final String connectorId;

        DelegatingIndexNotFoundActionListener(String connectorId, ActionListener<R> delegate, BiConsumer<ActionListener<R>, T> bc) {
            super(delegate);
            this.bc = bc;
            this.connectorId = connectorId;
        }

        @Override
        public void onResponse(T t) {
            bc.accept(delegate, t);
        }

        @Override
        public void onFailure(Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof IndexNotFoundException) {
                delegate.onFailure(new ResourceNotFoundException("connector [" + connectorId + "] not found"));
                return;
            }
            delegate.onFailure(e);
        }
    }
}
