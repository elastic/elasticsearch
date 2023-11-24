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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.ToXContent;

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
     * @param connector The connector object.
     * @param listener  The action listener to invoke on response/failure.
     */
    public void putConnector(Connector connector, ActionListener<DocWriteResponse> listener) {
        try {
            final IndexRequest indexRequest = new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                .id(connector.getConnectorId())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(connector.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS));
            clientWithOrigin.index(indexRequest, listener);
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
    public void getConnector(String connectorId, ActionListener<BytesReference> listener) {

        final GetRequest getRequest = new GetRequest(CONNECTOR_INDEX_NAME).id(connectorId).realtime(true);

        clientWithOrigin.get(getRequest, new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, getResponse) -> {
            if (getResponse.isExists() == false) {
                l.onFailure(new ResourceNotFoundException(connectorId));
                return;
            }
            l.onResponse(getResponse.getSourceAsBytesRef());
        }));
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
     * List the {@link Connector} in ascending order of their ids.
     *
     * @param from From index to start the search from.
     * @param size The maximum number of {@link ConnectorListItem}s to return.
     * @param listener The action listener to invoke on response/failure.
     */
    public void listConnectors(int from, int size, ActionListener<ConnectorIndexService.ConnectorResult> listener) {
        try {
            final SearchSourceBuilder source = new SearchSourceBuilder().from(from)
                .size(size)
                .query(new MatchAllQueryBuilder())
                .fetchSource(new String[] { Connector.ID_FIELD.getPreferredName(), Connector.NAME_FIELD.getPreferredName() }, null)
                .sort(Connector.ID_FIELD.getPreferredName(), SortOrder.ASC);
            final SearchRequest req = new SearchRequest(CONNECTOR_INDEX_NAME).source(source);
            clientWithOrigin.search(req, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    listener.onResponse(mapSearchResponseToConnectorList(searchResponse));
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

    private static ConnectorIndexService.ConnectorResult mapSearchResponseToConnectorList(SearchResponse response) {
        final List<ConnectorListItem> connectorResults = Arrays.stream(response.getHits().getHits())
            .map(ConnectorIndexService::hitToConnectorListItem)
            .toList();
        return new ConnectorIndexService.ConnectorResult(connectorResults, (int) response.getHits().getTotalHits().value);
    }

    private static ConnectorListItem hitToConnectorListItem(SearchHit searchHit) {

        // todo: don't return sensitive data in list endpoint

        final Map<String, Object> sourceMap = searchHit.getSourceAsMap();
        final String connectorId = (String) sourceMap.get(Connector.ID_FIELD.getPreferredName());
        final String name = (String) sourceMap.get(Connector.NAME_FIELD.getPreferredName());

        return new ConnectorListItem(connectorId, name);
    }

    public record ConnectorResult(List<ConnectorListItem> connectors, long totalResults) {}

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
