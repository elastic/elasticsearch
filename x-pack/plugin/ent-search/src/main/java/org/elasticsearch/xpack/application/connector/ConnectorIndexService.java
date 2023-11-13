/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.ClusterSettings;
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

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class ConnectorIndexService {

    private final Client clientWithOrigin;
    private final ClusterSettings clusterSettings;

    public static final String CONNECTOR_INDEX_NAME = ConnectorTemplateRegistry.CONNECTOR_INDEX_NAME_PATTERN;

    public ConnectorIndexService(Client clientWithOrigin, ClusterSettings clusterSettings) {
        this.clientWithOrigin = clientWithOrigin;
        this.clusterSettings = clusterSettings;
    }

    private void validateConnector(Connector connector) {
        // todo: index service level validations
    }

    public void putConnector(Connector connector, ActionListener<DocWriteResponse> listener) {
        try {
            validateConnector(connector);
            final IndexRequest indexRequest = new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                .id(connector.id())
                .opType(DocWriteRequest.OpType.INDEX)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(connector.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS));
            clientWithOrigin.index(indexRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void deleteConnector(String connectorId, ActionListener<DeleteResponse> listener) {
        final DeleteRequest deleteRequest = new DeleteRequest(CONNECTOR_INDEX_NAME).id(connectorId)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        clientWithOrigin.delete(deleteRequest, new ActionListener<>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    listener.onFailure(new ResourceNotFoundException(connectorId));
                    return;
                }
                listener.onResponse(deleteResponse);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof IndexNotFoundException) {
                    listener.onFailure(new ResourceNotFoundException(connectorId));
                }
                listener.onFailure(e);
            }
        });

    }

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
}
