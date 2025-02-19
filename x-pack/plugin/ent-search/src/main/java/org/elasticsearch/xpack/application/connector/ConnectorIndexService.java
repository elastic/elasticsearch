/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.connector.action.ConnectorCreateActionResponse;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorApiKeyIdAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorConfigurationAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorIndexNameAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorLastSyncStatsAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorNameAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorNativeAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorPipelineAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorSchedulingAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorServiceTypeAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorStatusAction;
import org.elasticsearch.xpack.application.connector.filtering.FilteringAdvancedSnippet;
import org.elasticsearch.xpack.application.connector.filtering.FilteringRule;
import org.elasticsearch.xpack.application.connector.filtering.FilteringRules;
import org.elasticsearch.xpack.application.connector.filtering.FilteringValidationInfo;
import org.elasticsearch.xpack.application.connector.filtering.FilteringValidationState;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJob;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobIndexService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.application.connector.ConnectorFiltering.fromXContentBytesConnectorFiltering;
import static org.elasticsearch.xpack.application.connector.ConnectorFiltering.sortFilteringRulesByOrder;
import static org.elasticsearch.xpack.application.connector.ConnectorTemplateRegistry.MANAGED_CONNECTOR_INDEX_PREFIX;
import static org.elasticsearch.xpack.core.ClientHelper.CONNECTORS_ORIGIN;

/**
 * A service that manages persistent {@link Connector} configurations.
 */
public class ConnectorIndexService {

    // The client to interact with the system index (internal user).
    private final Client clientWithOrigin;

    public static final String CONNECTOR_INDEX_NAME = ConnectorTemplateRegistry.CONNECTOR_INDEX_NAME_PATTERN;

    /**
     * @param client A client for executing actions on the connector index
     */
    public ConnectorIndexService(Client client) {
        this.clientWithOrigin = new OriginSettingClient(client, CONNECTORS_ORIGIN);
    }

    /**
     * Creates or updates the {@link Connector} in the underlying index with a specific doc ID
     * if connectorId is provided. Otherwise, the connector doc is indexed with auto-generated doc ID.
     * @param connectorId The id of the connector object. If null, id will be auto-generated.
     * @param description The description of the connector.
     * @param indexName   The name of the index associated with the connector. It can be null to indicate that index is not attached yet.
     * @param isNative    Flag indicating if the connector is native; defaults to false if null.
     * @param language    The language supported by the connector.
     * @param name        The name of the connector; defaults to an empty string if null.
     * @param serviceType The type of service the connector integrates with.
     * @param listener  The action listener to invoke on response/failure.
     */
    public void createConnector(
        String connectorId,
        String description,
        String indexName,
        Boolean isNative,
        String language,
        String name,
        String serviceType,
        ActionListener<ConnectorCreateActionResponse> listener
    ) {
        Connector connector = createConnectorWithDefaultValues(description, indexName, isNative, language, name, serviceType);
        try {
            isDataIndexNameAlreadyInUse(indexName, connectorId, listener.delegateFailure((l, isIndexNameInUse) -> {
                if (isIndexNameInUse) {
                    l.onFailure(
                        new ElasticsearchStatusException(
                            "Index name [" + indexName + "] is used by another connector.",
                            RestStatus.BAD_REQUEST
                        )
                    );
                    return;
                }
                try {
                    IndexRequest indexRequest = new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(connector.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS));

                    if (Strings.isNullOrEmpty(connectorId) == false) {
                        indexRequest = indexRequest.id(connectorId);
                    }

                    clientWithOrigin.index(
                        indexRequest,
                        listener.delegateFailureAndWrap(
                            (ll, indexResponse) -> ll.onResponse(
                                new ConnectorCreateActionResponse(indexResponse.getId(), indexResponse.getResult())
                            )
                        )
                    );
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Creates a Connector with default values and specified parameters.
     *
     * @param description The description of the connector.
     * @param indexName   The name of the index associated with the connector. It can be null to indicate that index is not attached yet.
     * @param isNative    Flag indicating if the connector is native; defaults to false if null.
     * @param language    The language supported by the connector.
     * @param name        The name of the connector; defaults to an empty string if null.
     * @param serviceType The type of service the connector integrates with.
     * @return A new instance of Connector with the specified values and default settings.
     */
    private Connector createConnectorWithDefaultValues(
        String description,
        String indexName,
        Boolean isNative,
        String language,
        String name,
        String serviceType
    ) {
        boolean isNativeConnector = Objects.requireNonNullElse(isNative, false);
        ConnectorStatus status = isNativeConnector ? ConnectorStatus.NEEDS_CONFIGURATION : ConnectorStatus.CREATED;

        return new Connector.Builder().setConfiguration(Collections.emptyMap())
            .setCustomScheduling(Collections.emptyMap())
            .setDescription(description)
            .setFiltering(List.of(ConnectorFiltering.getDefaultConnectorFilteringConfig()))
            .setIndexName(indexName)
            .setIsNative(isNativeConnector)
            .setLanguage(language)
            .setSyncInfo(new ConnectorSyncInfo.Builder().build())
            .setName(Objects.requireNonNullElse(name, ""))
            .setScheduling(ConnectorScheduling.getDefaultConnectorScheduling())
            .setServiceType(serviceType)
            .setStatus(status)
            .build();
    }

    /**
     * Gets the {@link Connector} from the underlying index.
     *
     * @param connectorId     The id of the connector object.
     * @param includeDeleted  If false, returns only the non-deleted connector with the matching ID;
     *                        if true, returns the connector with the matching ID.
     * @param listener        The action listener to invoke on response/failure.
     */
    public void getConnector(String connectorId, boolean includeDeleted, ActionListener<ConnectorSearchResult> listener) {

        try {
            final GetRequest getRequest = new GetRequest(CONNECTOR_INDEX_NAME).id(connectorId).realtime(true);

            clientWithOrigin.get(getRequest, new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, getResponse) -> {
                if (getResponse.isExists() == false) {
                    l.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
                    return;
                }
                try {
                    final ConnectorSearchResult connector = new ConnectorSearchResult.Builder().setId(connectorId)
                        .setResultBytes(getResponse.getSourceAsBytesRef())
                        .setResultMap(getResponse.getSourceAsMap())
                        .build();

                    boolean connectorIsSoftDeleted = includeDeleted == false && isConnectorDeleted(connector);

                    if (connectorIsSoftDeleted) {
                        l.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
                        return;
                    }
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
     * Deletes the {@link Connector} and optionally removes the related instances of {@link ConnectorSyncJob} in the underlying index.
     *
     * @param connectorId          The id of the {@link Connector}.
     * @param hardDelete           If set to true, the {@link Connector} is permanently deleted; otherwise, it is soft-deleted.
     * @param shouldDeleteSyncJobs The flag indicating if {@link ConnectorSyncJob} should also be deleted.
     * @param listener             The action listener to invoke on response/failure.
     */
    public void deleteConnector(
        String connectorId,
        boolean hardDelete,
        boolean shouldDeleteSyncJobs,
        ActionListener<DocWriteResponse> listener
    ) {

        try {
            if (hardDelete) {
                final DeleteRequest deleteRequest = new DeleteRequest(CONNECTOR_INDEX_NAME).id(connectorId)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

                clientWithOrigin.delete(
                    deleteRequest,
                    new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, deleteResponse) -> {
                        if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                            l.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
                            return;
                        }
                        if (shouldDeleteSyncJobs) {
                            new ConnectorSyncJobIndexService(clientWithOrigin).deleteAllSyncJobsByConnectorId(
                                connectorId,
                                l.map(r -> deleteResponse)
                            );
                        } else {
                            l.onResponse(deleteResponse);
                        }
                    })
                );
            } else {
                getConnector(connectorId, false, listener.delegateFailure((l, connector) -> {
                    final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).setRefreshPolicy(
                        WriteRequest.RefreshPolicy.IMMEDIATE
                    )
                        .doc(
                            new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                                .id(connectorId)
                                .source(Map.of(Connector.IS_DELETED_FIELD.getPreferredName(), true))
                        );
                    clientWithOrigin.update(
                        updateRequest,
                        new DelegatingIndexNotFoundActionListener<>(connectorId, l, (ll, updateResponse) -> {
                            if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                                ll.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
                                return;
                            }
                            if (shouldDeleteSyncJobs) {
                                new ConnectorSyncJobIndexService(clientWithOrigin).deleteAllSyncJobsByConnectorId(
                                    connectorId,
                                    ll.map(r -> updateResponse)
                                );
                            } else {
                                ll.onResponse(updateResponse);
                            }
                        })
                    );
                }));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Lists {@link Connector}s in ascending order of index names, filtered by specified criteria.
     *
     * @param from Starting index for the search.
     * @param size Maximum number of {@link Connector}s to retrieve.
     * @param indexNames Filter connectors by these index names, if provided.
     * @param connectorNames Filter connectors by connector names, if provided.
     * @param serviceTypes Filter connectors by service types, if provided.
     * @param searchQuery Apply a wildcard search on index name, connector name, and description, if provided.
     * @param includeDeleted  If false, filters to include only non-deleted connectors; if true, no filter is applied.
     * @param listener Invoked with search results or upon failure.
     */
    public void listConnectors(
        int from,
        int size,
        List<String> indexNames,
        List<String> connectorNames,
        List<String> serviceTypes,
        String searchQuery,
        boolean includeDeleted,
        ActionListener<ConnectorIndexService.ConnectorResult> listener
    ) {
        try {
            final SearchSourceBuilder source = new SearchSourceBuilder().from(from)
                .size(size)
                .query(buildListQuery(indexNames, connectorNames, serviceTypes, searchQuery, includeDeleted))
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
     * Builds a query to filter {@link Connector} instances by index names, connector names, service type, and/or search query.
     * Returns a {@link MatchAllQueryBuilder} if no filters are applied, otherwise constructs a boolean query with the specified filters.
     *
     * @param indexNames List of index names for filtering, or null/empty to skip.
     * @param connectorNames List of connector names for filtering, or null/empty to skip.
     * @param serviceTypes List of connector service types for filtering, or null/empty to skip.
     * @param searchQuery Search query for wildcard filtering on index name, connector name, and description, or null/empty to skip.
     * @param includeDeleted  If false, filters to include only non-deleted connectors; if true, no filter is applied.
     * @return A {@link QueryBuilder} customized based on provided filters.
     */
    private QueryBuilder buildListQuery(
        List<String> indexNames,
        List<String> connectorNames,
        List<String> serviceTypes,
        String searchQuery,
        boolean includeDeleted
    ) {
        boolean filterByIndexNames = indexNames != null && indexNames.isEmpty() == false;
        boolean filterByConnectorNames = indexNames != null && connectorNames.isEmpty() == false;
        boolean filterByServiceTypes = serviceTypes != null && serviceTypes.isEmpty() == false;
        boolean filterBySearchQuery = Strings.isNullOrEmpty(searchQuery) == false;
        boolean usesFilter = filterByIndexNames || filterByConnectorNames || filterByServiceTypes || filterBySearchQuery;

        BoolQueryBuilder boolFilterQueryBuilder = new BoolQueryBuilder();

        if (usesFilter) {
            if (filterByIndexNames) {
                boolFilterQueryBuilder.must().add(new TermsQueryBuilder(Connector.INDEX_NAME_FIELD.getPreferredName(), indexNames));
            }
            if (filterByConnectorNames) {
                boolFilterQueryBuilder.must().add(new TermsQueryBuilder(Connector.NAME_FIELD.getPreferredName(), connectorNames));
            }
            if (filterByServiceTypes) {
                boolFilterQueryBuilder.must().add(new TermsQueryBuilder(Connector.SERVICE_TYPE_FIELD.getPreferredName(), serviceTypes));
            }
            if (filterBySearchQuery) {
                String wildcardQueryValue = '*' + searchQuery + '*';
                boolFilterQueryBuilder.must()
                    .add(
                        new BoolQueryBuilder().should(
                            new WildcardQueryBuilder(Connector.INDEX_NAME_FIELD.getPreferredName(), wildcardQueryValue)
                        )
                            .should(new WildcardQueryBuilder(Connector.NAME_FIELD.getPreferredName(), wildcardQueryValue))
                            .should(new WildcardQueryBuilder(Connector.DESCRIPTION_FIELD.getPreferredName(), wildcardQueryValue))
                    );
            }
        }

        if (includeDeleted == false) {
            boolFilterQueryBuilder.mustNot(new TermQueryBuilder(Connector.IS_DELETED_FIELD.getPreferredName(), true));
        }

        return boolFilterQueryBuilder;
    }

    /**
     * Updates the {@link ConnectorConfiguration} property of a {@link Connector}.
     * This method supports full configuration replacement or individual configuration value updates.
     * If a full configuration is provided, it overwrites all existing configurations in non-additive way.
     * If only configuration values are provided, the existing {@link ConnectorConfiguration} is updated with new values
     * provided in the request.
     *
     * @param request   Request for updating connector configuration property.
     * @param listener  Listener to respond to a successful response or an error.
     */
    public void updateConnectorConfiguration(UpdateConnectorConfigurationAction.Request request, ActionListener<UpdateResponse> listener) {
        try {
            Map<String, ConnectorConfiguration> fullConfiguration = request.getConfiguration();
            Map<String, Object> configurationValues = request.getConfigurationValues();
            String connectorId = request.getConnectorId();

            getConnector(connectorId, false, listener.delegateFailure((l, connector) -> {

                UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).setRefreshPolicy(
                    WriteRequest.RefreshPolicy.IMMEDIATE
                );

                // Completely override [configuration] field with script
                if (fullConfiguration != null) {
                    String updateConfigurationScript = String.format(
                        Locale.ROOT,
                        """
                            ctx._source.%s = params.%s;
                            ctx._source.%s = params.%s;
                            """,
                        Connector.CONFIGURATION_FIELD.getPreferredName(),
                        Connector.CONFIGURATION_FIELD.getPreferredName(),
                        Connector.STATUS_FIELD.getPreferredName(),
                        Connector.STATUS_FIELD.getPreferredName()
                    );
                    Script script = new Script(
                        ScriptType.INLINE,
                        "painless",
                        updateConfigurationScript,
                        Map.of(
                            Connector.CONFIGURATION_FIELD.getPreferredName(),
                            request.getConfigurationAsMap(),
                            Connector.STATUS_FIELD.getPreferredName(),
                            ConnectorStatus.CONFIGURED.toString()
                        )
                    );
                    updateRequest = updateRequest.script(script);

                }
                // Only update configuration values for (key, value) pairs provided
                else if (configurationValues != null) {

                    Set<String> existingKeys = getConnectorConfigurationFromSearchResult(connector).keySet();
                    Set<String> newConfigurationKeys = configurationValues.keySet();

                    // Fail request it could result in updating values for unknown configuration keys
                    if (existingKeys.containsAll(newConfigurationKeys) == false) {

                        Set<String> unknownConfigKeys = newConfigurationKeys.stream()
                            .filter(key -> existingKeys.contains(key) == false)
                            .collect(Collectors.toSet());

                        l.onFailure(
                            new ElasticsearchStatusException(
                                "Unknown [configuration] fields in the request payload: ["
                                    + String.join(", ", unknownConfigKeys)
                                    + "]. Remove them from request or register their schema first.",
                                RestStatus.BAD_REQUEST
                            )
                        );
                        return;
                    }

                    Map<String, Object> configurationValuesUpdatePayload = configurationValues.entrySet()
                        .stream()
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> Map.of(ConnectorConfiguration.VALUE_FIELD.getPreferredName(), entry.getValue())
                            )
                        );

                    updateRequest = updateRequest.doc(
                        new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                            .id(connectorId)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                            .source(
                                Map.of(
                                    Connector.CONFIGURATION_FIELD.getPreferredName(),
                                    configurationValuesUpdatePayload,
                                    Connector.STATUS_FIELD.getPreferredName(),
                                    ConnectorStatus.CONFIGURED.toString()
                                )
                            )
                    );
                } else {
                    l.onFailure(
                        new ElasticsearchStatusException("[configuration] and [values] cannot both be null.", RestStatus.BAD_REQUEST)
                    );
                    return;
                }

                clientWithOrigin.update(updateRequest, new DelegatingIndexNotFoundActionListener<>(connectorId, l, (ll, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        ll.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
                        return;
                    }
                    ll.onResponse(updateResponse);
                }));
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Updates the error property of a {@link Connector}. If error is non-null the resulting {@link ConnectorStatus}
     * is 'error', otherwise it's 'connected'.
     *
     * @param connectorId The ID of the {@link Connector} to be updated.
     * @param error       An instance of error property of {@link Connector}, can be reset to [null].
     * @param listener    The listener for handling responses, including successful updates or errors.
     */
    public void updateConnectorError(String connectorId, String error, ActionListener<UpdateResponse> listener) {
        try {

            ConnectorStatus connectorStatus = Strings.isNullOrEmpty(error) ? ConnectorStatus.CONNECTED : ConnectorStatus.ERROR;

            final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                    .id(connectorId)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(new HashMap<>() {
                        {
                            put(Connector.ERROR_FIELD.getPreferredName(), error);
                            put(Connector.STATUS_FIELD.getPreferredName(), connectorStatus.toString());
                        }
                    })
            );
            clientWithOrigin.update(
                updateRequest,
                new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        l.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
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
                        l.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
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
     * Sets the {@link ConnectorFiltering} property of a {@link Connector}.
     *
     * @param connectorId The ID of the {@link Connector} to update.
     * @param filtering   The list of {@link ConnectorFiltering} .
     * @param listener    Listener to respond to a successful response or an error.
     */
    public void updateConnectorFiltering(String connectorId, List<ConnectorFiltering> filtering, ActionListener<UpdateResponse> listener) {
        try {
            final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                    .id(connectorId)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(Map.of(Connector.FILTERING_FIELD.getPreferredName(), filtering))
            );
            clientWithOrigin.update(
                updateRequest,
                new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        l.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
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
     * Updates the features of a given {@link Connector}.
     *
     * @param connectorId The ID of the {@link Connector} to be updated.
     * @param features    An instance of {@link ConnectorFeatures}
     * @param listener    Listener to respond to a successful response or an error.
     */
    public void updateConnectorFeatures(String connectorId, ConnectorFeatures features, ActionListener<UpdateResponse> listener) {
        try {
            final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                    .id(connectorId)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(Map.of(Connector.FEATURES_FIELD.getPreferredName(), features))
            );
            clientWithOrigin.update(
                updateRequest,
                new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        l.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
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
     * Updates the draft filtering in a given {@link Connector}.
     *
     * @param connectorId     The ID of the {@link Connector} to be updated.
     * @param advancedSnippet An instance of {@link FilteringAdvancedSnippet}.
     * @param rules           A list of instances of {@link FilteringRule} to be applied.
     * @param listener        Listener to respond to a successful response or an error.
     */
    public void updateConnectorFilteringDraft(
        String connectorId,
        FilteringAdvancedSnippet advancedSnippet,
        List<FilteringRule> rules,
        ActionListener<UpdateResponse> listener
    ) {
        try {
            getConnector(connectorId, false, listener.delegateFailure((l, connector) -> {
                List<ConnectorFiltering> connectorFilteringList = fromXContentBytesConnectorFiltering(
                    connector.getSourceRef(),
                    XContentType.JSON
                );
                // Connectors represent their filtering configuration as a singleton list
                ConnectorFiltering connectorFilteringSingleton = connectorFilteringList.get(0);

                // If advanced snippet or rules are not defined, keep the current draft state
                FilteringAdvancedSnippet newDraftAdvancedSnippet = advancedSnippet == null
                    ? connectorFilteringSingleton.getDraft().getAdvancedSnippet()
                    : advancedSnippet;

                List<FilteringRule> newDraftRules = rules == null
                    ? connectorFilteringSingleton.getDraft().getRules()
                    : new ArrayList<>(rules);

                if (rules != null) {
                    newDraftRules = sortFilteringRulesByOrder(newDraftRules);
                }

                ConnectorFiltering connectorFilteringWithUpdatedDraft = connectorFilteringSingleton.setDraft(
                    new FilteringRules.Builder().setRules(newDraftRules)
                        .setAdvancedSnippet(newDraftAdvancedSnippet)
                        .setFilteringValidationInfo(FilteringValidationInfo.getInitialDraftValidationInfo())
                        .build()
                );

                final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                    new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                        .id(connectorId)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(Map.of(Connector.FILTERING_FIELD.getPreferredName(), List.of(connectorFilteringWithUpdatedDraft)))
                );

                clientWithOrigin.update(
                    updateRequest,
                    new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (ll, updateResponse) -> {
                        if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                            ll.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
                            return;
                        }
                        ll.onResponse(updateResponse);
                    })
                );
            }));

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Updates the {@link FilteringValidationInfo} of the draft {@link ConnectorFiltering} property of a {@link Connector}.
     *
     * @param connectorId  Request for updating {@link ConnectorFiltering}.
     * @param listener     Listener to respond to a successful response or an error.
     */
    public void updateConnectorDraftFilteringValidation(
        String connectorId,
        FilteringValidationInfo validation,
        ActionListener<UpdateResponse> listener
    ) {
        getConnector(connectorId, false, listener.delegateFailure((l, connector) -> {
            try {
                List<ConnectorFiltering> connectorFilteringList = fromXContentBytesConnectorFiltering(
                    connector.getSourceRef(),
                    XContentType.JSON
                );
                // Connectors represent their filtering configuration as a singleton list
                ConnectorFiltering connectorFilteringSingleton = connectorFilteringList.get(0);

                ConnectorFiltering activatedConnectorFiltering = connectorFilteringSingleton.setDraft(
                    new FilteringRules.Builder().setRules(connectorFilteringSingleton.getDraft().getRules())
                        .setAdvancedSnippet(connectorFilteringSingleton.getDraft().getAdvancedSnippet())
                        .setFilteringValidationInfo(validation)
                        .build()
                );

                final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                    new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                        .id(connectorId)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(Map.of(Connector.FILTERING_FIELD.getPreferredName(), List.of(activatedConnectorFiltering)))
                );

                clientWithOrigin.update(updateRequest, new DelegatingIndexNotFoundActionListener<>(connectorId, l, (ll, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        ll.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
                        return;
                    }
                    ll.onResponse(updateResponse);
                }));
            } catch (Exception e) {
                l.onFailure(e);
            }
        }));

    }

    /**
     * Activates the draft {@link ConnectorFiltering} property of a {@link Connector}.
     *
     * @param connectorId  Request for updating {@link ConnectorFiltering} property.
     * @param listener     Listener to respond to a successful response or an error.
     */
    public void activateConnectorDraftFiltering(String connectorId, ActionListener<UpdateResponse> listener) {
        getConnector(connectorId, false, listener.delegateFailure((l, connector) -> {
            try {
                List<ConnectorFiltering> connectorFilteringList = fromXContentBytesConnectorFiltering(
                    connector.getSourceRef(),
                    XContentType.JSON
                );
                // Connectors represent their filtering configuration as a singleton list
                ConnectorFiltering connectorFilteringSingleton = connectorFilteringList.get(0);

                FilteringValidationState currentValidationState = connectorFilteringSingleton.getDraft()
                    .getFilteringValidationInfo()
                    .getValidationState();

                if (currentValidationState != FilteringValidationState.VALID) {
                    throw new ElasticsearchStatusException(
                        "Filtering draft needs to be validated by the connector service before activation. "
                            + "Current filtering draft validation state ["
                            + currentValidationState.toString()
                            + "] is not equal to ["
                            + FilteringValidationState.VALID
                            + "].",
                        RestStatus.BAD_REQUEST
                    );
                }

                ConnectorFiltering activatedConnectorFiltering = connectorFilteringSingleton.setActive(
                    connectorFilteringSingleton.getDraft()
                );

                final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                    new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                        .id(connectorId)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(Map.of(Connector.FILTERING_FIELD.getPreferredName(), List.of(activatedConnectorFiltering)))
                );

                clientWithOrigin.update(updateRequest, new DelegatingIndexNotFoundActionListener<>(connectorId, l, (ll, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        ll.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
                        return;
                    }
                    ll.onResponse(updateResponse);
                }));
            } catch (Exception e) {
                l.onFailure(e);
            }
        }));

    }

    /**
     * Updates the lastSeen property of a {@link Connector}.
     *
     * @param connectorId The id of the connector object.
     * @param listener    The listener for handling responses, including successful updates or errors.
     */
    public void checkInConnector(String connectorId, ActionListener<UpdateResponse> listener) {
        try {
            final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                    .id(connectorId)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(Map.of(Connector.LAST_SEEN_FIELD.getPreferredName(), Instant.now()))
            );
            clientWithOrigin.update(
                updateRequest,
                new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        l.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
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
                        l.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
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
     * Updates the is_native property of a {@link Connector}. It sets the {@link ConnectorStatus} to
     * CONFIGURED when connector is in CONNECTED state to indicate that connector needs to reconnect.
     *
     * @param request  The request for updating the connector's is_native property.
     * @param listener The listener for handling responses, including successful updates or errors.
     */
    public void updateConnectorNative(UpdateConnectorNativeAction.Request request, ActionListener<UpdateResponse> listener) {
        try {
            String connectorId = request.getConnectorId();
            boolean isNative = request.isNative();

            getConnector(connectorId, false, listener.delegateFailure((l, connector) -> {

                String indexName = getConnectorIndexNameFromSearchResult(connector);

                boolean doesNotHaveContentPrefix = indexName != null && isValidManagedConnectorIndexName(indexName) == false;
                // Ensure attached content index is prefixed correctly
                if (isNative && doesNotHaveContentPrefix) {
                    l.onFailure(
                        new ElasticsearchStatusException(
                            "The index name ["
                                + indexName
                                + "] attached to the connector ["
                                + connectorId
                                + "] must start with the required prefix: ["
                                + MANAGED_CONNECTOR_INDEX_PREFIX
                                + "] to be Elastic-managed. Please update the attached index first to comply with this requirement.",
                            RestStatus.BAD_REQUEST
                        )
                    );
                    return;
                }

                ConnectorStatus status = getConnectorStatusFromSearchResult(connector);

                // If connector was connected already, change its status to CONFIGURED as we need to re-connect
                boolean isConnected = status == ConnectorStatus.CONNECTED;
                boolean isValidTransitionToConfigured = ConnectorStateMachine.isValidTransition(status, ConnectorStatus.CONFIGURED);
                if (isConnected && isValidTransitionToConfigured) {
                    status = ConnectorStatus.CONFIGURED;
                }

                final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).setRefreshPolicy(
                    WriteRequest.RefreshPolicy.IMMEDIATE
                )
                    .doc(
                        new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                            .id(connectorId)
                            .source(
                                Map.of(
                                    Connector.IS_NATIVE_FIELD.getPreferredName(),
                                    isNative,
                                    Connector.STATUS_FIELD.getPreferredName(),
                                    status.toString()
                                )
                            )
                    );
                clientWithOrigin.update(
                    updateRequest,
                    new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (ll, updateResponse) -> {
                        if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                            ll.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
                            return;
                        }
                        ll.onResponse(updateResponse);
                    })
                );
            }));
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
                    .source(Map.of(Connector.PIPELINE_FIELD.getPreferredName(), request.getPipeline()))
                    .source(request.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS))
            );
            clientWithOrigin.update(
                updateRequest,
                new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        l.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
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
     * Updates the index name property of a {@link Connector}. Index name can be set to null to indicate that the connector
     * is not associated with any index.
     *
     * @param request  The request for updating the connector's index name.
     * @param listener The listener for handling responses, including successful updates or errors.
     */
    public void updateConnectorIndexName(UpdateConnectorIndexNameAction.Request request, ActionListener<UpdateResponse> listener) {
        try {
            String connectorId = request.getConnectorId();
            String indexName = request.getIndexName();

            isDataIndexNameAlreadyInUse(indexName, connectorId, listener.delegateFailure((l, isIndexNameInUse) -> {

                if (isIndexNameInUse) {
                    l.onFailure(
                        new ElasticsearchStatusException(
                            "Index name [" + indexName + "] is used by another connector.",
                            RestStatus.BAD_REQUEST
                        )
                    );
                    return;
                }

                getConnector(connectorId, false, l.delegateFailure((ll, connector) -> {

                    Boolean isNativeConnector = getConnectorIsNativeFlagFromSearchResult(connector);
                    Boolean doesNotHaveContentPrefix = indexName != null && isValidManagedConnectorIndexName(indexName) == false;

                    if (isNativeConnector && doesNotHaveContentPrefix) {
                        ll.onFailure(
                            new ElasticsearchStatusException(
                                "Index attached to an Elastic-managed connector must start with the prefix: ["
                                    + MANAGED_CONNECTOR_INDEX_PREFIX
                                    + "]. The index name in the payload ["
                                    + indexName
                                    + "] doesn't comply with this requirement.",
                                RestStatus.BAD_REQUEST
                            )
                        );
                        return;
                    }

                    final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                        new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                            .id(connectorId)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                            .source(new HashMap<>() {
                                {
                                    put(Connector.INDEX_NAME_FIELD.getPreferredName(), request.getIndexName());
                                }
                            })
                    );
                    clientWithOrigin.update(
                        updateRequest,
                        new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (lll, updateResponse) -> {
                            if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                                lll.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
                                return;
                            }
                            lll.onResponse(updateResponse);
                        })
                    );
                }));
            }));

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
                    .source(Map.of(Connector.SCHEDULING_FIELD.getPreferredName(), request.getScheduling()))
            );
            clientWithOrigin.update(
                updateRequest,
                new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (l, updateResponse) -> {
                    if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                        l.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
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
     * Updates the service type property of a {@link Connector} and its {@link ConnectorStatus}.
     *
     * @param request  The request for updating the connector's service type.
     * @param listener The listener for handling responses, including successful updates or errors.
     */
    public void updateConnectorServiceType(UpdateConnectorServiceTypeAction.Request request, ActionListener<UpdateResponse> listener) {
        try {
            String connectorId = request.getConnectorId();
            getConnector(connectorId, false, listener.delegateFailure((l, connector) -> {

                ConnectorStatus prevStatus = getConnectorStatusFromSearchResult(connector);
                ConnectorStatus newStatus = prevStatus == ConnectorStatus.CREATED
                    ? ConnectorStatus.CREATED
                    : ConnectorStatus.NEEDS_CONFIGURATION;

                final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                    new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                        .id(connectorId)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(
                            Map.of(
                                Connector.SERVICE_TYPE_FIELD.getPreferredName(),
                                request.getServiceType(),
                                Connector.STATUS_FIELD.getPreferredName(),
                                newStatus.toString()
                            )
                        )

                );
                clientWithOrigin.update(
                    updateRequest,
                    new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (updateListener, updateResponse) -> {
                        if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                            updateListener.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
                            return;
                        }
                        updateListener.onResponse(updateResponse);
                    })
                );
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Updates the {@link ConnectorStatus} property of a {@link Connector}.
     *
     * @param request  The request for updating the connector's status.
     * @param listener The listener for handling responses, including successful updates or errors.
     */
    public void updateConnectorStatus(UpdateConnectorStatusAction.Request request, ActionListener<UpdateResponse> listener) {
        try {
            String connectorId = request.getConnectorId();
            ConnectorStatus newStatus = request.getStatus();
            getConnector(connectorId, false, listener.delegateFailure((l, connector) -> {

                ConnectorStatus prevStatus = getConnectorStatusFromSearchResult(connector);

                try {
                    ConnectorStateMachine.assertValidStateTransition(prevStatus, newStatus);
                } catch (ConnectorInvalidStatusTransitionException e) {
                    l.onFailure(new ElasticsearchStatusException(e.getMessage(), RestStatus.BAD_REQUEST, e));
                    return;
                }

                final UpdateRequest updateRequest = new UpdateRequest(CONNECTOR_INDEX_NAME, connectorId).doc(
                    new IndexRequest(CONNECTOR_INDEX_NAME).opType(DocWriteRequest.OpType.INDEX)
                        .id(connectorId)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .source(Map.of(Connector.STATUS_FIELD.getPreferredName(), request.getStatus()))
                );
                clientWithOrigin.update(
                    updateRequest,
                    new DelegatingIndexNotFoundActionListener<>(connectorId, listener, (updateListener, updateResponse) -> {
                        if (updateResponse.getResult() == UpdateResponse.Result.NOT_FOUND) {
                            updateListener.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
                            return;
                        }
                        updateListener.onResponse(updateResponse);
                    })
                );
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void updateConnectorApiKeyIdOrApiKeySecretId(
        UpdateConnectorApiKeyIdAction.Request request,
        ActionListener<UpdateResponse> listener
    ) {
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
                        l.onFailure(new ResourceNotFoundException(connectorNotFoundErrorMsg(connectorId)));
                        return;
                    }
                    l.onResponse(updateResponse);
                })
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private ConnectorStatus getConnectorStatusFromSearchResult(ConnectorSearchResult searchResult) {
        return ConnectorStatus.connectorStatus((String) searchResult.getResultMap().get(Connector.STATUS_FIELD.getPreferredName()));
    }

    private Boolean getConnectorIsNativeFlagFromSearchResult(ConnectorSearchResult searchResult) {
        return (Boolean) searchResult.getResultMap().get(Connector.IS_NATIVE_FIELD.getPreferredName());
    }

    private String getConnectorIndexNameFromSearchResult(ConnectorSearchResult searchResult) {
        return (String) searchResult.getResultMap().get(Connector.INDEX_NAME_FIELD.getPreferredName());
    }

    private boolean isValidManagedConnectorIndexName(String indexName) {
        return indexName.startsWith(MANAGED_CONNECTOR_INDEX_PREFIX);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getConnectorConfigurationFromSearchResult(ConnectorSearchResult searchResult) {
        return (Map<String, Object>) searchResult.getResultMap().get(Connector.CONFIGURATION_FIELD.getPreferredName());
    }

    private boolean isConnectorDeleted(ConnectorSearchResult searchResult) {
        Boolean isDeletedFlag = (Boolean) searchResult.getResultMap().get(Connector.IS_DELETED_FIELD.getPreferredName());
        return Boolean.TRUE.equals(isDeletedFlag);
    }

    private static ConnectorIndexService.ConnectorResult mapSearchResponseToConnectorList(SearchResponse response) {
        final List<ConnectorSearchResult> connectorResults = Arrays.stream(response.getHits().getHits())
            .map(ConnectorIndexService::hitToConnector)
            .toList();
        return new ConnectorIndexService.ConnectorResult(connectorResults, (int) response.getHits().getTotalHits().value());
    }

    private static ConnectorSearchResult hitToConnector(SearchHit searchHit) {

        // todo: don't return sensitive data from configuration in list endpoint

        return new ConnectorSearchResult.Builder().setId(searchHit.getId())
            .setResultBytes(searchHit.getSourceRef())
            .setResultMap(searchHit.getSourceAsMap())
            .build();
    }

    /**
     * This method determines if any documents in the connector index have the same index name as the one specified,
     * excluding the docs marked as deleted (soft-deleted) and document with the given _id if it is provided.
     *
     * @param indexName    The name of the index to check for existence in the connector index.
     * @param connectorId  The ID of the {@link Connector} to exclude from the search. Can be null if no document should be excluded.
     * @param listener     The listener for handling boolean responses and errors.
     */
    private void isDataIndexNameAlreadyInUse(String indexName, String connectorId, ActionListener<Boolean> listener) {
        if (indexName == null) {
            listener.onResponse(false);
            return;
        }
        try {
            BoolQueryBuilder boolFilterQueryBuilder = new BoolQueryBuilder();

            boolFilterQueryBuilder.must().add(new TermQueryBuilder(Connector.INDEX_NAME_FIELD.getPreferredName(), indexName));

            // exclude soft-deleted connectors
            boolFilterQueryBuilder.mustNot(new TermQueryBuilder(Connector.IS_DELETED_FIELD.getPreferredName(), true));

            // If we know the connector _id, exclude this from search query
            if (connectorId != null) {
                boolFilterQueryBuilder.mustNot(new IdsQueryBuilder().addIds(connectorId));
            }

            final SearchSourceBuilder searchSource = new SearchSourceBuilder().query(boolFilterQueryBuilder);

            final SearchRequest searchRequest = new SearchRequest(CONNECTOR_INDEX_NAME).source(searchSource);
            clientWithOrigin.search(searchRequest, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    boolean indexNameIsInUse = searchResponse.getHits().getTotalHits().value() > 0L;
                    listener.onResponse(indexNameIsInUse);
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof IndexNotFoundException) {
                        listener.onResponse(false);
                        return;
                    }
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private String connectorNotFoundErrorMsg(String connectorId) {
        return "connector [" + connectorId + "] not found";
    }

    public record ConnectorResult(List<ConnectorSearchResult> connectors, long totalResults) {}

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
