/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.core.ShardsAcknowledgedResponse;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.client.indices.CloseIndexRequest;
import org.elasticsearch.client.indices.CloseIndexResponse;
import org.elasticsearch.client.indices.ComposableIndexTemplateExistRequest;
import org.elasticsearch.client.indices.CreateDataStreamRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.DataStreamsStatsRequest;
import org.elasticsearch.client.indices.DataStreamsStatsResponse;
import org.elasticsearch.client.indices.DeleteAliasRequest;
import org.elasticsearch.client.indices.DeleteComposableIndexTemplateRequest;
import org.elasticsearch.client.indices.DeleteDataStreamRequest;
import org.elasticsearch.client.indices.FreezeIndexRequest;
import org.elasticsearch.client.indices.GetComposableIndexTemplateRequest;
import org.elasticsearch.client.indices.GetComposableIndexTemplatesResponse;
import org.elasticsearch.client.indices.GetDataStreamRequest;
import org.elasticsearch.client.indices.GetDataStreamResponse;
import org.elasticsearch.client.indices.GetFieldMappingsRequest;
import org.elasticsearch.client.indices.GetFieldMappingsResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.client.indices.GetIndexTemplatesRequest;
import org.elasticsearch.client.indices.GetIndexTemplatesResponse;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.client.indices.IndexTemplatesExistRequest;
import org.elasticsearch.client.indices.PutComposableIndexTemplateRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.client.indices.ReloadAnalyzersRequest;
import org.elasticsearch.client.indices.ReloadAnalyzersResponse;
import org.elasticsearch.client.indices.ResizeRequest;
import org.elasticsearch.client.indices.ResizeResponse;
import org.elasticsearch.client.indices.SimulateIndexTemplateRequest;
import org.elasticsearch.client.indices.SimulateIndexTemplateResponse;
import org.elasticsearch.client.indices.UnfreezeIndexRequest;
import org.elasticsearch.client.indices.rollover.RolloverRequest;
import org.elasticsearch.client.indices.rollover.RolloverResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for accessing the Indices API.
 * <p>
 * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices.html">Indices API on elastic.co</a>
 *
 * @deprecated The High Level Rest Client is deprecated in favor of the
 * <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/introduction.html">
 * Elasticsearch Java API Client</a>
 */
@Deprecated(since = "7.16.0", forRemoval = true)
@SuppressWarnings("removal")
public final class IndicesClient {
    private final RestHighLevelClient restHighLevelClient;

    IndicesClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Deletes an index using the Delete Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html">
     * Delete Index API on elastic.co</a>
     * @param deleteIndexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse delete(DeleteIndexRequest deleteIndexRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            deleteIndexRequest,
            IndicesRequestConverters::deleteIndex,
            options,
            AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously deletes an index using the Delete Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html">
     * Delete Index API on elastic.co</a>
     * @param deleteIndexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteAsync(
        DeleteIndexRequest deleteIndexRequest,
        RequestOptions options,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            deleteIndexRequest,
            IndicesRequestConverters::deleteIndex,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Creates an index using the Create Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html">
     * Create Index API on elastic.co</a>
     * @param createIndexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public CreateIndexResponse create(CreateIndexRequest createIndexRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            createIndexRequest,
            IndicesRequestConverters::createIndex,
            options,
            CreateIndexResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously creates an index using the Create Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html">
     * Create Index API on elastic.co</a>
     * @param createIndexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable createAsync(
        CreateIndexRequest createIndexRequest,
        RequestOptions options,
        ActionListener<CreateIndexResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            createIndexRequest,
            IndicesRequestConverters::createIndex,
            options,
            CreateIndexResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Creates a data stream using the Create Data Stream API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-data-streams.html">
     * Data Streams API on elastic.co</a>
     *
     * @param createDataStreamRequest the request
     * @param options                 the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be
     *                                customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse createDataStream(CreateDataStreamRequest createDataStreamRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            createDataStreamRequest,
            IndicesRequestConverters::putDataStream,
            options,
            AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously creates a data stream using the Create Data Stream API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-data-streams.html">
     * Data Streams API on elastic.co</a>
     *
     * @param createDataStreamRequest the request
     * @param options                 the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be
     *                                customized
     * @param listener                the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable createDataStreamAsync(
        CreateDataStreamRequest createDataStreamRequest,
        RequestOptions options,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            createDataStreamRequest,
            IndicesRequestConverters::putDataStream,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Deletes a data stream using the Delete Data Stream API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-data-streams.html">
     * Data Streams API on elastic.co</a>
     *
     * @param deleteDataStreamRequest the request
     * @param options                 the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be
     *                                customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse deleteDataStream(DeleteDataStreamRequest deleteDataStreamRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            deleteDataStreamRequest,
            IndicesRequestConverters::deleteDataStream,
            options,
            AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously deletes a data stream using the Delete Data Stream API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-data-streams.html">
     * Data Streams API on elastic.co</a>
     *
     * @param deleteDataStreamRequest the request
     * @param options                 the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be
     *                                customized
     * @param listener                the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteDataStreamAsync(
        DeleteDataStreamRequest deleteDataStreamRequest,
        RequestOptions options,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            deleteDataStreamRequest,
            IndicesRequestConverters::deleteDataStream,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Gets one or more data streams using the Get Data Stream API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html"> Data Streams API on
     * elastic.co</a>
     *
     * @param dataStreamRequest the request
     * @param options           the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetDataStreamResponse getDataStream(GetDataStreamRequest dataStreamRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            dataStreamRequest,
            IndicesRequestConverters::getDataStreams,
            options,
            GetDataStreamResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously gets one or more data streams using the Get Data Stream API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html"> Data Streams API on
     * elastic.co</a>
     *
     * @param dataStreamRequest the request
     * @param options           the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener          the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getDataStreamAsync(
        GetDataStreamRequest dataStreamRequest,
        RequestOptions options,
        ActionListener<GetDataStreamResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            dataStreamRequest,
            IndicesRequestConverters::getDataStreams,
            options,
            GetDataStreamResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Gets statistics about one or more data streams using the Get Data Streams Stats API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html"> Data Streams API on
     * elastic.co</a>
     *
     * @param dataStreamsStatsRequest the request
     * @param options                 the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be
     *                                customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public DataStreamsStatsResponse dataStreamsStats(DataStreamsStatsRequest dataStreamsStatsRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            dataStreamsStatsRequest,
            IndicesRequestConverters::dataStreamsStats,
            options,
            DataStreamsStatsResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously gets statistics about one or more data streams using the Get Data Streams Stats API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html"> Data Streams API on
     * elastic.co</a>
     *
     * @param dataStreamsStatsRequest the request
     * @param options                 the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be
     *                                customized
     * @param listener                the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable dataStreamsStatsAsync(
        DataStreamsStatsRequest dataStreamsStatsRequest,
        RequestOptions options,
        ActionListener<DataStreamsStatsResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            dataStreamsStatsRequest,
            IndicesRequestConverters::dataStreamsStats,
            options,
            DataStreamsStatsResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Updates the mappings on an index using the Put Mapping API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-put-mapping.html">
     * Put Mapping API on elastic.co</a>
     * @param putMappingRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse putMapping(PutMappingRequest putMappingRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            putMappingRequest,
            IndicesRequestConverters::putMapping,
            options,
            AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously updates the mappings on an index using the Put Mapping API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-put-mapping.html">
     * Put Mapping API on elastic.co</a>
     * @param putMappingRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putMappingAsync(
        PutMappingRequest putMappingRequest,
        RequestOptions options,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            putMappingRequest,
            IndicesRequestConverters::putMapping,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Retrieves the mappings on an index or indices using the Get Mapping API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html">
     * Get Mapping API on elastic.co</a>
     * @param getMappingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetMappingsResponse getMapping(GetMappingsRequest getMappingsRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            getMappingsRequest,
            IndicesRequestConverters::getMappings,
            options,
            GetMappingsResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously retrieves the mappings on an index on indices using the Get Mapping API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html">
     * Get Mapping API on elastic.co</a>
     * @param getMappingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getMappingAsync(
        GetMappingsRequest getMappingsRequest,
        RequestOptions options,
        ActionListener<GetMappingsResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            getMappingsRequest,
            IndicesRequestConverters::getMappings,
            options,
            GetMappingsResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Retrieves the field mappings on an index or indices using the Get Field Mapping API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-field-mapping.html">
     * Get Field Mapping API on elastic.co</a>
     * @param getFieldMappingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetFieldMappingsResponse getFieldMapping(GetFieldMappingsRequest getFieldMappingsRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            getFieldMappingsRequest,
            IndicesRequestConverters::getFieldMapping,
            options,
            GetFieldMappingsResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously retrieves the field mappings on an index or indices using the Get Field Mapping API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-field-mapping.html">
     * Get Field Mapping API on elastic.co</a>
     * @param getFieldMappingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getFieldMappingAsync(
        GetFieldMappingsRequest getFieldMappingsRequest,
        RequestOptions options,
        ActionListener<GetFieldMappingsResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            getFieldMappingsRequest,
            IndicesRequestConverters::getFieldMapping,
            options,
            GetFieldMappingsResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Updates aliases using the Index Aliases API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html">
     * Index Aliases API on elastic.co</a>
     * @param indicesAliasesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse updateAliases(IndicesAliasesRequest indicesAliasesRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            indicesAliasesRequest,
            IndicesRequestConverters::updateAliases,
            options,
            AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously updates aliases using the Index Aliases API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html">
     * Index Aliases API on elastic.co</a>
     * @param indicesAliasesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable updateAliasesAsync(
        IndicesAliasesRequest indicesAliasesRequest,
        RequestOptions options,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            indicesAliasesRequest,
            IndicesRequestConverters::updateAliases,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Opens an index using the Open Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html">
     * Open Index API on elastic.co</a>
     * @param openIndexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public OpenIndexResponse open(OpenIndexRequest openIndexRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            openIndexRequest,
            IndicesRequestConverters::openIndex,
            options,
            OpenIndexResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously opens an index using the Open Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html">
     * Open Index API on elastic.co</a>
     * @param openIndexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable openAsync(OpenIndexRequest openIndexRequest, RequestOptions options, ActionListener<OpenIndexResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            openIndexRequest,
            IndicesRequestConverters::openIndex,
            options,
            OpenIndexResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Closes an index using the Close Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html">
     * Close Index API on elastic.co</a>
     * @param closeIndexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public CloseIndexResponse close(CloseIndexRequest closeIndexRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            closeIndexRequest,
            IndicesRequestConverters::closeIndex,
            options,
            CloseIndexResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously closes an index using the Close Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html">
     * Close Index API on elastic.co</a>
     * @param closeIndexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable closeAsync(
        CloseIndexRequest closeIndexRequest,
        RequestOptions options,
        ActionListener<CloseIndexResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            closeIndexRequest,
            IndicesRequestConverters::closeIndex,
            options,
            CloseIndexResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Checks if one or more aliases exist using the Aliases Exist API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html">
     * Indices Aliases API on elastic.co</a>
     * @param getAliasesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request
     */
    public boolean existsAlias(GetAliasesRequest getAliasesRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequest(
            getAliasesRequest,
            IndicesRequestConverters::existsAlias,
            options,
            RestHighLevelClient::convertExistsResponse,
            emptySet()
        );
    }

    /**
     * Asynchronously checks if one or more aliases exist using the Aliases Exist API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html">
     * Indices Aliases API on elastic.co</a>
     * @param getAliasesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable existsAliasAsync(GetAliasesRequest getAliasesRequest, RequestOptions options, ActionListener<Boolean> listener) {
        return restHighLevelClient.performRequestAsync(
            getAliasesRequest,
            IndicesRequestConverters::existsAlias,
            options,
            RestHighLevelClient::convertExistsResponse,
            listener,
            emptySet()
        );
    }

    /**
     * Refresh one or more indices using the Refresh API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-refresh.html"> Refresh API on elastic.co</a>
     * @param refreshRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public RefreshResponse refresh(RefreshRequest refreshRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            refreshRequest,
            IndicesRequestConverters::refresh,
            options,
            RefreshResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously refresh one or more indices using the Refresh API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-refresh.html"> Refresh API on elastic.co</a>
     * @param refreshRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable refreshAsync(RefreshRequest refreshRequest, RequestOptions options, ActionListener<RefreshResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            refreshRequest,
            IndicesRequestConverters::refresh,
            options,
            RefreshResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Flush one or more indices using the Flush API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-flush.html"> Flush API on elastic.co</a>
     * @param flushRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public FlushResponse flush(FlushRequest flushRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            flushRequest,
            IndicesRequestConverters::flush,
            options,
            FlushResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously flush one or more indices using the Flush API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-flush.html"> Flush API on elastic.co</a>
     * @param flushRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable flushAsync(FlushRequest flushRequest, RequestOptions options, ActionListener<FlushResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            flushRequest,
            IndicesRequestConverters::flush,
            options,
            FlushResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Retrieve the settings of one or more indices.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-settings.html">
     * Indices Get Settings API on elastic.co</a>
     * @param getSettingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetSettingsResponse getSettings(GetSettingsRequest getSettingsRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            getSettingsRequest,
            IndicesRequestConverters::getSettings,
            options,
            GetSettingsResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously retrieve the settings of one or more indices.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-settings.html">
     * Indices Get Settings API on elastic.co</a>
     * @param getSettingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getSettingsAsync(
        GetSettingsRequest getSettingsRequest,
        RequestOptions options,
        ActionListener<GetSettingsResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            getSettingsRequest,
            IndicesRequestConverters::getSettings,
            options,
            GetSettingsResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Retrieve information about one or more indexes
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-index.html">
     * Indices Get Index API on elastic.co</a>
     * @param getIndexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetIndexResponse get(GetIndexRequest getIndexRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            getIndexRequest,
            IndicesRequestConverters::getIndex,
            options,
            GetIndexResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Retrieve information about one or more indexes
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-index.html">
     * Indices Get Index API on elastic.co</a>
     * @param getIndexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getAsync(GetIndexRequest getIndexRequest, RequestOptions options, ActionListener<GetIndexResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            getIndexRequest,
            IndicesRequestConverters::getIndex,
            options,
            GetIndexResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Force merge one or more indices using the Force Merge API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-forcemerge.html">
     * Force Merge API on elastic.co</a>
     * @param forceMergeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     * @deprecated use {@link #forcemerge(ForceMergeRequest, RequestOptions)} instead
     */
    @Deprecated
    public ForceMergeResponse forceMerge(ForceMergeRequest forceMergeRequest, RequestOptions options) throws IOException {
        return forcemerge(forceMergeRequest, options);
    }

    /**
     * Force merge one or more indices using the Force Merge API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-forcemerge.html">
     * Force Merge API on elastic.co</a>
     * @param forceMergeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ForceMergeResponse forcemerge(ForceMergeRequest forceMergeRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            forceMergeRequest,
            IndicesRequestConverters::forceMerge,
            options,
            ForceMergeResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously force merge one or more indices using the Force Merge API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-forcemerge.html">
     * Force Merge API on elastic.co</a>
     * @param forceMergeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @deprecated use {@link #forcemergeAsync(ForceMergeRequest, RequestOptions, ActionListener)} instead
     * @return cancellable that may be used to cancel the request
     */
    @Deprecated
    public Cancellable forceMergeAsync(
        ForceMergeRequest forceMergeRequest,
        RequestOptions options,
        ActionListener<ForceMergeResponse> listener
    ) {
        return forcemergeAsync(forceMergeRequest, options, listener);
    }

    /**
     * Asynchronously force merge one or more indices using the Force Merge API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-forcemerge.html">
     * Force Merge API on elastic.co</a>
     * @param forceMergeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable forcemergeAsync(
        ForceMergeRequest forceMergeRequest,
        RequestOptions options,
        ActionListener<ForceMergeResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            forceMergeRequest,
            IndicesRequestConverters::forceMerge,
            options,
            ForceMergeResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Clears the cache of one or more indices using the Clear Cache API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-clearcache.html">
     * Clear Cache API on elastic.co</a>
     * @param clearIndicesCacheRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ClearIndicesCacheResponse clearCache(ClearIndicesCacheRequest clearIndicesCacheRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            clearIndicesCacheRequest,
            IndicesRequestConverters::clearCache,
            options,
            ClearIndicesCacheResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously clears the cache of one or more indices using the Clear Cache API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-clearcache.html">
     * Clear Cache API on elastic.co</a>
     * @param clearIndicesCacheRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable clearCacheAsync(
        ClearIndicesCacheRequest clearIndicesCacheRequest,
        RequestOptions options,
        ActionListener<ClearIndicesCacheResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            clearIndicesCacheRequest,
            IndicesRequestConverters::clearCache,
            options,
            ClearIndicesCacheResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Checks if the index (indices) exists or not.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-exists.html">
     * Indices Exists API on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request
     */
    public boolean exists(GetIndexRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequest(
            request,
            IndicesRequestConverters::indicesExist,
            options,
            RestHighLevelClient::convertExistsResponse,
            Collections.emptySet()
        );
    }

    /**
     * Asynchronously checks if the index (indices) exists or not.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-exists.html">
     * Indices Exists API on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable existsAsync(GetIndexRequest request, RequestOptions options, ActionListener<Boolean> listener) {
        return restHighLevelClient.performRequestAsync(
            request,
            IndicesRequestConverters::indicesExist,
            options,
            RestHighLevelClient::convertExistsResponse,
            listener,
            Collections.emptySet()
        );
    }

    /**
     * Shrinks an index using the Shrink Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-shrink-index.html">
     * Shrink Index API on elastic.co</a>
     * @param resizeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ResizeResponse shrink(ResizeRequest resizeRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            resizeRequest,
            IndicesRequestConverters::shrink,
            options,
            ResizeResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Shrinks an index using the Shrink Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-shrink-index.html">
     * Shrink Index API on elastic.co</a>
     * @param resizeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     * @deprecated use {@link #shrink(ResizeRequest, RequestOptions)}
     */
    @Deprecated
    public org.elasticsearch.action.admin.indices.shrink.ResizeResponse shrink(
        org.elasticsearch.action.admin.indices.shrink.ResizeRequest resizeRequest,
        RequestOptions options
    ) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            resizeRequest,
            IndicesRequestConverters::shrink,
            options,
            org.elasticsearch.action.admin.indices.shrink.ResizeResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously shrinks an index using the Shrink index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-shrink-index.html">
     * Shrink Index API on elastic.co</a>
     * @param resizeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable shrinkAsync(ResizeRequest resizeRequest, RequestOptions options, ActionListener<ResizeResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            resizeRequest,
            IndicesRequestConverters::shrink,
            options,
            ResizeResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Asynchronously shrinks an index using the Shrink index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-shrink-index.html">
     * Shrink Index API on elastic.co</a>
     * @param resizeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     * @deprecated use {@link #shrinkAsync(ResizeRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public Cancellable shrinkAsync(
        org.elasticsearch.action.admin.indices.shrink.ResizeRequest resizeRequest,
        RequestOptions options,
        ActionListener<org.elasticsearch.action.admin.indices.shrink.ResizeResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            resizeRequest,
            IndicesRequestConverters::shrink,
            options,
            org.elasticsearch.action.admin.indices.shrink.ResizeResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Splits an index using the Split Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-split-index.html">
     * Split Index API on elastic.co</a>
     * @param resizeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ResizeResponse split(ResizeRequest resizeRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            resizeRequest,
            IndicesRequestConverters::split,
            options,
            ResizeResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Splits an index using the Split Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-split-index.html">
     * Split Index API on elastic.co</a>
     * @param resizeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     * @deprecated use {@link #split(ResizeRequest, RequestOptions)}
     */
    @Deprecated
    public org.elasticsearch.action.admin.indices.shrink.ResizeResponse split(
        org.elasticsearch.action.admin.indices.shrink.ResizeRequest resizeRequest,
        RequestOptions options
    ) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            resizeRequest,
            IndicesRequestConverters::split,
            options,
            org.elasticsearch.action.admin.indices.shrink.ResizeResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously splits an index using the Split Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-split-index.html">
     * Split Index API on elastic.co</a>
     * @param resizeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable splitAsync(ResizeRequest resizeRequest, RequestOptions options, ActionListener<ResizeResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            resizeRequest,
            IndicesRequestConverters::split,
            options,
            ResizeResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Asynchronously splits an index using the Split Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-split-index.html">
     * Split Index API on elastic.co</a>
     * @param resizeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     * @deprecated use {@link #splitAsync(ResizeRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public Cancellable splitAsync(
        org.elasticsearch.action.admin.indices.shrink.ResizeRequest resizeRequest,
        RequestOptions options,
        ActionListener<org.elasticsearch.action.admin.indices.shrink.ResizeResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            resizeRequest,
            IndicesRequestConverters::split,
            options,
            org.elasticsearch.action.admin.indices.shrink.ResizeResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Clones an index using the Clone Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-clone-index.html">
     * Clone Index API on elastic.co</a>
     * @param resizeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ResizeResponse clone(ResizeRequest resizeRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            resizeRequest,
            IndicesRequestConverters::clone,
            options,
            ResizeResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Clones an index using the Clone Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-clone-index.html">
     * Clone Index API on elastic.co</a>
     * @param resizeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     * @deprecated use {@link #clone(ResizeRequest, RequestOptions)}
     */
    @Deprecated
    public org.elasticsearch.action.admin.indices.shrink.ResizeResponse clone(
        org.elasticsearch.action.admin.indices.shrink.ResizeRequest resizeRequest,
        RequestOptions options
    ) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            resizeRequest,
            IndicesRequestConverters::clone,
            options,
            org.elasticsearch.action.admin.indices.shrink.ResizeResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously clones an index using the Clone Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-clone-index.html">
     * Clone Index API on elastic.co</a>
     * @param resizeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable cloneAsync(ResizeRequest resizeRequest, RequestOptions options, ActionListener<ResizeResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            resizeRequest,
            IndicesRequestConverters::clone,
            options,
            ResizeResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Asynchronously clones an index using the Clone Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-clone-index.html">
     * Clone Index API on elastic.co</a>
     * @param resizeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     * @deprecated use {@link #cloneAsync(ResizeRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public Cancellable cloneAsync(
        org.elasticsearch.action.admin.indices.shrink.ResizeRequest resizeRequest,
        RequestOptions options,
        ActionListener<org.elasticsearch.action.admin.indices.shrink.ResizeResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            resizeRequest,
            IndicesRequestConverters::clone,
            options,
            org.elasticsearch.action.admin.indices.shrink.ResizeResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Rolls over an index using the Rollover Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-rollover-index.html">
     * Rollover Index API on elastic.co</a>
     * @param rolloverRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public RolloverResponse rollover(RolloverRequest rolloverRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            rolloverRequest,
            IndicesRequestConverters::rollover,
            options,
            RolloverResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously rolls over an index using the Rollover Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-rollover-index.html">
     * Rollover Index API on elastic.co</a>
     * @param rolloverRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable rolloverAsync(RolloverRequest rolloverRequest, RequestOptions options, ActionListener<RolloverResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            rolloverRequest,
            IndicesRequestConverters::rollover,
            options,
            RolloverResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Gets one or more aliases using the Get Index Aliases API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html"> Indices Aliases API on
     * elastic.co</a>
     * @param getAliasesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetAliasesResponse getAlias(GetAliasesRequest getAliasesRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            getAliasesRequest,
            IndicesRequestConverters::getAlias,
            options,
            GetAliasesResponse::fromXContent,
            singleton(RestStatus.NOT_FOUND.getStatus())
        );
    }

    /**
     * Asynchronously gets one or more aliases using the Get Index Aliases API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html"> Indices Aliases API on
     * elastic.co</a>
     * @param getAliasesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getAliasAsync(
        GetAliasesRequest getAliasesRequest,
        RequestOptions options,
        ActionListener<GetAliasesResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            getAliasesRequest,
            IndicesRequestConverters::getAlias,
            options,
            GetAliasesResponse::fromXContent,
            listener,
            singleton(RestStatus.NOT_FOUND.getStatus())
        );
    }

    /**
     * Updates specific index level settings using the Update Indices Settings API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html"> Update Indices Settings
     * API on elastic.co</a>
     * @param updateSettingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse putSettings(UpdateSettingsRequest updateSettingsRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            updateSettingsRequest,
            IndicesRequestConverters::indexPutSettings,
            options,
            AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously updates specific index level settings using the Update Indices Settings API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html"> Update Indices Settings
     * API on elastic.co</a>
     * @param updateSettingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putSettingsAsync(
        UpdateSettingsRequest updateSettingsRequest,
        RequestOptions options,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            updateSettingsRequest,
            IndicesRequestConverters::indexPutSettings,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Puts an index template using the Index Templates API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html"> Index Templates API
     * on elastic.co</a>
     * @param putIndexTemplateRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse putTemplate(PutIndexTemplateRequest putIndexTemplateRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            putIndexTemplateRequest,
            IndicesRequestConverters::putTemplate,
            options,
            AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously puts an index template using the Index Templates API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html"> Index Templates API
     * on elastic.co</a>
     * @param putIndexTemplateRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putTemplateAsync(
        PutIndexTemplateRequest putIndexTemplateRequest,
        RequestOptions options,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            putIndexTemplateRequest,
            IndicesRequestConverters::putTemplate,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Puts an index template using the Index Templates API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html"> Index Templates API
     * on elastic.co</a>
     * @param putIndexTemplateRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse putIndexTemplate(PutComposableIndexTemplateRequest putIndexTemplateRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            putIndexTemplateRequest,
            IndicesRequestConverters::putIndexTemplate,
            options,
            AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously puts an index template using the Index Templates API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html"> Index Templates API
     * on elastic.co</a>
     * @param putIndexTemplateRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putIndexTemplateAsync(
        PutComposableIndexTemplateRequest putIndexTemplateRequest,
        RequestOptions options,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            putIndexTemplateRequest,
            IndicesRequestConverters::putIndexTemplate,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Simulates matching index name against the existing index templates in the system.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html"> Index Templates API
     * on elastic.co</a>
     *
     * @param simulateIndexTemplateRequest the request
     * @param options                      the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be
     *                                     customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public SimulateIndexTemplateResponse simulateIndexTemplate(
        SimulateIndexTemplateRequest simulateIndexTemplateRequest,
        RequestOptions options
    ) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            simulateIndexTemplateRequest,
            IndicesRequestConverters::simulateIndexTemplate,
            options,
            SimulateIndexTemplateResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously simulates matching index name against the existing index templates in the system.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html"> Index Templates API
     * on elastic.co</a>
     *
     * @param simulateIndexTemplateRequest the request
     * @param options                      the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be
     *                                     customized
     * @param listener                     the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable simulateIndexTemplateAsync(
        SimulateIndexTemplateRequest simulateIndexTemplateRequest,
        RequestOptions options,
        ActionListener<SimulateIndexTemplateResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            simulateIndexTemplateRequest,
            IndicesRequestConverters::simulateIndexTemplate,
            options,
            SimulateIndexTemplateResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Validate a potentially expensive query without executing it.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-validate.html"> Validate Query API
     * on elastic.co</a>
     * @param validateQueryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ValidateQueryResponse validateQuery(ValidateQueryRequest validateQueryRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            validateQueryRequest,
            IndicesRequestConverters::validateQuery,
            options,
            ValidateQueryResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously validate a potentially expensive query without executing it.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-validate.html"> Validate Query API
     * on elastic.co</a>
     * @param validateQueryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable validateQueryAsync(
        ValidateQueryRequest validateQueryRequest,
        RequestOptions options,
        ActionListener<ValidateQueryResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            validateQueryRequest,
            IndicesRequestConverters::validateQuery,
            options,
            ValidateQueryResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Gets index templates using the Index Templates API
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html"> Index Templates API
     * on elastic.co</a>
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param getIndexTemplatesRequest the request
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetComposableIndexTemplatesResponse getIndexTemplate(
        GetComposableIndexTemplateRequest getIndexTemplatesRequest,
        RequestOptions options
    ) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            getIndexTemplatesRequest,
            IndicesRequestConverters::getIndexTemplates,
            options,
            GetComposableIndexTemplatesResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously gets index templates using the Index Templates API
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html"> Index Templates API
     * on elastic.co</a>
     * @param getIndexTemplatesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getIndexTemplateAsync(
        GetComposableIndexTemplateRequest getIndexTemplatesRequest,
        RequestOptions options,
        ActionListener<GetComposableIndexTemplatesResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            getIndexTemplatesRequest,
            IndicesRequestConverters::getIndexTemplates,
            options,
            GetComposableIndexTemplatesResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Gets index templates using the Index Templates API
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html"> Index Templates API
     * on elastic.co</a>
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param getIndexTemplatesRequest the request
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetIndexTemplatesResponse getIndexTemplate(GetIndexTemplatesRequest getIndexTemplatesRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            getIndexTemplatesRequest,
            IndicesRequestConverters::getTemplates,
            options,
            GetIndexTemplatesResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously gets index templates using the Index Templates API
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html"> Index Templates API
     * on elastic.co</a>
     * @param getIndexTemplatesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getIndexTemplateAsync(
        GetIndexTemplatesRequest getIndexTemplatesRequest,
        RequestOptions options,
        ActionListener<GetIndexTemplatesResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            getIndexTemplatesRequest,
            IndicesRequestConverters::getTemplates,
            options,
            GetIndexTemplatesResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Uses the Index Templates API to determine if index templates exist
     *
     * @param indexTemplatesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return true if any index templates in the request exist, false otherwise
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public boolean existsTemplate(IndexTemplatesExistRequest indexTemplatesRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequest(
            indexTemplatesRequest,
            IndicesRequestConverters::templatesExist,
            options,
            RestHighLevelClient::convertExistsResponse,
            emptySet()
        );
    }

    /**
     * Uses the Index Templates API to determine if index templates exist
     * @param indexTemplatesExistRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion. The listener will be called with the value {@code true}
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable existsTemplateAsync(
        IndexTemplatesExistRequest indexTemplatesExistRequest,
        RequestOptions options,
        ActionListener<Boolean> listener
    ) {

        return restHighLevelClient.performRequestAsync(
            indexTemplatesExistRequest,
            IndicesRequestConverters::templatesExist,
            options,
            RestHighLevelClient::convertExistsResponse,
            listener,
            emptySet()
        );
    }

    /**
     * Uses the Index Templates API to determine if index templates exist
     *
     * @param indexTemplatesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return true if any index templates in the request exist, false otherwise
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public boolean existsIndexTemplate(ComposableIndexTemplateExistRequest indexTemplatesRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequest(
            indexTemplatesRequest,
            IndicesRequestConverters::templatesExist,
            options,
            RestHighLevelClient::convertExistsResponse,
            emptySet()
        );
    }

    /**
     * Uses the Index Templates API to determine if index templates exist
     * @param indexTemplatesExistRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion. The listener will be called with the value {@code true}
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable existsIndexTemplateAsync(
        ComposableIndexTemplateExistRequest indexTemplatesExistRequest,
        RequestOptions options,
        ActionListener<Boolean> listener
    ) {

        return restHighLevelClient.performRequestAsync(
            indexTemplatesExistRequest,
            IndicesRequestConverters::templatesExist,
            options,
            RestHighLevelClient::convertExistsResponse,
            listener,
            emptySet()
        );
    }

    /**
     * Calls the analyze API
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-analyze.html">Analyze API on elastic.co</a>
     *
     * @param request   the request
     * @param options   the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     */
    public AnalyzeResponse analyze(AnalyzeRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            IndicesRequestConverters::analyze,
            options,
            AnalyzeResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously calls the analyze API
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-analyze.html">Analyze API on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable analyzeAsync(AnalyzeRequest request, RequestOptions options, ActionListener<AnalyzeResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            IndicesRequestConverters::analyze,
            options,
            AnalyzeResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Synchronously calls the _freeze API
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     */
    public ShardsAcknowledgedResponse freeze(FreezeIndexRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            IndicesRequestConverters::freezeIndex,
            options,
            ShardsAcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously calls the _freeze API
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable freezeAsync(
        FreezeIndexRequest request,
        RequestOptions options,
        ActionListener<ShardsAcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            IndicesRequestConverters::freezeIndex,
            options,
            ShardsAcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Synchronously calls the _unfreeze API
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     */
    public ShardsAcknowledgedResponse unfreeze(UnfreezeIndexRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            IndicesRequestConverters::unfreezeIndex,
            options,
            ShardsAcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously calls the _unfreeze API
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable unfreezeAsync(
        UnfreezeIndexRequest request,
        RequestOptions options,
        ActionListener<ShardsAcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            IndicesRequestConverters::unfreezeIndex,
            options,
            ShardsAcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Delete an index template using the Index Templates API
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html"> Index Templates API
     * on elastic.co</a>
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse deleteTemplate(DeleteIndexTemplateRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            IndicesRequestConverters::deleteTemplate,
            options,
            AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously delete an index template using the Index Templates API
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html"> Index Templates API
     * on elastic.co</a>
     * @param request  the request
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteTemplateAsync(
        DeleteIndexTemplateRequest request,
        RequestOptions options,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            IndicesRequestConverters::deleteTemplate,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Delete an index template using the Index Templates API
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html"> Index Templates API
     * on elastic.co</a>
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse deleteIndexTemplate(DeleteComposableIndexTemplateRequest request, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            IndicesRequestConverters::deleteIndexTemplate,
            options,
            AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously delete an index template using the Index Templates API
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html"> Index Templates API
     * on elastic.co</a>
     * @param request  the request
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteIndexTemplateAsync(
        DeleteComposableIndexTemplateRequest request,
        RequestOptions options,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            IndicesRequestConverters::deleteIndexTemplate,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Synchronously calls the _reload_search_analyzers API
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     */
    public ReloadAnalyzersResponse reloadAnalyzers(ReloadAnalyzersRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            IndicesRequestConverters::reloadAnalyzers,
            options,
            ReloadAnalyzersResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously calls the _reload_search_analyzers API
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable reloadAnalyzersAsync(
        ReloadAnalyzersRequest request,
        RequestOptions options,
        ActionListener<ReloadAnalyzersResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            IndicesRequestConverters::reloadAnalyzers,
            options,
            ReloadAnalyzersResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Synchronously calls the delete alias api
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     */
    public org.elasticsearch.client.core.AcknowledgedResponse deleteAlias(DeleteAliasRequest request, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            IndicesRequestConverters::deleteAlias,
            options,
            org.elasticsearch.client.core.AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously calls the delete alias api
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteAliasAsync(
        DeleteAliasRequest request,
        RequestOptions options,
        ActionListener<org.elasticsearch.client.core.AcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            IndicesRequestConverters::deleteAlias,
            options,
            org.elasticsearch.client.core.AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }
}
