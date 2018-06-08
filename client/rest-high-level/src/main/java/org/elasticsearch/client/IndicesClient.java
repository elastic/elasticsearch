/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.apache.http.Header;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;

import java.io.IOException;
import java.util.Collections;

import static java.util.Collections.emptySet;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for accessing the Indices API.
 * <p>
 * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices.html">Indices API on elastic.co</a>
 */
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
    public DeleteIndexResponse delete(DeleteIndexRequest deleteIndexRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(deleteIndexRequest, RequestConverters::deleteIndex, options,
                DeleteIndexResponse::fromXContent, emptySet());
    }

    /**
     * Deletes an index using the Delete Index API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html">
     * Delete Index API on elastic.co</a>
     * @deprecated Prefer {@link #delete(DeleteIndexRequest, RequestOptions)}
     */
    @Deprecated
    public DeleteIndexResponse delete(DeleteIndexRequest deleteIndexRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(deleteIndexRequest, RequestConverters::deleteIndex,
                DeleteIndexResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously deletes an index using the Delete Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html">
     * Delete Index API on elastic.co</a>
     * @param deleteIndexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void deleteAsync(DeleteIndexRequest deleteIndexRequest, RequestOptions options, ActionListener<DeleteIndexResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(deleteIndexRequest, RequestConverters::deleteIndex, options,
                DeleteIndexResponse::fromXContent, listener, emptySet());
    }

    /**
     * Asynchronously deletes an index using the Delete Index API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html">
     * Delete Index API on elastic.co</a>
     * @deprecated Prefer {@link #deleteAsync(DeleteIndexRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void deleteAsync(DeleteIndexRequest deleteIndexRequest, ActionListener<DeleteIndexResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(deleteIndexRequest, RequestConverters::deleteIndex,
                DeleteIndexResponse::fromXContent, listener, emptySet(), headers);
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
        return restHighLevelClient.performRequestAndParseEntity(createIndexRequest, RequestConverters::createIndex, options,
                CreateIndexResponse::fromXContent, emptySet());
    }

    /**
     * Creates an index using the Create Index API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html">
     * Create Index API on elastic.co</a>
     * @deprecated Prefer {@link #create(CreateIndexRequest, RequestOptions)}
     */
    @Deprecated
    public CreateIndexResponse create(CreateIndexRequest createIndexRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(createIndexRequest, RequestConverters::createIndex,
                CreateIndexResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously creates an index using the Create Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html">
     * Create Index API on elastic.co</a>
     * @param createIndexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void createAsync(CreateIndexRequest createIndexRequest, RequestOptions options, ActionListener<CreateIndexResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(createIndexRequest, RequestConverters::createIndex, options,
                CreateIndexResponse::fromXContent, listener, emptySet());
    }

    /**
     * Asynchronously creates an index using the Create Index API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html">
     * Create Index API on elastic.co</a>
     * @deprecated Prefer {@link #createAsync(CreateIndexRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void createAsync(CreateIndexRequest createIndexRequest, ActionListener<CreateIndexResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(createIndexRequest, RequestConverters::createIndex,
                CreateIndexResponse::fromXContent, listener, emptySet(), headers);
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
    public PutMappingResponse putMapping(PutMappingRequest putMappingRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(putMappingRequest, RequestConverters::putMapping, options,
                PutMappingResponse::fromXContent, emptySet());
    }

    /**
     * Updates the mappings on an index using the Put Mapping API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-put-mapping.html">
     * Put Mapping API on elastic.co</a>
     * @deprecated Prefer {@link #putMapping(PutMappingRequest, RequestOptions)}
     */
    @Deprecated
    public PutMappingResponse putMapping(PutMappingRequest putMappingRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(putMappingRequest, RequestConverters::putMapping,
                PutMappingResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously updates the mappings on an index using the Put Mapping API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-put-mapping.html">
     * Put Mapping API on elastic.co</a>
     * @param putMappingRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void putMappingAsync(PutMappingRequest putMappingRequest, RequestOptions options,  ActionListener<PutMappingResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(putMappingRequest, RequestConverters::putMapping, options,
                PutMappingResponse::fromXContent, listener, emptySet());
    }

    /**
     * Asynchronously updates the mappings on an index using the Put Mapping API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-put-mapping.html">
     * Put Mapping API on elastic.co</a>
     * @deprecated Prefer {@link #putMappingAsync(PutMappingRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void putMappingAsync(PutMappingRequest putMappingRequest, ActionListener<PutMappingResponse> listener,
                                Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(putMappingRequest, RequestConverters::putMapping,
                PutMappingResponse::fromXContent, listener, emptySet(), headers);
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
    public GetMappingsResponse getMappings(GetMappingsRequest getMappingsRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(getMappingsRequest, RequestConverters::getMappings, options,
            GetMappingsResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously retrieves the mappings on an index on indices using the Get Mapping API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html">
     * Get Mapping API on elastic.co</a>
     * @param getMappingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void getMappingsAsync(GetMappingsRequest getMappingsRequest, RequestOptions options,
                                 ActionListener<GetMappingsResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(getMappingsRequest, RequestConverters::getMappings, options,
            GetMappingsResponse::fromXContent, listener, emptySet());
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
    public IndicesAliasesResponse updateAliases(IndicesAliasesRequest indicesAliasesRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(indicesAliasesRequest, RequestConverters::updateAliases, options,
                IndicesAliasesResponse::fromXContent, emptySet());
    }

    /**
     * Updates aliases using the Index Aliases API.
     * <p>
     * See <a href=
     * "https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html">
     * Index Aliases API on elastic.co</a>
     * @deprecated {@link #updateAliases(IndicesAliasesRequest, RequestOptions)}
     */
    @Deprecated
    public IndicesAliasesResponse updateAliases(IndicesAliasesRequest indicesAliasesRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(indicesAliasesRequest, RequestConverters::updateAliases,
                IndicesAliasesResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously updates aliases using the Index Aliases API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html">
     * Index Aliases API on elastic.co</a>
     * @param indicesAliasesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void updateAliasesAsync(IndicesAliasesRequest indicesAliasesRequest, RequestOptions options,
                                   ActionListener<IndicesAliasesResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(indicesAliasesRequest, RequestConverters::updateAliases, options,
                IndicesAliasesResponse::fromXContent, listener, emptySet());
    }

    /**
     * Asynchronously updates aliases using the Index Aliases API.
     * <p>
     * See <a href=
     * "https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html">
     * Index Aliases API on elastic.co</a>
     * @deprecated Prefer {@link #updateAliasesAsync(IndicesAliasesRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void updateAliasesAsync(IndicesAliasesRequest indicesAliasesRequest, ActionListener<IndicesAliasesResponse> listener,
                                   Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(indicesAliasesRequest, RequestConverters::updateAliases,
                IndicesAliasesResponse::fromXContent, listener, emptySet(), headers);
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
        return restHighLevelClient.performRequestAndParseEntity(openIndexRequest, RequestConverters::openIndex, options,
                OpenIndexResponse::fromXContent, emptySet());
    }

    /**
     * Opens an index using the Open Index API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html">
     * Open Index API on elastic.co</a>
     * @deprecated Prefer {@link #open(OpenIndexRequest, RequestOptions)}
     */
    @Deprecated
    public OpenIndexResponse open(OpenIndexRequest openIndexRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(openIndexRequest, RequestConverters::openIndex,
                OpenIndexResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously opens an index using the Open Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html">
     * Open Index API on elastic.co</a>
     * @param openIndexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void openAsync(OpenIndexRequest openIndexRequest, RequestOptions options, ActionListener<OpenIndexResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(openIndexRequest, RequestConverters::openIndex, options,
                OpenIndexResponse::fromXContent, listener, emptySet());
    }

    /**
     * Asynchronously opens an index using the Open Index API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html">
     * Open Index API on elastic.co</a>
     * @deprecated Prefer {@link #openAsync(OpenIndexRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void openAsync(OpenIndexRequest openIndexRequest, ActionListener<OpenIndexResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(openIndexRequest, RequestConverters::openIndex,
                OpenIndexResponse::fromXContent, listener, emptySet(), headers);
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
        return restHighLevelClient.performRequestAndParseEntity(closeIndexRequest, RequestConverters::closeIndex, options,
                CloseIndexResponse::fromXContent, emptySet());
    }

    /**
     * Closes an index using the Close Index API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html">
     * Close Index API on elastic.co</a>
     * @deprecated Prefer {@link #close(CloseIndexRequest, RequestOptions)}
     */
    @Deprecated
    public CloseIndexResponse close(CloseIndexRequest closeIndexRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(closeIndexRequest, RequestConverters::closeIndex,
                CloseIndexResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously closes an index using the Close Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html">
     * Close Index API on elastic.co</a>
     * @param closeIndexRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void closeAsync(CloseIndexRequest closeIndexRequest, RequestOptions options, ActionListener<CloseIndexResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(closeIndexRequest, RequestConverters::closeIndex, options,
                CloseIndexResponse::fromXContent, listener, emptySet());
    }


    /**
     * Asynchronously closes an index using the Close Index API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html">
     * Close Index API on elastic.co</a>
     * @deprecated Prefer {@link #closeAsync(CloseIndexRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void closeAsync(CloseIndexRequest closeIndexRequest, ActionListener<CloseIndexResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(closeIndexRequest, RequestConverters::closeIndex,
                CloseIndexResponse::fromXContent, listener, emptySet(), headers);
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
        return restHighLevelClient.performRequest(getAliasesRequest, RequestConverters::existsAlias, options,
                RestHighLevelClient::convertExistsResponse, emptySet());
    }

    /**
     * Checks if one or more aliases exist using the Aliases Exist API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html">
     * Indices Aliases API on elastic.co</a>
     * @deprecated Prefer {@link #existsAlias(GetAliasesRequest, RequestOptions)}
     */
    @Deprecated
    public boolean existsAlias(GetAliasesRequest getAliasesRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequest(getAliasesRequest, RequestConverters::existsAlias,
                RestHighLevelClient::convertExistsResponse, emptySet(), headers);
    }

    /**
     * Asynchronously checks if one or more aliases exist using the Aliases Exist API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html">
     * Indices Aliases API on elastic.co</a>
     * @param getAliasesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void existsAliasAsync(GetAliasesRequest getAliasesRequest, RequestOptions options, ActionListener<Boolean> listener) {
        restHighLevelClient.performRequestAsync(getAliasesRequest, RequestConverters::existsAlias, options,
                RestHighLevelClient::convertExistsResponse, listener, emptySet());
    }

    /**
     * Asynchronously checks if one or more aliases exist using the Aliases Exist API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html">
     * Indices Aliases API on elastic.co</a>
     * @deprecated Prefer {@link #existsAliasAsync(GetAliasesRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void existsAliasAsync(GetAliasesRequest getAliasesRequest, ActionListener<Boolean> listener, Header... headers) {
        restHighLevelClient.performRequestAsync(getAliasesRequest, RequestConverters::existsAlias,
                RestHighLevelClient::convertExistsResponse, listener, emptySet(), headers);
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
        return restHighLevelClient.performRequestAndParseEntity(refreshRequest, RequestConverters::refresh, options,
                RefreshResponse::fromXContent, emptySet());
    }

    /**
     * Refresh one or more indices using the Refresh API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-refresh.html"> Refresh API on elastic.co</a>
     * @deprecated Prefer {@link #refresh(RefreshRequest, RequestOptions)}
     */
    @Deprecated
    public RefreshResponse refresh(RefreshRequest refreshRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(refreshRequest, RequestConverters::refresh, RefreshResponse::fromXContent,
                emptySet(), headers);
    }

    /**
     * Asynchronously refresh one or more indices using the Refresh API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-refresh.html"> Refresh API on elastic.co</a>
     * @param refreshRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void refreshAsync(RefreshRequest refreshRequest, RequestOptions options, ActionListener<RefreshResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(refreshRequest, RequestConverters::refresh, options,
                RefreshResponse::fromXContent, listener, emptySet());
    }

    /**
     * Asynchronously refresh one or more indices using the Refresh API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-refresh.html"> Refresh API on elastic.co</a>
     * @deprecated Prefer {@link #refreshAsync(RefreshRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void refreshAsync(RefreshRequest refreshRequest, ActionListener<RefreshResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(refreshRequest, RequestConverters::refresh, RefreshResponse::fromXContent,
                listener, emptySet(), headers);
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
        return restHighLevelClient.performRequestAndParseEntity(flushRequest, RequestConverters::flush, options,
                FlushResponse::fromXContent, emptySet());
    }

    /**
     * Flush one or more indices using the Flush API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-flush.html"> Flush API on elastic.co</a>
     * @deprecated Prefer {@link #flush(FlushRequest, RequestOptions)}
     */
    @Deprecated
    public FlushResponse flush(FlushRequest flushRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(flushRequest, RequestConverters::flush, FlushResponse::fromXContent,
                emptySet(), headers);
    }

    /**
     * Asynchronously flush one or more indices using the Flush API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-flush.html"> Flush API on elastic.co</a>
     * @param flushRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void flushAsync(FlushRequest flushRequest, RequestOptions options, ActionListener<FlushResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(flushRequest, RequestConverters::flush, options,
                FlushResponse::fromXContent, listener, emptySet());
    }

    /**
     * Asynchronously flush one or more indices using the Flush API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-flush.html"> Flush API on elastic.co</a>
     * @deprecated Prefer {@link #flushAsync(FlushRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void flushAsync(FlushRequest flushRequest, ActionListener<FlushResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(flushRequest, RequestConverters::flush, FlushResponse::fromXContent,
                listener, emptySet(), headers);
    }

    /**
     * Initiate a synced flush manually using the synced flush API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-synced-flush.html">
     *     Synced flush API on elastic.co</a>
     * @param syncedFlushRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public SyncedFlushResponse flushSynced(SyncedFlushRequest syncedFlushRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(syncedFlushRequest, RequestConverters::flushSynced, options,
                SyncedFlushResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously initiate a synced flush manually using the synced flush API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-synced-flush.html">
     *     Synced flush API on elastic.co</a>
     * @param syncedFlushRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void flushSyncedAsync(SyncedFlushRequest syncedFlushRequest, RequestOptions options,
                                 ActionListener<SyncedFlushResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(syncedFlushRequest, RequestConverters::flushSynced, options,
                SyncedFlushResponse::fromXContent, listener, emptySet());
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
        return restHighLevelClient.performRequestAndParseEntity(getSettingsRequest, RequestConverters::getSettings, options,
            GetSettingsResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously retrieve the settings of one or more indices.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-settings.html">
     * Indices Get Settings API on elastic.co</a>
     * @param getSettingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void getSettingsAsync(GetSettingsRequest getSettingsRequest, RequestOptions options,
                                 ActionListener<GetSettingsResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(getSettingsRequest, RequestConverters::getSettings, options,
            GetSettingsResponse::fromXContent, listener, emptySet());
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
    public ForceMergeResponse forceMerge(ForceMergeRequest forceMergeRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(forceMergeRequest, RequestConverters::forceMerge, options,
                ForceMergeResponse::fromXContent, emptySet());
    }

    /**
     * Force merge one or more indices using the Force Merge API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-forcemerge.html">
     * Force Merge API on elastic.co</a>
     * @deprecated Prefer {@link #forceMerge(ForceMergeRequest, RequestOptions)}
     */
    @Deprecated
    public ForceMergeResponse forceMerge(ForceMergeRequest forceMergeRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(forceMergeRequest, RequestConverters::forceMerge,
                ForceMergeResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously force merge one or more indices using the Force Merge API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-forcemerge.html">
     * Force Merge API on elastic.co</a>
     * @param forceMergeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void forceMergeAsync(ForceMergeRequest forceMergeRequest, RequestOptions options, ActionListener<ForceMergeResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(forceMergeRequest, RequestConverters::forceMerge, options,
                ForceMergeResponse::fromXContent, listener, emptySet());
    }

    /**
     * Asynchronously force merge one or more indices using the Force Merge API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-forcemerge.html">
     * Force Merge API on elastic.co</a>
     * @deprecated Prefer {@link #forceMergeAsync(ForceMergeRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void forceMergeAsync(ForceMergeRequest forceMergeRequest, ActionListener<ForceMergeResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(forceMergeRequest, RequestConverters::forceMerge,
                ForceMergeResponse::fromXContent, listener, emptySet(), headers);
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
    public ClearIndicesCacheResponse clearCache(ClearIndicesCacheRequest clearIndicesCacheRequest,
                                                RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(clearIndicesCacheRequest, RequestConverters::clearCache, options,
                ClearIndicesCacheResponse::fromXContent, emptySet());
    }

    /**
     * Clears the cache of one or more indices using the Clear Cache API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-clearcache.html">
     * Clear Cache API on elastic.co</a>
     * @deprecated Prefer {@link #clearCache(ClearIndicesCacheRequest, RequestOptions)}
     */
    @Deprecated
    public ClearIndicesCacheResponse clearCache(ClearIndicesCacheRequest clearIndicesCacheRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(clearIndicesCacheRequest, RequestConverters::clearCache,
                ClearIndicesCacheResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously clears the cache of one or more indices using the Clear Cache API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-clearcache.html">
     * Clear Cache API on elastic.co</a>
     * @param clearIndicesCacheRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void clearCacheAsync(ClearIndicesCacheRequest clearIndicesCacheRequest, RequestOptions options,
                                ActionListener<ClearIndicesCacheResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(clearIndicesCacheRequest, RequestConverters::clearCache, options,
                ClearIndicesCacheResponse::fromXContent, listener, emptySet());
    }

    /**
     * Asynchronously clears the cache of one or more indices using the Clear Cache API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-clearcache.html">
     * Clear Cache API on elastic.co</a>
     * @deprecated Prefer {@link #clearCacheAsync(ClearIndicesCacheRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void clearCacheAsync(ClearIndicesCacheRequest clearIndicesCacheRequest, ActionListener<ClearIndicesCacheResponse> listener,
                           Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(clearIndicesCacheRequest, RequestConverters::clearCache,
                ClearIndicesCacheResponse::fromXContent, listener, emptySet(), headers);
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
            RequestConverters::indicesExist,
            options,
            RestHighLevelClient::convertExistsResponse,
            Collections.emptySet()
        );
    }

    /**
     * Checks if the index (indices) exists or not.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-exists.html">
     * Indices Exists API on elastic.co</a>
     * @deprecated Prefer {@link #exists(GetIndexRequest, RequestOptions)}
     */
    @Deprecated
    public boolean exists(GetIndexRequest request, Header... headers) throws IOException {
        return restHighLevelClient.performRequest(
                request,
                RequestConverters::indicesExist,
                RestHighLevelClient::convertExistsResponse,
                Collections.emptySet(),
                headers
        );
    }

    /**
     * Asynchronously checks if the index (indices) exists or not.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-exists.html">
     * Indices Exists API on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void existsAsync(GetIndexRequest request, RequestOptions options, ActionListener<Boolean> listener) {
        restHighLevelClient.performRequestAsync(
                request,
                RequestConverters::indicesExist,
                options,
                RestHighLevelClient::convertExistsResponse,
                listener,
                Collections.emptySet()
        );
    }

    /**
     * Asynchronously checks if the index (indices) exists or not.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-exists.html">
     * Indices Exists API on elastic.co</a>
     * @deprecated Prefer {@link #existsAsync(GetIndexRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void existsAsync(GetIndexRequest request, ActionListener<Boolean> listener, Header... headers) {
        restHighLevelClient.performRequestAsync(
            request,
            RequestConverters::indicesExist,
            RestHighLevelClient::convertExistsResponse,
            listener,
            Collections.emptySet(),
            headers
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
        return restHighLevelClient.performRequestAndParseEntity(resizeRequest, RequestConverters::shrink, options,
                ResizeResponse::fromXContent, emptySet());
    }

    /**
     * Shrinks an index using the Shrink Index API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-shrink-index.html">
     * Shrink Index API on elastic.co</a>
     * @deprecated Prefer {@link #shrink(ResizeRequest, RequestOptions)}
     */
    @Deprecated
    public ResizeResponse shrink(ResizeRequest resizeRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(resizeRequest, RequestConverters::shrink, ResizeResponse::fromXContent,
                emptySet(), headers);
    }

    /**
     * Asynchronously shrinks an index using the Shrink index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-shrink-index.html">
     * Shrink Index API on elastic.co</a>
     * @param resizeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void shrinkAsync(ResizeRequest resizeRequest, RequestOptions options, ActionListener<ResizeResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(resizeRequest, RequestConverters::shrink, options,
                ResizeResponse::fromXContent, listener, emptySet());
    }

    /**
     * Asynchronously shrinks an index using the Shrink index API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-shrink-index.html">
     * Shrink Index API on elastic.co</a>
     * @deprecated Prefer {@link #shrinkAsync(ResizeRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void shrinkAsync(ResizeRequest resizeRequest, ActionListener<ResizeResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(resizeRequest, RequestConverters::shrink, ResizeResponse::fromXContent,
                listener, emptySet(), headers);
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
        return restHighLevelClient.performRequestAndParseEntity(resizeRequest, RequestConverters::split, options,
                ResizeResponse::fromXContent, emptySet());
    }

    /**
     * Splits an index using the Split Index API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-split-index.html">
     * Split Index API on elastic.co</a>
     * @deprecated {@link #split(ResizeRequest, RequestOptions)}
     */
    @Deprecated
    public ResizeResponse split(ResizeRequest resizeRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(resizeRequest, RequestConverters::split, ResizeResponse::fromXContent,
                emptySet(), headers);
    }

    /**
     * Asynchronously splits an index using the Split Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-split-index.html">
     * Split Index API on elastic.co</a>
     * @param resizeRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void splitAsync(ResizeRequest resizeRequest, RequestOptions options, ActionListener<ResizeResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(resizeRequest, RequestConverters::split, options,
                ResizeResponse::fromXContent, listener, emptySet());
    }

    /**
     * Asynchronously splits an index using the Split Index API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-split-index.html">
     * Split Index API on elastic.co</a>
     * @deprecated Prefer {@link #splitAsync(ResizeRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void splitAsync(ResizeRequest resizeRequest, ActionListener<ResizeResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(resizeRequest, RequestConverters::split, ResizeResponse::fromXContent,
                listener, emptySet(), headers);
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
        return restHighLevelClient.performRequestAndParseEntity(rolloverRequest, RequestConverters::rollover, options,
                RolloverResponse::fromXContent, emptySet());
    }

    /**
     * Rolls over an index using the Rollover Index API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-rollover-index.html">
     * Rollover Index API on elastic.co</a>
     * @deprecated Prefer {@link #rollover(RolloverRequest, RequestOptions)}
     */
    @Deprecated
    public RolloverResponse rollover(RolloverRequest rolloverRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(rolloverRequest, RequestConverters::rollover,
                RolloverResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously rolls over an index using the Rollover Index API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-rollover-index.html">
     * Rollover Index API on elastic.co</a>
     * @param rolloverRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void rolloverAsync(RolloverRequest rolloverRequest, RequestOptions options, ActionListener<RolloverResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(rolloverRequest, RequestConverters::rollover, options,
                RolloverResponse::fromXContent, listener, emptySet());
    }

    /**
     * Asynchronously rolls over an index using the Rollover Index API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-rollover-index.html">
     * Rollover Index API on elastic.co</a>
     * @deprecated Prefer {@link #rolloverAsync(RolloverRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void rolloverAsync(RolloverRequest rolloverRequest, ActionListener<RolloverResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(rolloverRequest, RequestConverters::rollover, RolloverResponse::fromXContent,
                listener, emptySet(), headers);
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
    public UpdateSettingsResponse putSettings(UpdateSettingsRequest updateSettingsRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(updateSettingsRequest, RequestConverters::indexPutSettings, options,
                UpdateSettingsResponse::fromXContent, emptySet());
    }

    /**
     * Updates specific index level settings using the Update Indices Settings API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html"> Update Indices Settings
     * API on elastic.co</a>
     * @deprecated Prefer {@link #putSettings(UpdateSettingsRequest, RequestOptions)}
     */
    @Deprecated
    public UpdateSettingsResponse putSettings(UpdateSettingsRequest updateSettingsRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(updateSettingsRequest, RequestConverters::indexPutSettings,
                UpdateSettingsResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously updates specific index level settings using the Update Indices Settings API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html"> Update Indices Settings
     * API on elastic.co</a>
     * @param updateSettingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void putSettingsAsync(UpdateSettingsRequest updateSettingsRequest, RequestOptions options,
                                 ActionListener<UpdateSettingsResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(updateSettingsRequest, RequestConverters::indexPutSettings, options,
                UpdateSettingsResponse::fromXContent, listener, emptySet());
    }

    /**
     * Asynchronously updates specific index level settings using the Update Indices Settings API.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html"> Update Indices Settings
     * API on elastic.co</a>
     * @deprecated Prefer {@link #putSettingsAsync(UpdateSettingsRequest, RequestOptions, ActionListener)}
     */
    @Deprecated
    public void putSettingsAsync(UpdateSettingsRequest updateSettingsRequest, ActionListener<UpdateSettingsResponse> listener,
            Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(updateSettingsRequest, RequestConverters::indexPutSettings,
                UpdateSettingsResponse::fromXContent, listener, emptySet(), headers);
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
    public PutIndexTemplateResponse putTemplate(PutIndexTemplateRequest putIndexTemplateRequest,
                                                RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(putIndexTemplateRequest, RequestConverters::putTemplate, options,
            PutIndexTemplateResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously puts an index template using the Index Templates API.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html"> Index Templates API
     * on elastic.co</a>
     * @param putIndexTemplateRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void putTemplateAsync(PutIndexTemplateRequest putIndexTemplateRequest, RequestOptions options,
                                 ActionListener<PutIndexTemplateResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(putIndexTemplateRequest, RequestConverters::putTemplate, options,
            PutIndexTemplateResponse::fromXContent, listener, emptySet());
    }
}
