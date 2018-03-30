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
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;

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
     * Deletes an index using the Delete Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html">
     * Delete Index API on elastic.co</a>
     */
    public DeleteIndexResponse delete(DeleteIndexRequest deleteIndexRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(deleteIndexRequest, Request::deleteIndex, DeleteIndexResponse::fromXContent,
                emptySet(), headers);
    }

    /**
     * Asynchronously deletes an index using the Delete Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html">
     * Delete Index API on elastic.co</a>
     */
    public void deleteAsync(DeleteIndexRequest deleteIndexRequest, ActionListener<DeleteIndexResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(deleteIndexRequest, Request::deleteIndex, DeleteIndexResponse::fromXContent,
                listener, emptySet(), headers);
    }

    /**
     * Creates an index using the Create Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html">
     * Create Index API on elastic.co</a>
     */
    public CreateIndexResponse create(CreateIndexRequest createIndexRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(createIndexRequest, Request::createIndex, CreateIndexResponse::fromXContent,
                emptySet(), headers);
    }

    /**
     * Asynchronously creates an index using the Create Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html">
     * Create Index API on elastic.co</a>
     */
    public void createAsync(CreateIndexRequest createIndexRequest, ActionListener<CreateIndexResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(createIndexRequest, Request::createIndex, CreateIndexResponse::fromXContent,
                listener, emptySet(), headers);
    }

    /**
     * Updates the mappings on an index using the Put Mapping API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-put-mapping.html">
     * Put Mapping API on elastic.co</a>
     */
    public PutMappingResponse putMapping(PutMappingRequest putMappingRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(putMappingRequest, Request::putMapping, PutMappingResponse::fromXContent,
                emptySet(), headers);
    }

    /**
     * Asynchronously updates the mappings on an index using the Put Mapping API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-put-mapping.html">
     * Put Mapping API on elastic.co</a>
     */
    public void putMappingAsync(PutMappingRequest putMappingRequest, ActionListener<PutMappingResponse> listener,
                                       Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(putMappingRequest, Request::putMapping, PutMappingResponse::fromXContent,
                listener, emptySet(), headers);
    }

    /**
     * Updates aliases using the Index Aliases API
     * <p>
     * See <a href=
     * "https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html">
     * Index Aliases API on elastic.co</a>
     */
    public IndicesAliasesResponse updateAliases(IndicesAliasesRequest indicesAliasesRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(indicesAliasesRequest, Request::updateAliases,
                IndicesAliasesResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously updates aliases using the Index Aliases API
     * <p>
     * See <a href=
     * "https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html">
     * Index Aliases API on elastic.co</a>
     */
    public void updateAliasesAsync(IndicesAliasesRequest indicesAliasesRequest, ActionListener<IndicesAliasesResponse> listener,
            Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(indicesAliasesRequest, Request::updateAliases,
                IndicesAliasesResponse::fromXContent, listener, emptySet(), headers);
    }

    /**
     * Opens an index using the Open Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html">
     * Open Index API on elastic.co</a>
     */
    public OpenIndexResponse open(OpenIndexRequest openIndexRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(openIndexRequest, Request::openIndex, OpenIndexResponse::fromXContent,
                emptySet(), headers);
    }

    /**
     * Asynchronously opens an index using the Open Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html">
     * Open Index API on elastic.co</a>
     */
    public void openAsync(OpenIndexRequest openIndexRequest, ActionListener<OpenIndexResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(openIndexRequest, Request::openIndex, OpenIndexResponse::fromXContent,
                listener, emptySet(), headers);
    }

    /**
     * Closes an index using the Close Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html">
     * Close Index API on elastic.co</a>
     */
    public CloseIndexResponse close(CloseIndexRequest closeIndexRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(closeIndexRequest, Request::closeIndex, CloseIndexResponse::fromXContent,
                emptySet(), headers);
    }

    /**
     * Asynchronously closes an index using the Close Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html">
     * Close Index API on elastic.co</a>
     */
    public void closeAsync(CloseIndexRequest closeIndexRequest, ActionListener<CloseIndexResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(closeIndexRequest, Request::closeIndex, CloseIndexResponse::fromXContent,
                listener, emptySet(), headers);
    }

    /**
     * Checks if one or more aliases exist using the Aliases Exist API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html">
     * Indices Aliases API on elastic.co</a>
     */
    public boolean existsAlias(GetAliasesRequest getAliasesRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequest(getAliasesRequest, Request::existsAlias, RestHighLevelClient::convertExistsResponse,
                emptySet(), headers);
    }

    /**
     * Asynchronously checks if one or more aliases exist using the Aliases Exist API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html">
     * Indices Aliases API on elastic.co</a>
     */
    public void existsAliasAsync(GetAliasesRequest getAliasesRequest, ActionListener<Boolean> listener, Header... headers) {
        restHighLevelClient.performRequestAsync(getAliasesRequest, Request::existsAlias, RestHighLevelClient::convertExistsResponse,
                listener, emptySet(), headers);
    }

    /**
     * Refresh one or more indices using the Refresh API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-refresh.html"> Refresh API on elastic.co</a>
     */
    public RefreshResponse refresh(RefreshRequest refreshRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(refreshRequest, Request::refresh, RefreshResponse::fromXContent,
                emptySet(), headers);
    }

    /**
     * Asynchronously refresh one or more indices using the Refresh API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-refresh.html"> Refresh API on elastic.co</a>
     */
    public void refreshAsync(RefreshRequest refreshRequest, ActionListener<RefreshResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(refreshRequest, Request::refresh, RefreshResponse::fromXContent,
                listener, emptySet(), headers);
    }

    /**
     * Flush one or more indices using the Flush API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-flush.html"> Flush API on elastic.co</a>
     */
    public FlushResponse flush(FlushRequest flushRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(flushRequest, Request::flush, FlushResponse::fromXContent,
                emptySet(), headers);
    }

    /**
     * Asynchronously flush one or more indices using the Flush API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-flush.html"> Flush API on elastic.co</a>
     */
    public void flushAsync(FlushRequest flushRequest, ActionListener<FlushResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(flushRequest, Request::flush, FlushResponse::fromXContent,
                listener, emptySet(), headers);
    }

    /**
     * Force merge one or more indices using the Force Merge API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-forcemerge.html">
     * Force Merge API on elastic.co</a>
     */
    public ForceMergeResponse forceMerge(ForceMergeRequest forceMergeRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(forceMergeRequest, Request::forceMerge, ForceMergeResponse::fromXContent,
            emptySet(), headers);
    }

    /**
     * Asynchronously force merge one or more indices using the Force Merge API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-forcemerge.html">
     * Force Merge API on elastic.co</a>
     */
    public void forceMergeAsync(ForceMergeRequest forceMergeRequest, ActionListener<ForceMergeResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(forceMergeRequest, Request::forceMerge, ForceMergeResponse::fromXContent,
                listener, emptySet(), headers);
    }

    /**
     * Clears the cache of one or more indices using the Clear Cache API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-clearcache.html">
     * Clear Cache API on elastic.co</a>
     */
    public ClearIndicesCacheResponse clearCache(ClearIndicesCacheRequest clearIndicesCacheRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(clearIndicesCacheRequest, Request::clearCache,
                ClearIndicesCacheResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously clears the cache of one or more indices using the Clear Cache API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-clearcache.html">
     * Clear Cache API on elastic.co</a>
     */
    public void clearCacheAsync(ClearIndicesCacheRequest clearIndicesCacheRequest, ActionListener<ClearIndicesCacheResponse> listener,
                           Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(clearIndicesCacheRequest, Request::clearCache,
                ClearIndicesCacheResponse::fromXContent, listener, emptySet(), headers);
    }

    /**
     * Checks if the index (indices) exists or not.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-exists.html">
     * Indices Exists API on elastic.co</a>
     */
    public boolean exists(GetIndexRequest request, Header... headers) throws IOException {
        return restHighLevelClient.performRequest(
            request,
            Request::indicesExist,
            RestHighLevelClient::convertExistsResponse,
            Collections.emptySet(),
            headers
        );
    }

    /**
     * Asynchronously checks if the index (indices) exists or not.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-exists.html">
     * Indices Exists API on elastic.co</a>
     */
    public void existsAsync(GetIndexRequest request, ActionListener<Boolean> listener, Header... headers) {
        restHighLevelClient.performRequestAsync(
            request,
            Request::indicesExist,
            RestHighLevelClient::convertExistsResponse,
            listener,
            Collections.emptySet(),
            headers
        );
    }

    /**
     * Shrinks an index using the Shrink Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-shrink-index.html">
     * Shrink Index API on elastic.co</a>
     */
    public ResizeResponse shrink(ResizeRequest resizeRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(resizeRequest, Request::shrink, ResizeResponse::fromXContent,
                emptySet(), headers);
    }

    /**
     * Asynchronously shrinks an index using the Shrink index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-shrink-index.html">
     * Shrink Index API on elastic.co</a>
     */
    public void shrinkAsync(ResizeRequest resizeRequest, ActionListener<ResizeResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(resizeRequest, Request::shrink, ResizeResponse::fromXContent,
                listener, emptySet(), headers);
    }

    /**
     * Splits an index using the Split Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-split-index.html">
     * Split Index API on elastic.co</a>
     */
    public ResizeResponse split(ResizeRequest resizeRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(resizeRequest, Request::split, ResizeResponse::fromXContent,
                emptySet(), headers);
    }

    /**
     * Asynchronously splits an index using the Split Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-split-index.html">
     * Split Index API on elastic.co</a>
     */
    public void splitAsync(ResizeRequest resizeRequest, ActionListener<ResizeResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(resizeRequest, Request::split, ResizeResponse::fromXContent,
                listener, emptySet(), headers);
    }

    /**
     * Rolls over an index using the Rollover Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-rollover-index.html">
     * Rollover Index API on elastic.co</a>
     */
    public RolloverResponse rollover(RolloverRequest rolloverRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(rolloverRequest, Request::rollover, RolloverResponse::fromXContent,
                emptySet(), headers);
    }

    /**
     * Asynchronously rolls over an index using the Rollover Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-rollover-index.html">
     * Rollover Index API on elastic.co</a>
     */
    public void rolloverAsync(RolloverRequest rolloverRequest, ActionListener<RolloverResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(rolloverRequest, Request::rollover, RolloverResponse::fromXContent,
                listener, emptySet(), headers);
    }
}
