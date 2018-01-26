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
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;

import java.io.IOException;

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
    public void updateAliasesAsync(IndicesAliasesRequest indicesAliasesRequestRequest, ActionListener<IndicesAliasesResponse> listener,
            Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(indicesAliasesRequestRequest, Request::updateAliases,
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
}
