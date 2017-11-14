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
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;

import java.io.IOException;

import static java.util.Collections.emptySet;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for accessing the Indices API.
 *
 * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices.html">Indices API on elastic.co</a>
 */
public final class IndicesClient {
    private final RestHighLevelClient restHighLevelClient;

    public IndicesClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Deletes an index using the Delete Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html">
     * Delete Index API on elastic.co</a>
     */
    public DeleteIndexResponse deleteIndex(DeleteIndexRequest deleteIndexRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(deleteIndexRequest, Request::deleteIndex, DeleteIndexResponse::fromXContent,
            emptySet(), headers);
    }

    /**
     * Asynchronously deletes an index using the Delete Index API
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html">
     * Delete Index API on elastic.co</a>
     */
    public void deleteIndexAsync(DeleteIndexRequest deleteIndexRequest, ActionListener<DeleteIndexResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(deleteIndexRequest, Request::deleteIndex, DeleteIndexResponse::fromXContent,
            listener, emptySet(), headers);
    }

    public IndicesExistsResponse exists(IndicesExistsRequest indicesExistsRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequest(
            indicesExistsRequest,
            Request::indicesExist,
            this::parseIndicesExistResp,
            emptySet(),
            headers
        );
    }

    private IndicesExistsResponse parseIndicesExistResp(Response response) {
        return new IndicesExistsResponse(response.getStatusLine().getStatusCode() == 200);
    }

    public void existsAsync(IndicesExistsRequest indicesExistsRequest, ActionListener<IndicesExistsResponse> listener, Header... headers) throws IOException {
        restHighLevelClient.performRequestAsync(
            indicesExistsRequest,
            Request::indicesExist,
            this::parseIndicesExistResp,
            listener,
            emptySet(),
            headers
        );
    }
}
