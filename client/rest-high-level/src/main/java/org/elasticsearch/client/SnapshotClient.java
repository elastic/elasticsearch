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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;

import java.io.IOException;

import static java.util.Collections.emptySet;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for accessing the Snapshot API.
 * <p>
 * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html">Snapshot API on elastic.co</a>
 */
public final class SnapshotClient {
    private final RestHighLevelClient restHighLevelClient;

    SnapshotClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Gets a list of snapshot repositories. If the list of repositories is empty or it contains a single element "_all", all
     * registered repositories are returned.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param getRepositoriesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetRepositoriesResponse getRepositories(GetRepositoriesRequest getRepositoriesRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(getRepositoriesRequest, RequestConverters::getRepositories, options,
            GetRepositoriesResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously gets a list of snapshot repositories. If the list of repositories is empty or it contains a single element "_all", all
     * registered repositories are returned.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param getRepositoriesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void getRepositoriesAsync(GetRepositoriesRequest getRepositoriesRequest, RequestOptions options,
                                     ActionListener<GetRepositoriesResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(getRepositoriesRequest, RequestConverters::getRepositories, options,
            GetRepositoriesResponse::fromXContent, listener, emptySet());
    }

    /**
     * Creates a snapshot repository.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param putRepositoryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public PutRepositoryResponse createRepository(PutRepositoryRequest putRepositoryRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(putRepositoryRequest, RequestConverters::createRepository, options,
            PutRepositoryResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously creates a snapshot repository.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param putRepositoryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void createRepositoryAsync(PutRepositoryRequest putRepositoryRequest, RequestOptions options,
                                      ActionListener<PutRepositoryResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(putRepositoryRequest, RequestConverters::createRepository, options,
            PutRepositoryResponse::fromXContent, listener, emptySet());
    }

    /**
     * Deletes a snapshot repository.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param deleteRepositoryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public DeleteRepositoryResponse deleteRepository(DeleteRepositoryRequest deleteRepositoryRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(deleteRepositoryRequest, RequestConverters::deleteRepository, options,
            DeleteRepositoryResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously deletes a snapshot repository.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param deleteRepositoryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void deleteRepositoryAsync(DeleteRepositoryRequest deleteRepositoryRequest, RequestOptions options,
                                      ActionListener<DeleteRepositoryResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(deleteRepositoryRequest, RequestConverters::deleteRepository, options,
            DeleteRepositoryResponse::fromXContent, listener, emptySet());
    }

    /**
     * Verifies a snapshot repository.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param verifyRepositoryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public VerifyRepositoryResponse verifyRepository(VerifyRepositoryRequest verifyRepositoryRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(verifyRepositoryRequest, RequestConverters::verifyRepository, options,
            VerifyRepositoryResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously verifies a snapshot repository.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     * @param verifyRepositoryRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void verifyRepositoryAsync(VerifyRepositoryRequest verifyRepositoryRequest, RequestOptions options,
                                      ActionListener<VerifyRepositoryResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(verifyRepositoryRequest, RequestConverters::verifyRepository, options,
            VerifyRepositoryResponse::fromXContent, listener, emptySet());
    }
}
