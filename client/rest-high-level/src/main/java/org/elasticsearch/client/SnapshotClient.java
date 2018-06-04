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
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     */
    public GetRepositoriesResponse getRepositories(GetRepositoriesRequest getRepositoriesRequest, Header... headers)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(getRepositoriesRequest, RequestConverters::getRepositories,
            GetRepositoriesResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously gets a list of snapshot repositories. If the list of repositories is empty or it contains a single element "_all", all
     * registered repositories are returned.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     */
    public void getRepositoriesAsync(GetRepositoriesRequest getRepositoriesRequest,
                                     ActionListener<GetRepositoriesResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(getRepositoriesRequest, RequestConverters::getRepositories,
            GetRepositoriesResponse::fromXContent, listener, emptySet(), headers);
    }

    /**
     * Creates a snapshot repository.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     */
    public PutRepositoryResponse createRepository(PutRepositoryRequest putRepositoryRequest, Header... headers) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(putRepositoryRequest, RequestConverters::createRepository,
            PutRepositoryResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously creates a snapshot repository.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     */
    public void createRepositoryAsync(PutRepositoryRequest putRepositoryRequest,
                                      ActionListener<PutRepositoryResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(putRepositoryRequest, RequestConverters::createRepository,
            PutRepositoryResponse::fromXContent, listener, emptySet(), headers);
    }

    /**
     * Deletes a snapshot repository.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     */
    public DeleteRepositoryResponse deleteRepository(DeleteRepositoryRequest deleteRepositoryRequest, Header... headers)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(deleteRepositoryRequest, RequestConverters::deleteRepository,
            DeleteRepositoryResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously deletes a snapshot repository.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     */
    public void deleteRepositoryAsync(DeleteRepositoryRequest deleteRepositoryRequest,
                                      ActionListener<DeleteRepositoryResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(deleteRepositoryRequest, RequestConverters::deleteRepository,
            DeleteRepositoryResponse::fromXContent, listener, emptySet(), headers);
    }

    /**
     * Verifies a snapshot repository.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     */
    public VerifyRepositoryResponse verifyRepository(VerifyRepositoryRequest verifyRepositoryRequest, Header... headers)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(verifyRepositoryRequest, RequestConverters::verifyRepository,
            VerifyRepositoryResponse::fromXContent, emptySet(), headers);
    }

    /**
     * Asynchronously verifies a snapshot repository.
     * <p>
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html"> Snapshot and Restore
     * API on elastic.co</a>
     */
    public void verifyRepositoryAsync(VerifyRepositoryRequest verifyRepositoryRequest,
                                      ActionListener<VerifyRepositoryResponse> listener, Header... headers) {
        restHighLevelClient.performRequestAsyncAndParseEntity(verifyRepositoryRequest, RequestConverters::verifyRepository,
            VerifyRepositoryResponse::fromXContent, listener, emptySet(), headers);
    }
}
