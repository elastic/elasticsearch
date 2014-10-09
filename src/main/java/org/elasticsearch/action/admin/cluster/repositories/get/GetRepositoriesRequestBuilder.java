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

package org.elasticsearch.action.admin.cluster.repositories.get;

import com.google.common.collect.ObjectArrays;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;

/**
 * Get repository request builder
 */
public class GetRepositoriesRequestBuilder extends MasterNodeReadOperationRequestBuilder<GetRepositoriesRequest, GetRepositoriesResponse, GetRepositoriesRequestBuilder, ClusterAdminClient> {

    /**
     * Creates new get repository request builder
     *
     * @param clusterAdminClient cluster admin client
     */
    public GetRepositoriesRequestBuilder(ClusterAdminClient clusterAdminClient) {
        super(clusterAdminClient, new GetRepositoriesRequest());
    }

    /**
     * Creates new get repository request builder
     *
     * @param clusterAdminClient cluster admin client
     * @param repositories       list of repositories to get
     */
    public GetRepositoriesRequestBuilder(ClusterAdminClient clusterAdminClient, String... repositories) {
        super(clusterAdminClient, new GetRepositoriesRequest(repositories));
    }

    /**
     * Sets list of repositories to get
     *
     * @param repositories list of repositories
     * @return builder
     */
    public GetRepositoriesRequestBuilder setRepositories(String... repositories) {
        request.repositories(repositories);
        return this;
    }

    /**
     * Adds repositories to the list of repositories to get
     *
     * @param repositories list of repositories
     * @return builder
     */
    public GetRepositoriesRequestBuilder addRepositories(String... repositories) {
        request.repositories(ObjectArrays.concat(request.repositories(), repositories, String.class));
        return this;
    }

    @Override
    protected void doExecute(ActionListener<GetRepositoriesResponse> listener) {
        client.getRepositories(request, listener);
    }
}
