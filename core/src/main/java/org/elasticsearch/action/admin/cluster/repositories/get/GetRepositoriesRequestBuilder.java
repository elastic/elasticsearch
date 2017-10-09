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

import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.util.ArrayUtils;

/**
 * Get repository request builder
 */
public class GetRepositoriesRequestBuilder extends MasterNodeReadOperationRequestBuilder<GetRepositoriesRequest, GetRepositoriesResponse, GetRepositoriesRequestBuilder> {

    /**
     * Creates new get repository request builder
     */
    public GetRepositoriesRequestBuilder(ElasticsearchClient client, GetRepositoriesAction action) {
        super(client, action, new GetRepositoriesRequest());
    }

    /**
     * Creates new get repository request builder
     */
    public GetRepositoriesRequestBuilder(ElasticsearchClient client, GetRepositoriesAction action, String... repositories) {
        super(client, action, new GetRepositoriesRequest(repositories));
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
        request.repositories(ArrayUtils.concat(request.repositories(), repositories));
        return this;
    }
}
