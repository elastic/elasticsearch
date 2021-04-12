/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.repositories.get;

import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.util.ArrayUtils;

/**
 * Get repository request builder
 */
public class GetRepositoriesRequestBuilder extends MasterNodeReadOperationRequestBuilder<
    GetRepositoriesRequest,
    GetRepositoriesResponse,
    GetRepositoriesRequestBuilder> {

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
