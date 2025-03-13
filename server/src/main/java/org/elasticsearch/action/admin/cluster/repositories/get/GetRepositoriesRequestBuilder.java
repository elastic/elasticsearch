/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.repositories.get;

import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.core.TimeValue;

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
    public GetRepositoriesRequestBuilder(ElasticsearchClient client, TimeValue masterNodeTimeout, String... repositories) {
        super(client, GetRepositoriesAction.INSTANCE, new GetRepositoriesRequest(masterNodeTimeout, repositories));
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
