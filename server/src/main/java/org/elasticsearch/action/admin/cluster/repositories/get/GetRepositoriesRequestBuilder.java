/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.repositories.get;

import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

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
    public GetRepositoriesRequestBuilder(ElasticsearchClient client, GetRepositoriesAction action, String... repositories) {
        super(client, action, new GetRepositoriesRequest(repositories));
    }

}
