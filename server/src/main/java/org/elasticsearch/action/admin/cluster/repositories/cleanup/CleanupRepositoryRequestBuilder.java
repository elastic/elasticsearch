/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.repositories.cleanup;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class CleanupRepositoryRequestBuilder extends MasterNodeOperationRequestBuilder<
    CleanupRepositoryRequest,
    CleanupRepositoryResponse,
    CleanupRepositoryRequestBuilder> {

    public CleanupRepositoryRequestBuilder(ElasticsearchClient client, ActionType<CleanupRepositoryResponse> action, String repository) {
        super(client, action, new CleanupRepositoryRequest(repository));
    }

    public CleanupRepositoryRequestBuilder setName(String repository) {
        request.name(repository);
        return this;
    }
}
