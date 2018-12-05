/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

class DeleteInternalCcrRepositoryRequestBuilder extends ActionRequestBuilder<DeleteInternalCcrRepositoryRequest,
    DeleteInternalCcrRepositoryAction.DeleteInternalCcrRepositoryResponse, DeleteInternalCcrRepositoryRequestBuilder> {

    DeleteInternalCcrRepositoryRequestBuilder(final ElasticsearchClient client) {
        super(client, DeleteInternalCcrRepositoryAction.INSTANCE, new DeleteInternalCcrRepositoryRequest());
    }

    void setName(String name) {
        request.setName(name);
    }
}
