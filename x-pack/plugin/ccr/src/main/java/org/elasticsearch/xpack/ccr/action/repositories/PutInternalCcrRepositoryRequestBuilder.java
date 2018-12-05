/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

class PutInternalCcrRepositoryRequestBuilder extends ActionRequestBuilder<PutInternalCcrRepositoryRequest,
    PutInternalCcrRepositoryAction.PutInternalCcrRepositoryResponse, PutInternalCcrRepositoryRequestBuilder> {

    PutInternalCcrRepositoryRequestBuilder(final ElasticsearchClient client) {
        super(client, PutInternalCcrRepositoryAction.INSTANCE, new PutInternalCcrRepositoryRequest());
    }

    void setName(String name) {
        request.setName(name);
    }

    void setType(String type) {
        request.setType(type);
    }
}
