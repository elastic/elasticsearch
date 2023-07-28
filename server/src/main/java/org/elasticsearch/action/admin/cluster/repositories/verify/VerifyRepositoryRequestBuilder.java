/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.repositories.verify;

import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

/**
 * Builder for verify repository request
 */
public class VerifyRepositoryRequestBuilder extends MasterNodeOperationRequestBuilder<
    VerifyRepositoryRequest,
    VerifyRepositoryResponse,
    VerifyRepositoryRequestBuilder> {

    /**
     * Constructs unregister repository request builder with specified repository name
     */
    public VerifyRepositoryRequestBuilder(ElasticsearchClient client, VerifyRepositoryAction action, String name) {
        super(client, action, new VerifyRepositoryRequest(name));
    }

}
