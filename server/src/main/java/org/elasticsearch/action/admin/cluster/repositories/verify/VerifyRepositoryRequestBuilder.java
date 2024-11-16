/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.repositories.verify;

import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;

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
    public VerifyRepositoryRequestBuilder(ElasticsearchClient client, TimeValue masterNodeTimeout, TimeValue ackTimeout, String name) {
        super(client, VerifyRepositoryAction.INSTANCE, new VerifyRepositoryRequest(masterNodeTimeout, ackTimeout, name));
    }

    /**
     * Sets the repository name
     *
     * @param name the repository name
     */
    public VerifyRepositoryRequestBuilder setName(String name) {
        request.name(name);
        return this;
    }
}
