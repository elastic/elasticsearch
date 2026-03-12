/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.repositories.delete;

import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;

/**
 * Builder for unregister repository request
 */
public class DeleteRepositoryRequestBuilder extends AcknowledgedRequestBuilder<
    DeleteRepositoryRequest,
    AcknowledgedResponse,
    DeleteRepositoryRequestBuilder> {

    /**
     * Constructs unregister repository request builder with specified repository name
     */
    public DeleteRepositoryRequestBuilder(ElasticsearchClient client, TimeValue masterNodeTimeout, TimeValue ackTimeout, String name) {
        super(client, TransportDeleteRepositoryAction.TYPE, new DeleteRepositoryRequest(masterNodeTimeout, ackTimeout, name));
    }

    /**
     * Sets the repository name
     *
     * @param name the repository name
     */
    public DeleteRepositoryRequestBuilder setName(String name) {
        request.name(name);
        return this;
    }
}
