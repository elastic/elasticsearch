/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Builder for {@link DeletePrivilegesRequest}
 */
public final class DeletePrivilegesRequestBuilder extends ActionRequestBuilder<DeletePrivilegesRequest, DeletePrivilegesResponse>
        implements WriteRequestBuilder<DeletePrivilegesRequestBuilder> {

    public DeletePrivilegesRequestBuilder(ElasticsearchClient client) {
        super(client, DeletePrivilegesAction.INSTANCE, new DeletePrivilegesRequest());
    }

    public DeletePrivilegesRequestBuilder privileges(String[] privileges) {
        request.privileges(privileges);
        return this;
    }

    public DeletePrivilegesRequestBuilder application(String applicationName) {
        request.application(applicationName);
        return this;
    }
}
