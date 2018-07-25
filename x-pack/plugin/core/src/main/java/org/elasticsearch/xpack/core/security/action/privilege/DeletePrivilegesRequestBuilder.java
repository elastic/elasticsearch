/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Builder for {@link DeletePrivilegesRequest}
 */
public final class DeletePrivilegesRequestBuilder
    extends ActionRequestBuilder<DeletePrivilegesRequest, DeletePrivilegesResponse, DeletePrivilegesRequestBuilder>
    implements WriteRequestBuilder<DeletePrivilegesRequestBuilder> {

    public DeletePrivilegesRequestBuilder(ElasticsearchClient client, DeletePrivilegesAction action) {
        super(client, action, new DeletePrivilegesRequest());
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
