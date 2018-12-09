/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Action for putting (adding/updating) one or more application privileges.
 */
public final class PutPrivilegesAction extends Action<PutPrivilegesRequest, PutPrivilegesResponse, PutPrivilegesRequestBuilder> {

    public static final PutPrivilegesAction INSTANCE = new PutPrivilegesAction();
    public static final String NAME = "cluster:admin/xpack/security/privilege/put";

    private PutPrivilegesAction() {
        super(NAME);
    }

    @Override
    public PutPrivilegesResponse newResponse() {
        return new PutPrivilegesResponse();
    }

    @Override
    public PutPrivilegesRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new PutPrivilegesRequestBuilder(client, INSTANCE);
    }
}
