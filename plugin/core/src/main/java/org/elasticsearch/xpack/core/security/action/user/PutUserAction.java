/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Action for putting (adding/updating) a native user.
 */
public class PutUserAction extends Action<PutUserRequest, PutUserResponse, PutUserRequestBuilder> {

    public static final PutUserAction INSTANCE = new PutUserAction();
    public static final String NAME = "cluster:admin/xpack/security/user/put";

    protected PutUserAction() {
        super(NAME);
    }

    @Override
    public PutUserRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new PutUserRequestBuilder(client, this);
    }

    @Override
    public PutUserResponse newResponse() {
        return new PutUserResponse();
    }
}
