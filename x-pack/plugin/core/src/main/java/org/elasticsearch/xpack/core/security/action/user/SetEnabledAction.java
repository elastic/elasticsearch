/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * This action is for setting the enabled flag on a native or reserved user
 */
public class SetEnabledAction extends Action<SetEnabledRequest, SetEnabledResponse, SetEnabledRequestBuilder> {

    public static final SetEnabledAction INSTANCE = new SetEnabledAction();
    public static final String NAME = "cluster:admin/xpack/security/user/set_enabled";

    private SetEnabledAction() {
        super(NAME);
    }

    @Override
    public SetEnabledRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new SetEnabledRequestBuilder(client);
    }

    @Override
    public SetEnabledResponse newResponse() {
        return new SetEnabledResponse();
    }
}
