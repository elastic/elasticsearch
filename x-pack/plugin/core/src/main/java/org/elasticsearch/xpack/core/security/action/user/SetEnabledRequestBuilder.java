/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Request builder for setting a user as enabled or disabled
 */
public class SetEnabledRequestBuilder extends ActionRequestBuilder<SetEnabledRequest, SetEnabledResponse>
        implements WriteRequestBuilder<SetEnabledRequestBuilder> {

    public SetEnabledRequestBuilder(ElasticsearchClient client) {
        super(client, SetEnabledAction.INSTANCE, new SetEnabledRequest());
    }

    /**
     * Set the username of the user that should enabled or disabled. Must not be {@code null}
     */
    public SetEnabledRequestBuilder username(String username) {
        request.username(username);
        return this;
    }

    /**
     * Set whether the user should be enabled or not
     */
    public SetEnabledRequestBuilder enabled(boolean enabled) {
        request.enabled(enabled);
        return this;
    }
}
