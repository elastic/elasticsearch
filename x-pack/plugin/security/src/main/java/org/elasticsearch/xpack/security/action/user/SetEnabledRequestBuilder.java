/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledRequest;

/**
 * Request builder for setting a user as enabled or disabled
 */
public class SetEnabledRequestBuilder extends ActionRequestBuilder<SetEnabledRequest, ActionResponse.Empty>
    implements
        WriteRequestBuilder<SetEnabledRequestBuilder> {

    public SetEnabledRequestBuilder(ElasticsearchClient client) {
        super(client, TransportSetEnabledAction.TYPE, new SetEnabledRequest());
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
