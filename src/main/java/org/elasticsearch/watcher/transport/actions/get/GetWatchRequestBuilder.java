/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.VersionType;

/**
 * A delete document action request builder.
 */
public class GetWatchRequestBuilder extends ActionRequestBuilder<GetWatchRequest, GetWatchResponse, GetWatchRequestBuilder, Client> {

    public GetWatchRequestBuilder(Client client, String id) {
        super(client, new GetWatchRequest(id));
    }


    public GetWatchRequestBuilder(Client client) {
        super(client, new GetWatchRequest());
    }

    public GetWatchRequestBuilder setId(String id) {
        request.setId(id);
        return this;
    }

    /**
     * Sets the type of versioning to use. Defaults to {@link org.elasticsearch.index.VersionType#INTERNAL}.
     */
    public GetWatchRequestBuilder setVersionType(VersionType versionType) {
        request.setVersionType(versionType);
        return this;
    }

    @Override
    protected void doExecute(final ActionListener<GetWatchResponse> listener) {
        new WatcherClient(client).getWatch(request, listener);
    }
}
