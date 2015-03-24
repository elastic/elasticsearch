/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;

public class PutWatchRequestBuilder extends MasterNodeOperationRequestBuilder<PutWatchRequest, PutWatchResponse, PutWatchRequestBuilder, Client> {

    public PutWatchRequestBuilder(Client client) {
        super(client, new PutWatchRequest());
    }

    public PutWatchRequestBuilder(Client client, String watchName) {
        super(client, new PutWatchRequest());
        request.setName(watchName);
    }

    /**
     * @param watchName The watch name to be created
     */
    public PutWatchRequestBuilder watchName(String watchName){
        request.setName(watchName);
        return this;
    }

    /**
     * @param source the source of the watch to be created
     */
    public PutWatchRequestBuilder source(BytesReference source) {
        request.source(source);
        return this;
    }

    /**
     * @param source the source of the watch to be created
     */
    public PutWatchRequestBuilder source(WatchSourceBuilder source) {
        request.source(source);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<PutWatchResponse> listener) {
        new WatcherClient(client).putWatch(request, listener);
    }
}
