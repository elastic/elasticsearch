/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.common.bytes.BytesReference;

public class PutWatchRequestBuilder extends MasterNodeOperationRequestBuilder<PutWatchRequest, PutWatchResponse, PutWatchRequestBuilder> {

    public PutWatchRequestBuilder(ElasticsearchClient client) {
        super(client, PutWatchAction.INSTANCE, new PutWatchRequest());
    }

    public PutWatchRequestBuilder(ElasticsearchClient client, String id) {
        super(client, PutWatchAction.INSTANCE, new PutWatchRequest());
        request.setId(id);
    }

    /**
     * @param id The watch id to be created
     */
    public PutWatchRequestBuilder setId(String id){
        request.setId(id);
        return this;
    }

    /**
     * @param source the source of the watch to be created
     */
    public PutWatchRequestBuilder setSource(BytesReference source) {
        request.setSource(source);
        return this;
    }

    /**
     * @param source the source of the watch to be created
     */
    public PutWatchRequestBuilder setSource(WatchSourceBuilder source) {
        request.setSource(source);
        return this;
    }
}
