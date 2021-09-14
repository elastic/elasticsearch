/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.put;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.xpack.core.watcher.client.WatchSourceBuilder;

public class PutWatchRequestBuilder extends ActionRequestBuilder<PutWatchRequest, PutWatchResponse> {

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
     * @param xContentType the content type of the source
     */
    public PutWatchRequestBuilder setSource(BytesReference source, XContentType xContentType) {
        request.setSource(source, xContentType);
        return this;
    }

    /**
     * @param source the source of the watch to be created
     */
    public PutWatchRequestBuilder setSource(WatchSourceBuilder source) {
        request.setSource(source.buildAsBytes(XContentType.JSON), XContentType.JSON);
        return this;
    }

    /**
     * @param active Sets whether the watcher is in/active by default
     */
    public PutWatchRequestBuilder setActive(boolean active) {
        request.setActive(active);
        return this;
    }

    /**
     * @param version Sets the version to be set when running the update
     */
    public PutWatchRequestBuilder setVersion(long version) {
        request.setVersion(version);
        return this;
    }

}
