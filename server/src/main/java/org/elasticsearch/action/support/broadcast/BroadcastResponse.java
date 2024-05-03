/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support.broadcast;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class BroadcastResponse extends BaseBroadcastResponse implements ToXContentObject {

    public BroadcastResponse(StreamInput in) throws IOException {
        super(in);
    }

    public BroadcastResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        RestActions.buildBroadcastShardsHeader(builder, params, this);
        addCustomXContentFields(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Override in subclass to add custom fields following the common `_shards` field
     */
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {}
}
