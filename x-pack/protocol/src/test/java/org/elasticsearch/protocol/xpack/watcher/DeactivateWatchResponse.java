/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.watcher;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class DeactivateWatchResponse extends ActionResponse implements ToXContentObject {

    private static final ObjectParser<DeactivateWatchResponse, Void> PARSER
        = new ObjectParser<>("x_pack_deactivate_watch_response", DeactivateWatchResponse::new);
    static {
        PARSER.declareString(DeactivateWatchResponse::setId, new ParseField("_id"));
        PARSER.declareLong(DeactivateWatchResponse::setVersion, new ParseField("_version"));
        PARSER.declareString(DeactivateWatchResponse::setStatus, new ParseField("status"));
    }

    public static DeactivateWatchResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private String id;
    private long version;
    // TODO: Replace with migrated WatchStatus class
    private String status;

    // TODO: Do we need some kind of actions object here?

    public DeactivateWatchResponse() {

    }

    public DeactivateWatchResponse(String id, long version, String status) {
        this.id = id;
        this.version = version;
        this.status = status;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    public String getId() {
        return id;
    }

    private void setId(String id) {
        this.id = id;
    }

    public long getVersion() {
        return version;
    }

    private void setVersion(long version) {
        this.version = version;
    }

    public String getStatus() {
        return status;
    }

    private void setStatus(String status) {
        this.status = status;
    }
}
