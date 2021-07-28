/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.watcher;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class DeleteWatchResponse {

    private static final ObjectParser<DeleteWatchResponse, Void> PARSER
        = new ObjectParser<>("x_pack_delete_watch_response", true, DeleteWatchResponse::new);
    static {
        PARSER.declareString(DeleteWatchResponse::setId, new ParseField("_id"));
        PARSER.declareLong(DeleteWatchResponse::setVersion, new ParseField("_version"));
        PARSER.declareBoolean(DeleteWatchResponse::setFound, new ParseField("found"));
    }

    private String id;
    private long version;
    private boolean found;

    public DeleteWatchResponse() {
    }

    public DeleteWatchResponse(String id, long version, boolean found) {
        this.id = id;
        this.version = version;
        this.found = found;
    }

    public String getId() {
        return id;
    }

    public long getVersion() {
        return version;
    }

    public boolean isFound() {
        return found;
    }

    private void setId(String id) {
        this.id = id;
    }

    private void setVersion(long version) {
        this.version = version;
    }

    private void setFound(boolean found) {
        this.found = found;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeleteWatchResponse that = (DeleteWatchResponse) o;

        return Objects.equals(id, that.id) && Objects.equals(version, that.version) && Objects.equals(found, that.found);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version, found);
    }

    public static DeleteWatchResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
