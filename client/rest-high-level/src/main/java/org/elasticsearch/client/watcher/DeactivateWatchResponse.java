/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.watcher;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class DeactivateWatchResponse {
    private WatchStatus status;

    private static final ParseField STATUS_FIELD = new ParseField("status");
    private static final ConstructingObjectParser<DeactivateWatchResponse, Void> PARSER
        = new ConstructingObjectParser<>("x_pack_deactivate_watch_response", true,
        (fields) -> new DeactivateWatchResponse((WatchStatus) fields[0]));
    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(),
            (parser, context) -> WatchStatus.parse(parser),
            STATUS_FIELD);
    }

    public static DeactivateWatchResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public DeactivateWatchResponse(WatchStatus status) {
        this.status = status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeactivateWatchResponse that = (DeactivateWatchResponse) o;
        return Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status);
    }

    public WatchStatus getStatus() {
        return status;
    }
}
