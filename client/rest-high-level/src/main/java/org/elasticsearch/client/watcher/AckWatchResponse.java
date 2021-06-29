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

/**
 * The response from an 'ack watch' request.
 */
public class AckWatchResponse {

    private final WatchStatus status;

    public AckWatchResponse(WatchStatus status) {
        this.status = status;
    }

    /**
     * @return the status of the requested watch. If an action was
     * successfully acknowledged, this will be reflected in its status.
     */
    public WatchStatus getStatus() {
        return status;
    }

    private static final ParseField STATUS_FIELD = new ParseField("status");
    private static final ConstructingObjectParser<AckWatchResponse, Void> PARSER =
        new ConstructingObjectParser<>("ack_watch_response", true,
            a -> new AckWatchResponse((WatchStatus) a[0]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(),
            (parser, context) -> WatchStatus.parse(parser),
            STATUS_FIELD);
    }

    public static AckWatchResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
