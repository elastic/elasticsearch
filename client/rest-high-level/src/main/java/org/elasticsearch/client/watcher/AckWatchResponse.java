/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.watcher;

import org.elasticsearch.common.ParseField;
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
