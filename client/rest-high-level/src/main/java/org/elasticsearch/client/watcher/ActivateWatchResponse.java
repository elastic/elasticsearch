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
import java.util.Objects;

/**
 * Response from an 'activate watch' request.
 */
public final class ActivateWatchResponse {

    private static final ParseField STATUS_FIELD = new ParseField("status");
    private static final ConstructingObjectParser<ActivateWatchResponse, Void> PARSER =
        new ConstructingObjectParser<>("activate_watch_response", true,
            a -> new ActivateWatchResponse((WatchStatus) a[0]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(),
            (parser, context) -> WatchStatus.parse(parser),
            STATUS_FIELD);
    }

    private final WatchStatus status;

    public ActivateWatchResponse(WatchStatus status) {
        this.status = status;
    }

    public WatchStatus getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ActivateWatchResponse that = (ActivateWatchResponse) o;
        return Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status);
    }

    public static ActivateWatchResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
