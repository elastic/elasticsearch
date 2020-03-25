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
