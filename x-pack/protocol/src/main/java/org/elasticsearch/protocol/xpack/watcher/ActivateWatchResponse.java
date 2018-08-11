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
package org.elasticsearch.protocol.xpack.watcher;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.watcher.status.WatchStatus;

import java.io.IOException;
import java.util.Objects;

/**
 * This class contains the status of the watch. If the watch was successfully de/activates
 * this will reflected the new state of the watch.
 */
public class ActivateWatchResponse extends ActionResponse implements ToXContent {
    private static final ObjectParser<ActivateWatchResponse, String> PARSER
            = new ObjectParser<>("x_pack_activate_watch_response", ActivateWatchResponse::new);
    static {
        PARSER.declareField((parser, resp, watchId) -> resp.setStatus(WatchStatus.parse(watchId, parser)),
                new ParseField("status"), ObjectParser
                .ValueType.OBJECT);
    }

    private WatchStatus status;

    public ActivateWatchResponse() {
    }

    public ActivateWatchResponse(@Nullable WatchStatus status) {
        this.status = status;
    }

    /**
     * @return The watch status
     */
    public WatchStatus getStatus() {
        return status;
    }

    private void setStatus(WatchStatus status){
        this.status = status;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        status = in.readBoolean() ? WatchStatus.read(in) : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(status != null);
        if (status != null) {
            status.writeTo(out);
        }
    }

    public static ActivateWatchResponse fromXContent(String watchId, XContentParser parser) throws IOException {
        return PARSER.parse(parser, watchId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field("status", status, params);
    }

    @Override
    public String toString() {
        return "ActivateWatchResponse{" +
                "status=" + status +
                '}';
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
        return Objects.hashCode(status);
    }
}
