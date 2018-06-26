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

package org.elasticsearch.action.admin.cluster.snapshots.create;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.util.Objects;

/**
 * Create snapshot response
 */
public class CreateSnapshotResponse extends ActionResponse implements ToXContentObject {

    @Nullable
    private SnapshotInfo snapshotInfo;

    CreateSnapshotResponse(@Nullable SnapshotInfo snapshotInfo) {
        this.snapshotInfo = snapshotInfo;
    }

    CreateSnapshotResponse() {
    }

    void setSnapshotInfo(SnapshotInfo snapshotInfo) {
        this.snapshotInfo = snapshotInfo;
    }

    /**
     * Returns snapshot information if snapshot was completed by the time this method returned or null otherwise.
     *
     * @return snapshot information or null
     */
    public SnapshotInfo getSnapshotInfo() {
        return snapshotInfo;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        snapshotInfo = in.readOptionalWriteable(SnapshotInfo::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(snapshotInfo);
    }

    /**
     * Returns HTTP status
     * <ul>
     * <li>{@link RestStatus#ACCEPTED} if snapshot is still in progress</li>
     * <li>{@link RestStatus#OK} if snapshot was successful or partially successful</li>
     * <li>{@link RestStatus#INTERNAL_SERVER_ERROR} if snapshot failed completely</li>
     * </ul>
     */
    public RestStatus status() {
        if (snapshotInfo == null) {
            return RestStatus.ACCEPTED;
        }
        return snapshotInfo.status();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (snapshotInfo != null) {
            builder.field("snapshot");
            snapshotInfo.toXContent(builder, params);
        } else {
            builder.field("accepted", true);
        }
        builder.endObject();
        return builder;
    }

    public static CreateSnapshotResponse fromXContent(XContentParser parser) throws IOException {
        CreateSnapshotResponse createSnapshotResponse = new CreateSnapshotResponse();

        parser.nextToken(); // move to '{'

        if (parser.currentToken() != Token.START_OBJECT) {
            throw new IllegalArgumentException("unexpected token [" + parser.currentToken() + "], expected ['{']");
        }

        parser.nextToken(); // move to 'snapshot' || 'accepted'

        if ("snapshot".equals(parser.currentName())) {
            createSnapshotResponse.snapshotInfo = SnapshotInfo.fromXContent(parser);
        } else if ("accepted".equals(parser.currentName())) {
            parser.nextToken(); // move to 'accepted' field value

            if (parser.booleanValue()) {
                // ensure accepted is a boolean value
            }

            parser.nextToken(); // move past 'true'/'false'
        } else {
            throw new IllegalArgumentException("unexpected token [" + parser.currentToken() + "] expected ['snapshot', 'accepted']");
        }

        if (parser.currentToken() != Token.END_OBJECT) {
            throw new IllegalArgumentException("unexpected token [" + parser.currentToken() + "], expected ['}']");
        }

        parser.nextToken(); // move past '}'

        return createSnapshotResponse;
    }

    @Override
    public String toString() {
        return "CreateSnapshotResponse{" +
            "snapshotInfo=" + snapshotInfo +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateSnapshotResponse that = (CreateSnapshotResponse) o;
        return Objects.equals(snapshotInfo, that.snapshotInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotInfo);
    }
}
