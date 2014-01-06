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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.IOException;

/**
 * Create snapshot response
 */
public class CreateSnapshotResponse extends ActionResponse implements ToXContent {

    @Nullable
    private SnapshotInfo snapshotInfo;

    CreateSnapshotResponse(@Nullable SnapshotInfo snapshotInfo) {
        this.snapshotInfo = snapshotInfo;
    }

    CreateSnapshotResponse() {
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
        snapshotInfo = SnapshotInfo.readOptionalSnapshotInfo(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalStreamable(snapshotInfo);
    }

    /**
     * Returns HTTP status
     * <p/>
     * <ul>
     * <li>{@link RestStatus#ACCEPTED}</li> if snapshot is still in progress
     * <li>{@link RestStatus#OK}</li> if snapshot was successful or partially successful
     * <li>{@link RestStatus#INTERNAL_SERVER_ERROR}</li> if snapshot failed completely
     * </ul>
     *
     * @return
     */
    public RestStatus status() {
        if (snapshotInfo == null) {
            return RestStatus.ACCEPTED;
        }
        return snapshotInfo.status();
    }

    static final class Fields {
        static final XContentBuilderString SNAPSHOT = new XContentBuilderString("snapshot");
        static final XContentBuilderString ACCEPTED = new XContentBuilderString("accepted");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (snapshotInfo != null) {
            builder.field(Fields.SNAPSHOT);
            snapshotInfo.toXContent(builder, params);
        } else {
            builder.field(Fields.ACCEPTED, true);
        }
        return builder;
    }
}
