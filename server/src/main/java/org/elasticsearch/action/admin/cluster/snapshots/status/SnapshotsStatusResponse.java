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

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Snapshot status response
 */
public class SnapshotsStatusResponse extends ActionResponse implements ToXContentObject {

    private List<SnapshotStatus> snapshots = Collections.emptyList();

    SnapshotsStatusResponse() {
    }

    SnapshotsStatusResponse(List<SnapshotStatus> snapshots) {
        this.snapshots = snapshots;
    }

    /**
     * Returns the list of snapshots
     *
     * @return the list of snapshots
     */
    public List<SnapshotStatus> getSnapshots() {
        return snapshots;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        List<SnapshotStatus> builder = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            builder.add(SnapshotStatus.readSnapshotStatus(in));
        }
        snapshots = Collections.unmodifiableList(builder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(snapshots.size());
        for (SnapshotStatus snapshotInfo : snapshots) {
            snapshotInfo.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("snapshots");
        for (SnapshotStatus snapshot : snapshots) {
            snapshot.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static SnapshotsStatusResponse fromXContent(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        List<SnapshotStatus> statuses = new ArrayList<>();
        XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.FIELD_NAME) {
            String snapshotField = parser.currentName();
            if (snapshotField.equals("snapshots")) {
                token = parser.nextToken();
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser::getTokenLocation);
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    statuses.add(SnapshotStatus.fromXContent(parser));
                }
            } else {
                throw new ElasticsearchParseException("failed to parse snapshots status, unknown field [{}]", snapshotField);
            }
        } else {
            throw new ElasticsearchParseException("failed to parse snapshots status");
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
        return new SnapshotsStatusResponse(statuses);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SnapshotsStatusResponse response = (SnapshotsStatusResponse) o;

        return snapshots != null ? snapshots.equals(response.snapshots) : response.snapshots == null;
    }

    @Override
    public int hashCode() {
        return snapshots != null ? snapshots.hashCode() : 0;
    }
}
