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
package org.elasticsearch.client.ml;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class UpgradeJobModelSnapshotResponse implements ToXContentObject {

    private static final ParseField COMPLETED = new ParseField("completed");
    private static final ParseField NODE = new ParseField("node");

    public static final ConstructingObjectParser<UpgradeJobModelSnapshotResponse, Void> PARSER =
        new ConstructingObjectParser<>("upgrade_job_snapshot_response", true,
            (a) -> new UpgradeJobModelSnapshotResponse((Boolean) a[0], (String) a[1]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), COMPLETED);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), NODE);
    }

    private final boolean completed;
    private final String node;

    public UpgradeJobModelSnapshotResponse(Boolean opened, String node) {
        this.completed = opened != null && opened;
        this.node = node;
    }

    public static UpgradeJobModelSnapshotResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public boolean isCompleted() {
        return completed;
    }

    /**
     * The node that the job was assigned to
     *
     * @return The ID of a node if the job was assigned to a node.
     */
    public String getNode() {
        return node;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        UpgradeJobModelSnapshotResponse that = (UpgradeJobModelSnapshotResponse) other;
        return completed == that.completed
            && Objects.equals(node, that.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(completed, node);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(COMPLETED.getPreferredName(), completed);
        if (node != null) {
            builder.field(NODE.getPreferredName(), node);
        }
        builder.endObject();
        return builder;
    }
}
