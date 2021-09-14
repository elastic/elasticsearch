/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.xcontent.ParseField;
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
