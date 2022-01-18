/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.desirednodes;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public record DesiredNodes(String historyID, int version, List<DesiredNode> desiredNodes) implements Writeable, ToXContentObject {

    private static final ParseField HISTORY_ID_FIELD = new ParseField("history_id");
    private static final ParseField VERSION_FIELD = new ParseField("version");
    private static final ParseField NODES_FIELD = new ParseField("nodes");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DesiredNodes, Void> PARSER = new ConstructingObjectParser<>(
        "desired_nodes",
        false,
        (args, unused) -> new DesiredNodes((String) args[0], (int) args[1], (List<DesiredNode>) args[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), HISTORY_ID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), VERSION_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> DesiredNode.fromXContent(p), NODES_FIELD);
    }

    public DesiredNodes(StreamInput in) throws IOException {
        this(in.readString(), in.readInt(), in.readList(DesiredNode::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(historyID);
        out.writeInt(version);
        out.writeList(desiredNodes);
    }

    static DesiredNodes fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(HISTORY_ID_FIELD.getPreferredName(), historyID);
        builder.field(VERSION_FIELD.getPreferredName(), version);
        builder.xContentList(NODES_FIELD.getPreferredName(), desiredNodes);
        builder.endObject();
        return builder;
    }

    public boolean isSupersededBy(DesiredNodes otherDesiredNodes) {
        return historyID.equals(otherDesiredNodes.historyID) == false || version < otherDesiredNodes.version;
    }

    public boolean hasSameVersion(DesiredNodes other) {
        return historyID.equals(other.historyID) && version == other.version;
    }
}
