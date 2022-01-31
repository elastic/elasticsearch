/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static java.lang.String.format;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;

public record DesiredNodes(String historyID, long version, List<DesiredNode> nodes) implements Writeable, ToXContentObject {

    private static final ParseField HISTORY_ID_FIELD = new ParseField("history_id");
    private static final ParseField VERSION_FIELD = new ParseField("version");
    private static final ParseField NODES_FIELD = new ParseField("nodes");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DesiredNodes, Void> PARSER = new ConstructingObjectParser<>(
        "desired_nodes",
        false,
        (args, unused) -> new DesiredNodes((String) args[0], (long) args[1], (List<DesiredNode>) args[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), HISTORY_ID_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> DesiredNode.fromXContent(p), NODES_FIELD);
    }

    public DesiredNodes {
        assert historyID != null && historyID.isBlank() == false;
        assert version != Long.MIN_VALUE;
        checkForDuplicatedExternalIDs(nodes);
    }

    public DesiredNodes(StreamInput in) throws IOException {
        this(in.readString(), in.readLong(), in.readList(DesiredNode::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(historyID);
        out.writeLong(version);
        out.writeList(nodes);
    }

    static DesiredNodes fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(HISTORY_ID_FIELD.getPreferredName(), historyID);
        builder.field(VERSION_FIELD.getPreferredName(), version);
        builder.xContentList(NODES_FIELD.getPreferredName(), nodes);
        builder.endObject();
        return builder;
    }

    public boolean isSupersededBy(DesiredNodes otherDesiredNodes) {
        return historyID.equals(otherDesiredNodes.historyID) == false || version < otherDesiredNodes.version;
    }

    public boolean hasSameVersion(DesiredNodes other) {
        return historyID.equals(other.historyID) && version == other.version;
    }

    public boolean hasSameHistoryId(DesiredNodes other) {
        return historyID.equals(other.historyID);
    }

    private static void checkForDuplicatedExternalIDs(List<DesiredNode> nodes) {
        Set<String> nodeIDs = new HashSet<>(nodes.size());
        Set<String> duplicatedIDs = new HashSet<>();
        for (DesiredNode node : nodes) {
            String externalID = node.externalId();
            if (externalID != null) {
                if (nodeIDs.add(externalID) == false) {
                    duplicatedIDs.add(externalID);
                }
            }
        }
        if (duplicatedIDs.isEmpty() == false) {
            throw new IllegalArgumentException(
                format(
                    Locale.ROOT,
                    "Some nodes contain the same setting value %s for [%s]",
                    duplicatedIDs,
                    NODE_EXTERNAL_ID_SETTING.getKey()
                )
            );
        }
    }
}
