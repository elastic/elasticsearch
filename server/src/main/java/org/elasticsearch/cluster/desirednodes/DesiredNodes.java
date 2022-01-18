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

import java.io.IOException;
import java.util.List;

public record DesiredNodes(List<DesiredNode> desiredNodes) implements Writeable, ToXContentObject {
    private static final ParseField NODES_FIELD = new ParseField("nodes");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DesiredNodes, Void> PARSER = new ConstructingObjectParser<>(
        "desired_nodes",
        false,
        (args, unused) -> new DesiredNodes((List<DesiredNode>) args[0])
    );

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> DesiredNode.fromXContent(p), NODES_FIELD);
    }

    public DesiredNodes(StreamInput in) throws IOException {
        this(in.readList(DesiredNode::new));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.xContentList(NODES_FIELD.getPreferredName(), desiredNodes);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(desiredNodes);
    }
}
