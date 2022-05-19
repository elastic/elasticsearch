/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class UpdateDesiredNodesRequest extends AcknowledgedRequest<UpdateDesiredNodesRequest> {
    private final String historyID;
    private final long version;
    private final List<DesiredNode> nodes;

    private static final ParseField NODES_FIELD = new ParseField("nodes");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<List<DesiredNode>, Void> PARSER = new ConstructingObjectParser<>(
        "update_desired_nodes_request",
        false,
        (args, unused) -> (List<DesiredNode>) args[0]
    );

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> DesiredNode.fromXContent(p), NODES_FIELD);
    }

    public UpdateDesiredNodesRequest(String historyID, long version, List<DesiredNode> nodes) {
        assert historyID != null;
        assert nodes != null;
        this.historyID = historyID;
        this.version = version;
        this.nodes = nodes;
    }

    public UpdateDesiredNodesRequest(StreamInput in) throws IOException {
        super(in);
        this.historyID = in.readString();
        this.version = in.readLong();
        this.nodes = in.readList(DesiredNode::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(historyID);
        out.writeLong(version);
        out.writeList(nodes);
    }

    public static UpdateDesiredNodesRequest fromXContent(String historyID, long version, XContentParser parser) throws IOException {
        List<DesiredNode> nodes = PARSER.parse(parser, null);
        return new UpdateDesiredNodesRequest(historyID, version, nodes);
    }

    public String getHistoryID() {
        return historyID;
    }

    public long getVersion() {
        return version;
    }

    public List<DesiredNode> getNodes() {
        return nodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UpdateDesiredNodesRequest that = (UpdateDesiredNodesRequest) o;
        return version == that.version && Objects.equals(historyID, that.historyID) && Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(historyID, version, nodes);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (historyID.isBlank()) {
            validationException = ValidateActions.addValidationError("historyID should not be empty", null);
        }

        if (version < 0) {
            validationException = ValidateActions.addValidationError("version must be positive", validationException);
        }

        if (nodes.stream().anyMatch(DesiredNode::hasMasterRole) == false) {
            validationException = ValidateActions.addValidationError("nodes must contain at least one master node", validationException);
        }

        return validationException;
    }
}
