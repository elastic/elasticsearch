/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.desirednodes;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

public class DesiredNodesMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {
    private static final Version MIN_VERSION = Version.V_8_1_0;
    public static final String TYPE = "desired_nodes";

    public static final DesiredNodesMetadata EMPTY = new DesiredNodesMetadata(Collections.emptyList());

    private static final ParseField DESIRED_NODES_FIELD = new ParseField("desired_nodes");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DesiredNodesMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        false,
        (args, unused) -> new DesiredNodesMetadata((List<DesiredNodes>) args[0])
    );

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> DesiredNodes.fromXContent(p), DESIRED_NODES_FIELD);
    }

    private final List<DesiredNodes> desiredNodes;

    public DesiredNodesMetadata(List<DesiredNodes> desiredNodes) {
        this.desiredNodes = desiredNodes;
    }

    public DesiredNodesMetadata(StreamInput in) throws IOException {
        this.desiredNodes = in.readList(DesiredNodes::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(desiredNodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.xContentList(DESIRED_NODES_FIELD.getPreferredName(), desiredNodes);
        builder.endObject();
        return builder;
    }

    @Nullable
    public DesiredNodes getCurrentDesiredNodes() {
        return desiredNodes.isEmpty() ? null : desiredNodes.get(0);
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new DesiredNodesMetadataDiff((DesiredNodesMetadata) previousState, this);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return MIN_VERSION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DesiredNodesMetadata that = (DesiredNodesMetadata) o;
        return Objects.equals(desiredNodes, that.desiredNodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(desiredNodes);
    }

    static class DesiredNodesMetadataDiff implements NamedDiff<Metadata.Custom> {
        DesiredNodesMetadataDiff(DesiredNodesMetadata before, DesiredNodesMetadata after) {}

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return null;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return MIN_VERSION;
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }
}
