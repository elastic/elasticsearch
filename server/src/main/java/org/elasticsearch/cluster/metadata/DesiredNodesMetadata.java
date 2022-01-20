/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Objects;

public class DesiredNodesMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {
    private static final Version MIN_SUPPORTED_VERSION = Version.V_8_1_0;
    public static final String TYPE = "desired_nodes";

    public static final DesiredNodesMetadata EMPTY = new DesiredNodesMetadata((DesiredNodes) null);

    private static final ParseField CURRENT_FIELD = new ParseField("current");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DesiredNodesMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        false,
        (args, unused) -> new DesiredNodesMetadata((DesiredNodes) args[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> DesiredNodes.fromXContent(p), CURRENT_FIELD);
    }

    private final DesiredNodes currentDesiredNodes;

    public DesiredNodesMetadata(DesiredNodes currentDesiredNodes) {
        this.currentDesiredNodes = currentDesiredNodes;
    }

    public DesiredNodesMetadata(StreamInput in) throws IOException {
        this.currentDesiredNodes = new DesiredNodes(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        currentDesiredNodes.writeTo(out);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.Custom.class, TYPE, in);
    }

    public static DesiredNodesMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(CURRENT_FIELD.getPreferredName(), currentDesiredNodes);
        return builder;
    }

    @Nullable
    public DesiredNodes getCurrentDesiredNodes() {
        return currentDesiredNodes;
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
        return MIN_SUPPORTED_VERSION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DesiredNodesMetadata that = (DesiredNodesMetadata) o;
        return Objects.equals(currentDesiredNodes, that.currentDesiredNodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentDesiredNodes);
    }
}
