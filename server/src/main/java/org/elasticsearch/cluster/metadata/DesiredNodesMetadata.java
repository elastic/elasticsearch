/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;

public class DesiredNodesMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {
    private static final TransportVersion MIN_SUPPORTED_VERSION = TransportVersion.V_8_1_0;
    public static final String TYPE = "desired_nodes";

    public static final DesiredNodesMetadata EMPTY = new DesiredNodesMetadata((DesiredNodes) null);

    private static final ParseField LATEST_FIELD = new ParseField("latest");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DesiredNodesMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        false,
        (args, unused) -> new DesiredNodesMetadata((DesiredNodes) args[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> DesiredNodes.fromXContent(p), LATEST_FIELD);
    }

    private final DesiredNodes latestDesiredNodes;

    public DesiredNodesMetadata(DesiredNodes latestDesiredNodes) {
        this.latestDesiredNodes = latestDesiredNodes;
    }

    public DesiredNodesMetadata(StreamInput in) throws IOException {
        this.latestDesiredNodes = DesiredNodes.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        latestDesiredNodes.writeTo(out);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.Custom.class, TYPE, in);
    }

    public static DesiredNodesMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.single((builder, params) -> builder.field(LATEST_FIELD.getPreferredName(), latestDesiredNodes, params));
    }

    public static DesiredNodesMetadata fromClusterState(ClusterState clusterState) {
        return clusterState.metadata().custom(TYPE, EMPTY);
    }

    @Nullable
    public DesiredNodes getLatestDesiredNodes() {
        return latestDesiredNodes;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.API_AND_GATEWAY;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return MIN_SUPPORTED_VERSION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DesiredNodesMetadata that = (DesiredNodesMetadata) o;
        return Objects.equals(latestDesiredNodes, that.latestDesiredNodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(latestDesiredNodes);
    }
}
