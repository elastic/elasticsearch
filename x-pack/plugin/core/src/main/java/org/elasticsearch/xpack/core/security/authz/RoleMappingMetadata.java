/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.Metadata.ALL_CONTEXTS;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public final class RoleMappingMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "role_mappings";

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<RoleMappingMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        // serialization tests rely on the order of the ExpressionRoleMapping
        args -> new RoleMappingMetadata(new LinkedHashSet<>((Collection<ExpressionRoleMapping>) args[0]))
    );

    static {
        PARSER.declareObjectArray(
            constructorArg(),
            // role mapping names are lost when the role mapping metadata is serialized
            (p, c) -> ExpressionRoleMapping.parse("name_not_available_after_deserialization", p),
            new ParseField(TYPE)
        );
    }

    private static final RoleMappingMetadata EMPTY = new RoleMappingMetadata(Set.of());

    public static RoleMappingMetadata getFromClusterState(ClusterState clusterState) {
        return clusterState.metadata().custom(RoleMappingMetadata.TYPE, RoleMappingMetadata.EMPTY);
    }

    private final Set<ExpressionRoleMapping> roleMappings;

    public RoleMappingMetadata(Set<ExpressionRoleMapping> roleMappings) {
        this.roleMappings = roleMappings;
    }

    public RoleMappingMetadata(StreamInput input) throws IOException {
        this.roleMappings = input.readCollectionAsSet(ExpressionRoleMapping::new);
    }

    public Set<ExpressionRoleMapping> getRoleMappings() {
        return this.roleMappings;
    }

    public boolean isEmpty() {
        return roleMappings.isEmpty();
    }

    public ClusterState updateClusterState(ClusterState clusterState) {
        if (isEmpty()) {
            // prefer no role mapping custom metadata to the empty role mapping metadata
            return clusterState.copyAndUpdateMetadata(b -> b.removeCustom(RoleMappingMetadata.TYPE));
        } else {
            return clusterState.copyAndUpdateMetadata(b -> b.putCustom(RoleMappingMetadata.TYPE, this));
        }
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput streamInput) throws IOException {
        return readDiffFrom(Metadata.Custom.class, TYPE, streamInput);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        // role mappings are serialized without their names
        return Iterators.concat(ChunkedToXContentHelper.startArray(TYPE), roleMappings.iterator(), ChunkedToXContentHelper.endArray());
    }

    public static RoleMappingMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(roleMappings);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final var other = (RoleMappingMetadata) o;
        return Objects.equals(roleMappings, other.roleMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roleMappings);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("RoleMapping[entries=[");
        final Iterator<ExpressionRoleMapping> entryList = roleMappings.iterator();
        boolean firstEntry = true;
        while (entryList.hasNext()) {
            if (firstEntry == false) {
                builder.append(",");
            }
            builder.append(entryList.next().toString());
            firstEntry = false;
        }
        return builder.append("]]").toString();
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        // It is safest to have this persisted to gateway and snapshots, although maybe redundant.
        // The persistence can become an issue in cases where {@link ReservedStateMetadata}
        // (which records the names of the role mappings last applied) is persisted,
        // but the role mappings themselves (stored here by the {@link RoleMappingMetadata})
        // are not persisted.
        return ALL_CONTEXTS;
    }
}
