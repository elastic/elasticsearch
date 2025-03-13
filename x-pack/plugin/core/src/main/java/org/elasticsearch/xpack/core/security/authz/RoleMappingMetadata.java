/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.Metadata.ALL_CONTEXTS;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public final class RoleMappingMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {

    private static final Logger logger = LogManager.getLogger(RoleMappingMetadata.class);

    public static final String TYPE = "role_mappings";
    public static final String METADATA_NAME_FIELD = "_es_reserved_role_mapping_name";
    public static final String FALLBACK_NAME = "name_not_available_after_deserialization";

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<RoleMappingMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        // serialization tests rely on the order of the ExpressionRoleMapping
        args -> new RoleMappingMetadata(new LinkedHashSet<>((Collection<ExpressionRoleMapping>) args[0]))
    );

    static {
        PARSER.declareObjectArray(constructorArg(), (p, c) -> parseWithNameFromMetadata(p), new ParseField(TYPE));
    }

    private static final RoleMappingMetadata EMPTY = new RoleMappingMetadata(Set.of());

    public static RoleMappingMetadata getFromProject(ProjectMetadata project) {
        return project.custom(RoleMappingMetadata.TYPE, RoleMappingMetadata.EMPTY);
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

    @Deprecated
    @FixForMultiProject(description = "Need to populate the correct project")
    public ClusterState updateClusterState(ClusterState clusterState) {
        return ClusterState.builder(clusterState).putProjectMetadata(updateProject(clusterState.getMetadata().getProject())).build();
    }

    public ProjectMetadata updateProject(ProjectMetadata project) {
        final ProjectMetadata.Builder builder = ProjectMetadata.builder(project);
        if (isEmpty()) {
            // prefer no role mapping custom metadata to the empty role mapping metadata
            builder.removeCustom(RoleMappingMetadata.TYPE);
        } else {
            builder.putCustom(RoleMappingMetadata.TYPE, this);
        }
        return builder.build();
    }

    public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput streamInput) throws IOException {
        return readDiffFrom(Metadata.ProjectCustom.class, TYPE, streamInput);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        // role mappings are serialized without their names
        return ChunkedToXContentHelper.array(TYPE, roleMappings.iterator());
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

    /**
     * Ensures role mapping names are preserved when stored on disk using XContent format,
     * which omits names. This method copies the role mapping's name into a reserved metadata field
     * during serialization, allowing recovery during deserialization (e.g., after a master-node restart).
     * {@link #parseWithNameFromMetadata(XContentParser)} restores the name during parsing.
     */
    public static ExpressionRoleMapping copyWithNameInMetadata(ExpressionRoleMapping roleMapping) {
        Map<String, Object> metadata = new HashMap<>(roleMapping.getMetadata());
        // note: can't use Maps.copyWith... since these create maps that don't support `null` values in map entries
        if (metadata.put(METADATA_NAME_FIELD, roleMapping.getName()) != null) {
            logger.error(
                "Metadata field [{}] is reserved and will be overwritten with an internal system value. "
                    + "Rename this field in your role mapping configuration.",
                METADATA_NAME_FIELD
            );
        }
        return new ExpressionRoleMapping(
            roleMapping.getName(),
            roleMapping.getExpression(),
            roleMapping.getRoles(),
            roleMapping.getRoleTemplates(),
            metadata,
            roleMapping.isEnabled()
        );
    }

    /**
     * If a role mapping does not yet have a name persisted in metadata, it will use a constant fallback name. This method checks if a
     * role mapping has the fallback name.
     */
    public static boolean hasFallbackName(ExpressionRoleMapping expressionRoleMapping) {
        return expressionRoleMapping.getName().equals(FALLBACK_NAME);
    }

    /**
     * Check if any of the role mappings have a fallback name
     * @return true if any role mappings have the fallback name
     */
    public boolean hasAnyMappingWithFallbackName() {
        return roleMappings.stream().anyMatch(RoleMappingMetadata::hasFallbackName);
    }

    /**
     * Parse a role mapping from XContent, restoring the name from a reserved metadata field.
     * Used to parse a role mapping annotated with its name in metadata via @see {@link #copyWithNameInMetadata(ExpressionRoleMapping)}.
     */
    public static ExpressionRoleMapping parseWithNameFromMetadata(XContentParser parser) throws IOException {
        ExpressionRoleMapping roleMapping = ExpressionRoleMapping.parse(FALLBACK_NAME, parser);
        return new ExpressionRoleMapping(
            getNameFromMetadata(roleMapping),
            roleMapping.getExpression(),
            roleMapping.getRoles(),
            roleMapping.getRoleTemplates(),
            roleMapping.getMetadata(),
            roleMapping.isEnabled()
        );
    }

    private static String getNameFromMetadata(ExpressionRoleMapping roleMapping) {
        Map<String, Object> metadata = roleMapping.getMetadata();
        if (metadata.containsKey(METADATA_NAME_FIELD) && metadata.get(METADATA_NAME_FIELD) instanceof String name) {
            return name;
        } else {
            // This is valid the first time we recover from cluster-state: the old format metadata won't have a name stored in metadata yet
            return FALLBACK_NAME;
        }
    }
}
