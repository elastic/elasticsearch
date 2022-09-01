/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RoleMappingsMetadata implements Metadata.Custom {
    public static final String TYPE = "role_mappings";
    public static final ParseField MAPPINGS_FIELD = new ParseField("mappings");
    public static final RoleMappingsMetadata EMPTY = new RoleMappingsMetadata(Collections.emptySortedMap());

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<RoleMappingsMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        c -> new RoleMappingsMetadata(
            new TreeMap<>(
                ((List<RoleMappingMetadata>) c[0]).stream()
                    .collect(Collectors.toMap(r -> r.getRoleMapping().getName(), Function.identity()))
            )
        )
    );
    static {
        PARSER.declareNamedObjects(
            ConstructingObjectParser.constructorArg(),
            (p, c, n) -> ExpressionRoleMapping.parse(n, p),
            v -> { throw new IllegalArgumentException("ordered " + MAPPINGS_FIELD.getPreferredName() + " are not supported"); },
            MAPPINGS_FIELD
        );
    }

    private final Map<String, RoleMappingMetadata> roleMappingMetadatas;

    public RoleMappingsMetadata(Map<String, RoleMappingMetadata> mappings) {
        this.roleMappingMetadatas = Collections.unmodifiableMap(mappings);
    }

    public RoleMappingsMetadata(StreamInput in) throws IOException {
        int size = in.readVInt();
        TreeMap<String, RoleMappingMetadata> mappings = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            mappings.put(in.readString(), new RoleMappingMetadata(in));
        }
        this.roleMappingMetadatas = mappings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(roleMappingMetadatas, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    public Map<String, RoleMappingMetadata> getRoleMappingMetadatas() {
        return roleMappingMetadatas;
    }

    public Map<String, ExpressionRoleMapping> getRoleMappings() {
        return roleMappingMetadatas.values()
            .stream()
            .map(RoleMappingMetadata::getRoleMapping)
            .collect(Collectors.toMap(ExpressionRoleMapping::getName, Function.identity()));
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new RoleMappingsMetadataDiff((RoleMappingsMetadata) previousState, this);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.xContentValuesMap(MAPPINGS_FIELD.getPreferredName(), roleMappingMetadatas);
        return builder;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public int hashCode() {
        return Objects.hash(roleMappingMetadatas);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        RoleMappingsMetadata other = (RoleMappingsMetadata) obj;
        return Objects.equals(roleMappingMetadatas, other.roleMappingMetadatas);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static class RoleMappingsMetadataDiff implements NamedDiff<Metadata.Custom> {
        final Diff<Map<String, RoleMappingMetadata>> mappings;

        RoleMappingsMetadataDiff(RoleMappingsMetadata before, RoleMappingsMetadata after) {
            this.mappings = DiffableUtils.diff(
                before.roleMappingMetadatas,
                after.roleMappingMetadatas,
                DiffableUtils.getStringKeySerializer()
            );
        }

        public RoleMappingsMetadataDiff(StreamInput in) throws IOException {
            this.mappings = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                RoleMappingMetadata::new,
                RoleMappingsMetadataDiff::readRoleMappingMetadataDiffFrom
            );
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            TreeMap<String, RoleMappingMetadata> newMappings = new TreeMap<>(
                mappings.apply(((RoleMappingsMetadata) part).roleMappingMetadatas)
            );
            return new RoleMappingsMetadata(newMappings);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            mappings.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT.minimumCompatibilityVersion();
        }

        static Diff<RoleMappingMetadata> readRoleMappingMetadataDiffFrom(StreamInput in) throws IOException {
            return SimpleDiffable.readDiffFrom(RoleMappingMetadata::new, in);
        }
    }
}
