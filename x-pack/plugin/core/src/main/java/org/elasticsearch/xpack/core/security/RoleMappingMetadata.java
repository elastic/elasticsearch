/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security;

import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;

import java.io.IOException;
import java.util.Objects;

public class RoleMappingMetadata implements SimpleDiffable<RoleMappingMetadata>, ToXContentObject {

    static final ParseField MAPPING_FIELD = new ParseField("mapping");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<RoleMappingMetadata, String> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>("role_mapping_metadata", a -> {
            final ExpressionRoleMapping roleMapping = (ExpressionRoleMapping) a[0];
            return new RoleMappingMetadata(roleMapping);
        });
        PARSER.declareNamedObject(ConstructingObjectParser.constructorArg(), (p, c, n) -> ExpressionRoleMapping.parse(n, p), MAPPING_FIELD);
    }

    public static RoleMappingMetadata parse(final XContentParser parser, final String name) {
        return PARSER.apply(parser, name);
    }

    private final ExpressionRoleMapping roleMapping;

    public ExpressionRoleMapping getRoleMapping() {
        return roleMapping;
    }

    public RoleMappingMetadata(final ExpressionRoleMapping roleMapping) {
        this.roleMapping = roleMapping;
    }

    public RoleMappingMetadata(final StreamInput in) throws IOException {
        roleMapping = new ExpressionRoleMapping(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        roleMapping.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.field(MAPPING_FIELD.getPreferredName(), roleMapping);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final RoleMappingMetadata that = (RoleMappingMetadata) o;
        return roleMapping.equals(that.roleMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roleMapping);
    }
}
