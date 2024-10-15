/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support.mapper;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public final class ClusterStateRoleMapping implements ToXContentObject, Writeable {

    private static final String RESERVED_METADATA_PREFIX = "_es_reserved_fields";

    private final ExpressionRoleMapping innerMapping;

    public ClusterStateRoleMapping(StreamInput in) throws IOException {
        this.innerMapping = new ExpressionRoleMapping(in);
    }

    public ClusterStateRoleMapping(ExpressionRoleMapping innerMapping) {
        this.innerMapping = markMetadata(innerMapping);
    }

    private static ExpressionRoleMapping markMetadata(ExpressionRoleMapping innerMapping) {
        var metadata = innerMapping.getMetadata();

        return innerMapping;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        innerMapping.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerMapping.innerToXContent(builder, params, false);
        builder.field("name", innerMapping.getName());
        return builder.endObject();
    }

    public ExpressionRoleMapping toExpressionRoleMapping() {
        return innerMapping;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return Objects.equals(innerMapping, ((ClusterStateRoleMapping) obj).innerMapping);
    }

    @Override
    public int hashCode() {
        return innerMapping.hashCode();
    }

    @Override
    public String toString() {
        return innerMapping.toString();
    }
}
