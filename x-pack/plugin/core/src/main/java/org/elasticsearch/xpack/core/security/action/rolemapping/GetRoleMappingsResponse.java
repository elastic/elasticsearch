/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.rolemapping;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;

import java.io.IOException;

/**
 * Response to {@link GetRoleMappingsAction get role-mappings API}.
 *
 * see org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore
 */
public class GetRoleMappingsResponse extends ActionResponse {

    private ExpressionRoleMapping[] mappings;

    public GetRoleMappingsResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        mappings = new ExpressionRoleMapping[size];
        for (int i = 0; i < size; i++) {
            mappings[i] = new ExpressionRoleMapping(in);
        }
    }

    public GetRoleMappingsResponse(ExpressionRoleMapping... mappings) {
        this.mappings = mappings;
    }

    public ExpressionRoleMapping[] mappings() {
        return mappings;
    }

    public boolean hasMappings() {
        return mappings.length > 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(mappings.length);
        for (ExpressionRoleMapping mapping : mappings) {
            mapping.writeTo(out);
        }
    }
}
