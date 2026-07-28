/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;

/**
 * Response when adding a role, includes a boolean for whether the role was
 * created or updated.
 */
public class PutRoleResponse extends ActionResponse implements ToXContentObject {

    private final boolean created;

    /**
     * The reliable "before" image of the role, captured by {@code NativeRolesStore} during an opt-in
     * compare-and-swap upsert and consumed by {@code LoggingAuditTrail} to emit a before/after diff audit record. This is
     * transient audit-only state: it is intentionally NOT serialized in {@link #writeTo} and NOT rendered in
     * {@link #toXContent}, so it never crosses the wire nor appears in the REST response body. It is {@code null} when diff
     * capture is disabled or when the role did not previously exist (i.e. the operation created a new role).
     */
    @Nullable
    private transient RoleDescriptor previousRoleDescriptor;

    public PutRoleResponse(boolean created) {
        this.created = created;
    }

    public boolean isCreated() {
        return created;
    }

    @Nullable
    public RoleDescriptor getPreviousRoleDescriptor() {
        return previousRoleDescriptor;
    }

    public void setPreviousRoleDescriptor(@Nullable RoleDescriptor previousRoleDescriptor) {
        this.previousRoleDescriptor = previousRoleDescriptor;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("created", created).endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(created);
    }

}
