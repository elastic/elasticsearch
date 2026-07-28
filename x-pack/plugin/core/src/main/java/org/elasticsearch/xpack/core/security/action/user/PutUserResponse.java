/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;

/**
 * Response when adding a user to the security index. Returns a
 * single boolean field for whether the user was created or updated.
 */
public class PutUserResponse extends ActionResponse implements ToXContentObject {

    private final boolean created;

    /**
     * The reliable "before" image of the user, captured by {@code NativeUsersStore} during an opt-in compare-and-swap upsert and
     * consumed by {@code LoggingAuditTrail} to emit a before/after diff audit record. This is transient audit-only state: it is
     * intentionally NOT serialized in {@link #writeTo} and NOT rendered in {@link #toXContent}, so it never crosses the wire nor
     * appears in the REST response body. It never contains the password hash. It is {@code null} when diff capture is disabled or
     * when the user did not previously exist (i.e. the operation created a new user).
     */
    @Nullable
    private transient User previousUser;

    /**
     * Whether the {@link #previousUser} had a password set. Transient audit-only state, paired with {@link #previousUser}; only
     * meaningful when {@link #previousUser} is non-null.
     */
    private transient boolean previousHadPassword;

    public PutUserResponse(boolean created) {
        this.created = created;
    }

    public boolean created() {
        return created;
    }

    @Nullable
    public User getPreviousUser() {
        return previousUser;
    }

    public boolean previousHadPassword() {
        return previousHadPassword;
    }

    public void setPreviousUser(@Nullable User previousUser, boolean previousHadPassword) {
        this.previousUser = previousUser;
        this.previousHadPassword = previousHadPassword;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(created);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("created", created).endObject();
    }
}
