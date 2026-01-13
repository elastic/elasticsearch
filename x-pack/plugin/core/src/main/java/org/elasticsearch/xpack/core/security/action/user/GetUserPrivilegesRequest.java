/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference;

import java.io.IOException;

/**
 * A request for checking a user's privileges
 */
public final class GetUserPrivilegesRequest extends LegacyActionRequest implements UserRequest {

    private static final TransportVersion TV_UNWRAP_ROLE = TransportVersion.fromName("get_user_priv_unwrap_role");

    private String username;

    @Nullable
    private RoleReference.ApiKeyRoleType unwrapInnerRole;

    /**
     * Package level access for {@link GetUserPrivilegesRequestBuilder}.
     */
    GetUserPrivilegesRequest() {}

    public GetUserPrivilegesRequest(StreamInput in) throws IOException {
        super(in);
        this.username = in.readString();
        if (in.getTransportVersion().supports(TV_UNWRAP_ROLE)) {
            this.unwrapInnerRole = in.readOptionalEnum(RoleReference.ApiKeyRoleType.class);
        } else {
            this.unwrapInnerRole = null;
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * @return the username that this request applies to.
     */
    public String username() {
        return username;
    }

    /**
     * Set the username that the request applies to. Must not be {@code null}
     */
    public void username(String username) {
        this.username = username;
    }

    @Override
    public String[] usernames() {
        return new String[] { username };
    }

    public RoleReference.ApiKeyRoleType unwrapLimitedRole() {
        return unwrapInnerRole;
    }

    public void unwrapLimitedRole(RoleReference.ApiKeyRoleType unwrapInnerRole) {
        this.unwrapInnerRole = unwrapInnerRole;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(username);
        if (out.getTransportVersion().supports(TV_UNWRAP_ROLE)) {
            out.writeOptionalEnum(unwrapInnerRole);
        }
    }

}
