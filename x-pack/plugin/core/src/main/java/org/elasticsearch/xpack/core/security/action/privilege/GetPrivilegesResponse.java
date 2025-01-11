/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

/**
 * Response containing one or more application privileges retrieved from the security index
 */
public final class GetPrivilegesResponse extends ActionResponse {

    private final ApplicationPrivilegeDescriptor[] privileges;

    public GetPrivilegesResponse(ApplicationPrivilegeDescriptor... privileges) {
        this.privileges = Objects.requireNonNull(privileges, "Application privileges cannot be null");
    }

    public GetPrivilegesResponse(Collection<ApplicationPrivilegeDescriptor> privileges) {
        this(privileges.toArray(new ApplicationPrivilegeDescriptor[0]));
    }

    public GetPrivilegesResponse(StreamInput in) throws IOException {
        this.privileges = in.readArray(ApplicationPrivilegeDescriptor::new, ApplicationPrivilegeDescriptor[]::new);
    }

    public ApplicationPrivilegeDescriptor[] privileges() {
        return privileges;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(privileges);
    }

    public boolean isEmpty() {
        return privileges.length == 0;
    }
}
