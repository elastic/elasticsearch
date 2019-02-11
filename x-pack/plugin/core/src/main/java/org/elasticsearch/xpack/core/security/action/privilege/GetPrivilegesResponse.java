/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;

import java.io.IOException;
import java.util.Collection;

/**
 * Response containing one or more application privileges retrieved from the security index
 */
public final class GetPrivilegesResponse extends ActionResponse {

    private ApplicationPrivilegeDescriptor[] privileges;

    public GetPrivilegesResponse(ApplicationPrivilegeDescriptor... privileges) {
        this.privileges = privileges;
    }

    public GetPrivilegesResponse(Collection<ApplicationPrivilegeDescriptor> privileges) {
        this(privileges.toArray(new ApplicationPrivilegeDescriptor[privileges.size()]));
    }

    public ApplicationPrivilegeDescriptor[] privileges() {
        return privileges;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.privileges = in.readArray(ApplicationPrivilegeDescriptor::new, ApplicationPrivilegeDescriptor[]::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(privileges);
    }

}
