/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Response containing one or more application privileges retrieved from the security index
 */
public final class GetPrivilegesResponse extends ActionResponse {

    private String[] clusterPrivileges;
    private String[] indexPrivileges;
    private ApplicationPrivilegeDescriptor[] applicationPrivileges;

    public GetPrivilegesResponse(String[] clusterPrivileges, String[] indexPrivileges,
                                  ApplicationPrivilegeDescriptor[] applicationPrivileges) {
        this.clusterPrivileges = Objects.requireNonNull(clusterPrivileges, "Cluster privileges cannot be null");
        this.indexPrivileges =  Objects.requireNonNull(indexPrivileges, "Index privileges cannot be null");
        this.applicationPrivileges =  Objects.requireNonNull(applicationPrivileges, "Application privileges cannot be null");
    }

    public GetPrivilegesResponse(Collection<String> clusterPrivileges,
                                 Collection<String> indexPrivileges,
                                 Collection<ApplicationPrivilegeDescriptor> applicationPrivileges) {
        this(clusterPrivileges.toArray(Strings.EMPTY_ARRAY), indexPrivileges.toArray(Strings.EMPTY_ARRAY),
            applicationPrivileges.toArray(new ApplicationPrivilegeDescriptor[0]));
    }

    public GetPrivilegesResponse() {
        this(Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    }

    public String[] getClusterPrivileges() {
        return clusterPrivileges;
    }

    public String[] getIndexPrivileges() {
        return indexPrivileges;
    }

    public ApplicationPrivilegeDescriptor[] applicationPrivileges() {
        return applicationPrivileges;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            this.clusterPrivileges = in.readStringArray();
            this.indexPrivileges = in.readStringArray();
        } else {
            this.clusterPrivileges = Strings.EMPTY_ARRAY;
            this.indexPrivileges = Strings.EMPTY_ARRAY;
        }
        this.applicationPrivileges = in.readArray(ApplicationPrivilegeDescriptor::new, ApplicationPrivilegeDescriptor[]::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeStringArray(clusterPrivileges);
            out.writeStringArray(indexPrivileges);
        }
        out.writeArray(applicationPrivileges);
    }

    public boolean isEmpty() {
        return clusterPrivileges.length == 0 && indexPrivileges.length == 0 && applicationPrivileges.length == 0;
    }
}
