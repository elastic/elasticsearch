/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ProfileHasPrivilegesRequest extends ActionRequest {

    private String[] uids;
    private String[] clusterPrivileges;
    private IndicesPrivileges[] indexPrivileges;
    private RoleDescriptor.ApplicationResourcePrivileges[] applicationPrivileges;

    public ProfileHasPrivilegesRequest() {}

    public ProfileHasPrivilegesRequest(StreamInput in) throws IOException {
        super(in);
        this.uids = in.readStringArray();
        this.clusterPrivileges = in.readStringArray();
        this.indexPrivileges = in.readArray(IndicesPrivileges::new, IndicesPrivileges[]::new);
        this.applicationPrivileges = in.readArray(
            RoleDescriptor.ApplicationResourcePrivileges::new,
            RoleDescriptor.ApplicationResourcePrivileges[]::new
        );
    }

    public void profileUids(String... uids) {
        this.uids = uids;
    }

    public void clusterPrivileges(String... privileges) {
        this.clusterPrivileges = privileges;
    }

    public void indexPrivileges(IndicesPrivileges... privileges) {
        this.indexPrivileges = privileges;
    }

    public void applicationPrivileges(RoleDescriptor.ApplicationResourcePrivileges... privileges) {
        this.applicationPrivileges = privileges;
    }

    public String[] profileUids() {
        return uids;
    }

    public String[] clusterPrivileges() {
        return clusterPrivileges;
    }

    public IndicesPrivileges[] indexPrivileges() {
        return indexPrivileges;
    }

    public RoleDescriptor.ApplicationResourcePrivileges[] applicationPrivileges() {
        return applicationPrivileges;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (uids == null) {
            validationException = addValidationError("profile uids must not be null", validationException);
        } else if (uids.length == 0) {
            validationException = addValidationError("profile uids array must not be empty", validationException);
        }
        return HasPrivilegesRequest.validateActionRequestPrivileges(
            validationException,
            clusterPrivileges,
            indexPrivileges,
            applicationPrivileges
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(uids);
        out.writeStringArray(clusterPrivileges);
        out.writeArray(IndicesPrivileges::write, indexPrivileges);
        out.writeArray(RoleDescriptor.ApplicationResourcePrivileges::write, applicationPrivileges);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProfileHasPrivilegesRequest that = (ProfileHasPrivilegesRequest) o;
        return Arrays.equals(uids, that.uids)
            && Arrays.equals(clusterPrivileges, that.clusterPrivileges)
            && Arrays.equals(indexPrivileges, that.indexPrivileges)
            && Arrays.equals(applicationPrivileges, that.applicationPrivileges);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(uids);
        result = 31 * result + Arrays.hashCode(clusterPrivileges);
        result = 31 * result + Arrays.hashCode(indexPrivileges);
        result = 31 * result + Arrays.hashCode(applicationPrivileges);
        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "{"
            + "uids="
            + Arrays.toString(uids)
            + ","
            + "privileges="
            + "{"
            + "cluster="
            + Arrays.toString(clusterPrivileges)
            + ","
            + "index="
            + Arrays.toString(indexPrivileges)
            + ","
            + "application="
            + Arrays.toString(applicationPrivileges)
            + "}"
            + "}";
    }
}
