/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ApplicationResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request for checking a user's privileges
 */
public class HasPrivilegesRequest extends ActionRequest implements UserRequest {

    private String username;
    private String[] clusterPrivileges;
    private RoleDescriptor.IndicesPrivileges[] indexPrivileges;
    private ApplicationResourcePrivileges[] applicationPrivileges;

    public HasPrivilegesRequest() {}

    public HasPrivilegesRequest(StreamInput in) throws IOException {
        super(in);
        this.username = in.readString();
        this.clusterPrivileges = in.readStringArray();
        int indexSize = in.readVInt();
        indexPrivileges = new RoleDescriptor.IndicesPrivileges[indexSize];
        for (int i = 0; i < indexSize; i++) {
            indexPrivileges[i] = new RoleDescriptor.IndicesPrivileges(in);
        }
        applicationPrivileges = in.readArray(ApplicationResourcePrivileges::new, ApplicationResourcePrivileges[]::new);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (clusterPrivileges == null) {
            validationException = addValidationError("clusterPrivileges must not be null", validationException);
        }
        if (indexPrivileges == null) {
            validationException = addValidationError("indexPrivileges must not be null", validationException);
        }
        if (applicationPrivileges == null) {
            validationException = addValidationError("applicationPrivileges must not be null", validationException);
        } else {
            for (ApplicationResourcePrivileges applicationPrivilege : applicationPrivileges) {
                try {
                    ApplicationPrivilege.validateApplicationName(applicationPrivilege.getApplication());
                } catch (IllegalArgumentException e) {
                    validationException = addValidationError(e.getMessage(), validationException);
                }
            }
        }
        if (clusterPrivileges != null && clusterPrivileges.length == 0
            && indexPrivileges != null && indexPrivileges.length == 0
            && applicationPrivileges != null && applicationPrivileges.length == 0) {
            validationException = addValidationError("must specify at least one privilege", validationException);
        }
        return validationException;
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

    public RoleDescriptor.IndicesPrivileges[] indexPrivileges() {
        return indexPrivileges;
    }

    public String[] clusterPrivileges() {
        return clusterPrivileges;
    }

    public ApplicationResourcePrivileges[] applicationPrivileges() {
        return applicationPrivileges;
    }

    public void indexPrivileges(RoleDescriptor.IndicesPrivileges... privileges) {
        this.indexPrivileges = privileges;
    }

    public void clusterPrivileges(String... privileges) {
        this.clusterPrivileges = privileges;
    }

    public void applicationPrivileges(ApplicationResourcePrivileges... appPrivileges) {
        this.applicationPrivileges = appPrivileges;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(username);
        out.writeStringArray(clusterPrivileges);
        out.writeVInt(indexPrivileges.length);
        for (RoleDescriptor.IndicesPrivileges priv : indexPrivileges) {
            priv.writeTo(out);
        }
        out.writeArray(ApplicationResourcePrivileges::write, applicationPrivileges);
    }

}
