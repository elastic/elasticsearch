/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.user;

import java.io.IOException;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request for checking a user's privileges
 */
public class HasPrivilegesRequest extends ActionRequest implements UserRequest {

    private String username;
    private String[] clusterPrivileges;
    private RoleDescriptor.IndicesPrivileges[] indexPrivileges;

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (clusterPrivileges == null) {
            validationException = addValidationError("clusterPrivileges must not be null", validationException);
        }
        if (indexPrivileges == null) {
            validationException = addValidationError("indexPrivileges must not be null", validationException);
        }
        if (clusterPrivileges != null && clusterPrivileges.length == 0 && indexPrivileges != null && indexPrivileges.length == 0) {
            validationException = addValidationError("clusterPrivileges and indexPrivileges cannot both be empty",
                    validationException);
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

    public void indexPrivileges(RoleDescriptor.IndicesPrivileges... privileges) {
        this.indexPrivileges = privileges;
    }

    public void clusterPrivileges(String... privileges) {
        this.clusterPrivileges = privileges;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.username = in.readString();
        this.clusterPrivileges = in.readStringArray();
        int indexSize = in.readVInt();
        indexPrivileges = new RoleDescriptor.IndicesPrivileges[indexSize];
        for (int i = 0; i < indexSize; i++) {
            indexPrivileges[i] = RoleDescriptor.IndicesPrivileges.createFrom(in);
        }
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
    }

}
