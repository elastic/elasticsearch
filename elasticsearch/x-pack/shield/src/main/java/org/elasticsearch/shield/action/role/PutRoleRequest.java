/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.role;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.shield.authz.RoleDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request object for adding a role to the shield index
 */
public class PutRoleRequest extends ActionRequest<PutRoleRequest> {

    private String name;
    private String[] clusterPrivileges = Strings.EMPTY_ARRAY;
    private List<RoleDescriptor.IndicesPrivileges> indicesPrivileges = new ArrayList<>();
    private String[] runAs = Strings.EMPTY_ARRAY;
    private boolean refresh = true;
    
    public PutRoleRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("role name is missing", validationException);
        }
        return validationException;
    }

    public void name(String name) {
        this.name = name;
    }

    public void cluster(String... clusterPrivileges) {
        this.clusterPrivileges = clusterPrivileges;
    }

    void addIndex(RoleDescriptor.IndicesPrivileges... privileges) {
        this.indicesPrivileges.addAll(Arrays.asList(privileges));
    }

    public void addIndex(String[] indices, String[] privileges, @Nullable String[] fields, @Nullable BytesReference query) {
        this.indicesPrivileges.add(RoleDescriptor.IndicesPrivileges.builder()
                .indices(indices)
                .privileges(privileges)
                .fields(fields)
                .query(query)
                .build());
    }

    public void runAs(String... usernames) {
        this.runAs = usernames;
    }

    public void refresh(boolean refresh) {
        this.refresh = refresh;
    }

    public String name() {
        return name;
    }

    public String[] cluster() {
        return clusterPrivileges;
    }

    public RoleDescriptor.IndicesPrivileges[] indices() {
        return indicesPrivileges.toArray(new RoleDescriptor.IndicesPrivileges[indicesPrivileges.size()]);
    }

    public String[] runAs() {
        return runAs;
    }

    public boolean refresh() {
        return refresh;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        name = in.readString();
        clusterPrivileges = in.readStringArray();
        int indicesSize = in.readVInt();
        indicesPrivileges = new ArrayList<>(indicesSize);
        for (int i = 0; i < indicesSize; i++) {
            indicesPrivileges.add(RoleDescriptor.IndicesPrivileges.createFrom(in));
        }
        runAs = in.readStringArray();
        refresh = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeStringArray(clusterPrivileges);
        out.writeVInt(indicesPrivileges.size());
        for (RoleDescriptor.IndicesPrivileges index : indicesPrivileges) {
            index.writeTo(out);
        }
        out.writeStringArray(runAs);
        out.writeBoolean(refresh);
    }

    RoleDescriptor roleDescriptor() {
        return new RoleDescriptor(name,
                clusterPrivileges,
                indicesPrivileges.toArray(new RoleDescriptor.IndicesPrivileges[indicesPrivileges.size()]),
                runAs);
    }
}