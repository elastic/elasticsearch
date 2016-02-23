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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.shield.authz.RoleDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request object for adding a role to the shield index
 */
public class PutRoleRequest extends ActionRequest<PutRoleRequest> implements ToXContent {

    private String name;
    private List<String> clusterPriv;
    // List of index names to privileges
    private List<RoleDescriptor.IndicesPrivileges> indices = new ArrayList<>();
    private List<String> runAs = new ArrayList<>();
    private RoleDescriptor roleDescriptor;
    
    public PutRoleRequest() {
    }

    public PutRoleRequest(BytesReference source) throws Exception {
        this.roleDescriptor = RoleDescriptor.source(source);
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

    public void cluster(String clusterPrivilege) {
        this.clusterPriv = Collections.singletonList(clusterPrivilege);
    }

    public void cluster(List<String> clusterPrivileges) {
        this.clusterPriv = clusterPrivileges;
    }

    public void addIndex(String[] indices, String[] privileges, @Nullable String[] fields, @Nullable BytesReference query) {
        this.indices.add(RoleDescriptor.IndicesPrivileges.builder()
                .indices(indices)
                .privileges(privileges)
                .fields(fields)
                .query(query)
                .build());
    }

    public void runAs(List<String> usernames) {
        this.runAs = usernames;
    }

    public String name() {
        return name;
    }

    public List<String> cluster() {
        return clusterPriv;
    }

    public List<RoleDescriptor.IndicesPrivileges> indices() {
        return indices;
    }

    public List<String> runAs() {
        return runAs;
    }

    private RoleDescriptor roleDescriptor() {
        if (this.roleDescriptor != null) {
            return this.roleDescriptor;
        }
        this.roleDescriptor = new RoleDescriptor(name, this.clusterPriv.toArray(Strings.EMPTY_ARRAY),
                this.indices, this.runAs.toArray(Strings.EMPTY_ARRAY));
        return this.roleDescriptor;
    }
    
    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        name = in.readString();
        int clusterSize = in.readVInt();
        List<String> tempCluster = new ArrayList<>(clusterSize);
        for (int i = 0; i < clusterSize; i++) {
            tempCluster.add(in.readString());
        }
        clusterPriv = tempCluster;
        int indicesSize = in.readVInt();
        indices = new ArrayList<>(indicesSize);
        for (int i = 0; i < indicesSize; i++) {
            indices.add(RoleDescriptor.IndicesPrivileges.readIndicesPrivileges(in));
        }
        if (in.readBoolean()) {
            int runAsSize = in.readVInt();
            runAs = new ArrayList<>(runAsSize);
            for (int i = 0; i < runAsSize; i++) {
                runAs.add(in.readString());
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeVInt(clusterPriv.size());
        for (String cluster : clusterPriv) {
            out.writeString(cluster);
        }
        out.writeVInt(indices.size());
        for (RoleDescriptor.IndicesPrivileges index : indices) {
            index.writeTo(out);
        }
        if (runAs.isEmpty() == false) {
            out.writeBoolean(true);
            out.writeVInt(runAs.size());
            for (String runAsUser : runAs) {
                out.writeString(runAsUser);
            }
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return this.roleDescriptor().toXContent(builder, params);
    }
}
