/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request object for adding a role to the security index
 */
public class PutRoleRequest extends ActionRequest implements WriteRequest<PutRoleRequest> {

    private String name;
    private String[] clusterPrivileges = Strings.EMPTY_ARRAY;
    private ConfigurableClusterPrivilege[] configurableClusterPrivileges = ConfigurableClusterPrivileges.EMPTY_ARRAY;
    private List<RoleDescriptor.IndicesPrivileges> indicesPrivileges = new ArrayList<>();
    private List<RoleDescriptor.ApplicationResourcePrivileges> applicationPrivileges = new ArrayList<>();
    private String[] runAs = Strings.EMPTY_ARRAY;
    private RefreshPolicy refreshPolicy = RefreshPolicy.IMMEDIATE;
    private Map<String, Object> metadata;

    public PutRoleRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
        clusterPrivileges = in.readStringArray();
        int indicesSize = in.readVInt();
        indicesPrivileges = new ArrayList<>(indicesSize);
        for (int i = 0; i < indicesSize; i++) {
            indicesPrivileges.add(new RoleDescriptor.IndicesPrivileges(in));
        }
        applicationPrivileges = in.readList(RoleDescriptor.ApplicationResourcePrivileges::new);
        configurableClusterPrivileges = ConfigurableClusterPrivileges.readArray(in);
        runAs = in.readStringArray();
        refreshPolicy = RefreshPolicy.readFrom(in);
        metadata = in.readMap();
    }

    public PutRoleRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("role name is missing", validationException);
        }
        if(applicationPrivileges != null) {
            for (RoleDescriptor.ApplicationResourcePrivileges privilege : applicationPrivileges) {
                try {
                    ApplicationPrivilege.validateApplicationNameOrWildcard(privilege.getApplication());
                } catch (IllegalArgumentException e) {
                    validationException = addValidationError(e.getMessage(), validationException);
                }
                for (String name : privilege.getPrivileges()) {
                    try {
                        ApplicationPrivilege.validatePrivilegeOrActionName(name);
                    } catch (IllegalArgumentException e) {
                        validationException = addValidationError(e.getMessage(), validationException);
                    }
                }
            }
        }
        if (metadata != null && MetadataUtils.containsReservedMetadata(metadata)) {
            validationException =
                addValidationError("metadata keys may not start with [" + MetadataUtils.RESERVED_PREFIX + "]", validationException);
        }
        return validationException;
    }

    public void name(String name) {
        this.name = name;
    }

    public void cluster(String... clusterPrivileges) {
        this.clusterPrivileges = clusterPrivileges;
    }

    void conditionalCluster(ConfigurableClusterPrivilege... configurableClusterPrivileges) {
        this.configurableClusterPrivileges = configurableClusterPrivileges;
    }

    void addIndex(RoleDescriptor.IndicesPrivileges... privileges) {
        this.indicesPrivileges.addAll(Arrays.asList(privileges));
    }

    public void addIndex(String[] indices, String[] privileges, String[] grantedFields, String[] deniedFields,
                         @Nullable BytesReference query, boolean allowRestrictedIndices) {
        this.indicesPrivileges.add(RoleDescriptor.IndicesPrivileges.builder()
                .indices(indices)
                .privileges(privileges)
                .grantedFields(grantedFields)
                .deniedFields(deniedFields)
                .query(query)
                .allowRestrictedIndices(allowRestrictedIndices)
                .build());
    }

    void addApplicationPrivileges(RoleDescriptor.ApplicationResourcePrivileges... privileges) {
        this.applicationPrivileges.addAll(Arrays.asList(privileges));
    }

    public void runAs(String... usernames) {
        this.runAs = usernames;
    }

    @Override
    public PutRoleRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    /**
     * Should this request trigger a refresh ({@linkplain RefreshPolicy#IMMEDIATE}, the default), wait for a refresh (
     * {@linkplain RefreshPolicy#WAIT_UNTIL}), or proceed ignore refreshes entirely ({@linkplain RefreshPolicy#NONE}).
     */
    @Override
    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public void metadata(Map<String, Object> metadata) {
        this.metadata = metadata;
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

    public List<RoleDescriptor.ApplicationResourcePrivileges> applicationPrivileges() {
        return Collections.unmodifiableList(applicationPrivileges);
    }

    public ConfigurableClusterPrivilege[] conditionalClusterPrivileges() {
        return configurableClusterPrivileges;
    }

    public String[] runAs() {
        return runAs;
    }

    public Map<String, Object> metadata() {
        return metadata;
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
        out.writeList(applicationPrivileges);
        ConfigurableClusterPrivileges.writeArray(out, this.configurableClusterPrivileges);
        out.writeStringArray(runAs);
        refreshPolicy.writeTo(out);
        out.writeMap(metadata);
    }

    public RoleDescriptor roleDescriptor() {
        return new RoleDescriptor(name,
                clusterPrivileges,
                indicesPrivileges.toArray(new RoleDescriptor.IndicesPrivileges[indicesPrivileges.size()]),
                applicationPrivileges.toArray(new RoleDescriptor.ApplicationResourcePrivileges[applicationPrivileges.size()]),
            configurableClusterPrivileges,
                runAs,
                metadata,
                Collections.emptyMap());
    }

}
