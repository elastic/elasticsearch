/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.core.security.support.Validation;

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
public class PutRoleRequest extends ActionRequest {

    private String name;
    private String[] clusterPrivileges = Strings.EMPTY_ARRAY;
    private ConfigurableClusterPrivilege[] configurableClusterPrivileges = ConfigurableClusterPrivileges.EMPTY_ARRAY;
    private List<RoleDescriptor.IndicesPrivileges> indicesPrivileges = new ArrayList<>();
    private List<RoleDescriptor.ApplicationResourcePrivileges> applicationPrivileges = new ArrayList<>();
    private String[] runAs = Strings.EMPTY_ARRAY;
    private WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE;
    private Map<String, Object> metadata;
    private List<RoleDescriptor.RemoteIndicesPrivileges> remoteIndicesPrivileges = new ArrayList<>();
    private RemoteClusterPermissions remoteClusterPermissions = RemoteClusterPermissions.NONE;
    private String description;

    public PutRoleRequest() {}

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        Validation.Error error = NativeRealmValidationUtil.validateRoleName(this.name, false);
        if (error != null) {
            validationException = addValidationError(error.toString(), validationException);
        }
        return RoleDescriptorRequestValidator.validate(roleDescriptor(), validationException);
    }

    public void name(String name) {
        this.name = name;
    }

    public void description(String description) {
        this.description = description;
    }

    public void cluster(String... clusterPrivilegesArray) {
        this.clusterPrivileges = clusterPrivilegesArray;
    }

    public void conditionalCluster(ConfigurableClusterPrivilege... configurableClusterPrivilegesArray) {
        this.configurableClusterPrivileges = configurableClusterPrivilegesArray;
    }

    public void addIndex(RoleDescriptor.IndicesPrivileges... privileges) {
        this.indicesPrivileges.addAll(Arrays.asList(privileges));
    }

    public void addRemoteIndex(RoleDescriptor.RemoteIndicesPrivileges... privileges) {
        remoteIndicesPrivileges.addAll(Arrays.asList(privileges));
    }

    public void putRemoteCluster(RemoteClusterPermissions remoteClusterPermissions) {
        this.remoteClusterPermissions = remoteClusterPermissions;
    }

    public void addRemoteIndex(
        final String[] remoteClusters,
        final String[] indices,
        final String[] privileges,
        final String[] grantedFields,
        final String[] deniedFields,
        final @Nullable BytesReference query,
        final boolean allowRestrictedIndices
    ) {
        remoteIndicesPrivileges.add(
            RoleDescriptor.RemoteIndicesPrivileges.builder(remoteClusters)
                .indices(indices)
                .privileges(privileges)
                .grantedFields(grantedFields)
                .deniedFields(deniedFields)
                .query(query)
                .allowRestrictedIndices(allowRestrictedIndices)
                .build()
        );
    }

    public void addIndex(
        String[] indices,
        String[] privileges,
        String[] grantedFields,
        String[] deniedFields,
        @Nullable BytesReference query,
        boolean allowRestrictedIndices
    ) {
        this.indicesPrivileges.add(
            RoleDescriptor.IndicesPrivileges.builder()
                .indices(indices)
                .privileges(privileges)
                .grantedFields(grantedFields)
                .deniedFields(deniedFields)
                .query(query)
                .allowRestrictedIndices(allowRestrictedIndices)
                .build()
        );
    }

    public void addApplicationPrivileges(RoleDescriptor.ApplicationResourcePrivileges... privileges) {
        this.applicationPrivileges.addAll(Arrays.asList(privileges));
    }

    public void runAs(String... usernames) {
        this.runAs = usernames;
    }

    public PutRoleRequest setRefreshPolicy(@Nullable String refreshPolicy) {
        if (refreshPolicy != null) {
            setRefreshPolicy(WriteRequest.RefreshPolicy.parse(refreshPolicy));
        }
        return this;
    }

    public PutRoleRequest setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public void metadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public String name() {
        return name;
    }

    public String description() {
        return description;
    }

    public String[] cluster() {
        return clusterPrivileges;
    }

    public RoleDescriptor.IndicesPrivileges[] indices() {
        return indicesPrivileges.toArray(new RoleDescriptor.IndicesPrivileges[indicesPrivileges.size()]);
    }

    public RoleDescriptor.RemoteIndicesPrivileges[] remoteIndices() {
        return remoteIndicesPrivileges.toArray(new RoleDescriptor.RemoteIndicesPrivileges[0]);
    }

    public boolean hasRemoteIndicesPrivileges() {
        return false == remoteIndicesPrivileges.isEmpty();
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
        TransportAction.localOnly();
    }

    public RoleDescriptor roleDescriptor() {
        return new RoleDescriptor(
            name,
            clusterPrivileges,
            indicesPrivileges.toArray(new RoleDescriptor.IndicesPrivileges[indicesPrivileges.size()]),
            applicationPrivileges.toArray(new RoleDescriptor.ApplicationResourcePrivileges[applicationPrivileges.size()]),
            configurableClusterPrivileges,
            runAs,
            metadata,
            Collections.emptyMap(),
            remoteIndicesPrivileges.toArray(new RoleDescriptor.RemoteIndicesPrivileges[0]),
            remoteClusterPermissions,
            null,
            description
        );
    }
}
