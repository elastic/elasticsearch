/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.core.security.support.ManagedServiceAccountIdValidator;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.core.security.support.Validation;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutManagedServiceAccountRequest extends UntypedActionRequest {

    private static final ParseField ROLES = new ParseField("roles");
    private static final ParseField ENABLED = new ParseField("enabled");
    private static final ParseField RUN_AS_FROM_ROLES = new ParseField("run_as_from_roles");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<PutManagedServiceAccountRequest, Void> PARSER = new ConstructingObjectParser<>(
        "put_managed_service_account_request",
        a -> new PutManagedServiceAccountRequest((List<String>) a[0], (Boolean) a[1], (List<String>) a[2])
    );

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), ROLES);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ENABLED);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), RUN_AS_FROM_ROLES);
    }

    private final String namespace;
    private final String serviceName;
    private final List<String> roles;
    private final List<String> runAsFromRoles;
    private final boolean enabled;
    private WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.WAIT_UNTIL;

    public PutManagedServiceAccountRequest(String namespace, String serviceName, List<String> roles, boolean enabled) {
        this(namespace, serviceName, roles, List.of(), enabled);
    }

    public PutManagedServiceAccountRequest(
        String namespace,
        String serviceName,
        List<String> roles,
        List<String> runAsFromRoles,
        boolean enabled
    ) {
        this.namespace = Objects.requireNonNull(namespace, "namespace cannot be null");
        this.serviceName = Objects.requireNonNull(serviceName, "service name cannot be null");
        this.roles = List.copyOf(Objects.requireNonNull(roles, "roles cannot be null"));
        this.runAsFromRoles = List.copyOf(Objects.requireNonNull(runAsFromRoles, "run_as_from_roles cannot be null"));
        this.enabled = enabled;
    }

    private PutManagedServiceAccountRequest(List<String> roles, Boolean enabled, List<String> runAsFromRoles) {
        this.namespace = null;
        this.serviceName = null;
        this.roles = List.copyOf(Objects.requireNonNull(roles, "roles cannot be null"));
        this.runAsFromRoles = runAsFromRoles == null ? List.of() : List.copyOf(runAsFromRoles);
        this.enabled = enabled == null || enabled;
    }

    public PutManagedServiceAccountRequest(StreamInput in) throws IOException {
        super(in);
        namespace = in.readString();
        serviceName = in.readString();
        roles = in.readStringCollectionAsList();
        enabled = in.readBoolean();
        refreshPolicy = WriteRequest.RefreshPolicy.readFrom(in);
        runAsFromRoles = in.readStringCollectionAsList();
    }

    public static PutManagedServiceAccountRequest parse(String namespace, String serviceName, XContentParser parser) throws IOException {
        final PutManagedServiceAccountRequest request = PARSER.parse(parser, null);
        return new PutManagedServiceAccountRequest(namespace, serviceName, request.roles, request.runAsFromRoles, request.enabled);
    }

    public String getNamespace() {
        return namespace;
    }

    public String getServiceName() {
        return serviceName;
    }

    public List<String> getRoles() {
        return roles;
    }

    public List<String> getRunAsFromRoles() {
        return runAsFromRoles;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public void setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
    }

    public ServiceAccount.ServiceAccountId getAccountId() {
        return new ServiceAccount.ServiceAccountId(namespace, serviceName);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        final String namespaceError = ManagedServiceAccountIdValidator.validateNamespace(namespace);
        if (namespaceError != null) {
            validationException = addValidationError(namespaceError, validationException);
        }
        final String serviceNameError = ManagedServiceAccountIdValidator.validateServiceName(serviceName);
        if (serviceNameError != null) {
            validationException = addValidationError(serviceNameError, validationException);
        }
        for (String role : roles) {
            final Validation.Error roleNameError = NativeRealmValidationUtil.validateRoleName(role, true);
            if (roleNameError != null) {
                validationException = addValidationError(roleNameError.toString(), validationException);
            }
        }
        for (String roleName : runAsFromRoles) {
            final String runAsFromRolesError = ManagedServiceAccountIdValidator.validateRunAsFromRole(roleName);
            if (runAsFromRolesError != null) {
                validationException = addValidationError(runAsFromRolesError, validationException);
            }
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(namespace);
        out.writeString(serviceName);
        out.writeStringCollection(roles);
        out.writeBoolean(enabled);
        refreshPolicy.writeTo(out);
        out.writeStringCollection(runAsFromRoles);
    }
}
