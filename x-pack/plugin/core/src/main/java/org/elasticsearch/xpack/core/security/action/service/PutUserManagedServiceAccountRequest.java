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
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.core.security.support.Validation;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Creates a user-managed service account, or replaces an existing one of the same name wholesale. A replacement is
 * not a partial update: an account whose {@code enabled} was set to false and is then written again without the field
 * comes back enabled, because the default applies to every write rather than only to the first.
 */
public class PutUserManagedServiceAccountRequest extends UntypedActionRequest {

    private static final ParseField ROLES = new ParseField("roles");
    private static final ParseField ENABLED = new ParseField("enabled");

    /**
     * The request body on its own. The account a request names comes from the path, so parsing produces this and the
     * two are joined afterward, rather than a request that exists for a moment without the account it is about.
     */
    private record Body(List<String> roles, boolean enabled) {}

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Body, Void> PARSER = new ConstructingObjectParser<>(
        "put_user_managed_service_account_request",
        false,
        args -> new Body((List<String>) args[0], args[1] == null || (Boolean) args[1])
    );

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), ROLES);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ENABLED);
    }

    private final String namespace;
    private final String serviceName;
    private final List<String> roles;
    private final boolean enabled;
    private WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.WAIT_UNTIL;

    public PutUserManagedServiceAccountRequest(String namespace, String serviceName, List<String> roles, boolean enabled) {
        this.namespace = Objects.requireNonNull(namespace, "namespace cannot be null");
        this.serviceName = Objects.requireNonNull(serviceName, "service name cannot be null");
        this.roles = List.copyOf(Objects.requireNonNull(roles, "roles cannot be null"));
        this.enabled = enabled;
    }

    public PutUserManagedServiceAccountRequest(StreamInput in) throws IOException {
        super(in);
        this.namespace = in.readString();
        this.serviceName = in.readString();
        this.roles = in.readStringCollectionAsList();
        this.enabled = in.readBoolean();
        this.refreshPolicy = WriteRequest.RefreshPolicy.readFrom(in);
    }

    /**
     * Parses the request body for the account named by {@code namespace} and {@code serviceName}. Whether those name
     * an account that may exist is left to {@link #validate()}, so that a caller is told what is wrong with the whole
     * request rather than only with its path.
     */
    public static PutUserManagedServiceAccountRequest parse(String namespace, String serviceName, XContentParser parser)
        throws IOException {
        final Body body = PARSER.parse(parser, null);
        return new PutUserManagedServiceAccountRequest(namespace, serviceName, body.roles(), body.enabled());
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

    public boolean isEnabled() {
        return enabled;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public void setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = Objects.requireNonNull(refreshPolicy, "refresh policy may not be null");
    }

    public ServiceAccountId getAccountId() {
        return new ServiceAccountId(namespace, serviceName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutUserManagedServiceAccountRequest that = (PutUserManagedServiceAccountRequest) o;
        return enabled == that.enabled
            && namespace.equals(that.namespace)
            && serviceName.equals(that.serviceName)
            && roles.equals(that.roles)
            && refreshPolicy == that.refreshPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, serviceName, roles, enabled, refreshPolicy);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(namespace);
        out.writeString(serviceName);
        out.writeStringCollection(roles);
        out.writeBoolean(enabled);
        refreshPolicy.writeTo(out);
    }

    /**
     * Reports every problem with the account's name and roles at once, so that a caller correcting a request does not
     * have to submit it again to find the next fault. The account store validates the same things for callers that do
     * not arrive through this request.
     */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        final Validation.Error namespaceError = Validation.UserManagedServiceAccounts.validateNamespace(namespace);
        if (namespaceError != null) {
            validationException = addValidationError(namespaceError.toString(), validationException);
        }
        final Validation.Error serviceNameError = Validation.UserManagedServiceAccounts.validateServiceName(serviceName);
        if (serviceNameError != null) {
            validationException = addValidationError(serviceNameError.toString(), validationException);
        }
        for (String role : roles) {
            final Validation.Error roleNameError = NativeRealmValidationUtil.validateRoleName(role, true);
            if (roleNameError != null) {
                validationException = addValidationError(roleNameError.toString(), validationException);
            }
        }
        return validationException;
    }
}
