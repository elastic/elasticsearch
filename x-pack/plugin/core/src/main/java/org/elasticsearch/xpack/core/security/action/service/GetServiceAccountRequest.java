/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Selects the service accounts to report on. Every part is a filter rather than a lookup: a namespace or service name
 * that no account could carry, reserved or malformed, matches nothing instead of being rejected, which is why
 * {@link #validate()} has nothing to say about either.
 */
public class GetServiceAccountRequest extends UntypedActionRequest {

    @Nullable
    private final String namespace;
    @Nullable
    private final String serviceName;
    private final EnumSet<ServiceAccountType> type;

    /**
     * Reports on built-in accounts only, which is what this request meant before user-managed accounts existed.
     */
    public GetServiceAccountRequest(@Nullable String namespace, @Nullable String serviceName) {
        this(namespace, serviceName, EnumSet.of(ServiceAccountType.BUILT_IN));
    }

    public GetServiceAccountRequest(@Nullable String namespace, @Nullable String serviceName, EnumSet<ServiceAccountType> type) {
        this.namespace = namespace;
        this.serviceName = serviceName;
        this.type = EnumSet.copyOf(Objects.requireNonNull(type, "type cannot be null"));
    }

    public GetServiceAccountRequest(StreamInput in) throws IOException {
        super(in);
        this.namespace = in.readOptionalString();
        this.serviceName = in.readOptionalString();
        // A node that predates user-managed accounts can only be asking about built-in ones.
        this.type = in.getTransportVersion().supports(ServiceAccountInfo.USER_MANAGED_SERVICE_ACCOUNT_INFO)
            ? in.readEnumSet(ServiceAccountType.class)
            : EnumSet.of(ServiceAccountType.BUILT_IN);
    }

    public String getNamespace() {
        return namespace;
    }

    public String getServiceName() {
        return serviceName;
    }

    public EnumSet<ServiceAccountType> getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetServiceAccountRequest that = (GetServiceAccountRequest) o;
        return Objects.equals(namespace, that.namespace) && Objects.equals(serviceName, that.serviceName) && type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, serviceName, type);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(namespace);
        out.writeOptionalString(serviceName);
        if (out.getTransportVersion().supports(ServiceAccountInfo.USER_MANAGED_SERVICE_ACCOUNT_INFO)) {
            out.writeEnumSet(type);
        } else if (type.equals(EnumSet.of(ServiceAccountType.BUILT_IN)) == false) {
            // Dropping the filter would leave a request an older node reads as asking for built-in accounts, so it
            // would answer with accounts the caller did not ask for rather than reporting that it cannot answer.
            throw new IllegalStateException(
                "cannot ask a node that does not support user-managed service accounts for accounts of type ["
                    + type.stream().map(ServiceAccountType::value).collect(Collectors.joining(", "))
                    + "]"
            );
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        if (type.isEmpty()) {
            return addValidationError("type must name at least one of [" + ServiceAccountType.values(", ") + "]", null);
        }
        return null;
    }
}
