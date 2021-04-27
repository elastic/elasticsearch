/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public final class GetServiceAccountsRequest implements Validatable, ToXContentObject {

    @Nullable
    private final String namespace;
    @Nullable
    private final String serviceName;

    public GetServiceAccountsRequest(@Nullable String namespace, @Nullable String serviceName) {
        this.namespace = namespace;
        this.serviceName = serviceName;
    }

    public GetServiceAccountsRequest(String namespace) {
        this(namespace, null);
    }

    public GetServiceAccountsRequest() {
        this(null, null);
    }

    public String getNamespace() {
        return namespace;
    }

    public String getServiceName() {
        return serviceName;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (namespace == null && serviceName != null) {
            final ValidationException validationException = new ValidationException();
            validationException.addValidationError("cannot specify service-name without namespace");
            return Optional.of(validationException);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        GetServiceAccountsRequest that = (GetServiceAccountsRequest) o;
        return Objects.equals(namespace, that.namespace) && Objects.equals(serviceName, that.serviceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, serviceName);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }
}
