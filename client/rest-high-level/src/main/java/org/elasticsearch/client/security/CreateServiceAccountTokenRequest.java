/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.core.Nullable;

import java.util.Objects;

/**
 * Request to create a service account token
 */
public final class CreateServiceAccountTokenRequest implements Validatable {

    private final String namespace;
    private final String serviceName;
    @Nullable
    private final String tokenName;
    @Nullable
    private final RefreshPolicy refreshPolicy;

    public CreateServiceAccountTokenRequest(
        String namespace,
        String serviceName,
        @Nullable String tokenName,
        @Nullable RefreshPolicy refreshPolicy
    ) {
        this.namespace = Objects.requireNonNull(namespace, "namespace is required");
        this.serviceName = Objects.requireNonNull(serviceName, "service-name is required");
        this.tokenName = tokenName;
        this.refreshPolicy = refreshPolicy;
    }

    public CreateServiceAccountTokenRequest(String namespace, String serviceName, String tokenName) {
        this(namespace, serviceName, tokenName, null);
    }

    public CreateServiceAccountTokenRequest(String namespace, String serviceName) {
        this(namespace, serviceName, null, null);
    }

    public String getNamespace() {
        return namespace;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getTokenName() {
        return tokenName;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateServiceAccountTokenRequest that = (CreateServiceAccountTokenRequest) o;
        return namespace.equals(that.namespace)
            && serviceName.equals(that.serviceName)
            && Objects.equals(tokenName, that.tokenName)
            && refreshPolicy == that.refreshPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, serviceName, tokenName, refreshPolicy);
    }
}
