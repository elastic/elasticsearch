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
 * Request to delete a service account token
 */
public class DeleteServiceAccountTokenRequest implements Validatable {

    private final String namespace;
    private final String serviceName;
    private final String tokenName;
    @Nullable
    private final RefreshPolicy refreshPolicy;

    public DeleteServiceAccountTokenRequest(String namespace, String serviceName, String tokenName,
                                            @Nullable RefreshPolicy refreshPolicy) {
        this.namespace = Objects.requireNonNull(namespace, "namespace is required");
        this.serviceName = Objects.requireNonNull(serviceName, "service-name is required");
        this.tokenName = Objects.requireNonNull(tokenName, "token name is required");
        this.refreshPolicy = refreshPolicy;
    }

    public DeleteServiceAccountTokenRequest(String namespace, String serviceName, String tokenName) {
        this(namespace, serviceName, tokenName, null);
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
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DeleteServiceAccountTokenRequest that = (DeleteServiceAccountTokenRequest) o;
        return namespace.equals(that.namespace) && serviceName.equals(that.serviceName)
            && tokenName.equals(that.tokenName) && refreshPolicy == that.refreshPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, serviceName, tokenName, refreshPolicy);
    }
}
