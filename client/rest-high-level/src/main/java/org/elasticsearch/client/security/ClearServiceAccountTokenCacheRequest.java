/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;

import java.util.Arrays;
import java.util.Objects;

/**
 * The request used to clear the service account token cache.
 */
public final class ClearServiceAccountTokenCacheRequest implements Validatable {

    private final String namespace;
    private final String serviceName;
    private final String[] tokenNames;

    /**
     * @param tokenNames An array of token names to be cleared from the specified cache.
     *                   If not specified, all entries will be cleared.
     */
    public ClearServiceAccountTokenCacheRequest(String namespace, String serviceName, String... tokenNames) {
        this.namespace = Objects.requireNonNull(namespace, "namespace is required");
        this.serviceName = Objects.requireNonNull(serviceName, "service-name is required");
        this.tokenNames = tokenNames;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String[] getTokenNames() {
        return tokenNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ClearServiceAccountTokenCacheRequest that = (ClearServiceAccountTokenCacheRequest) o;
        return namespace.equals(that.namespace) && serviceName.equals(that.serviceName) && Arrays.equals(tokenNames, that.tokenNames);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(namespace, serviceName);
        result = 31 * result + Arrays.hashCode(tokenNames);
        return result;
    }
}
