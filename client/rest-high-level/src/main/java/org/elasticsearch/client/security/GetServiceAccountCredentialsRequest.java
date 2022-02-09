/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;

import java.util.Objects;

/**
 * Request to retrieve credentials of a service account
 */
public final class GetServiceAccountCredentialsRequest implements Validatable {

    private final String namespace;
    private final String serviceName;

    public GetServiceAccountCredentialsRequest(String namespace, String serviceName) {
        this.namespace = Objects.requireNonNull(namespace, "namespace is required");
        this.serviceName = Objects.requireNonNull(serviceName, "service-name is required");
    }

    public String getNamespace() {
        return namespace;
    }

    public String getServiceName() {
        return serviceName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetServiceAccountCredentialsRequest that = (GetServiceAccountCredentialsRequest) o;
        return namespace.equals(that.namespace) && serviceName.equals(that.serviceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, serviceName);
    }
}
