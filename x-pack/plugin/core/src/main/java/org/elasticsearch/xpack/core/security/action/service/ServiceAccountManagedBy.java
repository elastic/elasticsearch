/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Says who manages a service account: {@link #ELASTIC} for the built-in accounts that ship with Elasticsearch in the
 * reserved {@link ServiceAccountSettings#BUILTIN_NAMESPACE} namespace, and {@link #USER} for the accounts created
 * through the service account API.
 */
public enum ServiceAccountManagedBy {

    ELASTIC("elastic"),
    USER("user");

    private final String value;

    ServiceAccountManagedBy(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static ServiceAccountManagedBy fromValue(String value) {
        for (ServiceAccountManagedBy managedBy : values()) {
            if (managedBy.value.equals(value)) {
                return managedBy;
            }
        }
        throw new IllegalArgumentException("invalid managed_by value [" + value + "]; must be one of [" + values(", ") + "]");
    }

    /**
     * Every value, in declaration order, joined by {@code separator}. Derived rather than written out so that a value
     * added here cannot leave an error message or an API description listing a subset of the values it accepts.
     */
    public static String values(String separator) {
        return Arrays.stream(values()).map(ServiceAccountManagedBy::value).collect(Collectors.joining(separator));
    }

    @Override
    public String toString() {
        return value;
    }
}
