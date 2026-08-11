/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import java.util.Locale;

/**
 * Discriminates the two kinds of service account in API responses and request filters:
 * accounts managed by Elastic (built-in, fixed role descriptors) and accounts managed by
 * the user (API-managed, named role references).
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
        throw new IllegalArgumentException(
            "invalid managed_by value [" + value + "]; must be [" + ELASTIC.value + "] or [" + USER.value + "]"
        );
    }

    @Override
    public String toString() {
        return value().toLowerCase(Locale.ROOT);
    }
}
