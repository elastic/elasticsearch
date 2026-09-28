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
 * The kind of service account: {@link #BUILT_IN} for the accounts that ship with Elasticsearch in the reserved
 * {@link ServiceAccountSettings#BUILTIN_NAMESPACE} namespace, and {@link #USER_MANAGED} for the accounts created
 * through the service account API.
 */
public enum ServiceAccountType {

    BUILT_IN("built_in"),
    USER_MANAGED("user_managed");

    private final String value;

    ServiceAccountType(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static ServiceAccountType fromValue(String value) {
        for (ServiceAccountType type : values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("invalid type value [" + value + "]; must be one of [" + values(", ") + "]");
    }

    /**
     * Every value, in declaration order, joined by {@code separator}. Derived rather than written out so that a value
     * added here cannot leave an error message or an API description listing a subset of the values it accepts.
     */
    public static String values(String separator) {
        return Arrays.stream(values()).map(ServiceAccountType::value).collect(Collectors.joining(separator));
    }

    @Override
    public String toString() {
        return value;
    }
}
