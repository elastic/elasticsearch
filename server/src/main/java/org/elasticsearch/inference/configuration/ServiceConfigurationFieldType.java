/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.configuration;

public enum ServiceConfigurationFieldType {
    STRING("str"),
    INTEGER("int"),
    LIST("list"),
    BOOLEAN("bool");

    private final String value;

    ServiceConfigurationFieldType(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return this.value;
    }

    public static ServiceConfigurationFieldType fieldType(String type) {
        for (ServiceConfigurationFieldType fieldType : ServiceConfigurationFieldType.values()) {
            if (fieldType.value.equals(type)) {
                return fieldType;
            }
        }
        throw new IllegalArgumentException("Unknown " + ServiceConfigurationFieldType.class.getSimpleName() + " [" + type + "].");
    }
}
