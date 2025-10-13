/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.configuration;

public enum ConfigurationFieldType {
    STRING("str"),
    INTEGER("int"),
    LIST("list"),
    BOOLEAN("bool");

    private final String value;

    ConfigurationFieldType(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return this.value;
    }

    public static ConfigurationFieldType fieldType(String type) {
        for (ConfigurationFieldType fieldType : ConfigurationFieldType.values()) {
            if (fieldType.value.equals(type)) {
                return fieldType;
            }
        }
        throw new IllegalArgumentException("Unknown " + ConfigurationFieldType.class.getSimpleName() + " [" + type + "].");
    }
}
