/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.configuration;

import java.util.Locale;

public enum ConfigurationValidationType {
    LESS_THAN,
    GREATER_THAN,
    LIST_TYPE,
    INCLUDED_IN,
    REGEX;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static ConfigurationValidationType validationType(String type) {
        for (ConfigurationValidationType displayType : ConfigurationValidationType.values()) {
            if (displayType.name().equalsIgnoreCase(type)) {
                return displayType;
            }
        }
        throw new IllegalArgumentException("Unknown " + ConfigurationValidationType.class.getSimpleName() + " [" + type + "].");
    }
}
