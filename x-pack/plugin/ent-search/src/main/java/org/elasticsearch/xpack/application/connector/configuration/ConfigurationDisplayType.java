/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.configuration;

import java.util.Locale;

public enum ConfigurationDisplayType {
    TEXT,
    TEXTBOX,
    TEXTAREA,
    NUMERIC,
    TOGGLE,
    DROPDOWN;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static ConfigurationDisplayType displayType(String type) {
        for (ConfigurationDisplayType displayType : ConfigurationDisplayType.values()) {
            if (displayType.name().equalsIgnoreCase(type)) {
                return displayType;
            }
        }
        throw new IllegalArgumentException("Unknown " + ConfigurationDisplayType.class.getSimpleName() + " [" + type + "].");
    }
}
