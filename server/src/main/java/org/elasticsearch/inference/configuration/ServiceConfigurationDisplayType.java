/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.configuration;

import java.util.Locale;

public enum ServiceConfigurationDisplayType {
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

    public static ServiceConfigurationDisplayType displayType(String type) {
        for (ServiceConfigurationDisplayType displayType : ServiceConfigurationDisplayType.values()) {
            if (displayType.name().equalsIgnoreCase(type)) {
                return displayType;
            }
        }
        throw new IllegalArgumentException("Unknown " + ServiceConfigurationDisplayType.class.getSimpleName() + " [" + type + "].");
    }
}
