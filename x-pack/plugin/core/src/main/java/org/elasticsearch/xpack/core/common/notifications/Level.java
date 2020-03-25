/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.common.notifications;

import java.util.Locale;

public enum Level {
    INFO, WARNING, ERROR;

    /**
     * Case-insensitive from string method.
     *
     * @param value
     *            String representation
     * @return The condition type
     */
    public static Level fromString(String value) {
        return Level.valueOf(value.toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
