/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.common.notifications;

import java.util.Locale;

public enum Level {
    INFO {
        public org.apache.logging.log4j.Level log4jLevel() {
            return org.apache.logging.log4j.Level.INFO;
        }
    },
    WARNING {
        public org.apache.logging.log4j.Level log4jLevel() {
            return org.apache.logging.log4j.Level.WARN;
        }
    },
    ERROR {
        public org.apache.logging.log4j.Level log4jLevel() {
            return org.apache.logging.log4j.Level.ERROR;
        }
    };

    public abstract org.apache.logging.log4j.Level log4jLevel();

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
