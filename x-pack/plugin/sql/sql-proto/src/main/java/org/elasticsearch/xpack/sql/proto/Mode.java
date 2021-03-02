/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto;

import java.util.Locale;

/**
 * SQL protocol mode
 */
public enum Mode {
    CLI,
    PLAIN,
    JDBC,
    ODBC;

    public static Mode fromString(String mode) {
        if (mode == null || mode.isEmpty()) {
            return PLAIN;
        }
        return Mode.valueOf(mode.toUpperCase(Locale.ROOT));
    }


    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }

    public static boolean isDriver(Mode mode) {
        return mode == JDBC || mode == ODBC;
    }

    public static boolean isDedicatedClient(Mode mode) {
        return mode == JDBC || mode == ODBC || mode == CLI;
    }
}
