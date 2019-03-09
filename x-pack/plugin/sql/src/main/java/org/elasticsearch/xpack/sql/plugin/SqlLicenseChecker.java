/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.xpack.sql.proto.Mode;

import java.util.function.Consumer;

/**
 * Determines if different features of SQL should be enabled
 */
public class SqlLicenseChecker {

    private final Consumer<Mode> checkIfSqlAllowed;

    public SqlLicenseChecker(Consumer<Mode> checkIfSqlAllowed) {
        this.checkIfSqlAllowed = checkIfSqlAllowed;
    }

    /**
     * Throws an ElasticsearchSecurityException if the specified mode is not allowed
     */
    public void checkIfSqlAllowed(Mode mode) {
        checkIfSqlAllowed.accept(mode);
    }
}
