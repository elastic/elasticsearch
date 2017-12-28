/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import java.util.function.Supplier;

/**
 * Determines if different features of SQL should be enabled
 */
public class SqlLicenseChecker {

    private final Runnable checkIfSqlAllowed;
    private final Runnable checkIfJdbcAllowed;
    private final Supplier<Boolean> isJdbcAllowed;

    public SqlLicenseChecker(Runnable checkIfSqlAllowed, Runnable checkIfJdbcAllowed, Supplier<Boolean> isJdbcAllowed) {
        this.checkIfSqlAllowed = checkIfSqlAllowed;
        this.checkIfJdbcAllowed = checkIfJdbcAllowed;
        this.isJdbcAllowed = isJdbcAllowed;
    }

    /**
     * Throws an ElasticsearchSecurityException if sql is not allowed
     */
    public void checkIfSqlAllowed() {
        checkIfSqlAllowed.run();
    }

    /**
     * Throws an ElasticsearchSecurityException if jdbc is not allowed
     */
    public void checkIfJdbcAllowed() {
        checkIfJdbcAllowed.run();
    }

    public boolean isJdbcAllowed() {
        return isJdbcAllowed.get();
    }
}
