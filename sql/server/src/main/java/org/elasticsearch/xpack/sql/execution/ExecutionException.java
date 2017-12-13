/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution;

import org.elasticsearch.xpack.sql.SqlException;

//TODO: beef up the exception or remove it
public class ExecutionException extends SqlException {

    public ExecutionException(String message, Object ...args) {
        super(message, args);
    }

    public ExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
