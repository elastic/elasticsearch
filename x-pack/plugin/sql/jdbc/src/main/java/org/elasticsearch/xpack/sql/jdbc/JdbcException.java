/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

public class JdbcException extends RuntimeException {
    
    public JdbcException(String message) {
        super(message);
    }

    public JdbcException(Throwable cause, String message) {
        super(message, cause);
    }
}
