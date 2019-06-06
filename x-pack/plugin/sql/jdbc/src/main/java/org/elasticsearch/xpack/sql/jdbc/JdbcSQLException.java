/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import java.sql.SQLException;

class JdbcSQLException extends SQLException {
    
    JdbcSQLException(String message) {
        super(message);
    }

    JdbcSQLException(Throwable cause, String message) {
        super(message, cause);
    }
}
