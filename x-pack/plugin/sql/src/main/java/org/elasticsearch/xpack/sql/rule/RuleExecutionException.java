/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.rule;

import org.elasticsearch.xpack.sql.ServerSqlException;

public class RuleExecutionException extends ServerSqlException {

    public RuleExecutionException(String message, Object... args) {
        super(message, args);
    }
}
