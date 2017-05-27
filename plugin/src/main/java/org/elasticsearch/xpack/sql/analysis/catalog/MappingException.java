/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import org.elasticsearch.xpack.sql.SqlException;

public class MappingException extends SqlException {

    public MappingException(String message, Object... args) {
        super(message, args);
    }

    public MappingException(String message, Throwable ex) {
        super(message, ex);
    }
}
