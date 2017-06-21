/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.jdbc;

import java.util.Locale;

import static java.lang.String.format;

public class JdbcException extends RuntimeException {
    public JdbcException(String message, Object... args) {
        // NOCOMMIT we very rarely use this on new classes in core, instead appending strings.
        super(format(Locale.ROOT, message, args));
    }

    public JdbcException(Throwable cause, String message, Object... args) {
        super(format(Locale.ROOT, message, args), cause);
    }
}
