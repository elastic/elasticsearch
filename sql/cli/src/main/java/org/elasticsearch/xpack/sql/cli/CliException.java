/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import java.util.Locale;

import static java.lang.String.format;

@SuppressWarnings("serial")
public class CliException extends RuntimeException {

    public CliException() {
        super();
    }

    public CliException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public CliException(String message, Object... args) {
        super(format(Locale.ROOT, message, args));
    }

    public CliException(Throwable cause, String message, Object... args) {
        super(format(Locale.ROOT, message, args), cause);
    }

    public CliException(Throwable cause) {
        super(cause);
    }

}
