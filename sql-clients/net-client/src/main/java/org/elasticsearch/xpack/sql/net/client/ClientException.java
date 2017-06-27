/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.net.client;

import java.util.Locale;

import static java.lang.String.format;

public class ClientException extends RuntimeException {
    public ClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public ClientException(String message) {
        super(message);
    }

    public ClientException(String message, Object... args) { // NOCOMMIT these are not popular in core any more....
        super(format(Locale.ROOT, message, args));
    }

    public ClientException(Throwable cause, String message, Object... args) {
        super(format(Locale.ROOT, message, args), cause);
    }

    public ClientException(Throwable cause) {
        super(cause);
    }
}
