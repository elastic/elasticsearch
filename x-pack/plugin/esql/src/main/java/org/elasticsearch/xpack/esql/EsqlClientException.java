/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql;

import org.elasticsearch.xpack.ql.QlClientException;

public abstract class EsqlClientException extends QlClientException {

    protected EsqlClientException(String message, Object... args) {
        super(message, args);
    }

    protected EsqlClientException(String message, Throwable cause) {
        super(message, cause);
    }

    protected EsqlClientException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

}
