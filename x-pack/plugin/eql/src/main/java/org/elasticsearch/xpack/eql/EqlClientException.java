/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql;

import org.elasticsearch.xpack.ql.QlClientException;

public class EqlClientException extends QlClientException {

    public EqlClientException(String message, Object... args) {
        super(message, args);
    }

    protected EqlClientException(String message, Throwable cause) {
        super(message, cause);
    }

    protected EqlClientException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

}
