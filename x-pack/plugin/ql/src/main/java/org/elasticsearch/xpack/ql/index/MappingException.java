/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.index;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.ql.QlClientException;

public class MappingException extends QlClientException {

    public MappingException(String message, Object... args) {
        super(message, args);
    }

    public MappingException(String message, Throwable ex) {
        super(message, ex);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
