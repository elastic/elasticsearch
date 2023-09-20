/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.EsqlClientException;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * Represents e.g. overflows when folding temporal values caused by a faulty query.
 * This is turned into a REST response with HTTP status (400).
 * Even though this is a pretty specific exception, we cannot use e.g. a anonymous class that inherits from {@link EsqlClientException},
 * because then the error message sent back in the REST response would have an empty `type` string.
 */
public class DateMathException extends EsqlClientException {
    protected DateMathException(String message, Object... args) {
        super(message, args);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    public static DateMathException fromArithmeticException(Source source, ArithmeticException e) {
        return new DateMathException("arithmetic exception in expression [{}]: [{}]", source.text(), e.getMessage());
    }
}
