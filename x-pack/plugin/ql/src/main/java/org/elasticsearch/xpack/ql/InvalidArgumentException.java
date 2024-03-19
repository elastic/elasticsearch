/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql;

/**
 * Exception thrown when unable to continue processing client request,
 * in cases such as invalid query parameter or failure to apply requested processing to given data.
 * It's meant as a generic equivalent to QlIllegalArgumentException (that's a server exception).
 * TODO: reason for [E|S|ES]QL specializations of QlIllegalArgumentException?
 * TODO: the intended use of ql.ParsingException, vs its [E|S|ES]QL equivalents, subclassed from the respective XxxClientException?
 *       Same for PlanningException.
 */
public class InvalidArgumentException extends QlClientException {

    public InvalidArgumentException(String message, Object... args) {
        super(message, args);
    }

    public InvalidArgumentException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

}
