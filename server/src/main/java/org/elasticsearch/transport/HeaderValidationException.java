/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

/**
 * This is used to pack the validation exception with the associated header.
 */
public class HeaderValidationException extends RuntimeException {
    public final Header header;
    public final Exception validationException;

    public HeaderValidationException(Header header, Exception validationException) {
        this.header = header;
        this.validationException = validationException;
    }
}
