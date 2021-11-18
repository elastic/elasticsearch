/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

/**
 * A base exception for problems that occur while trying to configure SSL.
 */
public class SslConfigException extends RuntimeException {
    public SslConfigException(String message, Exception cause) {
        super(message, cause);
    }

    public SslConfigException(String message) {
        super(message);
    }
}
