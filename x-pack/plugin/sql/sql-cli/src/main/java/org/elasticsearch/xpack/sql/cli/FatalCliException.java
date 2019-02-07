/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

/**
 * Throwing this except will cause the CLI to terminate
 */
public class FatalCliException  extends RuntimeException {
    public FatalCliException(String message, Throwable cause) {
        super(message, cause);
    }

    public FatalCliException(String message) {
        super(message);
    }
}
