/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql;

public class QlVersionMismatchException extends QlServerException {
    public QlVersionMismatchException(String message, Object... args) {
        super(message, args);
    }
}
