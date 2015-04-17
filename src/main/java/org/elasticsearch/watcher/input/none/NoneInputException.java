/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input.none;

import org.elasticsearch.watcher.input.InputException;

/**
 *
 */
public class NoneInputException extends InputException {

    public NoneInputException(String msg, Object... args) {
        super(msg, args);
    }

    public NoneInputException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}
