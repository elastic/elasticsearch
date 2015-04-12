/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.logging;

import org.elasticsearch.watcher.actions.ActionException;

/**
 *
 */
public class LoggingActionException extends ActionException {

    public LoggingActionException(String msg) {
        super(msg);
    }

    public LoggingActionException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
