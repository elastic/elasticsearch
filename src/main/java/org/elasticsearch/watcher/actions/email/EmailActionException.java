/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email;

import org.elasticsearch.watcher.actions.ActionException;

/**
 *
 */
public class EmailActionException extends ActionException {

    public EmailActionException(String msg, Object... args) {
        super(msg, args);
    }

    public EmailActionException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}
