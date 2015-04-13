/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.index;

import org.elasticsearch.watcher.actions.ActionException;

/**
 *
 */
public class IndexActionException extends ActionException {

    public IndexActionException(String msg, Object... args) {
        super(msg, args);
    }

    public IndexActionException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}
