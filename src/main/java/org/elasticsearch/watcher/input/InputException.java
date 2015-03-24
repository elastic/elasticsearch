/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input;

import org.elasticsearch.watcher.WatcherException;

/**
 *
 */
public class InputException extends WatcherException {

    public InputException(String msg) {
        super(msg);
    }

    public InputException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
