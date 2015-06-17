/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.watcher.WatcherException;

public class WatcherInactiveException extends WatcherException {

    public WatcherInactiveException(String msg, Object... args) {
        super(msg, args);
    }

    public WatcherInactiveException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}
