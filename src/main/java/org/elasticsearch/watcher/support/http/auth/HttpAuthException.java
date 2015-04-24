/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http.auth;

import org.elasticsearch.watcher.WatcherException;

/**
 */
public class HttpAuthException extends WatcherException {

    public HttpAuthException(String msg, Object... args) {
        super(msg, args);
    }

    public HttpAuthException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}