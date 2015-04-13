/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;

/**
 * A base class for all watcher exceptions
 */
public class WatcherException extends ElasticsearchException {

    public WatcherException(String msg, Object... args) {
        super(LoggerMessageFormat.format(msg, args));
    }

    public WatcherException(String msg, Throwable cause, Object... args) {
        super(LoggerMessageFormat.format(msg, args), cause);
    }
}
