/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform;

import org.elasticsearch.watcher.WatcherException;

/**
 *
 */
public class TransformException extends WatcherException {

    public TransformException(String msg) {
        super(msg);
    }

    public TransformException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
