/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.chain;

import org.elasticsearch.watcher.transform.TransformException;

/**
 *
 */
public class ChainTransformException extends TransformException {

    public ChainTransformException(String msg, Object... args) {
        super(msg, args);
    }

    public ChainTransformException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}
