/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.search;

import org.elasticsearch.watcher.transform.TransformException;

/**
 *
 */
public class SearchTransformException extends TransformException {

    public SearchTransformException(String msg, Object... args) {
        super(msg, args);
    }

    public SearchTransformException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}
