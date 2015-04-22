/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.script;

import org.elasticsearch.watcher.transform.TransformException;

/**
 *
 */
public class ScriptTransformException extends TransformException {

    public ScriptTransformException(String msg, Object... args) {
        super(msg, args);
    }

    public ScriptTransformException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}
