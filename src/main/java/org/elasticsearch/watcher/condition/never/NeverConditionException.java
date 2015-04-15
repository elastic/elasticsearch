/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.never;

import org.elasticsearch.watcher.condition.ConditionException;

/**
 *
 */
public class NeverConditionException extends ConditionException {

    public NeverConditionException(String msg, Object... args) {
        super(msg, args);
    }

    public NeverConditionException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}
