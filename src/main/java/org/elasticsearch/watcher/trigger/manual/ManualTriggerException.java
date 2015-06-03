/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.manual;

import org.elasticsearch.watcher.trigger.TriggerException;

/**
 *
 */
public class ManualTriggerException extends TriggerException {

    public ManualTriggerException(String msg, Object... args) {
        super(msg, args);
    }

    public ManualTriggerException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}
