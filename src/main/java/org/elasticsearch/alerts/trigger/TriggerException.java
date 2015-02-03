/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.trigger;

import org.elasticsearch.alerts.AlertsException;

/**
 *
 */
public class TriggerException extends AlertsException {

    public TriggerException(String msg) {
        super(msg);
    }

    public TriggerException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
