/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.history;

import org.elasticsearch.alerts.AlertsException;

/**
 */
public class HistoryException extends AlertsException {

    public HistoryException(String msg) {
        super(msg);
    }

    public HistoryException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
