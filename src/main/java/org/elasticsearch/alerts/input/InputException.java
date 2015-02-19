/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.input;

import org.elasticsearch.alerts.AlertsException;

/**
 *
 */
public class InputException extends AlertsException {

    public InputException(String msg) {
        super(msg);
    }

    public InputException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
