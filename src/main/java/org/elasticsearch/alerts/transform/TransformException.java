/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transform;

import org.elasticsearch.alerts.AlertsException;

/**
 *
 */
public class TransformException extends AlertsException {

    public TransformException(String msg) {
        super(msg);
    }

    public TransformException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
