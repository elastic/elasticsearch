/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.ElasticsearchException;

/**
 * A base class for all alerts exceptions
 */
public class AlertsException extends ElasticsearchException {

    public AlertsException(String msg) {
        super(msg);
    }

    public AlertsException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
