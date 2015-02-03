/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

/**
 *
 */
public class AlertsSettingsException extends AlertsException {

    public AlertsSettingsException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public AlertsSettingsException(String msg) {
        super(msg);
    }
}
