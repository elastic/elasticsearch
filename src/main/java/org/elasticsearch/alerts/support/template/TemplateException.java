/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.support.template;

import org.elasticsearch.alerts.AlertsException;

/**
 *
 */
public class TemplateException extends AlertsException {

    public TemplateException(String msg) {
        super(msg);
    }

    public TemplateException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
