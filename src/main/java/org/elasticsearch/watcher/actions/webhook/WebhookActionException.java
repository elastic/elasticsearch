/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.webhook;

import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.watcher.actions.ActionException;

/**
 *
 */
public class WebhookActionException extends ActionException {

    public WebhookActionException(String msg, Object... args) {
        super(LoggerMessageFormat.format(msg, args));
    }

    public WebhookActionException(String msg, Throwable cause, Object... args) {
        super(LoggerMessageFormat.format(msg, args), cause);
    }
}
