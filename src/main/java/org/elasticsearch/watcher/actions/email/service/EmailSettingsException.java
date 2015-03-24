/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service;

/**
 *
 */
public class EmailSettingsException extends EmailException {

    public EmailSettingsException(String msg) {
        super(msg);
    }

    public EmailSettingsException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
