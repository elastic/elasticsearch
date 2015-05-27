/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.validation;

import org.elasticsearch.watcher.WatcherException;

/**
 *
 */
public class WatcherSettingsException extends WatcherException {

    public WatcherSettingsException() {
        super("invalid settings");
    }

    public void addError(String error) {
        addSuppressed(new InvalidSettingException(error));
    }

    static class InvalidSettingException extends WatcherException {

        public InvalidSettingException(String error) {
            super(error);
        }
    }
}
