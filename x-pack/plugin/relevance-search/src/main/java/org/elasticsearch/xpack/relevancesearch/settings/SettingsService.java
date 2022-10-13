/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.settings;

public interface SettingsService<S extends Settings> {
    S getSettings(String settingsId) throws SettingsNotFoundException, InvalidSettingsException;

    class SettingsServiceException extends Exception {
        public SettingsServiceException(String message) {
            super(message);
        }
    }

    class SettingsNotFoundException extends SettingsServiceException {
        public SettingsNotFoundException(String message) {
            super(message);
        }
    }

    class InvalidSettingsException extends SettingsServiceException {
        public InvalidSettingsException(String message) {
            super(message);
        }
    }
}
