/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

/**
 *
 */
public class SecuritySettingsException extends SecurityException {

    public SecuritySettingsException(String msg) {
        super(msg);
    }

    public SecuritySettingsException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
