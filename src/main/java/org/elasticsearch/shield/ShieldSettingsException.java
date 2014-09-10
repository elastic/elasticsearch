/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

/**
 *
 */
public class ShieldSettingsException extends ShieldException {

    public ShieldSettingsException(String msg) {
        super(msg);
    }

    public ShieldSettingsException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
