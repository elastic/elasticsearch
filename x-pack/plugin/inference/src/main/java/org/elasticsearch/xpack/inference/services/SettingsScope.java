/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

/**
 * Represents the scope of a settings block, used to produce structured error messages such as
 * {@code [service_settings] does not contain the required setting [model_id]}.
 */
public enum SettingsScope {
    SERVICE_SETTINGS("service_settings"),
    TASK_SETTINGS("task_settings"),
    SECRET_SETTINGS("secret_settings"),
    INPUT_TYPE_TRANSLATOR("input_type_translator"),
    RATE_LIMIT("rate_limit");

    private final String value;

    SettingsScope(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
