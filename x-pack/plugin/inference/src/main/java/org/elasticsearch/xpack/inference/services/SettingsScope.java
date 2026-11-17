/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import java.util.Locale;

/**
 * Represents the scope of a settings block, used to produce structured error messages such as
 * {@code [service_settings] does not contain the required setting [model_id]}.
 */
public enum SettingsScope {
    SERVICE_SETTINGS,
    TASK_SETTINGS,
    SECRET_SETTINGS,
    INPUT_TYPE_TRANSLATOR,
    RATE_LIMIT;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
