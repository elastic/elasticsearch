/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.common.logging.action.ActionLogger.SEARCH_ACTION_LOGGER_ENABLED;
import static org.elasticsearch.test.ESIntegTestCase.updateClusterSettings;

public class ActionLoggingUtils {

    private static final String[] loggers = new String[] { "search", "sql", "esql", "eql" };

    public static void enableLoggers() {
        var builder = Settings.builder();
        for (String logger : loggers) {
            builder.put(SEARCH_ACTION_LOGGER_ENABLED.getConcreteSettingForNamespace(logger).getKey(), true);
        }
        updateClusterSettings(builder);
    }

    public static void disableLoggers() {
        var builder = Settings.builder();
        for (String logger : loggers) {
            builder.put(SEARCH_ACTION_LOGGER_ENABLED.getConcreteSettingForNamespace(logger).getKey(), (String) null);
        }
        updateClusterSettings(builder);
    }
}
