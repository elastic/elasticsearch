/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.logging;

import org.elasticsearch.index.LoggingFieldContext;
import org.elasticsearch.index.LoggingFields;
import org.elasticsearch.index.LoggingFieldsProvider;
import org.elasticsearch.xpack.security.Security;

import java.util.Map;

public class SecurityLoggingFieldsProvider implements LoggingFieldsProvider {
    private final Security plugin;

    private class SecurityLoggingFields extends LoggingFields {
        SecurityLoggingFields(LoggingFieldContext context) {
            super(context);
        }

        @Override
        public Map<String, String> logFields() {
            if (context.includeUserInformation()) {
                return plugin.getAuthContextForLogging();
            }
            return Map.of();
        }

    }

    public SecurityLoggingFieldsProvider() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public SecurityLoggingFieldsProvider(Security plugin) {
        this.plugin = plugin;
    }

    @Override
    public LoggingFields create(LoggingFieldContext context) {
        return new SecurityLoggingFields(context);
    }
}
