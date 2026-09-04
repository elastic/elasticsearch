/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.logging;

import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.index.ActionLoggingFieldsContext;
import org.elasticsearch.index.ActionLoggingFieldsProvider;
import org.elasticsearch.xpack.security.Security;

import java.util.Map;

public class SecurityActionLoggingFieldsProvider implements ActionLoggingFieldsProvider {
    private final Security plugin;

    private class SecurityActionLoggingFields extends ActionLoggingFields {
        SecurityActionLoggingFields(ActionLoggingFieldsContext context) {
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

    public SecurityActionLoggingFieldsProvider() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public SecurityActionLoggingFieldsProvider(Security plugin) {
        this.plugin = plugin;
    }

    @Override
    public ActionLoggingFields create(ActionLoggingFieldsContext context) {
        return new SecurityActionLoggingFields(context);
    }
}
