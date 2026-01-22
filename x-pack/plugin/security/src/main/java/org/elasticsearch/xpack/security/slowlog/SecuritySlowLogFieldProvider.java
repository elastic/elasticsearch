/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.slowlog;

import org.elasticsearch.index.SlowLogContext;
import org.elasticsearch.index.SlowLogFieldProvider;
import org.elasticsearch.index.SlowLogFields;
import org.elasticsearch.xpack.security.Security;

import java.util.Map;

public class SecuritySlowLogFieldProvider implements SlowLogFieldProvider {
    private final Security plugin;

    private class SecuritySlowLogFields extends SlowLogFields {
        SecuritySlowLogFields(SlowLogContext context) {
            super(context);
        }

        @Override
        public Map<String, String> logFields() {
            if (context.includeUserInformation()) {
                return plugin.getAuthContextForSlowLog();
            }
            return Map.of();
        }

    }

    public SecuritySlowLogFieldProvider() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public SecuritySlowLogFieldProvider(Security plugin) {
        this.plugin = plugin;
    }

    @Override
    public SlowLogFields create(SlowLogContext context) {
        return new SecuritySlowLogFields(context);
    }
}
