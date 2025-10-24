/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.reservedstate.ReservedProjectStateHandler;
import org.elasticsearch.reservedstate.ReservedStateHandlerProvider;

import java.util.Collection;

/**
 * Security Provider implementation for the {@link ReservedStateHandlerProvider} service interface
 */
public class ReservedSecurityStateHandlerProvider implements ReservedStateHandlerProvider {
    private final Security plugin;

    public ReservedSecurityStateHandlerProvider() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public ReservedSecurityStateHandlerProvider(Security plugin) {
        this.plugin = plugin;
    }

    @Override
    public Collection<ReservedProjectStateHandler<?>> projectHandlers() {
        return plugin.reservedProjectStateHandlers();
    }
}
