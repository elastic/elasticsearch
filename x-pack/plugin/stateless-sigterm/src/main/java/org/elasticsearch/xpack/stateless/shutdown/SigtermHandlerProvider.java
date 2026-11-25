/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.shutdown;

import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.node.internal.TerminationHandlerProvider;

/**
 * This is the class that's actually injected via SPI. Plumbing.
 */
public class SigtermHandlerProvider implements TerminationHandlerProvider {
    private final StatelessSigtermPlugin plugin;

    public SigtermHandlerProvider() {
        throw new IllegalStateException(this.getClass().getSimpleName() + " must be constructed using PluginsService");
    }

    public SigtermHandlerProvider(StatelessSigtermPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public TerminationHandler handler() {
        return this.plugin.getTerminationHandler();
    }
}
