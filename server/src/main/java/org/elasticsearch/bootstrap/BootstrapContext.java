/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.bootstrap;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

/**
 * Context that is passed to every bootstrap check to make decisions on.
 */
public class BootstrapContext {
    /**
     * The node's environment
     */
    private final Environment environment;

    /**
     * The node's local state metadata loaded on startup
     */
    private final Metadata metadata;

    public BootstrapContext(Environment environment, Metadata metadata) {
        this.environment = environment;
        this.metadata = metadata;
    }

    public Environment environment() {
        return environment;
    }

    public Settings settings() {
        return environment.settings();
    }

    public Metadata metadata() {
        return metadata;
    }
}
