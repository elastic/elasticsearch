/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.Modules;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.audit.AuditTrailModule;
import org.elasticsearch.shield.authc.AuthenticationModule;
import org.elasticsearch.shield.authz.AuthorizationModule;
import org.elasticsearch.shield.n2n.N2NModule;

/**
 *
 */
public class SecurityModule extends AbstractModule implements SpawnModules {

    private final Settings settings;

    public SecurityModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Iterable<? extends Module> spawnModules() {

        // don't spawn module in client mode
        if (settings.getAsBoolean("node.client", false)) {
            return ImmutableList.of();
        }

        // don't spawn modules if shield is explicitly disabled
        if (!settings.getComponentSettings(SecurityModule.class).getAsBoolean("enabled", true)) {
            return ImmutableList.of();
        }

        return ImmutableList.of(
                Modules.createModule(AuthenticationModule.class, settings),
                Modules.createModule(AuthorizationModule.class, settings),
                Modules.createModule(AuditTrailModule.class, settings),
                Modules.createModule(N2NModule.class, settings));
    }

    @Override
    protected void configure() {
    }
}
