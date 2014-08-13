/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.audit.AuditTrailModule;
import org.elasticsearch.shield.authc.AuthenticationModule;
import org.elasticsearch.shield.authz.AuthorizationModule;
import org.elasticsearch.shield.n2n.N2NModule;
import org.elasticsearch.shield.transport.SecuredTransportModule;

/**
 *
 */
public class SecurityModule extends AbstractModule implements SpawnModules, PreProcessModule {

    private final Settings settings;

    public SecurityModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    public void processModule(Module module) {
        if (module instanceof ActionModule) {
            ((ActionModule) module).registerFilter(SecurityFilter.Action.class);
        }
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
                new AuthenticationModule(settings),
                new AuthorizationModule(),
                new AuditTrailModule(settings),
                new N2NModule(),
                new SecuredTransportModule(settings));
    }

    @Override
    protected void configure() {
    }
}
