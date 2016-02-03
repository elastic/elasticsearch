/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.support.AbstractShieldModule;

/**
 *
 */
public class ShieldModule extends AbstractShieldModule {

    public ShieldModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configure(boolean clientMode) {
        if (!clientMode) {
            bind(SecurityContext.Secure.class).asEagerSingleton();
            bind(SecurityContext.class).to(SecurityContext.Secure.class);
            bind(ShieldLifecycleService.class).asEagerSingleton();
            bind(ShieldTemplateService.class).asEagerSingleton();
            bind(InternalClient.Secure.class).asEagerSingleton();
            bind(InternalClient.class).to(InternalClient.Secure.class);
        }
    }
}
