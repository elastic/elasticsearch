/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.action.authc.cache.ClearRealmCacheAction;
import org.elasticsearch.shield.action.authc.cache.TransportClearRealmCacheAction;
import org.elasticsearch.shield.support.AbstractShieldModule;

/**
 *
 */
public class ShieldActionModule extends AbstractShieldModule implements PreProcessModule {

    public ShieldActionModule(Settings settings) {
        super(settings);
    }

    @Override
    public void processModule(Module module) {
        if (module instanceof ActionModule) {

            // registering the security filter only for nodes
            if (!clientMode) {
                ((ActionModule) module).registerFilter(ShieldActionFilter.class);
            }

            // registering all shield actions
            ((ActionModule) module).registerAction(ClearRealmCacheAction.INSTANCE, TransportClearRealmCacheAction.class);
        }
    }

    @Override
    protected void configure(boolean clientMode) {
        if (!clientMode) {
            bind(ShieldActionMapper.class).asEagerSingleton();
            // we need to ensure that there's only a single instance of this filter.
            bind(ShieldActionFilter.class).asEagerSingleton();
        }
    }
}
