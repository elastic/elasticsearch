/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers;

import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.support.AbstractShieldModule;

/**
 *
 */
public class ESUsersModule extends AbstractShieldModule.Node {

    private final boolean enabled;

    public ESUsersModule(Settings settings) {
        super(settings);
        enabled = enabled(settings);
    }

    @Override
    protected void configureNode() {
        if (enabled) {
            bind(ESUsersRealm.class).asEagerSingleton();
            bind(FileUserPasswdStore.class).asEagerSingleton();
            bind(FileUserRolesStore.class).asEagerSingleton();
        } else {
            bind(ESUsersRealm.class).toProvider(Providers.<ESUsersRealm>of(null));
        }
    }

    static boolean enabled(Settings settings) {
        return settings.getComponentSettings(ESUsersModule.class).getAsBoolean("enabled", true);
    }
}
