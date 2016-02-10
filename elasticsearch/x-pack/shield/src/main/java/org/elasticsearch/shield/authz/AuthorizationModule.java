/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authz.store.CompositeRolesStore;
import org.elasticsearch.shield.authz.store.FileRolesStore;
import org.elasticsearch.shield.authz.store.NativeRolesStore;
import org.elasticsearch.shield.authz.store.ReservedRolesStore;
import org.elasticsearch.shield.authz.store.RolesStore;
import org.elasticsearch.shield.support.AbstractShieldModule;

/**
 * Module used to bind various classes necessary for authorization
 */
public class AuthorizationModule extends AbstractShieldModule.Node {

    public AuthorizationModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configureNode() {

        // First the file and native roles stores must be bound...
        bind(ReservedRolesStore.class).asEagerSingleton();
        bind(FileRolesStore.class).asEagerSingleton();
        bind(NativeRolesStore.class).asEagerSingleton();
        // Then the composite roles store (which combines both) can be bound
        bind(RolesStore.class).to(CompositeRolesStore.class).asEagerSingleton();
        bind(AuthorizationService.class).to(InternalAuthorizationService.class).asEagerSingleton();
    }

}
