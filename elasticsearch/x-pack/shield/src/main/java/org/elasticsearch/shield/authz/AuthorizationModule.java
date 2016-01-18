/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.esnative.ESNativeUsersStore;
import org.elasticsearch.shield.authz.esnative.ESNativeRolesStore;
import org.elasticsearch.shield.authz.store.CompositeRolesStore;
import org.elasticsearch.shield.authz.store.FileRolesStore;
import org.elasticsearch.shield.authz.store.RolesStore;
import org.elasticsearch.shield.support.AbstractShieldModule;

import java.util.HashSet;
import java.util.Set;

/**
 * Module used to bind various classes necessary for authorization
 */
public class AuthorizationModule extends AbstractShieldModule.Node {

    private final Set<Permission.Global.Role> reservedRoles = new HashSet<>();

    public AuthorizationModule(Settings settings) {
        super(settings);
    }

    public void registerReservedRole(Permission.Global.Role role) {
        reservedRoles.add(role);
    }

    @Override
    protected void configureNode() {

        Multibinder<Permission.Global.Role> reservedRolesBinder = Multibinder.newSetBinder(binder(), Permission.Global.Role.class);
        for (Permission.Global.Role reservedRole : reservedRoles) {
            reservedRolesBinder.addBinding().toInstance(reservedRole);
        }

        // First the file and native roles stores must be bound...
        bind(FileRolesStore.class).asEagerSingleton();
        bind(ESNativeRolesStore.class).asEagerSingleton();
        // Then the composite roles store (which combines both) can be bound
        bind(RolesStore.class).to(CompositeRolesStore.class).asEagerSingleton();
        bind(ESNativeUsersStore.class).asEagerSingleton();
        bind(AuthorizationService.class).to(InternalAuthorizationService.class).asEagerSingleton();
    }

}
