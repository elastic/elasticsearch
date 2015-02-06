/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.activedirectory.ActiveDirectoryRealm;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;
import org.elasticsearch.shield.authc.ldap.LdapRealm;
import org.elasticsearch.shield.support.AbstractShieldModule;

/**
 *
 */
public class AuthenticationModule extends AbstractShieldModule.Node {

    public AuthenticationModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configureNode() {
        MapBinder<String, Realm.Factory> mapBinder = MapBinder.newMapBinder(binder(), String.class, Realm.Factory.class);
        mapBinder.addBinding(ESUsersRealm.TYPE).to(ESUsersRealm.Factory.class).asEagerSingleton();
        mapBinder.addBinding(ActiveDirectoryRealm.TYPE).to(ActiveDirectoryRealm.Factory.class).asEagerSingleton();
        mapBinder.addBinding(LdapRealm.TYPE).to(LdapRealm.Factory.class).asEagerSingleton();

        bind(Realms.class).asEagerSingleton();
        bind(AuthenticationService.class).to(InternalAuthenticationService.class).asEagerSingleton();
    }
}
