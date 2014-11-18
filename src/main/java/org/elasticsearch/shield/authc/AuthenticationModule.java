/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.active_directory.ActiveDirectoryModule;
import org.elasticsearch.shield.authc.esusers.ESUsersModule;
import org.elasticsearch.shield.authc.ldap.LdapModule;
import org.elasticsearch.shield.authc.system.SystemRealm;
import org.elasticsearch.shield.support.AbstractShieldModule;

/**
 *
 */
public class AuthenticationModule extends AbstractShieldModule.Node.Spawn {

    public AuthenticationModule(Settings settings) {
        super(settings);
    }

    @Override
    public Iterable<? extends Node> spawnModules() {
        return ImmutableList.of(
                new SystemRealm.Module(settings),
                new ESUsersModule(settings),
                new LdapModule(settings),
                new ActiveDirectoryModule(settings)
        );
    }

    @Override
    protected void configureNode() {
        bind(AuthenticationService.class).to(InternalAuthenticationService.class).asEagerSingleton();
    }
}
