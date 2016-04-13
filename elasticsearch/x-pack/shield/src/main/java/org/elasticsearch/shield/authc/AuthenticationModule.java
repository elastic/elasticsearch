/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.activedirectory.ActiveDirectoryRealm;
import org.elasticsearch.shield.authc.esnative.NativeRealm;
import org.elasticsearch.shield.authc.esnative.NativeUsersStore;
import org.elasticsearch.shield.authc.file.FileRealm;
import org.elasticsearch.shield.authc.esnative.ReservedRealm;
import org.elasticsearch.shield.authc.ldap.LdapRealm;
import org.elasticsearch.shield.authc.pki.PkiRealm;
import org.elasticsearch.shield.support.AbstractShieldModule;
import org.elasticsearch.shield.user.AnonymousUser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 */
public class AuthenticationModule extends AbstractShieldModule.Node {

    static final List<String> INTERNAL_REALM_TYPES =
            Arrays.asList(ReservedRealm.TYPE, NativeRealm.TYPE, FileRealm.TYPE, ActiveDirectoryRealm.TYPE, LdapRealm.TYPE, PkiRealm.TYPE);

    private final Map<String, Class<? extends Realm.Factory<? extends Realm<? extends AuthenticationToken>>>> customRealms =
            new HashMap<>();

    private Class<? extends AuthenticationFailureHandler> authcFailureHandler = null;

    public AuthenticationModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configureNode() {
        AnonymousUser.initialize(settings);
        MapBinder<String, Realm.Factory> mapBinder = MapBinder.newMapBinder(binder(), String.class, Realm.Factory.class);
        mapBinder.addBinding(FileRealm.TYPE).to(FileRealm.Factory.class).asEagerSingleton();
        mapBinder.addBinding(NativeRealm.TYPE).to(NativeRealm.Factory.class).asEagerSingleton();
        mapBinder.addBinding(ActiveDirectoryRealm.TYPE).to(ActiveDirectoryRealm.Factory.class).asEagerSingleton();
        mapBinder.addBinding(LdapRealm.TYPE).to(LdapRealm.Factory.class).asEagerSingleton();
        mapBinder.addBinding(PkiRealm.TYPE).to(PkiRealm.Factory.class).asEagerSingleton();
        for (Entry<String, Class<? extends Realm.Factory<? extends Realm<? extends AuthenticationToken>>>> entry :
                customRealms.entrySet()) {
            mapBinder.addBinding(entry.getKey()).to(entry.getValue()).asEagerSingleton();
        }

        bind(ReservedRealm.class).asEagerSingleton();
        bind(Realms.class).asEagerSingleton();
        if (authcFailureHandler == null) {
            bind(AuthenticationFailureHandler.class).to(DefaultAuthenticationFailureHandler.class).asEagerSingleton();
        } else {
            bind(AuthenticationFailureHandler.class).to(authcFailureHandler).asEagerSingleton();
        }
        bind(NativeUsersStore.class).asEagerSingleton();
        bind(AuthenticationService.class).to(InternalAuthenticationService.class).asEagerSingleton();
    }

    /**
     * Registers a custom realm type and factory for use as a authentication realm
     *
     * @param type  the type of the realm that identifies it. Must not be null, empty, or the same value as one of the built-in realms
     * @param clazz the factory class that is used to create this type of realm. Must not be null
     */
    public void addCustomRealm(String type, Class<? extends Realm.Factory<? extends Realm<? extends AuthenticationToken>>> clazz) {
        if (type == null || type.isEmpty()) {
            throw new IllegalArgumentException("type must not be null or empty");
        } else if (clazz == null) {
            throw new IllegalArgumentException("realm factory class cannot be null");
        } else if (INTERNAL_REALM_TYPES.contains(type)) {
            throw new IllegalArgumentException("cannot redefine the type [" + type + "] with custom realm [" + clazz.getName() + "]");
        }
        customRealms.put(type, clazz);
    }

    /**
     * Sets the {@link AuthenticationFailureHandler} to the specified implementation
     */
    public void setAuthenticationFailureHandler(Class<? extends AuthenticationFailureHandler> clazz) {
        this.authcFailureHandler = clazz;
    }
}
