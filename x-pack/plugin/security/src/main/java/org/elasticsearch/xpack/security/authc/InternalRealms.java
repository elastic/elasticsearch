/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealm;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.elasticsearch.xpack.security.authc.ldap.LdapRealm;
import org.elasticsearch.xpack.security.authc.pki.PkiRealm;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;
import org.elasticsearch.xpack.security.authc.support.RoleMappingFileBootstrapCheck;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provides a single entry point into dealing with all standard XPack security {@link Realm realms}.
 * This class does not handle extensions.
 * @see Realms for the component that manages configured realms (including custom extension realms)
 */
public final class InternalRealms {

    /**
     * The list of all <em>internal</em> realm types, excluding {@link ReservedRealm#TYPE}.
     */
    private static final Set<String> XPACK_TYPES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            NativeRealmSettings.TYPE, FileRealmSettings.TYPE, LdapRealmSettings.AD_TYPE, LdapRealmSettings.LDAP_TYPE, PkiRealmSettings.TYPE,
            SamlRealmSettings.TYPE
    )));

    /**
     * The list of all standard realm types, which are those provided by x-pack and do not have extensive
     * interaction with third party sources
     */
    private static final Set<String> STANDARD_TYPES =
            Collections.unmodifiableSet(Sets.difference(XPACK_TYPES, Collections.singleton(SamlRealmSettings.TYPE)));

    /**
     * Determines whether <code>type</code> is an internal realm-type that is provided by x-pack,
     * including the {@link ReservedRealm}
     */
    static boolean isXPackRealm(String type) {
        if (XPACK_TYPES.contains(type)) {
            return true;
        }
        return ReservedRealm.TYPE.equals(type);
    }

    /**
     * Determines whether <code>type</code> is an internal realm-type that is provided by x-pack,
     * excluding the {@link ReservedRealm} and realms that have extensive interaction with
     * third party sources
     */
    static boolean isStandardRealm(String type) {
        return STANDARD_TYPES.contains(type);
    }

    /**
     * Creates {@link Realm.Factory factories} for each <em>internal</em> realm type.
     * This excludes the {@link ReservedRealm}, as it cannot be created dynamically.
     * @return A map from <em>realm-type</em> to <code>Factory</code>
     */
    public static Map<String, Realm.Factory> getFactories(ThreadPool threadPool, ResourceWatcherService resourceWatcherService,
                                                          SSLService sslService, NativeUsersStore nativeUsersStore,
                                                          NativeRoleMappingStore nativeRoleMappingStore,
                                                          SecurityIndexManager securityIndex) {

        Map<String, Realm.Factory> map = new HashMap<>();
        map.put(FileRealmSettings.TYPE, config -> new FileRealm(config, resourceWatcherService, threadPool));
        map.put(NativeRealmSettings.TYPE, config -> {
            final NativeRealm nativeRealm = new NativeRealm(config, nativeUsersStore, threadPool);
            securityIndex.addIndexStateListener(nativeRealm::onSecurityIndexStateChange);
            return nativeRealm;
        });
        map.put(LdapRealmSettings.AD_TYPE, config -> new LdapRealm(LdapRealmSettings.AD_TYPE, config, sslService,
                resourceWatcherService, nativeRoleMappingStore, threadPool));
        map.put(LdapRealmSettings.LDAP_TYPE, config -> new LdapRealm(LdapRealmSettings.LDAP_TYPE, config,
                sslService, resourceWatcherService, nativeRoleMappingStore, threadPool));
        map.put(PkiRealmSettings.TYPE, config -> new PkiRealm(config, resourceWatcherService, nativeRoleMappingStore));
        map.put(SamlRealmSettings.TYPE, config -> SamlRealm.create(config, sslService, resourceWatcherService, nativeRoleMappingStore));
        return Collections.unmodifiableMap(map);
    }

    private InternalRealms() {
    }

    public static List<BootstrapCheck> getBootstrapChecks(final Settings globalSettings, final Environment env) {
        final List<BootstrapCheck> checks = new ArrayList<>();
        final Map<String, Settings> settingsByRealm = RealmSettings.getRealmSettings(globalSettings);
        settingsByRealm.forEach((name, settings) -> {
            final RealmConfig realmConfig = new RealmConfig(name, settings, globalSettings, env, null);
            switch (realmConfig.type()) {
                case LdapRealmSettings.AD_TYPE:
                case LdapRealmSettings.LDAP_TYPE:
                case PkiRealmSettings.TYPE:
                    final BootstrapCheck check = RoleMappingFileBootstrapCheck.create(realmConfig);
                    if (check != null) {
                        checks.add(check);
                    }
            }
        });
        return checks;
    }
}
