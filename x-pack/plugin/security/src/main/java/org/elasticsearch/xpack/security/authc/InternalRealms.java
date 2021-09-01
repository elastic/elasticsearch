/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealm;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.elasticsearch.xpack.security.authc.kerberos.KerberosRealm;
import org.elasticsearch.xpack.security.authc.ldap.LdapRealm;
import org.elasticsearch.xpack.security.authc.oidc.OpenIdConnectRealm;
import org.elasticsearch.xpack.security.authc.pki.PkiRealm;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;
import org.elasticsearch.xpack.security.authc.support.RoleMappingFileBootstrapCheck;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides a single entry point into dealing with all standard XPack security {@link Realm realms}.
 * This class does not handle extensions.
 *
 * @see Realms for the component that manages configured realms (including custom extension realms)
 */
public final class InternalRealms {

    /**
     * The list of all <em>internal</em> realm types, excluding {@link ReservedRealm#TYPE}.
     */
    private static final Set<String> XPACK_TYPES = Collections
        .unmodifiableSet(Sets.newHashSet(NativeRealmSettings.TYPE, FileRealmSettings.TYPE, LdapRealmSettings.AD_TYPE,
            LdapRealmSettings.LDAP_TYPE, PkiRealmSettings.TYPE, SamlRealmSettings.TYPE, KerberosRealmSettings.TYPE,
            OpenIdConnectRealmSettings.TYPE));

    /**
     * The list of all standard realm types, which are those provided by x-pack and do not have extensive
     * interaction with third party sources
     */
    private static final Set<String> STANDARD_TYPES = Collections.unmodifiableSet(Sets.newHashSet(NativeRealmSettings.TYPE,
        FileRealmSettings.TYPE, LdapRealmSettings.AD_TYPE, LdapRealmSettings.LDAP_TYPE, PkiRealmSettings.TYPE));

    public static Collection<String> getConfigurableRealmsTypes() {
        return Collections.unmodifiableSet(XPACK_TYPES);
    }

    /**
     * Determines whether <code>type</code> is an internal realm-type that is provided by x-pack,
     * excluding the {@link ReservedRealm} and realms that have extensive interaction with
     * third party sources
     */
    static boolean isStandardRealm(String type) {
        return STANDARD_TYPES.contains(type);
    }

    static boolean isBuiltinRealm(String type) {
        return FileRealmSettings.TYPE.equals(type) || NativeRealmSettings.TYPE.equals(type);
    }

    /**
     * Creates {@link Realm.Factory factories} for each <em>internal</em> realm type.
     * This excludes the {@link ReservedRealm}, as it cannot be created dynamically.
     *
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
            securityIndex.addStateListener(nativeRealm::onSecurityIndexStateChange);
            return nativeRealm;
        });
        map.put(LdapRealmSettings.AD_TYPE, config -> new LdapRealm(config, sslService,
            resourceWatcherService, nativeRoleMappingStore, threadPool));
        map.put(LdapRealmSettings.LDAP_TYPE, config -> new LdapRealm(config,
            sslService, resourceWatcherService, nativeRoleMappingStore, threadPool));
        map.put(PkiRealmSettings.TYPE, config -> new PkiRealm(config, resourceWatcherService, nativeRoleMappingStore));
        map.put(SamlRealmSettings.TYPE, config -> SamlRealm.create(config, sslService, resourceWatcherService, nativeRoleMappingStore));
        map.put(KerberosRealmSettings.TYPE, config -> new KerberosRealm(config, nativeRoleMappingStore, threadPool));
        map.put(OpenIdConnectRealmSettings.TYPE, config -> new OpenIdConnectRealm(config, sslService, nativeRoleMappingStore,
            resourceWatcherService));
        return Collections.unmodifiableMap(map);
    }

    private InternalRealms() {
    }

    public static List<BootstrapCheck> getBootstrapChecks(final Settings globalSettings, final Environment env) {
        final Set<String> realmTypes = Sets.newHashSet(LdapRealmSettings.AD_TYPE, LdapRealmSettings.LDAP_TYPE, PkiRealmSettings.TYPE);
        final List<BootstrapCheck> checks = RealmSettings.getRealmSettings(globalSettings)
            .keySet().stream()
            .filter(id -> realmTypes.contains(id.getType()))
            .map(id -> new RealmConfig(id, globalSettings, env, null))
            .map(RoleMappingFileBootstrapCheck::create)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        return checks;
    }

}
