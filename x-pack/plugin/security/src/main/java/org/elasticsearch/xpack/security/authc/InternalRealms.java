/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.LicensedFeature;
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
import org.elasticsearch.xpack.security.Security;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides a single entry point into dealing with all standard XPack security {@link Realm realms}.
 * This class does not handle extensions.
 *
 * @see Realms for the component that manages configured realms (including custom extension realms)
 */
public final class InternalRealms {

    static final String NATIVE_TYPE = NativeRealmSettings.TYPE;
    static final String FILE_TYPE = FileRealmSettings.TYPE;
    static final String LDAP_TYPE = LdapRealmSettings.LDAP_TYPE;
    static final String AD_TYPE = LdapRealmSettings.AD_TYPE;
    static final String PKI_TYPE = PkiRealmSettings.TYPE;
    static final String SAML_TYPE = SamlRealmSettings.TYPE;
    static final String OIDC_TYPE = OpenIdConnectRealmSettings.TYPE;
    static final String KERBEROS_TYPE = KerberosRealmSettings.TYPE;

    /**
     * The map of all <em>internal</em> realm types, excluding {@link ReservedRealm#TYPE}, to their licensed feature (if any)
     */
    private static final Map<String, Optional<LicensedFeature.Persistent>> XPACK_TYPES = Map.ofEntries(
        Map.entry(NATIVE_TYPE, Optional.empty()),
        Map.entry(FILE_TYPE, Optional.empty()),
        Map.entry(AD_TYPE, Optional.of(Security.AD_REALM_FEATURE)),
        Map.entry(LDAP_TYPE, Optional.of(Security.LDAP_REALM_FEATURE)),
        Map.entry(PKI_TYPE, Optional.of(Security.PKI_REALM_FEATURE)),
        Map.entry(SAML_TYPE, Optional.of(Security.SAML_REALM_FEATURE)),
        Map.entry(KERBEROS_TYPE, Optional.of(Security.KERBEROS_REALM_FEATURE)),
        Map.entry(OIDC_TYPE, Optional.of(Security.OIDC_REALM_FEATURE))
    );

    /**
     * The list of all standard realm types, which are those provided by x-pack and do not have extensive
     * interaction with third party sources
     */
    private static final Set<String> STANDARD_TYPES = Set.of(NATIVE_TYPE, FILE_TYPE, AD_TYPE, LDAP_TYPE, PKI_TYPE);

    public static Collection<String> getConfigurableRealmsTypes() {
        return XPACK_TYPES.keySet();
    }

    static boolean isInternalRealm(String type) {
        return ReservedRealm.TYPE.equals(type) || XPACK_TYPES.containsKey(type);
    }

    /**
     * @return The licensed feature for the given realm type, or {@code null} if the realm does not require a specific license type
     * @throws IllegalArgumentException if the provided type is not an {@link #isInternalRealm(String) internal realm}
     */
    static LicensedFeature.Persistent getLicensedFeature(String type) {
        if (Strings.isNullOrEmpty(type)) {
            throw new IllegalArgumentException("Empty realm type [" + type + "]");
        }
        if (type.equals(ReservedRealm.TYPE)) {
            return null;
        }
        final Optional<LicensedFeature.Persistent> opt = XPACK_TYPES.get(type);
        if (opt == null) {
            throw new IllegalArgumentException("Unsupported realm type [" + type + "]");
        }
        return opt.orElse(null);
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

        return Map.of(
                // file realm
                FileRealmSettings.TYPE,
                config -> new FileRealm(config, resourceWatcherService, threadPool),
                // native realm
                NativeRealmSettings.TYPE,
                config -> {
                    final NativeRealm nativeRealm = new NativeRealm(config, nativeUsersStore, threadPool);
                    securityIndex.addStateListener(nativeRealm::onSecurityIndexStateChange);
                    return nativeRealm;
                },
                // active directory realm
                LdapRealmSettings.AD_TYPE,
                config -> new LdapRealm(config, sslService, resourceWatcherService, nativeRoleMappingStore, threadPool),
                // LDAP realm
                LdapRealmSettings.LDAP_TYPE,
                config -> new LdapRealm(config, sslService, resourceWatcherService, nativeRoleMappingStore, threadPool),
                // PKI realm
                PkiRealmSettings.TYPE,
                config -> new PkiRealm(config, resourceWatcherService, nativeRoleMappingStore),
                // SAML realm
                SamlRealmSettings.TYPE,
                config -> SamlRealm.create(config, sslService, resourceWatcherService, nativeRoleMappingStore),
                // Kerberos realm
                KerberosRealmSettings.TYPE,
                config -> new KerberosRealm(config, nativeRoleMappingStore, threadPool),
                // OpenID Connect realm
                OpenIdConnectRealmSettings.TYPE,
                config -> new OpenIdConnectRealm(config, sslService, nativeRoleMappingStore, resourceWatcherService));
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
