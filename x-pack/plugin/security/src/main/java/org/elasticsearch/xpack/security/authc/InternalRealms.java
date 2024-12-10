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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SingleSpSamlRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealm;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.elasticsearch.xpack.security.authc.jwt.JwtRealm;
import org.elasticsearch.xpack.security.authc.kerberos.KerberosRealm;
import org.elasticsearch.xpack.security.authc.ldap.LdapRealm;
import org.elasticsearch.xpack.security.authc.oidc.OpenIdConnectRealm;
import org.elasticsearch.xpack.security.authc.pki.PkiRealm;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;
import org.elasticsearch.xpack.security.authc.saml.SingleSamlSpConfiguration;
import org.elasticsearch.xpack.security.authc.support.RoleMappingFileBootstrapCheck;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Provides a single entry point into dealing with all standard XPack security {@link Realm realms}.
 * This class does not handle extensions.
 *
 * @see Realms for the component that manages configured realms (including custom extension realms)
 */
public final class InternalRealms {

    static final String RESERVED_TYPE = ReservedRealm.TYPE;
    static final String NATIVE_TYPE = NativeRealmSettings.TYPE;
    static final String FILE_TYPE = FileRealmSettings.TYPE;
    static final String LDAP_TYPE = LdapRealmSettings.LDAP_TYPE;
    static final String AD_TYPE = LdapRealmSettings.AD_TYPE;
    static final String PKI_TYPE = PkiRealmSettings.TYPE;
    static final String SAML_TYPE = SingleSpSamlRealmSettings.TYPE;
    static final String OIDC_TYPE = OpenIdConnectRealmSettings.TYPE;
    static final String JWT_TYPE = JwtRealmSettings.TYPE;
    static final String KERBEROS_TYPE = KerberosRealmSettings.TYPE;

    private static final Set<String> BUILTIN_TYPES = Set.of(NATIVE_TYPE, FILE_TYPE);

    /**
     * The map of all <em>licensed</em> internal realm types to their licensed feature
     */
    private static final Map<String, LicensedFeature.Persistent> LICENSED_REALMS;
    static {
        Map<String, LicensedFeature.Persistent> realms = new HashMap<>();
        realms.put(AD_TYPE, Security.AD_REALM_FEATURE);
        realms.put(LDAP_TYPE, Security.LDAP_REALM_FEATURE);
        realms.put(PKI_TYPE, Security.PKI_REALM_FEATURE);
        realms.put(SAML_TYPE, Security.SAML_REALM_FEATURE);
        realms.put(KERBEROS_TYPE, Security.KERBEROS_REALM_FEATURE);
        realms.put(OIDC_TYPE, Security.OIDC_REALM_FEATURE);
        realms.put(JWT_TYPE, Security.JWT_REALM_FEATURE);
        LICENSED_REALMS = Map.copyOf(realms);
    }

    /**
     * The set of all <em>internal</em> realm types, excluding {@link ReservedRealm#TYPE}
     * @deprecated Use of this method (other than in tests) is discouraged.
     */
    @Deprecated
    public static Collection<String> getConfigurableRealmsTypes() {
        return Set.copyOf(Sets.union(BUILTIN_TYPES, LICENSED_REALMS.keySet()));
    }

    static boolean isInternalRealm(String type) {
        return RESERVED_TYPE.equals(type) || BUILTIN_TYPES.contains(type) || LICENSED_REALMS.containsKey(type);
    }

    static boolean isBuiltinRealm(String type) {
        return BUILTIN_TYPES.contains(type);
    }

    /**
     * @return The licensed feature for the given realm type, or {@code null} if the realm does not require a specific license type
     * @throws IllegalArgumentException if the provided type is not an {@link #isInternalRealm(String) internal realm}
     */
    @Nullable
    static LicensedFeature.Persistent getLicensedFeature(String type) {
        if (Strings.isNullOrEmpty(type)) {
            throw new IllegalArgumentException("Empty realm type [" + type + "]");
        }
        if (type.equals(RESERVED_TYPE) || isBuiltinRealm(type)) {
            return null;
        }
        final LicensedFeature.Persistent feature = LICENSED_REALMS.get(type);
        if (feature == null) {
            throw new IllegalArgumentException("Unsupported realm type [" + type + "]");
        }
        return feature;
    }

    /**
     * Creates {@link Realm.Factory factories} for each <em>internal</em> realm type.
     * This excludes the {@link ReservedRealm}, as it cannot be created dynamically.
     *
     * @return A map from <em>realm-type</em> to <code>Factory</code>
     */
    public static Map<String, Realm.Factory> getFactories(
        ThreadPool threadPool,
        Settings settings,
        ResourceWatcherService resourceWatcherService,
        SSLService sslService,
        NativeUsersStore nativeUsersStore,
        UserRoleMapper userRoleMapper,
        SecurityIndexManager securityIndex
    ) {
        return Map.of(
            // file realm
            FileRealmSettings.TYPE,
            config -> new FileRealm(config, resourceWatcherService, threadPool),
            // native realm
            NativeRealmSettings.TYPE,
            config -> buildNativeRealm(threadPool, settings, nativeUsersStore, securityIndex, config),
            // active directory realm
            LdapRealmSettings.AD_TYPE,
            config -> new LdapRealm(config, sslService, resourceWatcherService, userRoleMapper, threadPool),
            // LDAP realm
            LdapRealmSettings.LDAP_TYPE,
            config -> new LdapRealm(config, sslService, resourceWatcherService, userRoleMapper, threadPool),
            // PKI realm
            PkiRealmSettings.TYPE,
            config -> new PkiRealm(config, resourceWatcherService, userRoleMapper),
            // SAML realm
            SingleSpSamlRealmSettings.TYPE,
            config -> SamlRealm.create(
                config,
                sslService,
                resourceWatcherService,
                userRoleMapper,
                SingleSamlSpConfiguration.create(config)
            ),
            // Kerberos realm
            KerberosRealmSettings.TYPE,
            config -> new KerberosRealm(config, userRoleMapper, threadPool),
            // OpenID Connect realm
            OpenIdConnectRealmSettings.TYPE,
            config -> new OpenIdConnectRealm(config, sslService, userRoleMapper, resourceWatcherService),
            // JWT realm
            JwtRealmSettings.TYPE,
            config -> new JwtRealm(config, sslService, userRoleMapper)
        );
    }

    private static NativeRealm buildNativeRealm(
        ThreadPool threadPool,
        Settings settings,
        NativeUsersStore nativeUsersStore,
        SecurityIndexManager securityIndex,
        RealmConfig config
    ) {
        if (settings.getAsBoolean(NativeRealmSettings.NATIVE_USERS_ENABLED, true) == false) {
            throw new IllegalArgumentException(
                "Cannot configure a ["
                    + NativeRealmSettings.TYPE
                    + "] realm when ["
                    + NativeRealmSettings.NATIVE_USERS_ENABLED
                    + "] is false"
            );
        }
        final NativeRealm nativeRealm = new NativeRealm(config, nativeUsersStore, threadPool);
        securityIndex.addStateListener(nativeRealm::onSecurityIndexStateChange);
        return nativeRealm;
    }

    private InternalRealms() {}

    public static List<BootstrapCheck> getBootstrapChecks(final Settings globalSettings, final Environment env) {
        final Set<String> realmTypes = Sets.newHashSet(LdapRealmSettings.AD_TYPE, LdapRealmSettings.LDAP_TYPE, PkiRealmSettings.TYPE);
        return RealmSettings.getRealmSettings(globalSettings)
            .keySet()
            .stream()
            .filter(id -> realmTypes.contains(id.getType()))
            .map(id -> new RealmConfig(id, globalSettings, env, null))
            .map(RoleMappingFileBootstrapCheck::create)
            .filter(Objects::nonNull)
            .toList();
    }

}
