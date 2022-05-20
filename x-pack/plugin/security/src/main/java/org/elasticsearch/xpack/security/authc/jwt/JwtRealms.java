/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmsSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.InternalRealms;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Common settings shared by all JwtRealm instances.
 * Common token parsing code shared by all JwtRealm instances.
 * @see JwtRealm
 */
public class JwtRealms {

    private final List<String> principalClaimNames;
    private final List<JwtRealm> jwtRealms = new ArrayList<>();

    /**
     * Parse all xpack settings passed in from {@link InternalRealms#getFactories}
     * @param settings All xpack settings
     */
    public JwtRealms(final Settings settings) {
        this.principalClaimNames = Collections.unmodifiableList(JwtRealmsSettings.PRINCIPAL_CLAIMS_SETTING.get(settings));
    }

    /**
     * Return prioritized list of principal claim names to use for computing realm cache keys for all JWT realms.
     * @return Prioritized list of principal claim names (ex: sub, oid, client_id, azp, appid, client_id, email).
     */
    public List<String> getPrincipalClaimNames() {
        return this.principalClaimNames;
    }

    /**
     * Call order is request => JwtRealm.token() => JwtRealms.token()
     * @param threadContext Request headers and parameters
     * @return JwtAuthenticationToken containing mandatory JWT header, optional client secret, and a realm order cache key
     */
    AuthenticationToken token(final ThreadContext threadContext) {
        // extract value from Authorization header with Bearer scheme prefix
        final SecureString authenticationParameterValue = JwtUtil.getHeaderValue(
            threadContext,
            JwtRealm.HEADER_END_USER_AUTHENTICATION,
            JwtRealm.HEADER_END_USER_AUTHENTICATION_SCHEME,
            false
        );
        if (authenticationParameterValue == null) {
            return null;
        }
        // extract value from ES-Client-Authentication header with SharedSecret scheme prefix
        final SecureString clientAuthenticationSharedSecretValue = JwtUtil.getHeaderValue(
            threadContext,
            JwtRealm.HEADER_CLIENT_AUTHENTICATION,
            JwtRealm.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME,
            true
        );
        return new JwtAuthenticationToken(this.principalClaimNames, authenticationParameterValue, clientAuthenticationSharedSecretValue);
    }

    public JwtRealm createJwtRealm(
        final RealmConfig config,
        final SSLService sslService,
        final NativeRoleMappingStore nativeRoleMappingStore
    ) {
        return this.addRegisteredJwtRealm(new JwtRealm(config, this, sslService, nativeRoleMappingStore));
    }

    /**
     * Register a JWT realm.
     * @param jwtRealm JWT realm to be registered.
     * @return The added JwtRealm.
     */
    public JwtRealm addRegisteredJwtRealm(final JwtRealm jwtRealm) {
        this.jwtRealms.add(jwtRealm);
        return jwtRealm;
    }

    /**
     * Unregister a JWT realm.
     * @param jwtRealm JWT realm to be unregistered.
     * @return The removed JwtRealm.
     */
    public JwtRealm removeRegisteredJwtRealm(final JwtRealm jwtRealm) {
        this.jwtRealms.remove(jwtRealm);
        return jwtRealm;
    }

    /**
     * Unregister all JWT realms.
     * @return List of removed JwtRealm.
     */
    public List<JwtRealm> clearRegisteredJwtRealms() {
        final List<JwtRealm> jwtRealmList = this.listRegisteredJwtRealms();
        this.jwtRealms.clear();
        return jwtRealmList;
    }

    /**
     * List all register JWT realms.
     * @return List of JwtRealm.
     */
    public List<JwtRealm> listRegisteredJwtRealms() {
        return Collections.unmodifiableList(this.jwtRealms);
    }
}
