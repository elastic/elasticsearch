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
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmsServiceSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.InternalRealms;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import java.util.Collections;
import java.util.List;

/**
 * Parse common settings shared by all JwtRealm instances on behalf of InternalRealms.
 * Construct JwtRealm instances on behalf of lambda defined in InternalRealms.
 * Construct AuthenticationToken instances on behalf of JwtRealm instances.
 * @see InternalRealms
 * @see JwtRealm
 */
public class JwtRealmsService {

    private final List<String> principalClaimNames;

    /**
     * Parse all xpack settings passed in from {@link InternalRealms#getFactories}
     * @param settings All xpack settings
     */
    public JwtRealmsService(final Settings settings) {
        this.principalClaimNames = Collections.unmodifiableList(JwtRealmsServiceSettings.PRINCIPAL_CLAIMS_SETTING.get(settings));
    }

    /**
     * Return prioritized list of principal claim names to use for computing realm cache keys for all JWT realms.
     * @return Prioritized list of principal claim names (ex: sub, oid, client_id, azp, appid, client_id, email).
     */
    public List<String> getPrincipalClaimNames() {
        return this.principalClaimNames;
    }

    /**
     * Construct JwtRealm instance using settings passed in via lambda defined in {@link InternalRealms#getFactories}
     * @param config Realm config
     * @param sslService SSL service settings
     * @param nativeRoleMappingStore Native role mapping store
     */
    public JwtRealm createJwtRealm(
        final RealmConfig config,
        final SSLService sslService,
        final NativeRoleMappingStore nativeRoleMappingStore
    ) {
        return new JwtRealm(config, this, sslService, nativeRoleMappingStore);
    }

    /**
     * Construct JwtAuthenticationToken instance using request passed in via JwtRealm.token.
     * @param threadContext Request headers and parameters
     * @return JwtAuthenticationToken contains mandatory JWT header, optional client secret, and a realm order cache key
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
}
