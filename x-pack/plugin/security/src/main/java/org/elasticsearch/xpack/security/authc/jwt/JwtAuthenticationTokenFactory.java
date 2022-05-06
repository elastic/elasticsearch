/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

/**
 * A factory class for {@link JwtAuthenticationToken}.
 * This factory exists to support the configurable {@link #USERID_CLAIMS_SETTING}.
 */
public class JwtAuthenticationTokenFactory {

    static final List<String> DEFAULT_USERID_CLAIMS = List.of("sub", "client_id", "appid", "azp");
    public static final Setting<List<String>> USERID_CLAIMS_SETTING = Setting.listSetting(
        "xpack.security.authc.jwt.userid_claims",
        DEFAULT_USERID_CLAIMS,
        Function.identity(),
        Setting.Property.NodeScope
    );

    private final List<String> userIdClaims;

    public JwtAuthenticationTokenFactory(Settings settings) {
        this(USERID_CLAIMS_SETTING.get(settings));
    }

    // Package protected for testing
    JwtAuthenticationTokenFactory(List<String> userIdClaims) {
        this.userIdClaims = userIdClaims;
    }

    public static Collection<Setting<?>> getSettings() {
        return List.of(USERID_CLAIMS_SETTING);
    }

    public JwtAuthenticationToken getToken(final ThreadContext threadContext) {
        final SecureString authenticationParameterValue = JwtUtil.getHeaderValue(
            threadContext,
            JwtRealm.HEADER_END_USER_AUTHENTICATION,
            JwtRealm.HEADER_END_USER_AUTHENTICATION_SCHEME,
            false
        );
        if (authenticationParameterValue == null) {
            return null;
        }
        // Get all other possible parameters. A different JWT realm may do the actual authentication.
        final SecureString clientAuthenticationSharedSecretValue = JwtUtil.getHeaderValue(
            threadContext,
            JwtRealm.HEADER_CLIENT_AUTHENTICATION,
            JwtRealm.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME,
            true
        );
        return new JwtAuthenticationToken(authenticationParameterValue, userIdClaims, clientAuthenticationSharedSecretValue);
    }
}
