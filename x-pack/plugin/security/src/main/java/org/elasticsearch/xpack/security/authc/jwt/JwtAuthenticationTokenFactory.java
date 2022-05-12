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
 * This factory exists to support the configurable {@link #ENDUSERID_CLAIMS_SETTING}.
 */
public class JwtAuthenticationTokenFactory {

    static final List<String> DEFAULT_ENDUSERID_CLAIMS = List.of("sub", "oid", "client_id", "appid", "azp");
    public static final Setting<List<String>> ENDUSERID_CLAIMS_SETTING = Setting.listSetting(
        "xpack.security.authc.jwt.enduserid_claims",
        DEFAULT_ENDUSERID_CLAIMS,
        Function.identity(),
        Setting.Property.NodeScope
    );

    public static Collection<Setting<?>> getSettings() {
        return List.of(ENDUSERID_CLAIMS_SETTING);
    }

    private final List<String> endUserIdClaimNames;

    public JwtAuthenticationTokenFactory(final Settings settings) {
        this(ENDUSERID_CLAIMS_SETTING.get(settings));
    }

    // Package protected for testing
    JwtAuthenticationTokenFactory(final List<String> endUserIdClaimNames) {
        this.endUserIdClaimNames = endUserIdClaimNames;
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
        return new JwtAuthenticationToken(this.endUserIdClaimNames, authenticationParameterValue, clientAuthenticationSharedSecretValue);
    }
}
