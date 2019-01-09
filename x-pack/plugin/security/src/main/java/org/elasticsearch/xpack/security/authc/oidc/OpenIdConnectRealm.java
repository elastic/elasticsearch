/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationResponse;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_ISSUER;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_NAME;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_USERINFO_ENDPOINT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_ALLOWED_SCOPES;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_ALLOWED_SIGNATURE_ALGORITHMS;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_CLIENT_ID;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_REDIRECT_URI;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_RESPONSE_TYPE;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_REQUESTED_SCOPES;

public class OpenIdConnectRealm extends Realm implements Releasable {

    public static final String CONTEXT_TOKEN_DATA = "_oidc_tokendata";
    private static final SecureRandom RANDOM_INSTANCE = new SecureRandom();
    private static final Logger logger = LogManager.getLogger(OpenIdConnectRealm.class);
    private final OPConfiguration opConfiguration;
    private final RPConfiguration rpConfiguration;

    public OpenIdConnectRealm(RealmConfig config) {
        super(config);
        this.rpConfiguration = buildRPConfiguration(config);
        this.opConfiguration = buildOPConfiguration(config);
    }

    @Override
    public void close() {

    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return false;
    }

    @Override
    public AuthenticationToken token(ThreadContext context) {
        return null;
    }

    @Override
    public void authenticate(AuthenticationToken token, ActionListener<AuthenticationResult> listener) {

    }

    @Override
    public void lookupUser(String username, ActionListener<User> listener) {

    }

    private RPConfiguration buildRPConfiguration(RealmConfig config) {
        String redirectUri = require(config, RP_REDIRECT_URI);
        String clientId = require(config, RP_CLIENT_ID);
        String responseType = require(config, RP_RESPONSE_TYPE);
        List<String> requestedScopes = config.hasSetting(RP_REQUESTED_SCOPES) ?
            config.getSetting(RP_REQUESTED_SCOPES) : Collections.emptyList();
        List<String> allowedScopes = config.hasSetting(RP_ALLOWED_SCOPES) ?
            config.getSetting(RP_ALLOWED_SCOPES) : Collections.emptyList();
        List<String> allowedSignatureAlgorithms = requireListSetting(config, RP_ALLOWED_SIGNATURE_ALGORITHMS);

        return new RPConfiguration(clientId, redirectUri, responseType, allowedSignatureAlgorithms, requestedScopes, allowedScopes);
    }

    private OPConfiguration buildOPConfiguration(RealmConfig config) {
        String providerName = require(config, OP_NAME);
        String authorizationEndpoint = require(config, OP_AUTHORIZATION_ENDPOINT);
        String issuer = require(config, OP_ISSUER);
        String tokenEndpoint = config.getSetting(OP_TOKEN_ENDPOINT, () -> null);
        String userinfoEndpoint = config.getSetting(OP_USERINFO_ENDPOINT, () -> null);

        return new OPConfiguration(providerName, issuer, authorizationEndpoint, tokenEndpoint, userinfoEndpoint);
    }

    static String require(RealmConfig config, Setting.AffixSetting<String> setting) {
        final String value = config.getSetting(setting);
        if (value.isEmpty()) {
            throw new SettingsException("The configuration setting [" + RealmSettings.getFullSettingKey(config, setting)
                + "] is required");
        }
        return value;
    }

    static List<String> requireListSetting(RealmConfig config, Setting.AffixSetting<List<String>> setting) {
        final List<String> value = config.getSetting(setting);
        if (value.isEmpty()) {
            throw new SettingsException("The configuration setting [" + RealmSettings.getFullSettingKey(config, setting)
                + "] is required");
        }
        return value;
    }

    /**
     * Creates the URI for an OIDC Authentication Request from the realm configuration using URI Query String Serialization and possibly
     * generates a state parameter. It then returns the URI and state encapsulated in a {@link OpenIdConnectPrepareAuthenticationResponse}
     *
     * @param state The oAuth2 state parameter used for CSRF protection. If the facilitator doesn't supply one, we generate one ourselves
     * @param nonce String value used to associate a Client session with an ID Token, and to mitigate replay attacks. If the facilitator
     *              doesn't supply one, we don't set one for the authentication request
     * @return an {@link OpenIdConnectPrepareAuthenticationResponse}
     */
    public OpenIdConnectPrepareAuthenticationResponse buildAuthenticationRequestUri(@Nullable String state, @Nullable String nonce)
        throws ElasticsearchException {
        try {
            if (Strings.hasText(state) == false) {
                state = createNonceValue();
            }
            StringBuilder builder = new StringBuilder();
            builder.append(opConfiguration.getAuthorizationEndpoint());
            addParameter(builder, "response_type", rpConfiguration.getResponseType(), true);
            addParameter(builder, "scope", Strings.collectionToDelimitedString(rpConfiguration.getRequestedScopes(), " "));
            addParameter(builder, "client_id", rpConfiguration.getClientId());
            addParameter(builder, "state", state);
            if (Strings.hasText(nonce)) {
                addParameter(builder, "nonce", nonce);
            }
            addParameter(builder, "redirect_uri", rpConfiguration.getRedirectUri());
            return new OpenIdConnectPrepareAuthenticationResponse(builder.toString(), state);
        } catch (UnsupportedEncodingException e) {
            throw new ElasticsearchException("Cannot build OIDC Authentication Request", e);
        }
    }

    private void addParameter(StringBuilder builder, String parameter, String value, boolean isFirstParameter)
        throws UnsupportedEncodingException {
        char prefix = isFirstParameter ? '?' : '&';
        builder.append(prefix).append(parameter).append("=");
        builder.append(URLEncoder.encode(value, StandardCharsets.UTF_8.name()));
    }

    private void addParameter(StringBuilder builder, String parameter, String value) throws UnsupportedEncodingException {
        addParameter(builder, parameter, value, false);
    }

    /**
     * Creates a cryptographically secure alphanumeric string to be used as a nonce
     *
     * @return an alphanumeric string
     */
    private static String createNonceValue() {
        final byte[] randomBytes = new byte[16];
        RANDOM_INSTANCE.nextBytes(randomBytes);
        return MessageDigests.toHexString(randomBytes);
    }
}
