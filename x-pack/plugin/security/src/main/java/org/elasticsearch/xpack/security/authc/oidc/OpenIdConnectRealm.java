/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
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
import java.util.Base64;
import java.util.List;

import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_ISSUER;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_NAME;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_USERINFO_ENDPOINT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_CLIENT_ID;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_REDIRECT_URI;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_RESPONSE_TYPE;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_REQUESTED_SCOPES;

public class OpenIdConnectRealm extends Realm {

    public static final String CONTEXT_TOKEN_DATA = "_oidc_tokendata";
    private static final SecureRandom RANDOM_INSTANCE = new SecureRandom();
    private final OpenIdConnectProviderConfiguration opConfiguration;
    private final RelyingPartyConfiguration rpConfiguration;

    public OpenIdConnectRealm(RealmConfig config) {
        super(config);
        this.rpConfiguration = buildRelyingPartyConfiguration(config);
        this.opConfiguration = buildOpenIdConnectProviderConfiguration(config);
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

    private RelyingPartyConfiguration buildRelyingPartyConfiguration(RealmConfig config) {
        String redirectUri = require(config, RP_REDIRECT_URI);
        String clientId = require(config, RP_CLIENT_ID);
        String responseType = require(config, RP_RESPONSE_TYPE);
        if (responseType.equals("id_token") == false && responseType.equals("code") == false) {
            throw new SettingsException("The configuration setting [" + RealmSettings.getFullSettingKey(config, RP_RESPONSE_TYPE)
                + "] value can only be code or id_token");
        }
        List<String> requestedScopes = config.getSetting(RP_REQUESTED_SCOPES);

        return new RelyingPartyConfiguration(clientId, redirectUri, responseType, requestedScopes);
    }

    private OpenIdConnectProviderConfiguration buildOpenIdConnectProviderConfiguration(RealmConfig config) {
        String providerName = require(config, OP_NAME);
        String authorizationEndpoint = require(config, OP_AUTHORIZATION_ENDPOINT);
        String issuer = require(config, OP_ISSUER);
        String tokenEndpoint = config.getSetting(OP_TOKEN_ENDPOINT, () -> null);
        String userinfoEndpoint = config.getSetting(OP_USERINFO_ENDPOINT, () -> null);

        return new OpenIdConnectProviderConfiguration(providerName, issuer, authorizationEndpoint, tokenEndpoint, userinfoEndpoint);
    }

    static String require(RealmConfig config, Setting.AffixSetting<String> setting) {
        final String value = config.getSetting(setting);
        if (value.isEmpty()) {
            throw new SettingsException("The configuration setting [" + RealmSettings.getFullSettingKey(config, setting)
                + "] is required");
        }
        return value;
    }

    /**
     * Creates the URI for an OIDC Authentication Request from the realm configuration using URI Query String Serialization and
     * generates a state parameter and a nonce. It then returns the URI, state and nonce encapsulated in a
     * {@link OpenIdConnectPrepareAuthenticationResponse}
     *
     * @return an {@link OpenIdConnectPrepareAuthenticationResponse}
     */
    public OpenIdConnectPrepareAuthenticationResponse buildAuthenticationRequestUri() throws ElasticsearchException {
        try {
            final String state = createNonceValue();
            final String nonce = createNonceValue();
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
            return new OpenIdConnectPrepareAuthenticationResponse(builder.toString(), state, nonce);
        } catch (UnsupportedEncodingException e) {
            throw new ElasticsearchException("Cannot build OpenID Connect Authentication Request", e);
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
     * Creates a cryptographically secure alphanumeric string to be used as a nonce or state. It adheres to the
     * <a href="https://tools.ietf.org/html/rfc6749#section-10.10">specification's requirements</a> by using 180 bits for the random value.
     * The random string is encoded in a URL safe manner.
     *
     * @return an alphanumeric string
     */
    private static String createNonceValue() {
        final byte[] randomBytes = new byte[20];
        RANDOM_INSTANCE.nextBytes(randomBytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes);
    }
}
