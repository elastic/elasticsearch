/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jwt.JWTClaimsSet;

import com.nimbusds.oauth2.sdk.ResponseType;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.openid.connect.sdk.AuthenticationRequest;
import com.nimbusds.openid.connect.sdk.Nonce;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationResponse;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.DN_CLAIM;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.GROUPS_CLAIM;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.MAIL_CLAIM;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.NAME_CLAIM;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_ISSUER;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_JWKSET_URL;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_NAME;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_USERINFO_ENDPOINT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.POPULATE_USER_METADATA;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.PRINCIPAL_CLAIM;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_CLIENT_ID;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_CLIENT_SECRET;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_REDIRECT_URI;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_RESPONSE_TYPE;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_REQUESTED_SCOPES;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_SIGNATURE_VERIFICATION_ALGORITHM;

public class OpenIdConnectRealm extends Realm {

    public static final String CONTEXT_TOKEN_DATA = "_oidc_tokendata";
    private final OpenIdConnectProviderConfiguration opConfiguration;
    private final RelyingPartyConfiguration rpConfiguration;
    private final OpenIdConnectAuthenticator openIdConnectAuthenticator;
    private final ClaimParser principalAttribute;
    private final ClaimParser groupsAttribute;
    private final ClaimParser dnAttribute;
    private final ClaimParser nameAttribute;
    private final ClaimParser mailAttribute;
    private final Boolean populateUserMetadata;
    private final UserRoleMapper roleMapper;


    public OpenIdConnectRealm(RealmConfig config, SSLService sslService, NativeRoleMappingStore roleMapper) {
        super(config);
        this.roleMapper = roleMapper;
        this.rpConfiguration = buildRelyingPartyConfiguration(config);
        this.opConfiguration = buildOpenIdConnectProviderConfiguration(config);
        this.openIdConnectAuthenticator = new OpenIdConnectAuthenticator(config, opConfiguration, rpConfiguration, sslService);
        this.principalAttribute = ClaimParser.forSetting(logger, PRINCIPAL_CLAIM, config, true);
        this.groupsAttribute = ClaimParser.forSetting(logger, GROUPS_CLAIM, config, false);
        this.dnAttribute = ClaimParser.forSetting(logger, DN_CLAIM, config, false);
        this.nameAttribute = ClaimParser.forSetting(logger, NAME_CLAIM, config, false);
        this.mailAttribute = ClaimParser.forSetting(logger, MAIL_CLAIM, config, false);
        this.populateUserMetadata = config.getSetting(POPULATE_USER_METADATA);
    }

    OpenIdConnectRealm(RealmConfig config) {
        super(config);
        this.roleMapper = null;
        this.rpConfiguration = buildRelyingPartyConfiguration(config);
        this.opConfiguration = buildOpenIdConnectProviderConfiguration(config);
        this.openIdConnectAuthenticator = new OpenIdConnectAuthenticator(config, opConfiguration, rpConfiguration, null);
        this.principalAttribute = ClaimParser.forSetting(logger, PRINCIPAL_CLAIM, config, true);
        this.groupsAttribute = ClaimParser.forSetting(logger, GROUPS_CLAIM, config, false);
        this.dnAttribute = ClaimParser.forSetting(logger, DN_CLAIM, config, false);
        this.nameAttribute = ClaimParser.forSetting(logger, NAME_CLAIM, config, false);
        this.mailAttribute = ClaimParser.forSetting(logger, MAIL_CLAIM, config, false);
        this.populateUserMetadata = config.getSetting(POPULATE_USER_METADATA);
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return token instanceof OpenIdConnectToken;
    }

    @Override
    public AuthenticationToken token(ThreadContext context) {
        return null;
    }

    @Override
    public void authenticate(AuthenticationToken token, ActionListener<AuthenticationResult> listener) {
        if (token instanceof OpenIdConnectToken) {
            OpenIdConnectToken oidcToken = (OpenIdConnectToken) token;
            JWTClaimsSet claims = openIdConnectAuthenticator.authenticate(oidcToken);
            buildUserFromClaims(claims, listener);
        } else {
            listener.onResponse(AuthenticationResult.notHandled());
        }

    }

    @Override
    public void lookupUser(String username, ActionListener<User> listener) {
        listener.onResponse(null);
    }


    private void buildUserFromClaims(JWTClaimsSet claims, ActionListener<AuthenticationResult> authResultListener) {
        final String principal = principalAttribute.getClaimValue(claims);
        if (Strings.isNullOrEmpty(principal)) {
            authResultListener.onResponse(AuthenticationResult.unsuccessful(
                principalAttribute + "not found in " + claims.toJSONObject(), null));
            return;
        }
        final Map<String, Object> userMetadata = new HashMap<>();
        if (populateUserMetadata) {
            Map<String, Object> claimsMap = claims.getClaims();
            /*
             * We whitelist the Types that we want to parse as metadata from the Claims, explicitly filtering out {@link Date}s
             */
            Set<Map.Entry> allowedEntries = claimsMap.entrySet().stream().filter(entry -> {
                Object v = entry.getValue();
                return (v instanceof String || v instanceof Boolean || v instanceof Number || v instanceof Collections);
            }).collect(Collectors.toSet());
            for (Map.Entry entry : allowedEntries) {
                userMetadata.put("oidc(" + entry.getKey() + ")", entry.getValue());
            }
        }
        final List<String> groups = groupsAttribute.getClaimValues(claims);
        final String dn = dnAttribute.getClaimValue(claims);
        final String mail = mailAttribute.getClaimValue(claims);
        final String name = nameAttribute.getClaimValue(claims);
        UserRoleMapper.UserData userData = new UserRoleMapper.UserData(principal, dn, groups, userMetadata, config);
        roleMapper.resolveRoles(userData, ActionListener.wrap(roles -> {
            final User user = new User(principal, roles.toArray(new String[0]), name, mail, userMetadata, true);
            authResultListener.onResponse(AuthenticationResult.success(user));
        }, authResultListener::onFailure));

    }


    private RelyingPartyConfiguration buildRelyingPartyConfiguration(RealmConfig config) {
        final String redirectUriString = require(config, RP_REDIRECT_URI);
        final URI redirectUri;
        try {
            redirectUri = new URI(redirectUriString);
        } catch (URISyntaxException e) {
            // This should never happen as it's already validated in the settings
            throw new SettingsException("Invalid URI:" + RP_REDIRECT_URI.getKey(), e);
        }
        final ClientID clientId = new ClientID(require(config, RP_CLIENT_ID));
        final SecureString clientSecret = config.getSetting(RP_CLIENT_SECRET);
        final ResponseType responseType = new ResponseType(require(config, RP_RESPONSE_TYPE));
        final Scope requestedScope = new Scope(config.getSetting(RP_REQUESTED_SCOPES).toArray(new String[0]));
        final JWSAlgorithm signatureVerificationAlgorithm = JWSAlgorithm.parse(require(config, RP_SIGNATURE_VERIFICATION_ALGORITHM));

        return new RelyingPartyConfiguration(clientId, clientSecret, redirectUri, responseType, requestedScope,
            signatureVerificationAlgorithm);
    }

    private OpenIdConnectProviderConfiguration buildOpenIdConnectProviderConfiguration(RealmConfig config) {
        String providerName = require(config, OP_NAME);
        Issuer issuer = new Issuer(require(config, OP_ISSUER));
        URL jwkSetUrl;
        try {
            jwkSetUrl = new URL(require(config, OP_JWKSET_URL));
        } catch (MalformedURLException e) {
            // This should never happen as it's already validated in the settings
            throw new SettingsException("Invalid URL: " + OP_JWKSET_URL.getKey(), e);
        }
        URI authorizationEndpoint;
        try {
            authorizationEndpoint = new URI(require(config, OP_AUTHORIZATION_ENDPOINT));
        } catch (URISyntaxException e) {
            // This should never happen as it's already validated in the settings
            throw new SettingsException("Invalid URI: " + OP_AUTHORIZATION_ENDPOINT.getKey(), e);
        }
        URI tokenEndpoint;
        try {
            tokenEndpoint = new URI(require(config, OP_TOKEN_ENDPOINT));
        } catch (URISyntaxException e) {
            // This should never happen as it's already validated in the settings
            throw new SettingsException("Invalid URL: " + OP_TOKEN_ENDPOINT.getKey(), e);
        }
        URI userinfoEndpoint;
        try {
            userinfoEndpoint = (config.getSetting(OP_USERINFO_ENDPOINT, () -> null) == null) ? null :
                new URI(config.getSetting(OP_USERINFO_ENDPOINT, () -> null));
        } catch (URISyntaxException e) {
            // This should never happen as it's already validated in the settings
            throw new SettingsException("Invalid URI: " + OP_USERINFO_ENDPOINT.getKey(), e);
        }


        return new OpenIdConnectProviderConfiguration(providerName, issuer, jwkSetUrl, authorizationEndpoint, tokenEndpoint,
            userinfoEndpoint);
    }

    private static String require(RealmConfig config, Setting.AffixSetting<String> setting) {
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
        final State state = new State();
        final Nonce nonce = new Nonce();
        final AuthenticationRequest authenticationRequest = new AuthenticationRequest(
            opConfiguration.getAuthorizationEndpoint(),
            rpConfiguration.getResponseType(),
            rpConfiguration.getRequestedScope(),
            rpConfiguration.getClientId(),
            rpConfiguration.getRedirectUri(),
            state,
            nonce);

        return new OpenIdConnectPrepareAuthenticationResponse(authenticationRequest.toURI().toString(),
            state.getValue(), nonce.getValue());
    }

    static final class ClaimParser {
        private final String name;
        private final Function<JWTClaimsSet, List<String>> parser;

        ClaimParser(String name, Function<JWTClaimsSet, List<String>> parser) {
            this.name = name;
            this.parser = parser;
        }

        List<String> getClaimValues(JWTClaimsSet claims) {
            return parser.apply(claims);
        }

        String getClaimValue(JWTClaimsSet claims) {
            List<String> claimValues = parser.apply(claims);
            if (claimValues == null || claimValues.isEmpty()) {
                return null;
            } else {
                return claimValues.get(0);
            }
        }

        @Override
        public String toString() {
            return name;
        }

        static ClaimParser forSetting(Logger logger, OpenIdConnectRealmSettings.ClaimSetting setting, RealmConfig realmConfig,
                                      boolean required) {

            if (realmConfig.hasSetting(setting.getClaim())) {
                String claimName = realmConfig.getSetting(setting.getClaim());
                if (realmConfig.hasSetting(setting.getPattern())) {
                    Pattern regex = Pattern.compile(realmConfig.getSetting(setting.getPattern()));
                    return new ClaimParser(
                        "OpenID Connect Claim [" + claimName + "] with pattern [" + regex.pattern() + "] for ["
                            + setting.name(realmConfig) + "]",
                        claims -> {
                            Object claimValueObject = claims.getClaim(claimName);
                            List<String> values;
                            if (claimValueObject == null) {
                                values = Collections.emptyList();
                            } else if (claimValueObject instanceof String) {
                                values = Collections.singletonList((String) claimValueObject);
                            } else if (claimValueObject instanceof List == false) {
                                throw new SettingsException("Setting [" + RealmSettings.getFullSettingKey(realmConfig, setting.getClaim())
                                    + " expects a claim with String or a String Array value but found a " + claimValueObject.getClass().getName());
                            } else {
                                values = (List<String>) claimValueObject;
                            }
                            return values.stream().map(s -> {
                                final Matcher matcher = regex.matcher(s);
                                if (matcher.find() == false) {
                                    logger.debug("OpenID Connect Claim [{}] is [{}], which does not match [{}]", claimName, s, regex.pattern());
                                    return null;
                                }
                                final String value = matcher.group(1);
                                if (Strings.isNullOrEmpty(value)) {
                                    logger.debug("OpenID Connect Claim [{}] is [{}], which does match [{}] but group(1) is empty",
                                        claimName, s, regex.pattern());
                                    return null;
                                }
                                return value;
                            }).filter(Objects::nonNull).collect(Collectors.toList());
                        });
                } else {
                    return new ClaimParser(
                        "OpenID Connect Claim [" + claimName + "] for [" + setting.name(realmConfig) + "]",
                        claims -> {
                            Object claimValueObject = claims.getClaim(claimName);
                            if (claimValueObject == null) {
                                return Collections.emptyList();
                            } else if (claimValueObject instanceof String) {
                                return Collections.singletonList((String) claimValueObject);
                            } else if (claimValueObject instanceof List == false) {
                                throw new SettingsException("Setting [" + RealmSettings.getFullSettingKey(realmConfig, setting.getClaim())
                                    + " expects a claim with String or a String Array value but found a " + claimValueObject.getClass().getName());
                            }
                            return (List<String>) claimValueObject;
                        });
                }
            } else if (required) {
                throw new SettingsException("Setting [" + RealmSettings.getFullSettingKey(realmConfig, setting.getClaim())
                    + "] is required");
            } else if (realmConfig.hasSetting(setting.getPattern())) {
                throw new SettingsException("Setting [" + RealmSettings.getFullSettingKey(realmConfig, setting.getPattern())
                    + "] cannot be set unless [" + RealmSettings.getFullSettingKey(realmConfig, setting.getClaim())
                    + "] is also set");
            } else {
                return new ClaimParser("No OpenID Connect Claim for [" + setting.name(realmConfig) + "]",
                    attributes -> Collections.emptyList());
            }
        }
    }

}

