/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.ResponseType;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.openid.connect.sdk.AuthenticationRequest;
import com.nimbusds.openid.connect.sdk.LogoutRequest;
import com.nimbusds.openid.connect.sdk.Nonce;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectLogoutResponse;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationResponse;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.support.ClaimParser;
import org.elasticsearch.xpack.security.authc.support.DelegatedAuthorizationSupport;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.DN_CLAIM;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.GROUPS_CLAIM;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.MAIL_CLAIM;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.NAME_CLAIM;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_ENDSESSION_ENDPOINT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_ISSUER;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_JWKSET_PATH;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.OP_USERINFO_ENDPOINT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.POPULATE_USER_METADATA;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.PRINCIPAL_CLAIM;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_CLIENT_AUTH_JWT_SIGNATURE_ALGORITHM;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_CLIENT_AUTH_METHOD;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_CLIENT_ID;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_CLIENT_SECRET;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_POST_LOGOUT_REDIRECT_URI;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_REDIRECT_URI;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_REQUESTED_SCOPES;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_RESPONSE_TYPE;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.RP_SIGNATURE_ALGORITHM;

public class OpenIdConnectRealm extends Realm implements Releasable {

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

    private DelegatedAuthorizationSupport delegatedRealms;

    public OpenIdConnectRealm(RealmConfig config, SSLService sslService, UserRoleMapper roleMapper, ResourceWatcherService watcherService) {
        super(config);
        this.roleMapper = roleMapper;
        this.rpConfiguration = buildRelyingPartyConfiguration(config);
        this.opConfiguration = buildOpenIdConnectProviderConfiguration(config);
        this.principalAttribute = ClaimParser.forSetting(logger, PRINCIPAL_CLAIM, config, true);
        this.groupsAttribute = ClaimParser.forSetting(logger, GROUPS_CLAIM, config, false);
        this.dnAttribute = ClaimParser.forSetting(logger, DN_CLAIM, config, false);
        this.nameAttribute = ClaimParser.forSetting(logger, NAME_CLAIM, config, false);
        this.mailAttribute = ClaimParser.forSetting(logger, MAIL_CLAIM, config, false);
        this.populateUserMetadata = config.getSetting(POPULATE_USER_METADATA);
        if (TokenService.isTokenServiceEnabled(config.settings()) == false) {
            throw new IllegalStateException(
                "OpenID Connect Realm requires that the token service be enabled ("
                    + XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey()
                    + ")"
            );
        }
        this.openIdConnectAuthenticator = new OpenIdConnectAuthenticator(
            config,
            opConfiguration,
            rpConfiguration,
            sslService,
            watcherService
        );
    }

    // For testing
    OpenIdConnectRealm(RealmConfig config, OpenIdConnectAuthenticator authenticator, UserRoleMapper roleMapper) {
        super(config);
        this.roleMapper = roleMapper;
        this.rpConfiguration = buildRelyingPartyConfiguration(config);
        this.opConfiguration = buildOpenIdConnectProviderConfiguration(config);
        this.openIdConnectAuthenticator = authenticator;
        this.principalAttribute = ClaimParser.forSetting(logger, PRINCIPAL_CLAIM, config, true);
        this.groupsAttribute = ClaimParser.forSetting(logger, GROUPS_CLAIM, config, false);
        this.dnAttribute = ClaimParser.forSetting(logger, DN_CLAIM, config, false);
        this.nameAttribute = ClaimParser.forSetting(logger, NAME_CLAIM, config, false);
        this.mailAttribute = ClaimParser.forSetting(logger, MAIL_CLAIM, config, false);
        this.populateUserMetadata = config.getSetting(POPULATE_USER_METADATA);
    }

    @Override
    public void initialize(Iterable<Realm> realms, XPackLicenseState licenseState) {
        if (delegatedRealms != null) {
            throw new IllegalStateException("Realm has already been initialized");
        }
        delegatedRealms = new DelegatedAuthorizationSupport(realms, config, licenseState);
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return token instanceof OpenIdConnectToken;
    }

    private boolean isTokenForRealm(OpenIdConnectToken oidcToken) {
        if (oidcToken.getAuthenticatingRealm() == null) {
            return true;
        } else {
            return oidcToken.getAuthenticatingRealm().equals(this.name());
        }
    }

    @Override
    public AuthenticationToken token(ThreadContext context) {
        return null;
    }

    @Override
    public void authenticate(AuthenticationToken token, ActionListener<AuthenticationResult<User>> listener) {
        if (token instanceof OpenIdConnectToken oidcToken && isTokenForRealm(oidcToken)) {
            openIdConnectAuthenticator.authenticate(
                oidcToken,
                ActionListener.wrap(jwtClaimsSet -> { buildUserFromClaims(jwtClaimsSet, listener); }, e -> {
                    logger.debug("Failed to consume the OpenIdConnectToken ", e);
                    if (e instanceof ElasticsearchSecurityException) {
                        listener.onResponse(AuthenticationResult.unsuccessful("Failed to authenticate user with OpenID Connect", e));
                    } else {
                        listener.onFailure(e);
                    }
                })
            );
        } else {
            listener.onResponse(AuthenticationResult.notHandled());
        }
    }

    @Override
    public void lookupUser(String username, ActionListener<User> listener) {
        listener.onResponse(null);
    }

    private void buildUserFromClaims(JWTClaimsSet claims, ActionListener<AuthenticationResult<User>> authResultListener) {
        final String principal = principalAttribute.getClaimValue(claims);
        if (Strings.isNullOrEmpty(principal)) {
            authResultListener.onResponse(
                AuthenticationResult.unsuccessful(principalAttribute + "not found in " + claims.toJSONObject(), null)
            );
            return;
        }

        final Map<String, Object> tokenMetadata = new HashMap<>();
        tokenMetadata.put("id_token_hint", claims.getClaim("id_token_hint"));
        ActionListener<AuthenticationResult<User>> wrappedAuthResultListener = ActionListener.wrap(auth -> {
            if (auth.isAuthenticated()) {
                // Add the ID Token as metadata on the authentication, so that it can be used for logout requests
                Map<String, Object> metadata = new HashMap<>(auth.getMetadata());
                metadata.put(CONTEXT_TOKEN_DATA, tokenMetadata);
                auth = AuthenticationResult.success(auth.getValue(), metadata);
            }
            authResultListener.onResponse(auth);
        }, authResultListener::onFailure);

        if (delegatedRealms.hasDelegation()) {
            delegatedRealms.resolve(principal, wrappedAuthResultListener);
            return;
        }

        final Map<String, Object> userMetadata;
        if (populateUserMetadata) {
            userMetadata = claims.getClaims()
                .entrySet()
                .stream()
                .filter(entry -> isAllowedTypeForClaim(entry.getValue()))
                .collect(Collectors.toUnmodifiableMap(entry -> "oidc(" + entry.getKey() + ")", Map.Entry::getValue));
        } else {
            userMetadata = Map.of();
        }
        final List<String> groups = groupsAttribute.getClaimValues(claims);
        final String dn = dnAttribute.getClaimValue(claims);
        final String mail = mailAttribute.getClaimValue(claims);
        final String name = nameAttribute.getClaimValue(claims);
        UserRoleMapper.UserData userData = new UserRoleMapper.UserData(principal, dn, groups, userMetadata, config);
        roleMapper.resolveRoles(userData, ActionListener.wrap(roles -> {
            final User user = new User(principal, roles.toArray(Strings.EMPTY_ARRAY), name, mail, userMetadata, true);
            wrappedAuthResultListener.onResponse(AuthenticationResult.success(user));
        }, wrappedAuthResultListener::onFailure));

    }

    private static RelyingPartyConfiguration buildRelyingPartyConfiguration(RealmConfig config) {
        final String redirectUriString = require(config, RP_REDIRECT_URI);
        final URI redirectUri;
        try {
            redirectUri = new URI(redirectUriString);
        } catch (URISyntaxException e) {
            // This should never happen as it's already validated in the settings
            throw new SettingsException("Invalid URI:" + RP_REDIRECT_URI.getKey(), e);
        }
        final String postLogoutRedirectUriString = config.getSetting(RP_POST_LOGOUT_REDIRECT_URI);
        final URI postLogoutRedirectUri;
        try {
            postLogoutRedirectUri = new URI(postLogoutRedirectUriString);
        } catch (URISyntaxException e) {
            // This should never happen as it's already validated in the settings
            throw new SettingsException("Invalid URI:" + RP_POST_LOGOUT_REDIRECT_URI.getKey(), e);
        }
        final ClientID clientId = new ClientID(require(config, RP_CLIENT_ID));
        final SecureString clientSecret = config.getSetting(RP_CLIENT_SECRET);
        if (clientSecret.length() == 0) {
            throw new SettingsException(
                "The configuration setting [" + RealmSettings.getFullSettingKey(config, RP_CLIENT_SECRET) + "] is required"
            );
        }
        final ResponseType responseType;
        try {
            responseType = ResponseType.parse(require(config, RP_RESPONSE_TYPE));
        } catch (ParseException e) {
            // This should never happen as it's already validated in the settings
            throw new SettingsException("Invalid value for " + RP_RESPONSE_TYPE.getKey(), e);
        }

        final Scope requestedScope = new Scope(config.getSetting(RP_REQUESTED_SCOPES).toArray(Strings.EMPTY_ARRAY));
        if (requestedScope.contains("openid") == false) {
            requestedScope.add("openid");
        }
        final JWSAlgorithm signatureAlgorithm = JWSAlgorithm.parse(require(config, RP_SIGNATURE_ALGORITHM));
        final ClientAuthenticationMethod clientAuthenticationMethod = ClientAuthenticationMethod.parse(
            require(config, RP_CLIENT_AUTH_METHOD)
        );
        final JWSAlgorithm clientAuthJwtAlgorithm = JWSAlgorithm.parse(require(config, RP_CLIENT_AUTH_JWT_SIGNATURE_ALGORITHM));
        return new RelyingPartyConfiguration(
            clientId,
            clientSecret,
            redirectUri,
            responseType,
            requestedScope,
            signatureAlgorithm,
            clientAuthenticationMethod,
            clientAuthJwtAlgorithm,
            postLogoutRedirectUri
        );
    }

    private OpenIdConnectProviderConfiguration buildOpenIdConnectProviderConfiguration(RealmConfig config) {
        Issuer issuer = new Issuer(require(config, OP_ISSUER));

        String jwkSetUrl = require(config, OP_JWKSET_PATH);

        URI authorizationEndpoint;
        try {
            authorizationEndpoint = new URI(require(config, OP_AUTHORIZATION_ENDPOINT));
        } catch (URISyntaxException e) {
            // This should never happen as it's already validated in the settings
            throw new SettingsException("Invalid URI: " + OP_AUTHORIZATION_ENDPOINT.getKey(), e);
        }
        String responseType = require(config, RP_RESPONSE_TYPE);
        String tokenEndpointString = config.getSetting(OP_TOKEN_ENDPOINT);
        if (responseType.equals("code") && tokenEndpointString.isEmpty()) {
            throw new SettingsException(
                "The configuration setting ["
                    + OP_TOKEN_ENDPOINT.getConcreteSettingForNamespace(name()).getKey()
                    + "] is required when ["
                    + RP_RESPONSE_TYPE.getConcreteSettingForNamespace(name()).getKey()
                    + "] is set to \"code\""
            );
        }
        URI tokenEndpoint;
        try {
            tokenEndpoint = tokenEndpointString.isEmpty() ? null : new URI(tokenEndpointString);
        } catch (URISyntaxException e) {
            // This should never happen as it's already validated in the settings
            throw new SettingsException("Invalid URL: " + OP_TOKEN_ENDPOINT.getKey(), e);
        }
        URI userinfoEndpoint;
        try {
            userinfoEndpoint = (config.getSetting(OP_USERINFO_ENDPOINT).isEmpty())
                ? null
                : new URI(config.getSetting(OP_USERINFO_ENDPOINT));
        } catch (URISyntaxException e) {
            // This should never happen as it's already validated in the settings
            throw new SettingsException("Invalid URI: " + OP_USERINFO_ENDPOINT.getKey(), e);
        }
        URI endsessionEndpoint;
        try {
            endsessionEndpoint = (config.getSetting(OP_ENDSESSION_ENDPOINT).isEmpty())
                ? null
                : new URI(config.getSetting(OP_ENDSESSION_ENDPOINT));
        } catch (URISyntaxException e) {
            // This should never happen as it's already validated in the settings
            throw new SettingsException("Invalid URI: " + OP_ENDSESSION_ENDPOINT.getKey(), e);
        }

        return new OpenIdConnectProviderConfiguration(
            issuer,
            jwkSetUrl,
            authorizationEndpoint,
            tokenEndpoint,
            userinfoEndpoint,
            endsessionEndpoint
        );
    }

    private static String require(RealmConfig config, Setting.AffixSetting<String> setting) {
        final String value = config.getSetting(setting);
        if (value.isEmpty()) {
            throw new SettingsException("The configuration setting [" + RealmSettings.getFullSettingKey(config, setting) + "] is required");
        }
        return value;
    }

    /**
     * Creates the URI for an OIDC Authentication Request from the realm configuration using URI Query String Serialization and
     * possibly generates a state parameter and a nonce. It then returns the URI, state and nonce encapsulated in a
     * {@link OpenIdConnectPrepareAuthenticationResponse}. A facilitator can provide a state and a nonce parameter in two cases:
     * <ul>
     *     <li>In case of Kibana, it allows for a better UX by ensuring that all requests to an OpenID Connect Provider within
     *     the same browser context (even across tabs) will use the same state and nonce values.</li>
     *     <li>In case of custom facilitators, the implementer might require/support generating the state parameter in order
     *     to tie this to an anti-XSRF token.</li>
     * </ul>
     *
     *
     * @param existingState An existing state that can be reused or null if we need to generate one
     * @param existingNonce An existing nonce that can be reused or null if we need to generate one
     * @param loginHint A String with a login hint to add to the authentication request in case of a 3rd party initiated login
     *
     * @return an {@link OpenIdConnectPrepareAuthenticationResponse}
     */
    public OpenIdConnectPrepareAuthenticationResponse buildAuthenticationRequestUri(
        @Nullable String existingState,
        @Nullable String existingNonce,
        @Nullable String loginHint
    ) {
        final State state = existingState != null ? new State(existingState) : new State();
        final Nonce nonce = existingNonce != null ? new Nonce(existingNonce) : new Nonce();
        final AuthenticationRequest.Builder builder = new AuthenticationRequest.Builder(
            rpConfiguration.getResponseType(),
            rpConfiguration.getRequestedScope(),
            rpConfiguration.getClientId(),
            rpConfiguration.getRedirectUri()
        ).endpointURI(opConfiguration.getAuthorizationEndpoint()).state(state).nonce(nonce);
        if (Strings.hasText(loginHint)) {
            builder.loginHint(loginHint);
        }
        return new OpenIdConnectPrepareAuthenticationResponse(
            builder.build().toURI().toString(),
            state.getValue(),
            nonce.getValue(),
            this.name()
        );
    }

    public boolean isIssuerValid(String issuer) {
        return this.opConfiguration.getIssuer().getValue().equals(issuer);
    }

    public OpenIdConnectLogoutResponse buildLogoutResponse(JWT idTokenHint) {
        if (opConfiguration.getEndsessionEndpoint() != null) {
            final State state = new State();
            final LogoutRequest logoutRequest = new LogoutRequest(
                opConfiguration.getEndsessionEndpoint(),
                idTokenHint,
                rpConfiguration.getPostLogoutRedirectUri(),
                state
            );
            return new OpenIdConnectLogoutResponse(logoutRequest.toURI().toString());
        } else {
            return new OpenIdConnectLogoutResponse((String) null);
        }
    }

    @Override
    public void close() {
        openIdConnectAuthenticator.close();
    }

    /*
     * We only map claims that are of Type String, Boolean, or Number, or arrays that contain only these types
     */
    private static boolean isAllowedTypeForClaim(Object o) {
        return (o instanceof String
            || o instanceof Boolean
            || o instanceof Number
            || (o instanceof Collection
                && ((Collection<?>) o).stream().allMatch(c -> c instanceof String || c instanceof Boolean || c instanceof Number)));
    }

}
