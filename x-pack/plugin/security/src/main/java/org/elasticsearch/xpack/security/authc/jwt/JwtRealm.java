/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.support.ClaimParser;
import org.elasticsearch.xpack.security.authc.support.DelegatedAuthorizationSupport;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * JWT realms supports JWTs as bearer tokens for authenticating to Elasticsearch.
 * For security, it is recommended to authenticate the client too.
 */
public class JwtRealm extends Realm implements CachingRealm, Releasable {
    private static final Logger LOGGER = LogManager.getLogger(JwtRealm.class);

    record JwksAlgs(List<JWK> jwks, List<String> algs) {
        boolean isEmpty() {
            return jwks.isEmpty() && algs.isEmpty();
        }
    }

    public static final String HEADER_END_USER_AUTHENTICATION = "Authorization";
    public static final String HEADER_CLIENT_AUTHENTICATION = "X-Client-Authentication";
    public static final String HEADER_END_USER_AUTHENTICATION_SCHEME = "Bearer";
    public static final String HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME = "SharedSecret";

    final UserRoleMapper userRoleMapper;
    final String allowedIssuer;
    final List<String> allowedAudiences;
    final String jwkSetPath;
    final CloseableHttpAsyncClient httpClient;
    final JwtRealm.JwksAlgs jwksAlgsHmac;
    final JwtRealm.JwksAlgs jwksAlgsPkc;
    final TimeValue allowedClockSkew;
    final Boolean populateUserMetadata;
    final ClaimParser claimParserPrincipal;
    final ClaimParser claimParserGroups;
    final JwtRealmSettings.ClientAuthenticationType clientAuthenticationType;
    final SecureString clientAuthenticationSharedSecret;
    DelegatedAuthorizationSupport delegatedAuthorizationSupport = null;

    public JwtRealm(final RealmConfig realmConfig, final SSLService sslService, final UserRoleMapper userRoleMapper)
        throws SettingsException {
        super(realmConfig);
        this.userRoleMapper = userRoleMapper;
        this.userRoleMapper.refreshRealmOnChange(this);
        this.allowedIssuer = realmConfig.getSetting(JwtRealmSettings.ALLOWED_ISSUER);
        this.allowedAudiences = realmConfig.getSetting(JwtRealmSettings.ALLOWED_AUDIENCES);
        this.allowedClockSkew = realmConfig.getSetting(JwtRealmSettings.ALLOWED_CLOCK_SKEW);
        this.claimParserPrincipal = ClaimParser.forSetting(LOGGER, JwtRealmSettings.CLAIMS_PRINCIPAL, realmConfig, true);
        this.claimParserGroups = ClaimParser.forSetting(LOGGER, JwtRealmSettings.CLAIMS_GROUPS, realmConfig, false);
        this.populateUserMetadata = realmConfig.getSetting(JwtRealmSettings.POPULATE_USER_METADATA);
        this.clientAuthenticationType = realmConfig.getSetting(JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE);
        final SecureString sharedSecret = realmConfig.getSetting(JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET);
        this.clientAuthenticationSharedSecret = Strings.hasText(sharedSecret) ? sharedSecret : null; // convert "" to null

        // Validate Client Authentication settings. Throw SettingsException there was a problem.
        JwtUtil.validateClientAuthenticationSettings(
            RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE),
            this.clientAuthenticationType,
            RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET),
            this.clientAuthenticationSharedSecret
        );

        if (config.hasSetting(JwtRealmSettings.HMAC_KEY) == false
            && config.hasSetting(JwtRealmSettings.HMAC_JWKSET) == false
            && config.hasSetting(JwtRealmSettings.PKC_JWKSET_PATH) == false) {
            throw new SettingsException(
                "At least one of ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.HMAC_KEY)
                    + "] or ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.HMAC_JWKSET)
                    + "] or ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.PKC_JWKSET_PATH)
                    + "] must be set"
            );
        }

        // PKC JWKSet can be URL, file, or not set; only initialize HTTP client if PKC JWKSet is a URL.
        this.jwkSetPath = super.config.getSetting(JwtRealmSettings.PKC_JWKSET_PATH);
        if (Strings.hasText(this.jwkSetPath)) {
            final URI jwkSetPathPkcUri = JwtUtil.parseHttpsUri(this.jwkSetPath);
            if (jwkSetPathPkcUri == null) {
                this.httpClient = null; // local file means no HTTP client
            } else {
                this.httpClient = JwtUtil.createHttpClient(super.config, sslService);
            }
        } else {
            this.httpClient = null; // no setting means no HTTP client
        }

        this.jwksAlgsHmac = this.parseJwksAlgsHmac();
        this.jwksAlgsPkc = this.parseJwksAlgsPkc();
        this.verifyAnyAvailableJwkAndAlgPair();
    }

    // must call parseAlgsAndJwksHmac() before parseAlgsAndJwksPkc()
    private JwtRealm.JwksAlgs parseJwksAlgsHmac() {
        final JwtRealm.JwksAlgs jwksAlgsHmac;
        final SecureString hmacJwkSetContents = super.config.getSetting(JwtRealmSettings.HMAC_JWKSET);
        final SecureString hmacKeyContents = super.config.getSetting(JwtRealmSettings.HMAC_KEY);
        if (Strings.hasText(hmacJwkSetContents) && Strings.hasText(hmacKeyContents)) {
            // HMAC Key vs HMAC JWKSet settings must be mutually exclusive
            throw new SettingsException(
                "Settings ["
                    + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.HMAC_JWKSET)
                    + "] and ["
                    + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.HMAC_KEY)
                    + "] are not allowed at the same time."
            );
        } else if ((Strings.hasText(hmacJwkSetContents) == false) && (Strings.hasText(hmacKeyContents) == false)) {
            // If PKC JWKSet has at least one usable JWK, both HMAC Key vs HMAC JWKSet settings can be empty
            jwksAlgsHmac = new JwtRealm.JwksAlgs(Collections.emptyList(), Collections.emptyList());
        } else {
            // At this point, one-and-only-one of the HMAC Key or HMAC JWKSet settings are set
            List<JWK> jwksHmac;
            if (Strings.hasText(hmacJwkSetContents)) {
                jwksHmac = JwkValidateUtil.loadJwksFromJwkSetString(
                    RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.HMAC_JWKSET),
                    hmacJwkSetContents.toString()
                );
            } else {
                final OctetSequenceKey hmacKey = JwkValidateUtil.loadHmacJwkFromJwkString(
                    RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.HMAC_JWKSET),
                    hmacKeyContents
                );
                jwksHmac = List.of(hmacKey);
            }
            // Filter JWK(s) vs signature algorithms. Only keep JWKs with a matching alg. Only keep algs with a matching JWK.
            final List<String> algs = super.config.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
            final List<String> algsHmac = algs.stream().filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC::contains).toList();
            jwksAlgsHmac = JwkValidateUtil.filterJwksAndAlgorithms(jwksHmac, algsHmac);
        }
        LOGGER.info("Usable HMAC: JWKs [" + jwksAlgsHmac.jwks.size() + "]. Algorithms [" + String.join(",", jwksAlgsHmac.algs()) + "].");
        return jwksAlgsHmac;
    }

    private JwtRealm.JwksAlgs parseJwksAlgsPkc() {
        final JwtRealm.JwksAlgs jwksAlgsPkc;
        if (Strings.hasText(this.jwkSetPath) == false) {
            jwksAlgsPkc = new JwtRealm.JwksAlgs(Collections.emptyList(), Collections.emptyList());
        } else {
            // PKC JWKSet get contents from local file or remote HTTPS URL
            final byte[] jwkSetContentBytesPkc;
            if (this.httpClient == null) {
                jwkSetContentBytesPkc = JwtUtil.readFileContents(
                    RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.PKC_JWKSET_PATH),
                    this.jwkSetPath,
                    super.config.env()
                );
            } else {
                final URI jwkSetPathPkcUri = JwtUtil.parseHttpsUri(this.jwkSetPath);
                jwkSetContentBytesPkc = JwtUtil.readUriContents(
                    RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.PKC_JWKSET_PATH),
                    jwkSetPathPkcUri,
                    this.httpClient
                );
            }
            final String jwkSetContentsPkc = new String(jwkSetContentBytesPkc, StandardCharsets.UTF_8);

            // PKC JWKSet parse contents
            final List<JWK> jwksPkc = JwkValidateUtil.loadJwksFromJwkSetString(
                RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.PKC_JWKSET_PATH),
                jwkSetContentsPkc
            );

            // PKC JWKSet filter contents
            final List<String> algs = super.config.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
            final List<String> algsPkc = algs.stream().filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC::contains).toList();
            jwksAlgsPkc = JwkValidateUtil.filterJwksAndAlgorithms(jwksPkc, algsPkc);
        }
        LOGGER.info("Usable PKC: JWKs [" + jwksAlgsPkc.jwks().size() + "]. Algorithms [" + String.join(",", jwksAlgsPkc.algs()) + "].");
        return jwksAlgsPkc;
    }

    private void verifyAnyAvailableJwkAndAlgPair() {
        assert this.jwksAlgsHmac != null : "HMAC not initialized";
        assert this.jwksAlgsPkc != null : "PKC not initialized";
        if (((this.jwksAlgsHmac.jwks.isEmpty()) && (this.jwksAlgsPkc.jwks.isEmpty()))
            || ((this.jwksAlgsHmac.algs.isEmpty()) && (this.jwksAlgsPkc.algs.isEmpty()))) {
            final String msg = "No available JWK and algorithm for HMAC or PKC. Realm authentication expected to fail until this is fixed.";
            throw new SettingsException(msg);
        }
    }

    void ensureInitialized() {
        if (this.delegatedAuthorizationSupport == null) {
            throw new IllegalStateException("Realm has not been initialized");
        }
    }

    /**
     * If X-pack licensing allows it, initialize delegated authorization support.
     * JWT realm will use the list of all realms to link to its named authorization realms.
     * @param allRealms List of all realms containing authorization realms for this JWT realm.
     * @param xpackLicenseState X-pack license state.
     */
    @Override
    public void initialize(final Iterable<Realm> allRealms, final XPackLicenseState xpackLicenseState) {
        if (this.delegatedAuthorizationSupport != null) {
            throw new IllegalStateException("Realm " + super.name() + " has already been initialized");
        }
        // extract list of realms referenced by super.config.settings() value for DelegatedAuthorizationSettings.ROLES_REALMS
        this.delegatedAuthorizationSupport = new DelegatedAuthorizationSupport(allRealms, super.config, xpackLicenseState);
    }

    @Override
    public void close() {
        if (this.httpClient != null) {
            try {
                this.httpClient.close();
            } catch (IOException e) {
                LOGGER.warn("Exception closing HTTPS client for realm [" + super.name() + "]", e);
            }
        }
    }

    @Override
    public void lookupUser(final String username, final ActionListener<User> listener) {
        this.ensureInitialized();
        listener.onResponse(null); // Run-As and Delegated Authorization lookups are not supported by JWT realms
    }

    @Override
    public void expire(final String username) {
        this.ensureInitialized();
    }

    @Override
    public void expireAll() {
        this.ensureInitialized();
    }

    @Override
    public AuthenticationToken token(final ThreadContext threadContext) {
        this.ensureInitialized();
        final SecureString authenticationParameterValue = JwtUtil.getHeaderValue(
            threadContext,
            JwtRealm.HEADER_END_USER_AUTHENTICATION,
            JwtRealm.HEADER_END_USER_AUTHENTICATION_SCHEME,
            false
        );
        // Get all other possible parameters. A different JWT realm may do the actual authentication.
        final SecureString clientAuthenticationSharedSecretValue = JwtUtil.getHeaderValue(
            threadContext,
            JwtRealm.HEADER_CLIENT_AUTHENTICATION,
            JwtRealm.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME,
            true
        );
        return new JwtAuthenticationToken(authenticationParameterValue, clientAuthenticationSharedSecretValue);
    }

    @Override
    public boolean supports(final AuthenticationToken jwtAuthenticationToken) {
        return (jwtAuthenticationToken instanceof JwtAuthenticationToken);
    }

    @Override
    public void authenticate(final AuthenticationToken authenticationToken, final ActionListener<AuthenticationResult<User>> listener) {
        this.ensureInitialized();
        if (authenticationToken instanceof JwtAuthenticationToken jwtAuthenticationToken) {
            final String tokenPrincipal = jwtAuthenticationToken.principal();

            // Authenticate client: If client authc off, fall through. Otherwise, only fall through if secret matched.
            final SecureString clientSecret = jwtAuthenticationToken.getClientAuthenticationSharedSecret();
            try {
                JwtUtil.validateClientAuthentication(this.clientAuthenticationType, this.clientAuthenticationSharedSecret, clientSecret);
                LOGGER.trace("Realm [" + super.name() + "] client authentication succeeded for token=[" + tokenPrincipal + "].");
            } catch (Exception e) {
                final String msg = "Realm [" + super.name() + "] client authentication failed for token=[" + tokenPrincipal + "].";
                LOGGER.debug(msg, e);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, e));
                return; // FAILED (secret is missing or mismatched)
            }

            // Parse JWT: Extract claims for logs and role-mapping.
            final SecureString serializedJwt = jwtAuthenticationToken.getEndUserSignedJwt();
            final SignedJWT jwt;
            final JWTClaimsSet claimsSet;
            try {
                jwt = SignedJWT.parse(serializedJwt.toString());
                claimsSet = jwt.getJWTClaimsSet();
            } catch (Exception e) {
                final String msg = "Realm [" + super.name() + "] JWT parse failed for token=[" + tokenPrincipal + "].";
                LOGGER.debug(msg);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, e));
                return; // FAILED (JWT parse fail or regex parse fail)
            }

            // Validate JWT
            try {
                final String jwtAlg = jwt.getHeader().getAlgorithm().getName();
                final boolean isJwtAlgHmac = JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC.contains(jwtAlg);
                final JwtRealm.JwksAlgs jwksAndAlgs = isJwtAlgHmac ? this.jwksAlgsHmac : this.jwksAlgsPkc;
                JwtValidateUtil.validate(
                    jwt,
                    this.allowedIssuer,
                    this.allowedAudiences,
                    this.allowedClockSkew.seconds(),
                    jwksAndAlgs.algs,
                    jwksAndAlgs.jwks
                );
                LOGGER.trace("Realm [" + super.name() + "] JWT validation succeeded for token=[" + tokenPrincipal + "].");
            } catch (Exception e) {
                final String msg = "Realm [" + super.name() + "] JWT validation failed for token=[" + tokenPrincipal + "].";
                final AuthenticationResult<User> failure = AuthenticationResult.unsuccessful(msg, e);
                LOGGER.debug(msg, e);
                listener.onResponse(failure);
                return;
            }

            // At this point, JWT is validated. Parse the JWT claims using realm settings.

            final String principal = this.claimParserPrincipal.getClaimValue(claimsSet);
            if (Strings.hasText(principal) == false) {
                final String msg = "Realm ["
                    + super.name()
                    + "] no principal for token=["
                    + tokenPrincipal
                    + "] parser=["
                    + this.claimParserPrincipal
                    + "] claims=["
                    + claimsSet
                    + "].";
                LOGGER.debug(msg);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
                return;
            }
            final List<String> groups = this.claimParserGroups.getClaimValues(claimsSet);
            final Map<String, Object> userMetadata;
            try {
                userMetadata = this.populateUserMetadata ? JwtUtil.toUserMetadata(jwt) : Map.of();
            } catch (Exception e) {
                final String msg = "Realm [" + super.name() + "] parse metadata failed for principal=[" + principal + "].";
                final AuthenticationResult<User> unsuccessful = AuthenticationResult.unsuccessful(msg, e);
                LOGGER.debug(msg, e);
                listener.onResponse(unsuccessful);
                return;
            }

            // Delegated role lookup: If enabled, lookup in authz realms. Otherwise, fall through to JWT realm role mapping.
            if (this.delegatedAuthorizationSupport.hasDelegation()) {
                this.delegatedAuthorizationSupport.resolve(principal, ActionListener.wrap(result -> {
                    if (result.isAuthenticated()) {
                        // Intercept the delegated authorization listener response to log roles. Empty roles is OK.
                        final User user = result.getValue();
                        final String rolesString = Arrays.toString(user.roles());
                        LOGGER.debug(
                            "Realm [" + super.name() + "] delegated roles [" + rolesString + "] for principal=[" + principal + "]."
                        );
                    }
                    listener.onResponse(result);
                }, e -> {
                    final String msg = "Realm [" + super.name() + "] delegated roles failed for principal=[" + principal + "].";
                    LOGGER.warn(msg, e);
                    listener.onResponse(AuthenticationResult.unsuccessful(msg, e));
                }));
                return;
            }

            // Role resolution: Handle role mapping in JWT Realm.
            final UserRoleMapper.UserData userData = new UserRoleMapper.UserData(principal, null, groups, userMetadata, super.config);
            this.userRoleMapper.resolveRoles(userData, ActionListener.wrap(rolesSet -> {
                // Intercept the role mapper listener response to log the resolved roles here. Empty is OK.
                final String[] rolesArray = rolesSet.toArray(new String[0]);
                final User user = new User(principal, rolesArray, null, null, userData.getMetadata(), true);
                final String rolesString = Arrays.toString(rolesArray);
                LOGGER.debug("Realm [" + super.name() + "] mapped roles " + rolesString + " for principal=[" + principal + "].");
                listener.onResponse(AuthenticationResult.success(user));
            }, e -> {
                final String msg = "Realm [" + super.name() + "] mapped roles failed for principal=[" + principal + "].";
                LOGGER.warn(msg, e);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, e));
            }));
        } else {
            final String className = (authenticationToken == null) ? "null" : authenticationToken.getClass().getCanonicalName();
            final String msg = "Realm [" + super.name() + "] does not support AuthenticationToken [" + className + "].";
            LOGGER.trace(msg);
            listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
        }
    }

    @Override
    public void usageStats(final ActionListener<Map<String, Object>> listener) {
        this.ensureInitialized();
        super.usageStats(ActionListener.wrap(listener::onResponse, listener::onFailure));
    }
}
