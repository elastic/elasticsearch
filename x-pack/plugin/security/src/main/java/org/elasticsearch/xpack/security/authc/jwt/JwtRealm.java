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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
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
import org.elasticsearch.xpack.core.security.support.CacheIteratorHelper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.BytesKey;
import org.elasticsearch.xpack.security.authc.support.ClaimParser;
import org.elasticsearch.xpack.security.authc.support.DelegatedAuthorizationSupport;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * JWT realms supports JWTs as bearer tokens for authenticating to Elasticsearch.
 * For security, it is recommended to authenticate the client too.
 */
public class JwtRealm extends Realm implements CachingRealm, Releasable {
    private static final Logger LOGGER = LogManager.getLogger(JwtRealm.class);

    record ExpiringUser(User user, Date exp) {}

    record JwksAlgs(List<JWK> jwks, List<String> algs) {
        boolean isEmpty() {
            return jwks.isEmpty() && algs.isEmpty();
        }
    }

    public static final String HEADER_END_USER_AUTHENTICATION = "Authorization";
    public static final String HEADER_CLIENT_AUTHENTICATION = "ES-Client-Authentication";
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
    final ClaimParser claimParserDn;
    final ClaimParser claimParserMail;
    final ClaimParser claimParserName;
    final JwtRealmSettings.ClientAuthenticationType clientAuthenticationType;
    final SecureString clientAuthenticationSharedSecret;
    final Cache<BytesKey, ExpiringUser> jwtCache;
    final CacheIteratorHelper<BytesKey, ExpiringUser> jwtCacheHelper;
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
        this.claimParserDn = ClaimParser.forSetting(LOGGER, JwtRealmSettings.CLAIMS_DN, realmConfig, false);
        this.claimParserMail = ClaimParser.forSetting(LOGGER, JwtRealmSettings.CLAIMS_MAIL, realmConfig, false);
        this.claimParserName = ClaimParser.forSetting(LOGGER, JwtRealmSettings.CLAIMS_NAME, realmConfig, false);
        this.populateUserMetadata = realmConfig.getSetting(JwtRealmSettings.POPULATE_USER_METADATA);
        this.clientAuthenticationType = realmConfig.getSetting(JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE);
        final SecureString sharedSecret = realmConfig.getSetting(JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET);
        this.clientAuthenticationSharedSecret = Strings.hasText(sharedSecret) ? sharedSecret : null; // convert "" to null
        this.jwtCache = this.buildJwtCache();
        this.jwtCacheHelper = (this.jwtCache == null) ? null : new CacheIteratorHelper<>(this.jwtCache);

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

        // If HTTPS client was created in JWT realm, any exception after that point requires closing it to avoid a thread pool leak
        try {
            this.jwksAlgsHmac = this.parseJwksAlgsHmac();
            this.jwksAlgsPkc = this.parseJwksAlgsPkc();
            this.verifyAnyAvailableJwkAndAlgPair();
        } catch (Throwable t) {
            this.close();
            throw t;
        }
    }

    private Cache<BytesKey, ExpiringUser> buildJwtCache() {
        final TimeValue jwtCacheTtl = super.config.getSetting(JwtRealmSettings.JWT_CACHE_TTL);
        final int jwtCacheSize = super.config.getSetting(JwtRealmSettings.JWT_CACHE_SIZE);
        if ((jwtCacheTtl.getNanos() > 0) && (jwtCacheSize > 0)) {
            return CacheBuilder.<BytesKey, ExpiringUser>builder().setExpireAfterWrite(jwtCacheTtl).setMaximumWeight(jwtCacheSize).build();
        }
        return null;
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
        LOGGER.info("Usable HMAC: JWKs [{}]. Algorithms [{}].", jwksAlgsHmac.jwks.size(), String.join(",", jwksAlgsHmac.algs()));
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
        LOGGER.info("Usable PKC: JWKs [{}]. Algorithms [{}].", jwksAlgsPkc.jwks().size(), String.join(",", jwksAlgsPkc.algs()));
        return jwksAlgsPkc;
    }

    private void verifyAnyAvailableJwkAndAlgPair() {
        assert this.jwksAlgsHmac != null : "HMAC not initialized";
        assert this.jwksAlgsPkc != null : "PKC not initialized";
        if (this.jwksAlgsHmac.isEmpty() && this.jwksAlgsPkc.isEmpty()) {
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

    /**
     * Clean up JWT cache (if enabled).
     * Clean up HTTPS client cache (if enabled).
     */
    @Override
    public void close() {
        if (this.jwtCache != null) {
            try {
                this.jwtCache.invalidateAll();
            } catch (Exception e) {
                LOGGER.warn("Exception invalidating JWT cache for realm [" + super.name() + "]", e);
            }
        }
        if (this.httpClient != null) {
            try {
                this.httpClient.close();
            } catch (IOException e) {
                LOGGER.warn(new ParameterizedMessage("Exception closing HTTPS client for realm [{}]", super.name()), e);
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
        LOGGER.trace("Expiring JWT cache entries for realm [" + super.name() + "] principal=[" + username + "]");
        if (this.jwtCacheHelper != null) {
            this.jwtCacheHelper.removeValuesIf(expiringUser -> expiringUser.user.principal().equals(username));
        }
    }

    @Override
    public void expireAll() {
        this.ensureInitialized();
        if ((this.jwtCache != null) && (this.jwtCacheHelper != null)) {
            LOGGER.trace("Invalidating JWT cache for realm [" + super.name() + "]");
            try (ReleasableLock ignored = this.jwtCacheHelper.acquireUpdateLock()) {
                this.jwtCache.invalidateAll();
            }
        }
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
                LOGGER.trace("Realm [{}] client authentication succeeded for token=[{}].", super.name(), tokenPrincipal);
            } catch (Exception e) {
                final String msg = "Realm [" + super.name() + "] client authentication failed for token=[" + tokenPrincipal + "].";
                LOGGER.debug(msg, e);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, e));
                return; // FAILED (secret is missing or mismatched)
            }

            // JWT cache
            final SecureString serializedJwt = jwtAuthenticationToken.getEndUserSignedJwt();
            final BytesKey jwtCacheKey = (this.jwtCache == null) ? null : computeBytesKey(serializedJwt);
            if (jwtCacheKey != null) {
                final ExpiringUser expiringUser = this.jwtCache.get(jwtCacheKey);
                if (expiringUser == null) {
                    LOGGER.trace("Realm [" + super.name() + "] JWT cache miss token=[" + tokenPrincipal + "] key=[" + jwtCacheKey + "].");
                } else {
                    final User user = expiringUser.user;
                    final Date exp = expiringUser.exp; // claimsSet.getExpirationTime().getTime() + this.allowedClockSkew.getMillis()
                    final String principal = user.principal();
                    final Date now = new Date();
                    if (now.getTime() < exp.getTime()) {
                        LOGGER.trace(
                            "Realm ["
                                + super.name()
                                + "] JWT cache hit token=["
                                + tokenPrincipal
                                + "] key=["
                                + jwtCacheKey
                                + "] principal=["
                                + principal
                                + "] exp=["
                                + exp
                                + "] now=["
                                + now
                                + "]."
                        );
                        if (this.delegatedAuthorizationSupport.hasDelegation()) {
                            this.delegatedAuthorizationSupport.resolve(principal, listener);
                        } else {
                            listener.onResponse(AuthenticationResult.success(user));
                        }
                        return;
                    }
                    LOGGER.trace(
                        "Realm ["
                            + super.name()
                            + "] JWT cache exp token=["
                            + tokenPrincipal
                            + "] key=["
                            + jwtCacheKey
                            + "] principal=["
                            + principal
                            + "] exp=["
                            + exp
                            + "] now=["
                            + now
                            + "]."
                    );
                }
            }

            // Validate JWT: Extract JWT and claims set, and validate JWT.
            final SignedJWT jwt;
            final JWTClaimsSet claimsSet;
            try {
                jwt = SignedJWT.parse(serializedJwt.toString());
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
                claimsSet = jwt.getJWTClaimsSet();
                LOGGER.trace("Realm [{}] JWT validation succeeded for token=[{}].", super.name(), tokenPrincipal);
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

            // Roles listener: Log roles from delegated authz lookup or role mapping, and cache User if JWT cache is enabled.
            final ActionListener<AuthenticationResult<User>> logAndCacheListener = ActionListener.wrap(result -> {
                if (result.isAuthenticated()) {
                    final User user = result.getValue();
                    LOGGER.debug(
                        () -> new ParameterizedMessage(
                            "Realm [{}] roles [{}] for principal=[{}].",
                            super.name(),
                            String.join(",", user.roles()),
                            principal
                        )
                    );
                    if ((this.jwtCache != null) && (this.jwtCacheHelper != null)) {
                        try (ReleasableLock ignored = this.jwtCacheHelper.acquireUpdateLock()) {
                            final long expWallClockMillis = claimsSet.getExpirationTime().getTime() + this.allowedClockSkew.getMillis();
                            this.jwtCache.put(jwtCacheKey, new ExpiringUser(result.getValue(), new Date(expWallClockMillis)));
                        }
                    }
                }
                listener.onResponse(result);
            }, listener::onFailure);

            // Delegated role lookup or Role mapping: Use the above listener to log roles and cache User.
            if (this.delegatedAuthorizationSupport.hasDelegation()) {
                this.delegatedAuthorizationSupport.resolve(principal, logAndCacheListener);
                return;
            }

            // User metadata: If enabled, extract metadata from JWT claims set. Use it in UserRoleMapper.UserData and User constructors.
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

            // Role resolution: Handle role mapping in JWT Realm.
            final List<String> groups = this.claimParserGroups.getClaimValues(claimsSet);
            final String dn = this.claimParserDn.getClaimValue(claimsSet);
            final String mail = this.claimParserMail.getClaimValue(claimsSet);
            final String name = this.claimParserName.getClaimValue(claimsSet);
            final UserRoleMapper.UserData userData = new UserRoleMapper.UserData(principal, dn, groups, userMetadata, super.config);
            this.userRoleMapper.resolveRoles(userData, ActionListener.wrap(rolesSet -> {
                final User user = new User(principal, rolesSet.toArray(Strings.EMPTY_ARRAY), name, mail, userData.getMetadata(), true);
                logAndCacheListener.onResponse(AuthenticationResult.success(user));
            }, logAndCacheListener::onFailure));
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
        super.usageStats(ActionListener.wrap(stats -> {
            stats.put("jwt.cache", Collections.singletonMap("size", this.jwtCache == null ? -1 : this.jwtCache.count()));
            listener.onResponse(stats);
        }, listener::onFailure));
    }

    static BytesKey computeBytesKey(final CharSequence charSequence) {
        final MessageDigest messageDigest = MessageDigests.sha256();
        messageDigest.update(charSequence.toString().getBytes(StandardCharsets.UTF_8));
        return new BytesKey(messageDigest.digest());
    }
}
