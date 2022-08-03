/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.OctetSequenceKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
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
import org.elasticsearch.xpack.security.authc.support.ClaimParser;
import org.elasticsearch.xpack.security.authc.support.DelegatedAuthorizationSupport;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.join;
import static org.elasticsearch.core.Strings.format;

/**
 * JWT realms supports JWTs as bearer tokens for authenticating to Elasticsearch.
 * For security, it is recommended to authenticate the client too.
 */
public class JwtRealm extends Realm implements CachingRealm, Releasable {
    private static final Logger LOGGER = LogManager.getLogger(JwtRealm.class);

    private static final ContentAndJwksAlgs EMPTY_CONTENT_AND_JWKS_ALGS = new ContentAndJwksAlgs(null, new JwksAlgs(List.of(), List.of()));

    // Cached authenticated users, and adjusted JWT expiration date (=exp+skew) for checking if the JWT expired before the cache entry
    record ExpiringUser(User user, Date exp) {
        ExpiringUser {
            Objects.requireNonNull(user, "User must not be null");
            Objects.requireNonNull(exp, "Expiration date must not be null");
        }
    }

    // Original PKC/HMAC JWKSet or HMAC JWK content (for comparison during refresh), and filtered JWKs and Algs
    record ContentAndJwksAlgs(byte[] sha256, JwksAlgs jwksAlgs) {
        ContentAndJwksAlgs {
            Objects.requireNonNull(jwksAlgs, "Filters JWKs and Algs must not be null");
        }

        boolean isEmpty() {
            return ((this.sha256 == null) || this.sha256.length == 0) && this.jwksAlgs.isEmpty();
        }
    }

    // Filtered JWKs and Algs
    record JwksAlgs(List<JWK> jwks, List<String> algs) {
        JwksAlgs {
            Objects.requireNonNull(jwks, "JWKs must not be null");
            Objects.requireNonNull(algs, "Algs must not be null");
        }

        boolean isEmpty() {
            return this.jwks.isEmpty() && this.algs.isEmpty();
        }
    }

    public static final String HEADER_END_USER_AUTHENTICATION = "Authorization";
    public static final String HEADER_CLIENT_AUTHENTICATION = "ES-Client-Authentication";
    public static final String HEADER_END_USER_AUTHENTICATION_SCHEME = "Bearer";
    public static final String HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME = "SharedSecret";

    private final JwtRealmsService jwtRealmsService;
    final UserRoleMapper userRoleMapper;
    final String allowedIssuer;
    final List<String> allowedAudiences;
    final String jwkSetPath;
    final boolean isConfiguredJwkSetPkc;
    final boolean isConfiguredJwkSetHmac;
    final boolean isConfiguredJwkOidcHmac;
    final JwkSetLoader jwkSetLoader;
    final TimeValue allowedClockSkew;
    final Boolean populateUserMetadata;
    final ClaimParser claimParserPrincipal;
    final ClaimParser claimParserGroups;
    final ClaimParser claimParserDn;
    final ClaimParser claimParserMail;
    final ClaimParser claimParserName;
    final JwtRealmSettings.ClientAuthenticationType clientAuthenticationType;
    final SecureString clientAuthenticationSharedSecret;
    final Cache<BytesArray, ExpiringUser> jwtCache;
    final CacheIteratorHelper<BytesArray, ExpiringUser> jwtCacheHelper;
    final List<String> allowedJwksAlgsPkc;
    final List<String> allowedJwksAlgsHmac;
    DelegatedAuthorizationSupport delegatedAuthorizationSupport = null;
    ContentAndJwksAlgs contentAndJwksAlgsHmac;

    JwtRealm(
        final RealmConfig realmConfig,
        final JwtRealmsService jwtRealmsService,
        final SSLService sslService,
        final UserRoleMapper userRoleMapper
    ) throws SettingsException {
        super(realmConfig);
        this.jwtRealmsService = jwtRealmsService; // common configuration settings shared by all JwtRealm instances
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

        // Split configured signature algorithms by PKC and HMAC. Useful during validation, error logging, and JWK vs Alg filtering.
        final List<String> algs = super.config.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
        this.allowedJwksAlgsHmac = algs.stream().filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC::contains).toList();
        this.allowedJwksAlgsPkc = algs.stream().filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC::contains).toList();

        // PKC JWKSet can be URL, file, or not set; only initialize HTTP client if PKC JWKSet is a URL.
        this.jwkSetPath = super.config.getSetting(JwtRealmSettings.PKC_JWKSET_PATH);
        this.isConfiguredJwkSetPkc = Strings.hasText(this.jwkSetPath);
        this.isConfiguredJwkSetHmac = Strings.hasText(super.config.getSetting(JwtRealmSettings.HMAC_JWKSET));
        this.isConfiguredJwkOidcHmac = Strings.hasText(super.config.getSetting(JwtRealmSettings.HMAC_KEY));
        if (this.isConfiguredJwkSetPkc == false && this.isConfiguredJwkSetHmac == false && this.isConfiguredJwkOidcHmac == false) {
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

        // Any exception during loading requires closing JwkSetLoader's HTTP client to avoid a thread pool leak
        try {
            this.contentAndJwksAlgsHmac = this.parseJwksAlgsHmac();
            if (this.isConfiguredJwkSetPkc) {
                this.jwkSetLoader = new JwkSetLoader(sslService); // PKC JWKSet loader for HTTPS URL or local file path
            } else {
                this.jwkSetLoader = null;
            }
            this.verifyAnyAvailableJwkAndAlgPair();
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    private ContentAndJwksAlgs parseJwksAlgsHmac() {
        final JwtRealm.JwksAlgs jwksAlgsHmac;
        final SecureString hmacJwkSetContents = super.config.getSetting(JwtRealmSettings.HMAC_JWKSET);
        final SecureString hmacKeyContents = super.config.getSetting(JwtRealmSettings.HMAC_KEY);
        byte[] hmacStringContentsSha256 = null;
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
                hmacStringContentsSha256 = JwtUtil.sha256(hmacJwkSetContents.toString());
                jwksHmac = JwkValidateUtil.loadJwksFromJwkSetString(
                    RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.HMAC_JWKSET),
                    hmacJwkSetContents.toString()
                );
            } else {
                final OctetSequenceKey hmacKey = JwkValidateUtil.loadHmacJwkFromJwkString(
                    RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.HMAC_KEY),
                    hmacKeyContents
                );
                assert hmacKey != null : "Null HMAC key should not happen here";
                jwksHmac = List.of(hmacKey);
                hmacStringContentsSha256 = JwtUtil.sha256(hmacKeyContents.toString());
            }

            // Filter JWK(s) vs signature algorithms. Only keep JWKs with a matching alg. Only keep algs with a matching JWK.
            jwksAlgsHmac = JwkValidateUtil.filterJwksAndAlgorithms(jwksHmac, this.allowedJwksAlgsHmac);
        }
        LOGGER.info("Usable HMAC: JWKs [{}]. Algorithms [{}].", jwksAlgsHmac.jwks.size(), String.join(",", jwksAlgsHmac.algs()));
        return new ContentAndJwksAlgs(hmacStringContentsSha256, jwksAlgsHmac);
    }

    // Package private for test
    URI getJwkSetPathUri() {
        if (jwkSetLoader != null) {
            return jwkSetLoader.jwkSetPathUri;
        } else {
            return null;
        }
    }

    // Package private for test
    ContentAndJwksAlgs getJwksAlgsPkc() {
        if (this.isConfiguredJwkSetPkc == false) {
            return EMPTY_CONTENT_AND_JWKS_ALGS;
        } else {
            assert jwkSetLoader != null;
            return jwkSetLoader.contentAndJwksAlgsPkc;
        }
    }

    private Cache<BytesArray, ExpiringUser> buildJwtCache() {
        final TimeValue jwtCacheTtl = super.config.getSetting(JwtRealmSettings.JWT_CACHE_TTL);
        final int jwtCacheSize = super.config.getSetting(JwtRealmSettings.JWT_CACHE_SIZE);
        if ((jwtCacheTtl.getNanos() > 0) && (jwtCacheSize > 0)) {
            return CacheBuilder.<BytesArray, ExpiringUser>builder().setExpireAfterWrite(jwtCacheTtl).setMaximumWeight(jwtCacheSize).build();
        }
        return null;
    }

    private void verifyAnyAvailableJwkAndAlgPair() {
        assert this.contentAndJwksAlgsHmac != null : "HMAC not initialized";
        assert getJwksAlgsPkc() != null : "PKC not initialized";
        if (this.contentAndJwksAlgsHmac.jwksAlgs.isEmpty() && this.getJwksAlgsPkc().jwksAlgs.isEmpty()) {
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
        this.invalidateJwtCache();
        if (jwkSetLoader != null) {
            jwkSetLoader.close();
        }
    }

    /**
     * Clean up JWT cache (if enabled).
     */
    private void invalidateJwtCache() {
        if ((this.jwtCache != null) && (this.jwtCacheHelper != null)) {
            try {
                LOGGER.trace("Invalidating JWT cache for realm [{}]", super.name());
                try (ReleasableLock ignored = this.jwtCacheHelper.acquireUpdateLock()) {
                    this.jwtCache.invalidateAll();
                }
                LOGGER.debug("Invalidated JWT cache for realm [{}]", super.name());
            } catch (Exception e) {
                LOGGER.warn("Exception invalidating JWT cache for realm [" + super.name() + "]", e);
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
        if (this.jwtCacheHelper != null) {
            LOGGER.trace("Expiring JWT cache entries for realm [{}] principal=[{}]", super.name(), username);
            this.jwtCacheHelper.removeValuesIf(expiringUser -> expiringUser.user.principal().equals(username));
            LOGGER.trace("Expired JWT cache entries for realm [{}] principal=[{}]", super.name(), username);
        }
    }

    @Override
    public void expireAll() {
        this.ensureInitialized();
        this.invalidateJwtCache();
    }

    @Override
    public AuthenticationToken token(final ThreadContext threadContext) {
        this.ensureInitialized();
        // Token parsing is common code for all realms
        // First JWT realm will parse in a way that is compatible with all JWT realms,
        // taking into consideration each JWT realm might have a different principal claim name
        return this.jwtRealmsService.token(threadContext);
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
            final BytesArray jwtCacheKey = (this.jwtCache == null) ? null : new BytesArray(JwtUtil.sha256(serializedJwt));
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
            validateJwt(
                serializedJwt,
                tokenPrincipal,
                ActionListener.wrap(claimsSet -> processValidatedJwt(tokenPrincipal, jwtCacheKey, claimsSet, listener), ex -> {
                    final String msg = "Realm [" + super.name() + "] JWT validation failed for token=[" + tokenPrincipal + "].";
                    LOGGER.debug(msg, ex);
                    listener.onResponse(AuthenticationResult.unsuccessful(msg, ex));
                })
            );
        } else {
            final String className = (authenticationToken == null) ? "null" : authenticationToken.getClass().getCanonicalName();
            final String msg = "Realm [" + super.name() + "] does not support AuthenticationToken [" + className + "].";
            LOGGER.trace(msg);
            listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
        }
    }

    private void validateJwt(SecureString serializedJwt, String tokenPrincipal, ActionListener<JWTClaimsSet> listener) {
        final SignedJWT jwt;
        final JWSHeader header;
        final JWTClaimsSet claimsSet;
        final String alg;
        try {
            jwt = SignedJWT.parse(serializedJwt.toString());
            header = jwt.getHeader();
            alg = header.getAlgorithm().getName();
            claimsSet = jwt.getJWTClaimsSet();
            final Date now = new Date();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "Realm [{}] JWT parse succeeded for token=[{}]."
                        + "Validating JWT, now [{}], alg [{}], issuer [{}], audiences [{}], kty [{}],"
                        + " auth_time [{}], iat [{}], nbf [{}], exp [{}], kid [{}], jti [{}]",
                    super.name(),
                    tokenPrincipal,
                    now,
                    alg,
                    claimsSet.getIssuer(),
                    claimsSet.getAudience(),
                    header.getType(),
                    claimsSet.getDateClaim("auth_time"),
                    claimsSet.getIssueTime(),
                    claimsSet.getNotBeforeTime(),
                    claimsSet.getExpirationTime(),
                    header.getKeyID(),
                    claimsSet.getJWTID()
                );
            }
            // Validate all else before signature, because these checks are more helpful diagnostics than rejected signatures.
            final boolean isJwtAlgHmac = JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC.contains(alg);
            JwtValidateUtil.validateType(jwt);
            JwtValidateUtil.validateIssuer(jwt, allowedIssuer);
            JwtValidateUtil.validateAudiences(jwt, allowedAudiences);
            JwtValidateUtil.validateSignatureAlgorithm(jwt, isJwtAlgHmac ? this.allowedJwksAlgsHmac : this.allowedJwksAlgsPkc);
            JwtValidateUtil.validateAuthTime(jwt, now, this.allowedClockSkew.seconds());
            JwtValidateUtil.validateIssuedAtTime(jwt, now, this.allowedClockSkew.seconds());
            JwtValidateUtil.validateNotBeforeTime(jwt, now, this.allowedClockSkew.seconds());
            JwtValidateUtil.validateExpiredTime(jwt, now, this.allowedClockSkew.seconds());

            // At this point, client authc and JWT kty+alg+iss+aud+time filters passed. Do sig last, in case JWK reload is expensive.
            validateSignature(jwt, isJwtAlgHmac, tokenPrincipal, listener.map(ignored -> claimsSet));

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void processValidatedJwt(
        String tokenPrincipal,
        BytesArray jwtCacheKey,
        JWTClaimsSet claimsSet,
        ActionListener<AuthenticationResult<User>> listener
    ) {
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
                LOGGER.debug(() -> format("Realm [%s] roles [%s] for principal=[%s].", super.name(), join(",", user.roles()), principal));
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
            userMetadata = this.populateUserMetadata ? JwtUtil.toUserMetadata(claimsSet) : Map.of();
        } catch (Exception e) {
            final String msg = "Realm [" + super.name() + "] parse metadata failed for principal=[" + principal + "].";
            LOGGER.debug(msg, e);
            listener.onResponse(AuthenticationResult.unsuccessful(msg, e));
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
    }

    private void validateSignature(
        final SignedJWT jwt,
        final boolean isJwtAlgHmac,
        final String tokenPrincipal,
        final ActionListener<Void> listener
    ) throws Exception {
        try {
            JwtValidateUtil.validateSignature(
                jwt,
                isJwtAlgHmac ? this.contentAndJwksAlgsHmac.jwksAlgs.jwks : this.getJwksAlgsPkc().jwksAlgs.jwks
            );
            listener.onResponse(null);
        } catch (Exception primaryException) {
            if (isJwtAlgHmac || this.jwkSetLoader == null) {
                listener.onFailure(primaryException); // HMAC reload not supported at this time
                return;
            }

            LOGGER.debug(
                () -> org.elasticsearch.core.Strings.format(
                    "Signature verification failed for [%s] reloading JWKSet (was: #[%s] JWKs, #[%s] algs, sha256=[%s])",
                    tokenPrincipal,
                    this.getJwksAlgsPkc().jwksAlgs.jwks().size(),
                    this.getJwksAlgsPkc().jwksAlgs.algs().size(),
                    MessageDigests.toHexString(this.getJwksAlgsPkc().sha256())
                ),
                primaryException
            );

            this.jwkSetLoader.reload(ActionListener.wrap(isUpdated -> {
                if (false == isUpdated) {
                    // No change in JWKSet
                    logger.debug("Reloaded same PKC JWKs, can't retry verify JWT token=[{}]", tokenPrincipal);
                    listener.onFailure(primaryException);
                    return;
                }
                // If all PKC JWKs were replaced, all PKC JWT cache entries need to be invalidated.
                // Enhancement idea: Use separate caches for PKC vs HMAC JWKs, so only PKC entries get invalidated.
                // Enhancement idea: When some JWKs are retained (ex: rotation), only invalidate for removed JWKs.
                this.invalidateJwtCache();

                if (this.getJwksAlgsPkc().jwksAlgs.isEmpty()) {
                    logger.debug("Reloaded empty PKC JWKs, verification of JWT token will fail [{}]", tokenPrincipal);
                    // fall through and let try/catch below handle empty JWKs failure log and response
                }

                try {
                    JwtValidateUtil.validateSignature(jwt, this.getJwksAlgsPkc().jwksAlgs.jwks);
                    listener.onResponse(null);
                } catch (Exception secondaryException) {
                    logger.debug(
                        "Verification of JWT token for [{}] failed - original failure=[{}], failure after reload=[{}]",
                        tokenPrincipal,
                        primaryException.getMessage(),
                        secondaryException.getMessage()
                    );
                    secondaryException.addSuppressed(primaryException);
                    listener.onFailure(secondaryException);
                }
            }, listener::onFailure));
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

    private class JwkSetLoader implements Releasable {
        private final AtomicReference<ListenableFuture<ContentAndJwksAlgs>> reloadFutureRef = new AtomicReference<>();
        private final URI jwkSetPathUri;
        private final CloseableHttpAsyncClient httpClient;
        private volatile ContentAndJwksAlgs contentAndJwksAlgsPkc;

        JwkSetLoader(final SSLService sslService) {
            assert JwtRealm.this.isConfiguredJwkSetPkc;
            final URI jwkSetPathUri = JwtUtil.parseHttpsUri(jwkSetPath);
            if (jwkSetPathUri == null) {
                this.jwkSetPathUri = null; // local file path
                this.httpClient = null;
            } else {
                this.jwkSetPathUri = jwkSetPathUri; // HTTPS URL
                this.httpClient = JwtUtil.createHttpClient(JwtRealm.this.config, sslService);
            }
            // Any exception during loading requires closing JwkSetLoader's HTTP client to avoid a thread pool leak
            try {
                final PlainActionFuture<ContentAndJwksAlgs> future = new PlainActionFuture<>();
                load(future);
                // ASSUME: Blocking read operations are OK during startup
                contentAndJwksAlgsPkc = future.actionGet();
            } catch (Throwable t) {
                close();
                throw t;
            }
        }

        /**
         * Clean up HTTPS client cache (if enabled).
         */
        @Override
        public void close() {
            if (httpClient != null) {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    LOGGER.warn(() -> "Exception closing HTTPS client for realm [" + JwtRealm.this.name() + "]", e);
                }
            }
        }

        /**
         * Load the JWK sets and pass its content to the specified listener.
         */
        void load(final ActionListener<ContentAndJwksAlgs> listener) {
            final ListenableFuture<ContentAndJwksAlgs> future = this.getFuture();
            future.addListener(listener);
        }

        /**
         * Reload the JWK sets, compare to existing JWK sets and update it to the reloaded value if
         * they are different. The listener is called with false if the reloaded content is the same
         * as the existing one or true if they are different.
         */
        void reload(final ActionListener<Boolean> listener) {
            load(ActionListener.wrap(newContentAndJwksAlgs -> {
                if (Arrays.equals(contentAndJwksAlgsPkc.sha256, newContentAndJwksAlgs.sha256)) {
                    // No change in JWKSet
                    listener.onResponse(false);
                } else {
                    contentAndJwksAlgsPkc = newContentAndJwksAlgs;
                    listener.onResponse(true);
                }
            }, listener::onFailure));
        }

        private ListenableFuture<ContentAndJwksAlgs> getFuture() {
            for (;;) {
                final ListenableFuture<ContentAndJwksAlgs> existingFuture = this.reloadFutureRef.get();
                if (existingFuture != null) {
                    return existingFuture;
                }

                final ListenableFuture<ContentAndJwksAlgs> newFuture = new ListenableFuture<>();
                if (this.reloadFutureRef.compareAndSet(null, newFuture)) {
                    loadInternal(ActionListener.runAfter(newFuture, () -> {
                        final ListenableFuture<ContentAndJwksAlgs> oldValue = this.reloadFutureRef.getAndSet(null);
                        assert oldValue == newFuture : "future reference changed unexpectedly";
                    }));
                    return newFuture;
                }
                // else, Another thread set the future-ref before us, just try it all again
            }
        }

        private void loadInternal(final ActionListener<ContentAndJwksAlgs> listener) {
            // PKC JWKSet get contents from local file or remote HTTPS URL
            if (httpClient == null) {
                LOGGER.trace("Loading PKC JWKs from path [{}]", JwtRealm.this.jwkSetPath);
                listener.onResponse(
                    this.parseContent(
                        JwtUtil.readFileContents(
                            RealmSettings.getFullSettingKey(JwtRealm.this.config, JwtRealmSettings.PKC_JWKSET_PATH),
                            JwtRealm.this.jwkSetPath,
                            JwtRealm.this.config.env()
                        )
                    )
                );
            } else {
                LOGGER.trace("Loading PKC JWKs from https URI [{}]", jwkSetPathUri);
                JwtUtil.readUriContents(
                    RealmSettings.getFullSettingKey(JwtRealm.this.config, JwtRealmSettings.PKC_JWKSET_PATH),
                    jwkSetPathUri,
                    httpClient,
                    listener.map(bytes -> {
                        LOGGER.trace("Loaded bytes [{}] from [{}]", bytes.length, jwkSetPathUri);
                        return this.parseContent(bytes);
                    })
                );
            }
        }

        private ContentAndJwksAlgs parseContent(final byte[] jwkSetContentBytesPkc) {
            final String jwkSetContentsPkc = new String(jwkSetContentBytesPkc, StandardCharsets.UTF_8);
            final byte[] jwkSetContentsPkcSha256 = JwtUtil.sha256(jwkSetContentsPkc);

            // PKC JWKSet parse contents
            final List<JWK> jwksPkc = JwkValidateUtil.loadJwksFromJwkSetString(
                RealmSettings.getFullSettingKey(JwtRealm.this.config, JwtRealmSettings.PKC_JWKSET_PATH),
                jwkSetContentsPkc
            );
            // Filter JWK(s) vs signature algorithms. Only keep JWKs with a matching alg. Only keep algs with a matching JWK.
            final JwksAlgs jwksAlgsPkc = JwkValidateUtil.filterJwksAndAlgorithms(jwksPkc, JwtRealm.this.allowedJwksAlgsPkc);
            LOGGER.info(
                "Usable PKC: JWKs=[{}] algorithms=[{}] sha256=[{}]",
                jwksAlgsPkc.jwks().size(),
                String.join(",", jwksAlgsPkc.algs()),
                MessageDigests.toHexString(jwkSetContentsPkcSha256)
            );
            return new ContentAndJwksAlgs(jwkSetContentsPkcSha256, jwksAlgsPkc);
        }
    }
}
