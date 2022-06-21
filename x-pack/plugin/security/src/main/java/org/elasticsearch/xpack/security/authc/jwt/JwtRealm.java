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

import static java.lang.String.join;
import static org.elasticsearch.core.Strings.format;

/**
 * JWT realms supports JWTs as bearer tokens for authenticating to Elasticsearch.
 * For security, it is recommended to authenticate the client too.
 */
public class JwtRealm extends Realm implements CachingRealm, Releasable {
    private static final Logger LOGGER = LogManager.getLogger(JwtRealm.class);

    record ExpiringUser(User user, Date exp) {}

    record ContentAndJwksAlgs(String jwksContent, JwtRealm.JwksAlgs jwksAlgs) {}

    record JwksAlgs(List<JWK> jwks, List<String> algs) {
        boolean isEmpty() {
            return jwks.isEmpty() && algs.isEmpty();
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
    final CloseableHttpAsyncClient httpClient;
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
    JwtRealm.ContentAndJwksAlgs contentAndJwksAlgsPkc;
    JwtRealm.ContentAndJwksAlgs contentAndJwksAlgsHmac;
    List<String> allowedJwksAlgsPkc;
    List<String> allowedJwksAlgsHmac;

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
            this.contentAndJwksAlgsHmac = this.parseJwksAlgsHmac();
            this.contentAndJwksAlgsPkc = this.parseJwksAlgsPkc();
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

    /**
     * Tries to rotate the JWKSet either from HTTPS or a local file. If the retrieval fails we will do nothing and return false.
     * If the retrieval succeeds and the contents are different we will update the JWKSet, invalidate all caches and return true.
     * If however the JWKSet is invalid we will raise a SettingsException and return false.
     * @param isJwtAlgHmac true if HMAC JWKSet is being used
     * @return true if the JWKSet was successfully rotated, false otherwise
     * @throws SettingsException if the JWKSet is invalid
     */
    private boolean rotateJwksAlgs(final boolean isJwtAlgHmac) throws SettingsException {
        if (isJwtAlgHmac) {
            LOGGER.trace("Trying for a HMAC JWKSet rotation...");
            final JwtRealm.ContentAndJwksAlgs newContentAndJwksAlgsHmac;
            try {
                newContentAndJwksAlgsHmac = this.parseJwksAlgsHmac();
            } catch (Throwable t) {
                LOGGER.error("Unexpected error occurred when retrieving JWKSet. Skipping... " + t.getMessage());
                return false;
            }
            String oldJwkStringContent = this.contentAndJwksAlgsHmac.jwksContent;
            String newJwkStringContent = newContentAndJwksAlgsHmac.jwksContent;
            if (Strings.hasText(oldJwkStringContent) == false && Strings.hasText(newJwkStringContent) == false) {
                if (newJwkStringContent.equals(oldJwkStringContent)) {
                    LOGGER.trace("No change in JWKSet HMAC");
                    return false;
                }
            }
            LOGGER.debug("Rotated JWKSet HMAC detected: JWKs [{}]. Algorithms [{}].",
                newContentAndJwksAlgsHmac.jwksAlgs.jwks.size(), String.join(",", newContentAndJwksAlgsHmac.jwksAlgs().algs()));
            this.contentAndJwksAlgsHmac = newContentAndJwksAlgsHmac;

        } else {
            LOGGER.trace("Trying for a PKC JWKSet rotation...");
            final JwtRealm.ContentAndJwksAlgs newContentAndJwksAlgsPkc;
            try {
                newContentAndJwksAlgsPkc = this.parseJwksAlgsPkc();
            } catch (Throwable t) {
                LOGGER.error("Unexpected error occurred when retrieving JWKSet. Skipping... " + t.getMessage());
                return false;
            }
            String oldJwkStringContent = this.contentAndJwksAlgsPkc.jwksContent;
            String newJwkStringContent = newContentAndJwksAlgsPkc.jwksContent;
            if (Strings.hasText(oldJwkStringContent) == false && Strings.hasText(newJwkStringContent) == false) {
                if (newJwkStringContent.equals(oldJwkStringContent)) {
                    LOGGER.trace("No change in JWKSet PKC");
                    return false;
                }
            }
            LOGGER.debug("Rotated JWKSet PKC detected: JWKs [{}]. Algorithms [{}].",
                newContentAndJwksAlgsPkc.jwksAlgs().jwks().size(), String.join(",", newContentAndJwksAlgsPkc.jwksAlgs().algs()));
            this.contentAndJwksAlgsPkc = newContentAndJwksAlgsPkc;
        }
        this.invalidateJwtCache();
        this.verifyAnyAvailableJwkAndAlgPair(); // throws SettingsException
        return true;
    }

    // must call parseAlgsAndJwksHmac() before parseAlgsAndJwksPkc()
    private ContentAndJwksAlgs parseJwksAlgsHmac() {
        final JwtRealm.JwksAlgs jwksAlgsHmac;
        final SecureString hmacJwkSetContents = super.config.getSetting(JwtRealmSettings.HMAC_JWKSET);
        final SecureString hmacKeyContents = super.config.getSetting(JwtRealmSettings.HMAC_KEY);
        String hmacStringContent = null;
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
                hmacStringContent = hmacJwkSetContents.toString();
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
                hmacStringContent = hmacKeyContents.toString();
            }
            // Filter JWK(s) vs signature algorithms. Only keep JWKs with a matching alg. Only keep algs with a matching JWK.
            final List<String> algs = super.config.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
            this.allowedJwksAlgsHmac = algs.stream().filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC::contains).toList();
            jwksAlgsHmac = JwkValidateUtil.filterJwksAndAlgorithms(jwksHmac, this.allowedJwksAlgsHmac);
        }
        LOGGER.warn("Usable HMAC: JWKs [{}]. Algorithms [{}].", jwksAlgsHmac.jwks.size(), String.join(",", jwksAlgsHmac.algs()));
        return new ContentAndJwksAlgs(hmacStringContent, jwksAlgsHmac);
    }

    private ContentAndJwksAlgs parseJwksAlgsPkc() {
        final JwtRealm.JwksAlgs jwksAlgsPkc;
        String jwkSetContentsPkc = null;
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
            jwkSetContentsPkc = new String(jwkSetContentBytesPkc, StandardCharsets.UTF_8);

            // PKC JWKSet parse contents
            final List<JWK> jwksPkc = JwkValidateUtil.loadJwksFromJwkSetString(
                RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.PKC_JWKSET_PATH),
                jwkSetContentsPkc
            );

            // PKC JWKSet filter contents
            final List<String> algs = super.config.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
            this.allowedJwksAlgsPkc = algs.stream().filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC::contains).toList();
            jwksAlgsPkc = JwkValidateUtil.filterJwksAndAlgorithms(jwksPkc, this.allowedJwksAlgsPkc);
        }
        LOGGER.warn("Usable PKC: JWKs [{}]. Algorithms [{}].", jwksAlgsPkc.jwks().size(), String.join(",", jwksAlgsPkc.algs()));
        return new ContentAndJwksAlgs(jwkSetContentsPkc, jwksAlgsPkc);
    }

    private void verifyAnyAvailableJwkAndAlgPair() {
        assert this.contentAndJwksAlgsHmac != null : "HMAC not initialized";
        assert this.contentAndJwksAlgsPkc != null : "PKC not initialized";
        assert this.contentAndJwksAlgsHmac.jwksAlgs != null : "HMAC not initialized";
        assert this.contentAndJwksAlgsPkc.jwksAlgs != null : "PKC not initialized";
        if (this.contentAndJwksAlgsHmac.jwksAlgs.isEmpty() && this.contentAndJwksAlgsPkc.jwksAlgs.isEmpty()) {
            final String msg = "No available JWK and algorithm for HMAC or PKC. Realm authentication expected to fail until this is fixed.";
            throw new SettingsException(msg);
        }
    }

    /**
     * Clean up JWT cache (if enabled).
     */
    private void invalidateJwtCache() {
        if (this.jwtCache != null) {
            try {
                this.jwtCache.invalidateAll();
            } catch (Exception e) {
                LOGGER.warn("Exception invalidating JWT cache for realm [" + super.name() + "]", e);
            }
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
     * Clean up HTTPS client cache (if enabled).
     */
    private void closeHttpClient() {
        if (this.httpClient != null) {
            try {
                this.httpClient.close();
            } catch (IOException e) {
                LOGGER.warn(() -> "Exception closing HTTPS client for realm [" + super.name() + "]", e);
            }
        }
    }

    /**
     * Clean up JWT cache (if enabled).
     * Clean up HTTPS client cache (if enabled).
     */
    @Override
    public void close() {
        this.invalidateJwtCache();
        this.closeHttpClient();
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

                // Verify if any validation can be done using current/rotated JWKSet
                try {
                    this.verifyAnyAvailableJwkAndAlgPair();
                } catch (SettingsException e) {
                    // Try to automatically fix the realm from a degraded state
                    if (this.contentAndJwksAlgsHmac.jwksAlgs.isEmpty() && this.contentAndJwksAlgsPkc.jwksAlgs.isEmpty()) {
                        if (this.rotateJwksAlgs(isJwtAlgHmac)) {
                            LOGGER.warn("No available JWKs and algorithms for HMAC or PKC, but refresh succeeded.");
                        } else {
                            throw new Exception("No available JWKs and algorithms for HMAC or PKC, and refresh failed.");
                        }
                    }
                }

                try {
                    JwtRealm.JwksAlgs jwksAndAlgs = isJwtAlgHmac ? this.contentAndJwksAlgsHmac.jwksAlgs : this.contentAndJwksAlgsPkc.jwksAlgs;
                    List<String> allowedAlgs = isJwtAlgHmac ? this.allowedJwksAlgsHmac : this.allowedJwksAlgsPkc;

                    JwtValidateUtil.validate(
                        jwt,
                        this.allowedIssuer,
                        this.allowedAudiences,
                        this.allowedClockSkew.seconds(),
                        allowedAlgs,
                        jwksAndAlgs.jwks
                    );
                } catch (JwtValidateUtil.JWKSetValidationError e) {
                    /*
                     * Elasticsearch automatically caches the retrieved JWK set to avoid unnecessary HTTP requests,
                     * but will attempt to refresh the JWK upon JWKSet verification failure
                     * as this might indicate that the JWT was signed with a newer, rotated JWK.
                     */
                    if (this.rotateJwksAlgs(isJwtAlgHmac)) {
                        LOGGER.trace("Validating again using rotated JWKSet");
                        JwtRealm.JwksAlgs jwksAndAlgs = isJwtAlgHmac ? this.contentAndJwksAlgsHmac.jwksAlgs : this.contentAndJwksAlgsPkc.jwksAlgs;
                        List<String> allowedAlgs = isJwtAlgHmac ? this.allowedJwksAlgsHmac : this.allowedJwksAlgsPkc;

                        JwtValidateUtil.validate(
                            jwt,
                            this.allowedIssuer,
                            this.allowedAudiences,
                            this.allowedClockSkew.seconds(),
                            allowedAlgs,
                            jwksAndAlgs.jwks
                        );
                    } else {
                        LOGGER.trace("No JWKSet Rotation available");
                        throw new Exception(e);
                    }
                }

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
                        () -> format("Realm [%s] roles [%s] for principal=[%s].", super.name(), join(",", user.roles()), principal)
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
