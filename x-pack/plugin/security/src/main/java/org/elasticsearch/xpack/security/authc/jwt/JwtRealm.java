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

    // Cached authenticated users, and processed JWT expiration date for checking if the JWT expired before the cache entry
    record ExpiringUser(User user, Date exp) {}

    // Original PKC/HMAC JWKSet or HMAC JWK content (for comparison during refresh), and filtered JWKs and Algs
    record ContentAndFilteredJwksAlgs(String jwksContent, FilteredJwksAlgs filteredJwksAlgs) {
        boolean isEmpty() {
            return jwksContent.isEmpty() && filteredJwksAlgs.isEmpty();
        }
    }

    // Filtered JWKs and Algs
    record FilteredJwksAlgs(List<JWK> jwks, List<String> algs) {
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
    ContentAndFilteredJwksAlgs contentAndFilteredJwksAlgsPkc;
    ContentAndFilteredJwksAlgs contentAndFilteredJwksAlgsHmac;
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
            this.contentAndFilteredJwksAlgsHmac = this.loadContentAndFilterJwksAlgsHmac();
            this.contentAndFilteredJwksAlgsPkc = this.loadContentAndFilterJwksAlgsPkc();
            this.verifyNonEmptyHmacAndPkcJwksAlgs(); // throws SettingsException if realm has zero combined JWKs and Algs after filtering
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

    // TODO Check if this comment is still needed
    // must call loadContentAndFilterJwksAlgsHmac() before loadContentAndFilterJwksAlgsPkc()
    private ContentAndFilteredJwksAlgs loadContentAndFilterJwksAlgsHmac() {
        final FilteredJwksAlgs filteredJwksAlgsHmac;
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
            filteredJwksAlgsHmac = new FilteredJwksAlgs(Collections.emptyList(), Collections.emptyList());
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
                assert hmacKey != null : "Null HMAC key should not happen here";
                jwksHmac = List.of(hmacKey);
                hmacStringContent = hmacKeyContents.toString();
            }
            // Filter HMAC JWK(s) vs Algs. Only keep JWKs with a matching Alg. Only keep Algs with a matching JWK.
            // Zero HMAC JWKs and Algs after filtering is valid use case, as long as PKC has non-zero JWKs and Algs.
            final List<String> algs = super.config.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
            this.allowedJwksAlgsHmac = algs.stream().filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC::contains).toList();
            filteredJwksAlgsHmac = JwkValidateUtil.filterJwksAndAlgorithms(jwksHmac, this.allowedJwksAlgsHmac);
        }
        LOGGER.debug(
            "Usable HMAC: JWKs [{}]. Algorithms [{}].",
            filteredJwksAlgsHmac.jwks.size(),
            String.join(",", filteredJwksAlgsHmac.algs())
        );
        return new ContentAndFilteredJwksAlgs(hmacStringContent, filteredJwksAlgsHmac);
    }

    private ContentAndFilteredJwksAlgs loadContentAndFilterJwksAlgsPkc() {
        final FilteredJwksAlgs filteredJwksAlgsPkc;
        String jwkSetContentsPkc = null;
        if (Strings.hasText(this.jwkSetPath) == false) {
            filteredJwksAlgsPkc = new FilteredJwksAlgs(Collections.emptyList(), Collections.emptyList());
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

            // Filter PKC JWK(s) vs Algs. Only keep JWKs with a matching Alg. Only keep Algs with a matching JWK.
            // Zero PKC JWKs and Algs after filtering is valid use case, as long as HMAC has non-zero JWKs and Algs.
            final List<String> algs = super.config.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
            this.allowedJwksAlgsPkc = algs.stream().filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC::contains).toList();
            filteredJwksAlgsPkc = JwkValidateUtil.filterJwksAndAlgorithms(jwksPkc, this.allowedJwksAlgsPkc);
        }
        LOGGER.debug(
            "Usable PKC: JWKs [{}]. Algorithms [{}].",
            filteredJwksAlgsPkc.jwks().size(),
            String.join(",", filteredJwksAlgsPkc.algs())
        );
        return new ContentAndFilteredJwksAlgs(jwkSetContentsPkc, filteredJwksAlgsPkc);
    }

    private void verifyNonEmptyHmacAndPkcJwksAlgs() {
        assert this.contentAndFilteredJwksAlgsHmac != null : "HMAC not initialized";
        assert this.contentAndFilteredJwksAlgsPkc != null : "PKC not initialized";
        assert this.contentAndFilteredJwksAlgsHmac.filteredJwksAlgs != null : "HMAC not initialized";
        assert this.contentAndFilteredJwksAlgsPkc.filteredJwksAlgs != null : "PKC not initialized";
        assert this.contentAndFilteredJwksAlgsHmac.filteredJwksAlgs.jwks != null : "HMAC not initialized";
        assert this.contentAndFilteredJwksAlgsPkc.filteredJwksAlgs.jwks != null : "PKC not initialized";
        assert this.contentAndFilteredJwksAlgsHmac.filteredJwksAlgs.algs != null : "HMAC not initialized";
        assert this.contentAndFilteredJwksAlgsPkc.filteredJwksAlgs.algs != null : "PKC not initialized";
        if (this.contentAndFilteredJwksAlgsHmac.filteredJwksAlgs.isEmpty()
            && this.contentAndFilteredJwksAlgsPkc.filteredJwksAlgs.isEmpty()) {
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
        this.closeHttpClient();
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
                LOGGER.trace("Invalidating JWT cache for realm [{}]", super.name());
            } catch (Exception e) {
                final String msg = "Exception invalidating JWT cache for realm [" + super.name() + "]";
                LOGGER.debug(msg, e);
            }
        }
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

    @Override
    public void lookupUser(final String username, final ActionListener<User> listener) {
        this.ensureInitialized();
        listener.onResponse(null); // Run-As and Delegated Authorization lookups are not supported by JWT realms
    }

    @Override
    public void expire(final String username) {
        this.ensureInitialized();
        LOGGER.trace("Expiring JWT cache entries for realm [{}] principal=[{}]", super.name(), username);
        if (this.jwtCacheHelper != null) {
            this.jwtCacheHelper.removeValuesIf(expiringUser -> expiringUser.user.principal().equals(username));
        }
        LOGGER.trace("Expired JWT cache entries for realm [{}] principal=[{}]", super.name(), username);
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
            final BytesKey jwtCacheKey = (this.jwtCache == null) ? null : computeBytesKey(serializedJwt);
            if (jwtCacheKey != null) {
                final ExpiringUser expiringUser = this.jwtCache.get(jwtCacheKey);
                if (expiringUser == null) {
                    LOGGER.trace("Realm [" + super.name() + "] JWT cache miss token=[" + tokenPrincipal + "] key=[" + jwtCacheKey + "].");
                } else {
                    final User user = expiringUser.user;
                    final Date exp = expiringUser.exp; // jwtClaimsSet.getExpirationTime().getTime() + this.allowedClockSkew.getMillis()
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

            // Validate JWT: Parse and validate.
            final SignedJWT jwt;
            final JWSHeader jwtHeader;
            final JWTClaimsSet jwtClaimsSet;
            try {
                jwt = SignedJWT.parse(serializedJwt.toString());
                jwtHeader = jwt.getHeader();
                jwtClaimsSet = jwt.getJWTClaimsSet();
                final Date now = new Date();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "Realm [{}] JWT parse succeeded for token=[{}]."
                            + "Validating JWT, now [{}], alg [{}], issuer [{}], audiences [{}], typ [{}],"
                            + " auth_time [{}], iat [{}], nbf [{}], exp [{}], kid [{}], jti [{}]",
                        super.name(),
                        tokenPrincipal,
                        now,
                        jwtHeader.getAlgorithm(),
                        jwtClaimsSet.getIssuer(),
                        jwtClaimsSet.getAudience(),
                        jwtHeader.getType(),
                        jwtClaimsSet.getDateClaim("auth_time"),
                        jwtClaimsSet.getIssueTime(),
                        jwtClaimsSet.getNotBeforeTime(),
                        jwtClaimsSet.getExpirationTime(),
                        jwtHeader.getKeyID(),
                        jwtClaimsSet.getJWTID()
                    );
                }
                // Validate all else before signature, because these checks are more helpful diagnostics than rejected signatures.
                final boolean isJwtSignatureAlgHmac = JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC.contains(
                    jwtHeader.getAlgorithm().getName()
                );
                JwtValidateUtil.validateType(jwt);
                JwtValidateUtil.validateIssuer(jwt, allowedIssuer);
                JwtValidateUtil.validateAudiences(jwt, allowedAudiences);
                JwtValidateUtil.validateSignatureAlgorithm(jwt, isJwtSignatureAlgHmac ? this.allowedJwksAlgsHmac : this.allowedJwksAlgsPkc);
                JwtValidateUtil.validateAuthTime(jwt, now, this.allowedClockSkew.seconds());
                JwtValidateUtil.validateIssuedAtTime(jwt, now, this.allowedClockSkew.seconds());
                JwtValidateUtil.validateNotBeforeTime(jwt, now, this.allowedClockSkew.seconds());
                JwtValidateUtil.validateExpiredTime(jwt, now, this.allowedClockSkew.seconds());

                // Validate signature last, so expensive JWK reloads only happen after all other checks passed.
                final boolean reloadBefore = this.contentAndFilteredJwksAlgsHmac.isEmpty() && this.contentAndFilteredJwksAlgsPkc.isEmpty();
                final ContentAndFilteredJwksAlgs contentAndFilteredJwksAlgs = isJwtSignatureAlgHmac
                    ? this.contentAndFilteredJwksAlgsHmac
                    : this.contentAndFilteredJwksAlgsPkc;
                try {
                    if ((reloadBefore) && (this.reloadJwksHelper(listener, tokenPrincipal, isJwtSignatureAlgHmac) == false)) {
                        return; // Reload required before. Reload failed, or succeeded but no changes. Helper populates listener response.
                    }
                    // Throws AllVerifiesFailedException (some JWKs after filtering, all failed) or Exception (no JWKs after filtering)
                    JwtValidateUtil.validateSignature(jwt, contentAndFilteredJwksAlgs.filteredJwksAlgs.jwks);
                    // Fall through means success, exception will be caught by inner or outer catch
                } catch (JwtValidateUtil.AllVerifiesFailedException e) {
                    if ((reloadBefore == false) && (this.reloadJwksHelper(listener, tokenPrincipal, isJwtSignatureAlgHmac) == false)) {
                        return; // Reload required after. Reload failed, or succeeded but no changes. Helper populates listener response.
                    }
                    // Throws AllVerifiesFailedException (some JWKs after filtering, all failed) or Exception (no JWKs after filtering)
                    JwtValidateUtil.validateSignature(jwt, contentAndFilteredJwksAlgs.filteredJwksAlgs.jwks);
                    // Fall through means success, exception will be caught by outer catch
                }
            } catch (Exception e) {
                final String msg = "Realm [" + super.name() + "] JWT validation failed for token=[" + tokenPrincipal + "].";
                LOGGER.debug(msg, e);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, e));
                return;
            }

            // At this point, JWT is validated. Process the JWT claims using realm settings.

            final String principal = this.claimParserPrincipal.getClaimValue(jwtClaimsSet);
            if (Strings.hasText(principal) == false) {
                final String msg = "Realm ["
                    + super.name()
                    + "] no principal for token=["
                    + tokenPrincipal
                    + "] parser=["
                    + this.claimParserPrincipal
                    + "] claims=["
                    + jwtClaimsSet
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
                            final long expWallClockMillis = jwtClaimsSet.getExpirationTime().getTime() + this.allowedClockSkew.getMillis();
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
                LOGGER.debug(msg, e);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, e));
                return;
            }

            // Role resolution: Handle role mapping in JWT Realm.
            final List<String> groups = this.claimParserGroups.getClaimValues(jwtClaimsSet);
            final String dn = this.claimParserDn.getClaimValue(jwtClaimsSet);
            final String mail = this.claimParserMail.getClaimValue(jwtClaimsSet);
            final String name = this.claimParserName.getClaimValue(jwtClaimsSet);
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

    private boolean reloadJwksHelper(ActionListener<AuthenticationResult<User>> listener, String tokenPrincipal, boolean isJwtAlgHmac) {
        try {
            LOGGER.debug("Realm [" + super.name() + "] has no JWKs and Algs to verify JWT for token=[" + tokenPrincipal + "].");
            if (this.reloadJwks(isJwtAlgHmac) == false) {
                // Reload failed, or no JWK content changes found, so realm still has no JWKs and Algs to verify JWT
                final String msg = "Realm [" + super.name() + "] has no old JWKs to verify JWT for token=[" + tokenPrincipal + "].";
                LOGGER.error(msg);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
                return false;
            }
            LOGGER.trace("Realm [" + super.name() + "] loaded new JWKs to verify JWT for token=[" + tokenPrincipal + "].");
            return true;
        } catch (Exception e) {
            // Reload succeeded, but no JWKs and Algs remain
            final String msg = "Realm [" + super.name() + "] has no new JWKs to verify JWT for token=[" + tokenPrincipal + "].";
            LOGGER.debug(msg, e);
            listener.onResponse(AuthenticationResult.unsuccessful(msg, e));
            return false;
        }
    }

    /**
     * Tries to refresh the HMAC JWKSet or HMAC JWK from elasticsearch-keystore, or the PKC JWKSet from an HTTPS URL or local file.
     * For example, if a JWT is signed with HMAC and signature verification fails, only attempt to refresh HMAC JWKSet or HMAC JWK.
     * For example, if a JWT is signed with PKC and signature verification fails, only attempt to refresh PKC HTTPS URL or PKC local file.
     *
     * If reload succeeds and the contents are same, return false so the caller knows the JWKs (and filtered Algs) did not change.
     * If reload succeeds and the contents are different, filter the new JWKs vs the realm signature algorithms to see if they are valid.
     * If new JWKs are valid, return true so the caller knows the JWKs (and filtered Algs) changed, so retry JWK signature verification.
     * If new JWKs are not valid, the call to verifyAnyAvailableJwkAndAlgPair() will trigger a SettingsException. Caller must catch it.
     *
     * If there is a SettingsException, the realm keeps the new JWK content, but filters JWKs and Algs are empty.
     * Manual intervention will be needed to fix the PKC or HMAC JWKs, or adjust the realm signature algorithms.
     *
     * @param doHmac If a JWT is signed with an HMAC algorithm, only HMAC refresh is needed, otherwise only PKC refresh is needed.
     * @return True, if JWK contents were updated. False, is JWK content was unchanged.
     * @throws SettingsException If JWK contents were updated, and the realm has no usable JWKs and Algs after comparing them.
     */
    private boolean reloadJwks(final boolean doHmac) throws SettingsException {
        // If is valid for old filtered JWKs and algs to be empty
        LOGGER.trace("Reloading JWKs, doHmac: {}", doHmac);
        final ContentAndFilteredJwksAlgs oldContentAndFilteredJwksAlgs = doHmac
            ? this.contentAndFilteredJwksAlgsHmac
            : this.contentAndFilteredJwksAlgsPkc;
        final ContentAndFilteredJwksAlgs newContentAndFilteredJwksAlgs;
        try {
            // Ending up with zero JWKs and algs after filtering is a valid use case, as long as the other has non-zero JWKs and algs.
            newContentAndFilteredJwksAlgs = doHmac ? this.loadContentAndFilterJwksAlgsHmac() : this.loadContentAndFilterJwksAlgsPkc();
        } catch (Throwable t) {
            final String msg = "Failed to reload JWKs, doHmac: {" + doHmac + "}";
            LOGGER.error(msg, t);
            return false;
        }
        // TODO Do we need empty checks
        if ((Strings.hasText(oldContentAndFilteredJwksAlgs.jwksContent) == false)
            && (Strings.hasText(newContentAndFilteredJwksAlgs.jwksContent) == false)
            && (oldContentAndFilteredJwksAlgs.jwksContent.equals(newContentAndFilteredJwksAlgs.jwksContent))) {
            LOGGER.debug("Reload JWKs succeeded but content did not change, doHmac: {}", doHmac);
            return false;
        }
        // Accept the updated JWKs
        if (doHmac) {
            this.contentAndFilteredJwksAlgsHmac = newContentAndFilteredJwksAlgs;
        } else {
            this.contentAndFilteredJwksAlgsPkc = newContentAndFilteredJwksAlgs;
        }
        LOGGER.debug(
            "Reload JWKs succeeded and content changed, doHmac: {}. JWKs old: [{}], new: [{}]. Algorithms old: [{}] new: [{}].",
            doHmac,
            oldContentAndFilteredJwksAlgs.filteredJwksAlgs.jwks.size(),
            String.join(",", oldContentAndFilteredJwksAlgs.filteredJwksAlgs.algs()),
            newContentAndFilteredJwksAlgs.filteredJwksAlgs.jwks.size(),
            String.join(",", newContentAndFilteredJwksAlgs.filteredJwksAlgs.algs())
        );
        // ASSUMPTION: Either PKC JWKs or HMAC JWKs were replaced, so invalidate all JWT cache entries which used old replaced JWKs
        // Enhancement idea: Separate HMAC and PKC entries into separate caches, since only HMAC or PKC entries need to be invalidated.
        // Enhancement idea: Identify which JWKs were removed, and only invalidate affected entries. Entries using retained keys remain.
        this.invalidateJwtCache();
        this.verifyNonEmptyHmacAndPkcJwksAlgs(); // throws SettingsException if reload causes filtered PKC+HMAC JWKs and Algs to drop to 0
        return true;
    }
}
