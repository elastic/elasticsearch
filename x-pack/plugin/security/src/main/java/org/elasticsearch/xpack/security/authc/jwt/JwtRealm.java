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
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
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
import java.util.Optional;

/**
 * JWT realms supports JWTs as bearer tokens for authenticating to Elasticsearch.
 * For security, it is recommended to authenticate the client too.
 */
public class JwtRealm extends Realm implements CachingRealm, Releasable {
    private static final Logger LOGGER = LogManager.getLogger(JwtRealm.class);

    record JwksAlgs(List<JWK> jwks, List<String> algs) {}

    public static final String HEADER_END_USER_AUTHENTICATION = "Authorization";
    public static final String HEADER_CLIENT_AUTHENTICATION = "X-Client-Authentication";
    public static final String HEADER_END_USER_AUTHENTICATION_SCHEME = "Bearer";

    final UserRoleMapper userRoleMapper;
    final String allowedIssuer;
    final List<String> allowedAudiences;
    final String jwkSetPath;
    final CloseableHttpAsyncClient httpClient;
    final JwtRealm.JwksAlgs jwksAlgsHmac;
    JwtRealm.JwksAlgs jwksAlgsPkc; // reloadable
    final TimeValue allowedClockSkew;
    final Boolean populateUserMetadata;
    final ClaimParser claimParserPrincipal;
    final ClaimParser claimParserGroups;
    final String clientAuthenticationType;
    final SecureString clientAuthenticationSharedSecret;
    final Hasher hasher;
    final Cache<char[], Optional<AuthenticationResult<User>>> jwtValidationCache; // hash(JWT) => Optional<Result>
    final Cache<String, AuthenticationResult<User>> rolesLookupCache; // principal => Result
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
        this.hasher = Hasher.resolve(realmConfig.getSetting(JwtRealmSettings.JWT_VALIDATION_CACHE_HASH_ALGO));
        this.jwtValidationCache = this.buildJwtValidationCache();
        this.rolesLookupCache = this.buildRolesLookupCache();

        // Validate Client Authentication settings. Throw SettingsException there was a problem.
        JwtUtil.validateClientAuthenticationSettings(
            RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE),
            this.clientAuthenticationType,
            RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET),
            this.clientAuthenticationSharedSecret
        );

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

        this.jwksAlgsHmac = this.parseJwksAlgsHmac(); // not reloadable
        this.jwksAlgsPkc = this.parseJwksAlgsPkc(false); // reloadable
    }

    private Cache<char[], Optional<AuthenticationResult<User>>> buildJwtValidationCache() {
        if (super.config.getSetting(JwtRealmSettings.JWT_VALIDATION_CACHE_TTL).getNanos() > 0) {
            return CacheBuilder.<char[], Optional<AuthenticationResult<User>>>builder()
                .setExpireAfterWrite(super.config.getSetting(JwtRealmSettings.JWT_VALIDATION_CACHE_TTL))
                .setMaximumWeight(super.config.getSetting(JwtRealmSettings.JWT_VALIDATION_CACHE_MAX_USERS))
                .build();
        }
        return null;
    }

    private Cache<String, AuthenticationResult<User>> buildRolesLookupCache() {
        if (super.config.getSetting(JwtRealmSettings.ROLES_LOOKUP_CACHE_TTL).getNanos() > 0) {
            return CacheBuilder.<String, AuthenticationResult<User>>builder()
                .setExpireAfterWrite(super.config.getSetting(JwtRealmSettings.ROLES_LOOKUP_CACHE_TTL))
                .setMaximumWeight(super.config.getSetting(JwtRealmSettings.ROLES_LOOKUP_CACHE_MAX_USERS))
                .build();
        }
        return null;
    }

    // must call parseAlgsAndJwksHmac() before parseAlgsAndJwksPkc()
    private JwtRealm.JwksAlgs parseJwksAlgsHmac() {
        final SecureString hmacJwkSetContents = super.config.getSetting(JwtRealmSettings.HMAC_JWKSET);
        final SecureString hmacKeyContents = super.config.getSetting(JwtRealmSettings.HMAC_KEY);
        // HMAC Key vs HMAC JWKSet settings are mutually exclusive
        if (Strings.hasText(hmacJwkSetContents) && Strings.hasText(hmacKeyContents)) {
            throw new SettingsException("HMAC JWKSet and HMAC Key settings are not allowed at the same time.");
        } else if ((Strings.hasText(hmacJwkSetContents) == false) && (Strings.hasText(hmacKeyContents) == false)) {
            return new JwtRealm.JwksAlgs(Collections.emptyList(), Collections.emptyList()); // both empty OK, if PKC JWKSet non-empty
        }
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
        final JwtRealm.JwksAlgs jwksAlgsHmac = JwkValidateUtil.filterJwksAndAlgorithms(jwksHmac, algsHmac);
        LOGGER.debug("HMAC: JWKs [" + jwksAlgsHmac.jwks.size() + "]. Algorithms [" + String.join(",", jwksAlgsHmac.algs()) + "].");
        return jwksAlgsHmac;
    }

    private JwtRealm.JwksAlgs parseJwksAlgsPkc(final boolean isReload) {
        // ASSUME: parseJwksAlgsHmac() has been called at startup, before parseJwksAlgsPkc() during startup or reload
        assert this.jwksAlgsHmac != null : "HMAC not initialized, PKC validation not available";
        if (Strings.hasText(this.jwkSetPath) == false) {
            return new JwtRealm.JwksAlgs(Collections.emptyList(), Collections.emptyList());
        }
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
        final JwtRealm.JwksAlgs newJwksAlgsPkc = JwkValidateUtil.filterJwksAndAlgorithms(jwksPkc, algsPkc);
        LOGGER.debug("PKC: JWKs [" + newJwksAlgsPkc.jwks().size() + "]. Algorithms [" + String.join(",", newJwksAlgsPkc.algs()) + "].");

        // If HMAC has no content, PKC must have content. Fail hard during startup. Fail gracefully during reloads.
        if (((this.jwksAlgsHmac.algs.isEmpty()) && (newJwksAlgsPkc.jwks().isEmpty()))
            || ((this.jwksAlgsHmac.jwks.isEmpty()) && (newJwksAlgsPkc.algs().isEmpty()))) {
            if (isReload) {
                LOGGER.error("No usable PKC JWKs or algorithms. Realm authentication expected to fail until this is fixed.");
                return newJwksAlgsPkc;
            }
            throw new SettingsException("No usable PKC JWKs or algorithms. Realm authentication expected to fail until this is fixed.");
        }
        if (isReload) {
            // Only give delta feedback during reloads.
            if ((this.jwksAlgsPkc.jwks.isEmpty()) && (newJwksAlgsPkc.jwks().isEmpty() == false)) {
                LOGGER.info("PKC JWKs changed from none to [" + newJwksAlgsPkc.jwks().size() + "].");
            } else if ((this.jwksAlgsPkc.jwks.isEmpty() == false) && (newJwksAlgsPkc.jwks().isEmpty())) {
                LOGGER.warn("PKC JWKs changed from [" + this.jwksAlgsPkc.jwks.size() + "] to none.");
            } else if (this.jwksAlgsPkc.jwks.stream().sorted().toList().equals(newJwksAlgsPkc.jwks().stream().sorted().toList())) {
                LOGGER.debug("PKC JWKs changed from [" + this.jwksAlgsPkc.jwks.size() + "] to [" + newJwksAlgsPkc.jwks().size() + "].");
            } else {
                LOGGER.trace("PKC JWKs no change from [" + this.jwksAlgsPkc.algs + "].");
            }
            if ((newJwksAlgsPkc.jwks().isEmpty()) && (newJwksAlgsPkc.algs().isEmpty() == false)) {
                LOGGER.info("PKC algorithms changed from no usable content to having usable content " + newJwksAlgsPkc.algs() + ".");
            } else if ((this.jwksAlgsPkc.algs.isEmpty() == false) && (newJwksAlgsPkc.algs().isEmpty())) {
                LOGGER.warn("PKC algorithms changed from having usable content " + this.jwksAlgsPkc.algs + " to no usable content.");
            } else if (this.jwksAlgsPkc.algs.stream().sorted().toList().equals(newJwksAlgsPkc.algs().stream().sorted().toList())) {
                LOGGER.debug("PKC algorithms changed from usable content " + this.jwksAlgsHmac.algs + " to " + newJwksAlgsPkc.algs() + ".");
            } else {
                LOGGER.trace("PKC algorithms did not change from usable content " + this.jwksAlgsHmac.algs + ".");
            }
        }
        return newJwksAlgsPkc;
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
     * Clean up user cache (if enabled).
     * Clean up HTTPS client cache (if enabled).
     */
    @Override
    public void close() {
        if (this.jwtValidationCache != null) {
            try {
                this.jwtValidationCache.invalidateAll();
            } catch (Exception e) {
                LOGGER.warn("Exception invalidating JWT cache for realm [" + super.name() + "]", e);
            }
        }
        if (this.rolesLookupCache != null) {
            try {
                this.rolesLookupCache.invalidateAll();
            } catch (Exception e) {
                LOGGER.warn("Exception invalidating JWT cache for realm [" + super.name() + "]", e);
            }
        }
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
        if (this.rolesLookupCache != null) {
            LOGGER.trace("Invalidating user cache entry [" + username + "] for realm [" + super.name() + "]");
            this.rolesLookupCache.invalidate(username);
        }
    }

    @Override
    public void expireAll() {
        this.ensureInitialized();
        if (this.jwtValidationCache != null) {
            LOGGER.trace("Invalidating JWT cache for realm [" + super.name() + "]");
            this.jwtValidationCache.invalidateAll();
        }
        if (this.rolesLookupCache != null) {
            LOGGER.trace("Invalidating user cache for realm [" + super.name() + "]");
            this.rolesLookupCache.invalidateAll();
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
        // Get all other possible parameters. A different JWT realm may do the actual authentication.
        final SecureString clientAuthenticationSharedSecretValue = JwtUtil.getHeaderValue(
            threadContext,
            JwtRealm.HEADER_CLIENT_AUTHENTICATION,
            JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE_SHARED_SECRET,
            true
        );
        return new JwtAuthenticationToken(authenticationParameterValue, clientAuthenticationSharedSecretValue);
    }

    @Override
    public boolean supports(final AuthenticationToken jwtAuthenticationToken) {
        return (jwtAuthenticationToken instanceof JwtAuthenticationToken);
    }

    /**
     * If JwtAuthenticationToken, perform authentication of the client credential and signed JWT.
     *
     * Client authentication use cases:
     *  - Type None => All clients allowed. Log warning if secret is present.
     *  - Type SharedSecret => Only accept match. Always reject missing or mismatch.
     *
     * JWT authentication use cases with JWT Validation Cache (Pass||SigOnly||Fail) and Roles Cache (Pass|Fail):
     *  - JWT Hit(Pass|Fail) => DONE
     *  - JWT Hit(SigOnly) => Roles Hit(Pass|Fail) => DONE
     *  - JWT Hit(SigOnly) => Roles Miss => Roles Pass|Fail [Roles Cache(Pass|Fail), JWT Cache(Pass|Fail)] => DONE
     *  - JWT Miss => JWT Fail [JWT Cache(Fail)] => DONE
     *  - JWT Miss => JWT Pass [JWT Cache(SigOnly)] => Roles Hit [JWT Cache(Pass|Fail)] => DONE
     *  - JWT Miss => JWT Pass [JWT Cache(SigOnly)] => Roles Miss => Roles Pass|Fail [Roles Cache(Pass|Fail), JWT Cache(Pass|Fail)] => DONE
     *
     * JWT Cache(Pass) means JWT validation succeeded and authorization succeeded. Result is immediately known.
     * JWT Cache(SigOnly) means JWT validation succeeded, but authorization failed. If Roles cache miss, retry authz (ex: remote lookup).
     * JWT Cache(Fail) means JWT validation failed. Result is immediately known.
     * Note: If authorization always works (ex: local lookup) you will never see any Cache(SigOnly), only Cache(Pass|Fail).
     *
     * Roles Cache(Pass) means authorization succeeded. Could be role mapping or delegated authorization.
     * Roles Cache(Fail) means authorization failed. Role mapping failures do not go away. Delegation authorization failures may go away.
     *
     * @param authenticationToken Only JwtAuthenticationToken is accepted.
     * @param listener  The listener to pass the authentication result to
     */
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

            // JWT cache: Use cases are Off, Miss, Hit(Pass|Fail), or Hit(partial) which means JWT passed but authz failed.
            final SecureString serializedJwt = jwtAuthenticationToken.getEndUserSignedJwt();
            final char[] jwtValidationCacheKey; // If new, miss both caches, so roles lookup needs to insert Pass|Fail into both caches.
            final boolean jwtCacheMissOrOff; // False JWT Cache(SigOnly). JWT re-validation can be skipped, but authz can be retried.
            if (this.jwtValidationCache == null) {
                jwtValidationCacheKey = null;
                jwtCacheMissOrOff = true; // JWT Cache Off. Fall through. Always validate all JWTs.
            } else {
                jwtValidationCacheKey = this.hasher.hash(serializedJwt);
                final Optional<AuthenticationResult<User>> hit = this.jwtValidationCache.get(jwtValidationCacheKey);
                if (hit == null) {
                    LOGGER.trace("Realm [" + super.name() + "] JWT cache miss for token=[" + tokenPrincipal + "].");
                    jwtCacheMissOrOff = true; // JWT Cache Miss. Fall through. Only validate new JWTs, reissued JWTs, or old evicted JWTs.
                } else if (hit.isEmpty()) {
                    jwtCacheMissOrOff = false; // Fall through. Skip JWT re-validation. Failed authz may be retried.
                    LOGGER.debug("Realm [" + super.name() + "] JWT cache hit [partial] for token=[" + tokenPrincipal + "].");
                } else {
                    final AuthenticationResult<User> result = hit.get();
                    final boolean pass = result.isAuthenticated();
                    final String msg = "Realm [" + super.name() + "] JWT cache hit [" + pass + "] for token=[" + tokenPrincipal + "].";
                    if (result.getException() != null) {
                        LOGGER.debug(msg, result.getException()); // JWT Cache Hit(Fail) w/ exception, Hit(Pass) exception not possible
                    } else if (pass == false) {
                        LOGGER.debug(msg); // JWT Cache Hit(Fail) no exception
                    } else {
                        LOGGER.trace(msg); // JWT Cache Hit(Pass)
                    }
                    listener.onResponse(result); // JWT Cache Hit(Pass|Fail)
                    return; // Done. JWT Cache Hit(Pass|Fail) doesn't need to fall through.
                }
            }

            // Parse JWT: Extract claims for logs and role-mapping.
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

            // Validate JWT: Only for JWT Cache(Miss) or Off. Not for Hit(SigOnly). Hit(Pass|Fail) never falls through to here.
            final String jwtAlg = jwt.getHeader().getAlgorithm().getName();
            if (jwtCacheMissOrOff) {
                try {
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
                    if (this.jwtValidationCache != null) {
                        this.jwtValidationCache.put(jwtValidationCacheKey, Optional.empty()); // JWT Cache Hit(SigOnly)
                    }
                } catch (Exception e) {
                    final String msg = "Realm [" + super.name() + "] JWT validation failed for token=[" + tokenPrincipal + "].";
                    final AuthenticationResult<User> failure = AuthenticationResult.unsuccessful(msg, e);
                    LOGGER.debug(msg, e);
                    if (this.jwtValidationCache != null) {
                        this.jwtValidationCache.put(jwtValidationCacheKey, Optional.of(failure)); // JWT Cache Hit(Fail)
                    }
                    listener.onResponse(failure); // FAIL
                    return; // Done. JWT Cache Hit(Fail) doesn't need to fall through.
                }
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
                final AuthenticationResult<User> unsuccessful = AuthenticationResult.unsuccessful(msg, null);
                LOGGER.debug(msg);
                if (this.jwtValidationCache != null) {
                    this.jwtValidationCache.put(jwtValidationCacheKey, Optional.of(unsuccessful)); // JWT Cache Hit(Fail)
                }
                listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
                return; // Done. JWT Cache Hit(Fail) doesn't need to fall through.
            }
            final List<String> groups = this.claimParserGroups.getClaimValues(claimsSet);
            final Map<String, Object> userMetadata;
            try {
                userMetadata = this.populateUserMetadata ? JwtUtil.toUserMetadata(jwt) : Map.of();
            } catch (Exception e) {
                final String msg = "Realm [" + super.name() + "] parse metadata failed for principal=[" + principal + "].";
                final AuthenticationResult<User> unsuccessful = AuthenticationResult.unsuccessful(msg, e);
                LOGGER.debug(msg, e);
                if (this.jwtValidationCache != null) {
                    this.jwtValidationCache.put(jwtValidationCacheKey, Optional.of(unsuccessful)); // JWT Cache Hit(Fail)
                }
                listener.onResponse(unsuccessful); // FAIL
                return; // Done. JWT Cache Hit(Fail) doesn't need to fall through.
            }

            // Delegated role lookup: No caching. If enabled, lookup in authz realms. Otherwise, fall through to JWT realm role mapping.
            if (this.delegatedAuthorizationSupport.hasDelegation()) {
                this.delegatedAuthorizationSupport.resolve(principal, ActionListener.wrap(success -> {
                    // Intercept the delegated authorization listener response to log roles and update cache. Empty roles is OK.
                    final User user = success.getValue();
                    final String rolesString = Arrays.toString(user.roles());
                    LOGGER.debug("Realm [" + super.name() + "] delegated roles [" + rolesString + "] for principal=[" + principal + "].");
                    if (this.jwtValidationCache != null) {
                        this.jwtValidationCache.put(jwtValidationCacheKey, Optional.of(success)); // JWT Cache Hit(Pass)
                    }
                    listener.onResponse(success); // Return SUCCESS
                }, e -> {
                    final String msg = "Realm [" + super.name() + "] delegated roles failed for principal=[" + principal + "].";
                    LOGGER.warn(msg, e);
                    final AuthenticationResult<User> fail = AuthenticationResult.unsuccessful(msg, e);
                    if (this.jwtValidationCache != null) {
                        this.jwtValidationCache.put(jwtValidationCacheKey, Optional.of(fail)); // New JWT Cache Hit(Fail)
                    }
                    listener.onResponse(fail); // Return FAIL
                }));
                return; // Done. User Cache Hit(Pass|Fail) doesn't need to fall through.
            }

            // User cache: Only fall through to here if JWT is valid. Only JWT Cache(Miss||SigOnly) or Off can reach here.
            if (this.rolesLookupCache != null) {
                final AuthenticationResult<User> result = this.rolesLookupCache.get(principal);
                if (result == null) {
                    LOGGER.trace("Realm [" + super.name() + "] User cache miss for principal=[" + principal + "].");
                } else {
                    final boolean authc = result.isAuthenticated();
                    final String msg = "Realm [" + super.name() + "] User cache hit [" + authc + "] for principal=[" + principal + "].";
                    LOGGER.debug(msg, result.getException());
                    if (this.jwtValidationCache != null) {
                        this.jwtValidationCache.put(jwtValidationCacheKey, Optional.of(result)); // JWT Cache Hit(Pass|Fail)
                    }
                    listener.onResponse(result); // SUCCESS or FAIL
                    return; // Done. User Cache Hit(Pass|Fail) doesn't need to fall through.
                }
            }

            // Role resolution: Handle role mapping in JWT Realm.
            final UserRoleMapper.UserData userData = new UserRoleMapper.UserData(principal, null, groups, userMetadata, super.config);
            this.userRoleMapper.resolveRoles(userData, ActionListener.wrap(rolesSet -> {
                // Intercept the role mapper listener response to log the resolved roles here. Empty is OK.
                final String[] rolesArray = rolesSet.toArray(new String[0]);
                final User user = new User(principal, rolesArray, null, null, userData.getMetadata(), true);
                final AuthenticationResult<User> success = AuthenticationResult.success(user);
                final String rolesString = Arrays.toString(rolesArray);
                LOGGER.debug("Realm [" + super.name() + "] mapped roles " + rolesString + " for principal=[" + principal + "].");
                if (this.rolesLookupCache != null) {
                    this.rolesLookupCache.put(principal, success); // User Cache Hit(Pass)
                }
                if (this.jwtValidationCache != null) {
                    this.jwtValidationCache.put(jwtValidationCacheKey, Optional.of(success)); // JWT Cache Hit(Pass)
                }
                listener.onResponse(success); // Return SUCCESS
            }, e -> {
                final String msg = "Realm [" + super.name() + "] mapped roles failed for principal=[" + principal + "].";
                LOGGER.warn(msg, e);
                final AuthenticationResult<User> fail = AuthenticationResult.unsuccessful(msg, e);
                if (this.rolesLookupCache != null) {
                    this.rolesLookupCache.put(principal, fail); // New User Cache Hit(Fail)
                }
                if (this.jwtValidationCache != null) {
                    this.jwtValidationCache.put(jwtValidationCacheKey, Optional.of(fail)); // New JWT Cache Hit(Fail), or replace
                                                                                           // Hit(SigOnly)
                }
                listener.onResponse(fail); // Return FAIL
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
        super.usageStats(ActionListener.wrap(stats -> {
            stats.put("cache", Collections.singletonMap("size", this.getCacheSize()));
            listener.onResponse(stats);
        }, listener::onFailure));
    }

    public int getCacheSize() {
        this.ensureInitialized();
        return (this.jwtValidationCache == null) ? -1 : this.jwtValidationCache.count();
    }
}
