/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSAlgorithm;
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
import org.elasticsearch.core.Tuple;
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
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * JWT realms supports JWTs as bearer tokens for authenticating to Elasticsearch. For security, it is recommended to use a client,
 * credential too; examples follow to illustrate why this is important.
 *
 * In OIDC workflows, end-users are clients of OIDC RP applications. End-users are asked to authenticate to an OIDC OP, and the OIDC RP
 * receives a JWT from the OIDC OP to identify the end-user. Potentially, all OIDC RPs can use JWTs as bearer tokens in Elasticsearch.
 * JWT audience filtering (i.e. OIDC RP client ID) is not sufficient when JWTs are treated as bearer tokens. OIDC RPs may share JWTs with
 * helper applications (ex: microservices), or use them for authenticating to other applications. Client authentication locks down
 * exactly which applications are allowed to be JWT bearer token clients of Elasticsearch.
 *
 * In bespoke JWT workflows, end-users may obtain a JWT directly, and use it as a bearer token in Elasticsearch and other applications.
 * Client authentication prevents those other applications from becoming potential JWT bearer token clients of Elasticsearch too.
 */
@SuppressWarnings("checkstyle:MissingJavadocMethod")
public class JwtRealm extends Realm implements CachingRealm, Releasable {
    private static final Logger LOGGER = LogManager.getLogger(JwtRealm.class);

    public static final String HEADER_END_USER_AUTHENTICATION = "Authorization";
    public static final String HEADER_CLIENT_AUTHENTICATION = "X-Client-Authentication";
    public static final String HEADER_END_USER_AUTHENTICATION_SCHEME = "Bearer";

    final UserRoleMapper userRoleMapper;
    final String allowedIssuer;
    final List<String> allowedAudiences;
    final List<String> algorithmsHmac;
    final List<JWK> jwksHmac;
    final String jwkSetPath;
    final CloseableHttpAsyncClient httpClient;
    List<String> algorithmsPkc; // reloadable
    List<JWK> jwksPkc; // reloadable
    final TimeValue allowedClockSkew;
    final Boolean populateUserMetadata;
    final ClaimParser claimParserPrincipal;
    final ClaimParser claimParserGroups;
    final String clientAuthenticationType;
    final SecureString clientAuthenticationSharedSecret;
    final Hasher hasher;
    final Cache<String, JwtRealmCacheValue> cache;
    DelegatedAuthorizationSupport delegatedAuthorizationSupport = null;

    public JwtRealm(final RealmConfig realmConfig, final SSLService sslService, final UserRoleMapper userRoleMapper)
        throws SettingsException {
        super(realmConfig);
        this.userRoleMapper = userRoleMapper;
        this.allowedIssuer = realmConfig.getSetting(JwtRealmSettings.ALLOWED_ISSUER);
        this.allowedAudiences = realmConfig.getSetting(JwtRealmSettings.ALLOWED_AUDIENCES);
        this.allowedClockSkew = realmConfig.getSetting(JwtRealmSettings.ALLOWED_CLOCK_SKEW);
        this.claimParserPrincipal = ClaimParser.forSetting(LOGGER, JwtRealmSettings.CLAIMS_PRINCIPAL, realmConfig, true);
        this.claimParserGroups = ClaimParser.forSetting(LOGGER, JwtRealmSettings.CLAIMS_GROUPS, realmConfig, false);
        this.populateUserMetadata = realmConfig.getSetting(JwtRealmSettings.POPULATE_USER_METADATA);
        this.clientAuthenticationType = realmConfig.getSetting(JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE);
        this.clientAuthenticationSharedSecret = realmConfig.getSetting(JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET);
        this.hasher = Hasher.resolve(realmConfig.getSetting(JwtRealmSettings.CACHE_HASH_ALGO));
        this.cache = this.initializeJwtCache();

        // Validate Client Authentication settings. Throw SettingsException there was a problem.
        JwtUtil.validateClientAuthenticationSettings(
            RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE),
            this.clientAuthenticationType,
            RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET),
            this.clientAuthenticationSharedSecret
        );

        // PKC JWKSet can be URL, file, or not set; only initialize HTTP client if PKC JWKSet is a URL.
        this.jwkSetPath = super.config.getSetting(JwtRealmSettings.JWKSET_PKC_PATH);
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

        final Tuple<List<String>, List<JWK>> algsAndJwksHmac = this.parseAlgsAndJwksHmac();
        this.algorithmsHmac = algsAndJwksHmac.v1(); // not reloadable
        this.jwksHmac = algsAndJwksHmac.v2(); // not reloadable

        final Tuple<List<String>, List<JWK>> algsAndJwksPkc = this.parseAlgsAndJwksPkc(false);
        this.algorithmsPkc = algsAndJwksPkc.v1(); // reloadable
        this.jwksPkc = algsAndJwksPkc.v2(); // reloadable
    }

    private Cache<String, JwtRealmCacheValue> initializeJwtCache() {
        final TimeValue cacheTtl = super.config.getSetting(JwtRealmSettings.CACHE_TTL);
        if (cacheTtl.getNanos() > 0) {
            final Integer cacheMaxUsers = super.config.getSetting(JwtRealmSettings.CACHE_MAX_USERS);
            return CacheBuilder.<String, JwtRealmCacheValue>builder().setExpireAfterWrite(cacheTtl).setMaximumWeight(cacheMaxUsers).build();
        }
        return null;
    }

    // must call parseAlgsAndJwksHmac() before parseAlgsAndJwksPkc()
    private Tuple<List<String>, List<JWK>> parseAlgsAndJwksHmac() {
        final SecureString jwkSetContentsHmac = super.config.getSetting(JwtRealmSettings.JWKSET_HMAC_CONTENTS);
        if (Strings.hasText(jwkSetContentsHmac) == false) {
            return new Tuple<>(Collections.emptyList(), Collections.emptyList());
        }
        List<JWK> jwksHmac; // Parse as JWKSet, or fall back to byte array
        try {
            jwksHmac = JwkValidateUtil.loadJwksFromJwkSetString(
                RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_HMAC_CONTENTS),
                Strings.hasText(jwkSetContentsHmac) ? jwkSetContentsHmac.toString() : null
            );
        } catch (Exception e) {
            final byte[] hmacKeyBytes = jwkSetContentsHmac.toString().getBytes(StandardCharsets.UTF_8);
            jwksHmac = Collections.singletonList(new OctetSequenceKey.Builder(hmacKeyBytes).build());
        }
        final List<String> algs = super.config.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
        final List<String> algsHmac = algs.stream().filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC::contains).toList();
        final Tuple<List<String>, List<JWK>> algsAndJwksHmac = JwkValidateUtil.filterJwksAndAlgorithms(jwksHmac, algsHmac);
        LOGGER.debug("HMAC: Algorithms " + algsAndJwksHmac.v1() + ". JWKs [" + algsAndJwksHmac.v2() + "].");
        return algsAndJwksHmac;
    }

    // must call parseAlgsAndJwksHmac() before parseAlgsAndJwksPkc()
    private Tuple<List<String>, List<JWK>> parseAlgsAndJwksPkc(final boolean isReload) {
        if (Strings.hasText(this.jwkSetPath) == false) {
            return new Tuple<>(Collections.emptyList(), Collections.emptyList());
        }
        // PKC JWKSet get contents from local file or remote HTTPS URL
        final byte[] jwkSetContentBytesPkc;
        if (this.httpClient == null) {
            jwkSetContentBytesPkc = JwtUtil.readFileContents(
                RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_PKC_PATH),
                this.jwkSetPath,
                super.config.env()
            );
        } else {
            final URI jwkSetPathPkcUri = JwtUtil.parseHttpsUri(this.jwkSetPath);
            jwkSetContentBytesPkc = JwtUtil.readUriContents(
                RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_PKC_PATH),
                jwkSetPathPkcUri,
                this.httpClient
            );
        }
        final String jwkSetContentsPkc = new String(jwkSetContentBytesPkc, StandardCharsets.UTF_8);

        // PKC JWKSet parse contents
        final List<JWK> jwksPkc = JwkValidateUtil.loadJwksFromJwkSetString(
            RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_PKC_PATH),
            jwkSetContentsPkc
        );

        // PKC JWKSet filter contents
        final List<String> algs = super.config.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
        final List<String> algsPkc = algs.stream().filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC::contains).toList();
        final Tuple<List<String>, List<JWK>> newAlgsAndJwksPkc = JwkValidateUtil.filterJwksAndAlgorithms(jwksPkc, algsPkc);
        LOGGER.debug("PKC: Algorithms " + newAlgsAndJwksPkc.v1() + ". JWKs [" + newAlgsAndJwksPkc.v2() + "].");

        // If HMAC has no content, PKC much have content. Fail hard during startup. Fail gracefully during reloads.
        if (((this.algorithmsHmac.isEmpty()) && (newAlgsAndJwksPkc.v1().isEmpty()))
            || ((this.jwksHmac.isEmpty()) && (newAlgsAndJwksPkc.v2().isEmpty()))) {
            if (isReload) {
                LOGGER.error("No usable PKC algorithms or JWKs. They are required when no usable HMAC algorithms or JWKs.");
                return new Tuple<>(this.algorithmsPkc, this.jwksPkc); // return previous settings
            }
            throw new SettingsException("No usable PKC algorithms or JWKs. They are required when no usable HMAC algorithms or JWKs.");
        }
        if (isReload) {
            // Only give delta feedback during reloads.
            if ((newAlgsAndJwksPkc.v1().isEmpty()) && (newAlgsAndJwksPkc.v1().isEmpty() == false)) {
                LOGGER.info("PKC algorithms changed from no usable content to having usable content " + newAlgsAndJwksPkc.v1() + ".");
            } else if ((this.algorithmsPkc.isEmpty() == false) && (newAlgsAndJwksPkc.v1().isEmpty())) {
                LOGGER.warn("PKC algorithms changed from having usable content " + this.algorithmsPkc + " to no usable content.");
            } else if (this.algorithmsPkc.stream().sorted().toList().equals(newAlgsAndJwksPkc.v1().stream().sorted().toList())) {
                LOGGER.debug("PKC algorithms changed from usable content " + this.algorithmsHmac + " to " + newAlgsAndJwksPkc.v1() + ".");
            } else {
                LOGGER.trace("PKC algorithms did not change from usable content " + this.algorithmsHmac + ".");
            }
            if ((this.jwksPkc.isEmpty()) && (newAlgsAndJwksPkc.v2().isEmpty() == false)) {
                LOGGER.info("PKC JWKs changed from none to [" + newAlgsAndJwksPkc.v2().size() + "].");
            } else if ((this.jwksPkc.isEmpty() == false) && (newAlgsAndJwksPkc.v2().isEmpty())) {
                LOGGER.warn("PKC JWKs changed from [" + this.jwksPkc.size() + "] to none.");
            } else if (this.jwksPkc.stream().sorted().toList().equals(newAlgsAndJwksPkc.v2().stream().sorted().toList())) {
                LOGGER.debug("PKC JWKs changed from [" + this.jwksPkc.size() + "] to [" + newAlgsAndJwksPkc.v2().size() + "].");
            } else {
                LOGGER.trace("PKC JWKs no change from [" + this.algorithmsHmac + "].");
            }
        }
        return newAlgsAndJwksPkc;
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
        // extract list of realms referenced by super.config.settings() value for DelegatedAuthorizationSettings.AUTHZ_REALMS
        this.delegatedAuthorizationSupport = new DelegatedAuthorizationSupport(allRealms, super.config, xpackLicenseState);
    }

    /**
     * Clean up cache (if enabled).
     * Clean up HTTPS client cache (if enabled).
     */
    @Override
    public void close() {
        if (this.cache != null) {
            try {
                this.cache.invalidateAll();
            } catch (Exception e) {
                LOGGER.warn("Exception invalidating cache for realm [" + super.name() + "]", e);
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
        listener.onResponse(null); // Run-As and Delegated Authorization are not supported
    }

    @Override
    public void expire(final String username) {
        this.ensureInitialized();
        if (this.cache != null) {
            LOGGER.trace("Invalidating cache for user [" + username + "] in realm [" + super.name() + "]");
            this.cache.invalidate(username);
        }
    }

    @Override
    public void expireAll() {
        this.ensureInitialized();
        if (this.cache != null) {
            LOGGER.trace("Invalidating cache for realm [" + super.name() + "]");
            this.cache.invalidateAll();
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
            return null; // Could not find non-empty SchemeParameters in HTTP header "Authorization: Bearer <SchemeParameters>"
        }

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

    @Override
    public void authenticate(final AuthenticationToken authenticationToken, final ActionListener<AuthenticationResult<User>> listener) {
        this.ensureInitialized();
        if (authenticationToken instanceof JwtAuthenticationToken jwtAuthenticationToken) {
            final String tokenPrincipal = jwtAuthenticationToken.principal();
            LOGGER.trace("Realm [{}] received JwtAuthenticationToken for tokenPrincipal [{}].", super.name(), tokenPrincipal);

            // Attempt to authenticate from the cache
            final SecureString endUserSignedJwt = jwtAuthenticationToken.getEndUserSignedJwt();
            final SecureString clientAuthenticationSharedSecret = jwtAuthenticationToken.getClientAuthenticationSharedSecret();
            if (this.cache != null) {
                JwtRealmCacheValue jwtRealmCacheValue = this.cache.get(tokenPrincipal);
                if (jwtRealmCacheValue != null) {
                    final boolean same = jwtRealmCacheValue.verifySameCredentials(endUserSignedJwt, clientAuthenticationSharedSecret);
                    LOGGER.debug(
                        "Realm [" + super.name() + "] cache hit for tokenPrincipal=[" + tokenPrincipal + "], same=[" + same + "]."
                    );
                    listener.onResponse(jwtRealmCacheValue.get());
                    return;
                }
                LOGGER.trace("Realm [" + super.name() + "] cache miss for tokenPrincipal=[" + tokenPrincipal + "].");
            }

            try {
                JwtUtil.validateClientAuthentication(
                    this.clientAuthenticationType,
                    this.clientAuthenticationSharedSecret,
                    clientAuthenticationSharedSecret
                );
            } catch (Exception e) {
                final String msg = "Realm [" + super.name() + "] client validation failed for tokenPrincipal [" + tokenPrincipal + "].";
                LOGGER.debug(msg, e);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, e));
                return;
            }

            final SignedJWT signedJwt;
            final JWTClaimsSet jwtClaimsSet;
            try {
                signedJwt = SignedJWT.parse(jwtAuthenticationToken.getEndUserSignedJwt().toString());
                jwtClaimsSet = signedJwt.getJWTClaimsSet();
            } catch (ParseException e) {
                throw new IllegalArgumentException("Failed to parse JWT", e);
            }

            final JWSAlgorithm jwsAlgorithm = signedJwt.getHeader().getAlgorithm();
            final boolean isHmacJwtAlg = JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC.contains(jwsAlgorithm.getName());
            try {
                JwtValidateUtil.validate(
                    signedJwt,
                    isHmacJwtAlg ? this.jwksHmac : this.jwksPkc,
                    this.allowedIssuer,
                    this.allowedAudiences,
                    isHmacJwtAlg ? this.algorithmsHmac : this.algorithmsPkc,
                    this.allowedClockSkew.seconds()
                );
            } catch (Exception e) {
                final String msg = "Realm [" + super.name() + "] JWT validation failed for tokenPrincipal [" + tokenPrincipal + "].";
                LOGGER.debug(msg, e);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, e));
                return;
            }

            // Stop using tokenPrincipal. We trust the JWT after the above check, so compute the actual principal from the claims now.

            // Principal claim is mandatory
            final String principal = this.claimParserPrincipal.getClaimValue(jwtClaimsSet);
            final String messageAboutPrincipalClaim = "Realm ["
                + super.name()
                + "] got principal claim ["
                + principal
                + "] using parser ["
                + this.claimParserPrincipal.getClaimName()
                + "].";
            if (principal == null) {
                LOGGER.debug(messageAboutPrincipalClaim);
                listener.onResponse(AuthenticationResult.unsuccessful(messageAboutPrincipalClaim, null));
                return;
            } else {
                LOGGER.trace(messageAboutPrincipalClaim);
            }

            // Groups claim is optional
            final List<String> groups = this.claimParserGroups.getClaimValues(jwtClaimsSet);
            LOGGER.trace(
                "Realm ["
                    + super.name()
                    + "] principal ["
                    + principal
                    + "] got groups ["
                    + (groups == null ? "null" : String.join(",", groups))
                    + "] using parser ["
                    + (this.claimParserGroups.getClaimName() == null ? "null" : this.claimParserGroups.getClaimName())
                    + "]."
            );

            // Metadata is optional
            final Map<String, Object> userMetadata = this.populateUserMetadata ? jwtClaimsSet.getClaims() : Map.of();
            LOGGER.trace(
                "Realm ["
                    + super.name()
                    + "] principal ["
                    + principal
                    + "] populateUserMetadata ["
                    + this.populateUserMetadata
                    + "] got metadata ["
                    + userMetadata
                    + "] from JWTClaimsSet."
            );

            // Delegate authorization to other realms. If enabled, do lookup here and return; don't fall through.
            if (this.delegatedAuthorizationSupport.hasDelegation()) {
                final String delegatedAuthorizationSupportDetails = this.delegatedAuthorizationSupport.toString();
                this.delegatedAuthorizationSupport.resolve(principal, ActionListener.wrap(result -> {
                    // Intercept the delegated authorization listener response to log the resolved roles here. Empty roles is OK.
                    assert result != null : "JWT delegated authz should return a non-null AuthenticationResult<User>";
                    final User user = result.getValue();
                    assert user != null : "JWT delegated authz should return a non-null User";
                    final String[] roles = user.roles();
                    assert roles != null : "JWT delegated authz should return non-null Roles";
                    LOGGER.debug(
                        "Realm ["
                            + super.name()
                            + "] principal ["
                            + principal
                            + "] got lookup roles ["
                            + Arrays.toString(roles)
                            + "] via delegated authorization ["
                            + delegatedAuthorizationSupportDetails
                            + "]"
                    );
                    if (this.cache != null) {
                        this.cache.put(
                            tokenPrincipal,
                            new JwtRealmCacheValue(result, endUserSignedJwt, clientAuthenticationSharedSecret, hasher)
                        );
                    }
                    listener.onResponse(result);
                }, e -> {
                    LOGGER.debug(
                        "Realm ["
                            + super.name()
                            + "] principal ["
                            + principal
                            + "] failed to get lookup roles via delegated authorization ["
                            + delegatedAuthorizationSupportDetails
                            + "].",
                        e
                    );
                    if (this.cache != null) {
                        this.cache.invalidate(tokenPrincipal);
                        listener.onFailure(e);
                    }
                }));
                return;
            }

            // Handle role mapping in JWT Realm.
            final UserRoleMapper.UserData userData = new UserRoleMapper.UserData(principal, null, groups, userMetadata, super.config);
            this.userRoleMapper.resolveRoles(userData, ActionListener.wrap(setOfRoles -> {
                // Intercept the role mapper listener response to log the resolved roles here. Empty is OK.
                assert setOfRoles != null : "JWT role mapping should return non-null set of roles.";
                final String[] roles = new TreeSet<>(setOfRoles).toArray(new String[setOfRoles.size()]);
                LOGGER.debug(
                    "Realm ["
                        + super.name()
                        + "] principal ["
                        + principal
                        + "] groups ["
                        + (groups == null ? "null" : String.join(",", groups))
                        + "] metadata ["
                        + userMetadata
                        + "] got mapped roles ["
                        + Arrays.toString(roles)
                        + "]."
                );
                final User user = new User(principal, roles, null, null, userMetadata, true);
                final AuthenticationResult<User> result = AuthenticationResult.success(user);
                if (this.cache != null) {
                    this.cache.put(
                        tokenPrincipal,
                        new JwtRealmCacheValue(result, endUserSignedJwt, clientAuthenticationSharedSecret, hasher)
                    );
                }
                listener.onResponse(result);
            }, e -> {
                LOGGER.debug(
                    "Realm ["
                        + super.name()
                        + "] principal ["
                        + principal
                        + "] groups ["
                        + (groups == null ? "null" : String.join(",", groups))
                        + "] metadata ["
                        + userMetadata
                        + "] failed to get mapped roles.",
                    e
                );
                if (this.cache != null) {
                    this.cache.invalidate(tokenPrincipal);
                }
                listener.onFailure(e);
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
        return (this.cache == null) ? -1 : this.cache.count();
    }
}
