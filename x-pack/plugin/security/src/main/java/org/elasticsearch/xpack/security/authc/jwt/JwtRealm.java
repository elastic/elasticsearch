/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class JwtRealm extends Realm implements CachingRealm, Releasable {
    private static final Logger LOGGER = LogManager.getLogger(JwtRealm.class);

    final ThreadPool threadPool;
    final SSLService sslService;
    final UserRoleMapper userRoleMapper;
    final ResourceWatcherService resourceWatcherService;

    // JWT issuer settings
    final String allowedIssuer;
    final List<String> allowedSignatureAlgorithms;
    final TimeValue allowedClockSkew;
    final String jwkSetPath;
    final SecureString jwkSetContentsHmac;
    final List<String> allowedAudiences;
    // JWT end-user settings
    final Boolean populateUserMetadata;
    final ClaimParser claimParserPrincipal;
    final ClaimParser claimParserGroups;
    // JWT client settings
    final String clientAuthorizationType;
    final SecureString clientAuthorizationSharedSecret;
    // JWT cache settings
    final TimeValue cacheTtl;
    final Integer cacheMaxUsers;
    // Standard HTTP settings for outgoing connections to get JWT issuer jwkset_path
    final TimeValue httpConnectTimeout;
    final TimeValue httpConnectionReadTimeout;
    final TimeValue httpSocketTimeout;
    final Integer httpMaxConnections;
    final Integer httpMaxEndpointConnections;
    // Standard HTTP proxy settings for outgoing connections to get JWT issuer jwkset_path
    final String httpProxyScheme;
    final int httpProxyPort;
    final String httpProxyHost;

    // Helpers: Constructor creates these members
    final Hasher hasher;
    final Cache<String, CacheResult> cache;
    final CloseableHttpAsyncClient httpClient;
    final JWKSet jwkSetHmac;
    final JWKSet jwkSetPkc;

    // initialize sets this value, not the constructor, because all realms objects need to be constructed before linking any delegates
    DelegatedAuthorizationSupport delegatedAuthorizationSupport;

    // usage stats
    final AtomicLong counterTokenFail = new AtomicLong(0);
    final AtomicLong counterTokenSuccess = new AtomicLong(0);
    final AtomicLong counterAuthenticateFail = new AtomicLong(0);
    final AtomicLong counterAuthenticateSuccess = new AtomicLong(0);
    final AtomicLong counterCacheGetFail = new AtomicLong(0);
    final AtomicLong counterCacheGetOkWarning = new AtomicLong(0);
    final AtomicLong counterCacheGetOkNoWarning = new AtomicLong(0);
    final AtomicLong counterCacheAdd = new AtomicLong(0);

    // Only JwtRealmTest.addJwtRealm() uses these package private members, to generate test JWTs from these test JWKs and Users.
    List<JWK> testIssuerJwks = null;
    Map<String, User> testIssuerUsers = null;

    public JwtRealm(
        final RealmConfig realmConfig,
        final ThreadPool threadPool,
        final SSLService sslService,
        final UserRoleMapper userRoleMapper,
        final ResourceWatcherService resourceWatcherService
    ) throws SettingsException {
        super(realmConfig);

        // constructor saves parameters
        this.threadPool = threadPool;
        this.sslService = sslService;
        this.userRoleMapper = userRoleMapper;
        this.resourceWatcherService = resourceWatcherService;

        // JWT issuer settings
        this.allowedIssuer = realmConfig.getSetting(JwtRealmSettings.ALLOWED_ISSUER);
        this.allowedSignatureAlgorithms = realmConfig.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
        this.allowedClockSkew = realmConfig.getSetting(JwtRealmSettings.ALLOWED_CLOCK_SKEW);
        this.jwkSetPath = realmConfig.getSetting(JwtRealmSettings.JWKSET_PKC_PATH);
        this.jwkSetContentsHmac = realmConfig.getSetting(JwtRealmSettings.JWKSET_HMAC_CONTENTS);

        // JWT audience settings
        this.allowedAudiences = realmConfig.getSetting(JwtRealmSettings.ALLOWED_AUDIENCES);

        // JWT end-user settings
        this.claimParserPrincipal = ClaimParser.forSetting(LOGGER, JwtRealmSettings.CLAIMS_PRINCIPAL, realmConfig, true);
        this.claimParserGroups = ClaimParser.forSetting(LOGGER, JwtRealmSettings.CLAIMS_GROUPS, realmConfig, false);
        this.populateUserMetadata = realmConfig.getSetting(JwtRealmSettings.POPULATE_USER_METADATA);

        // JWT client settings
        this.clientAuthorizationType = realmConfig.getSetting(JwtRealmSettings.CLIENT_AUTHORIZATION_TYPE);
        this.clientAuthorizationSharedSecret = realmConfig.getSetting(JwtRealmSettings.CLIENT_AUTHORIZATION_SHARED_SECRET);

        // JWT cache settings
        this.cacheTtl = realmConfig.getSetting(JwtRealmSettings.CACHE_TTL);
        this.cacheMaxUsers = realmConfig.getSetting(JwtRealmSettings.CACHE_MAX_USERS);
        final String cacheHashAlgo = realmConfig.getSetting(JwtRealmSettings.CACHE_HASH_ALGO);

        // Standard HTTP settings for outgoing connections to get JWT issuer jwkset_path
        this.httpConnectTimeout = realmConfig.getSetting(JwtRealmSettings.HTTP_CONNECT_TIMEOUT);
        this.httpConnectionReadTimeout = realmConfig.getSetting(JwtRealmSettings.HTTP_CONNECTION_READ_TIMEOUT);
        this.httpSocketTimeout = realmConfig.getSetting(JwtRealmSettings.HTTP_SOCKET_TIMEOUT);
        this.httpMaxConnections = realmConfig.getSetting(JwtRealmSettings.HTTP_MAX_CONNECTIONS);
        this.httpMaxEndpointConnections = realmConfig.getSetting(JwtRealmSettings.HTTP_MAX_ENDPOINT_CONNECTIONS);

        // Standard HTTP proxy settings for outgoing connections to get JWT issuer jwkset_path
        this.httpProxyScheme = realmConfig.getSetting(JwtRealmSettings.HTTP_PROXY_SCHEME);
        this.httpProxyPort = realmConfig.getSetting(JwtRealmSettings.HTTP_PROXY_PORT);
        this.httpProxyHost = realmConfig.getSetting(JwtRealmSettings.HTTP_PROXY_HOST);

        // constructor derives these members
        this.hasher = Hasher.resolve(cacheHashAlgo);
        if (this.cacheTtl.getNanos() > 0) {
            this.cache = CacheBuilder.<String, CacheResult>builder()
                .setExpireAfterWrite(this.cacheTtl)
                .setMaximumWeight(this.cacheMaxUsers)
                .build();
        } else {
            this.cache = null;
        }

        // Validate Client Authorization settings. Throw SettingsException there was a problem.
        JwtUtil.validateClientAuthorizationSettings(
            RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHORIZATION_TYPE),
            this.clientAuthorizationType,
            RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHORIZATION_SHARED_SECRET),
            this.clientAuthorizationSharedSecret
        );

        // If set, get contents of URL or file. If set, throw SettingsException if there was a problem.
        final byte[] jwkSetContentsPkc;
        final URI jwkSetPathPkcUri = JwtUtil.parseHttpsUriNoException(this.jwkSetPath);
        if (jwkSetPathPkcUri == null) {
            this.httpClient = null;
            jwkSetContentsPkc = JwtUtil.readFileContents(
                RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_PKC_PATH),
                this.jwkSetPath,
                super.config.env()
            );
        } else {
            this.httpClient = JwtUtil.createHttpClient(realmConfig, sslService);
            jwkSetContentsPkc = JwtUtil.readUriContents(
                RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_PKC_PATH),
                jwkSetPathPkcUri,
                this.httpClient
            );
        }

        // Validate JWKSet contents. Throw SettingsException there was a problem.
        final Tuple<JWKSet, JWKSet> jwkSets = JwtUtil.validateJwkSets(
            RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS),
            this.allowedSignatureAlgorithms,
            RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_HMAC_CONTENTS),
            (this.jwkSetContentsHmac == null) ? null : this.jwkSetContentsHmac.toString(),
            RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_PKC_PATH),
            (jwkSetContentsPkc == null) ? null : new String(jwkSetContentsPkc, StandardCharsets.UTF_8)
        );
        this.jwkSetHmac = jwkSets.v1();
        this.jwkSetPkc = jwkSets.v2();
    }

    public void ensureInitialized() {
        if (this.delegatedAuthorizationSupport == null) {
            throw new IllegalStateException("Realm has not been initialized");
        }
    }

    @Override
    public void initialize(final Iterable<Realm> allRealms, final XPackLicenseState xpackLicenseState) {
        LOGGER.trace("Initializing realm [" + super.name() + "]");
        if (this.delegatedAuthorizationSupport != null) {
            throw new IllegalStateException("Realm " + super.name() + " has already been initialized");
        }
        // extract list of realms referenced by super.config.settings() value for DelegatedAuthorizationSettings.AUTHZ_REALMS
        this.delegatedAuthorizationSupport = new DelegatedAuthorizationSupport(allRealms, super.config, xpackLicenseState);
    }

    @Override
    public void close() {
        LOGGER.trace("Closing realm [" + super.name() + "]");
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
        JwtAuthenticationToken jwtAuthenticationToken = null;
        try {
            this.ensureInitialized();
            final SecureString authorizationParameterValue = JwtUtil.getHeaderSchemeParameters(
                threadContext,
                JwtRealmSettings.HEADER_END_USER_AUTHORIZATION,
                JwtRealmSettings.HEADER_END_USER_AUTHORIZATION_SCHEME,
                false
            );
            if (authorizationParameterValue == null) {
                return null; // Could not find non-empty SchemeParameters in HTTP header "Authorization: Bearer <SchemeParameters>"
            }

            // Get all other possible parameters. A different JWT realm may do the actual authentication.
            final SecureString clientAuthorizationSharedSecretValue = JwtUtil.getHeaderSchemeParameters(
                threadContext,
                JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION,
                JwtRealmSettings.CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET,
                true
            );

            jwtAuthenticationToken = new JwtAuthenticationToken(authorizationParameterValue, clientAuthorizationSharedSecretValue);
        } finally {
            if (jwtAuthenticationToken == null) {
                this.counterTokenFail.incrementAndGet();
            } else {
                this.counterTokenSuccess.incrementAndGet();
            }
        }
        return jwtAuthenticationToken;
    }

    @Override
    public boolean supports(final AuthenticationToken jwtAuthenticationToken) {
        return (jwtAuthenticationToken instanceof JwtAuthenticationToken);
    }

    @Override
    public void authenticate(final AuthenticationToken authenticationToken, final ActionListener<AuthenticationResult<User>> listener) {
        boolean authenticateSuccess = false;
        try {
            this.ensureInitialized();
            if (authenticationToken instanceof JwtAuthenticationToken jwtAuthenticationToken) {
                final String tokenPrincipal = jwtAuthenticationToken.principal();
                final SignedJWT signedJwt = jwtAuthenticationToken.getSignedJwt();
                final JWSAlgorithm jwsAlgorithm = signedJwt.getHeader().getAlgorithm();
                LOGGER.trace("Realm [{}] received JwtAuthenticationToken for tokenPrincipal [{}].", super.name(), tokenPrincipal);

                final SecureString endUserSignedJwt = jwtAuthenticationToken.getEndUserSignedJwt();
                final SecureString clientAuthorizationSharedSecret = jwtAuthenticationToken.getClientAuthorizationSharedSecret();
                if (this.cache != null) {
                    CacheResult cacheResult = null;
                    boolean same = false;
                    try {
                        cacheResult = this.cache.get(tokenPrincipal);
                        if (cacheResult != null) {
                            same = cacheResult.verifySameCredentials(endUserSignedJwt, clientAuthorizationSharedSecret);
                            LOGGER.debug(
                                "Realm [" + super.name() + "] cache hit for tokenPrincipal=[" + tokenPrincipal + "], same=[" + same + "]."
                            );
                            listener.onResponse(cacheResult.get());
                            return; // finally sets authenticateSuccess=true during return
                        }
                        LOGGER.trace("Realm [" + super.name() + "] cache miss for tokenPrincipal=[" + tokenPrincipal + "].");
                    } finally {
                        if (cacheResult == null) {
                            this.counterCacheGetFail.incrementAndGet();
                        } else {
                            if (same == false) {
                                this.counterCacheGetOkWarning.incrementAndGet();
                            } else {
                                this.counterCacheGetOkNoWarning.incrementAndGet();
                            }
                            authenticateSuccess = true; // finally sets authenticateSuccess=true during return
                        }
                    }
                }

                try {
                    JwtUtil.validateClientAuthorization(
                        this.clientAuthorizationType,
                        this.clientAuthorizationSharedSecret,
                        clientAuthorizationSharedSecret
                    );
                } catch (Exception e) {
                    final String msg = "Realm [" + super.name() + "] client validation failed for tokenPrincipal [" + tokenPrincipal + "].";
                    LOGGER.debug(msg, e);
                    listener.onResponse(AuthenticationResult.unsuccessful(msg, e));
                    return;
                }

                try {
                    JwtUtil.validateSignedJwt(
                        signedJwt,
                        JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC.contains(jwsAlgorithm.getName())
                            ? this.jwkSetHmac
                            : this.jwkSetPkc,
                        this.allowedIssuer,
                        this.allowedAudiences,
                        this.allowedSignatureAlgorithms,
                        this.allowedClockSkew.seconds()
                    );
                } catch (Exception e) {
                    final String msg = "Realm [" + super.name() + "] JWT validation failed for tokenPrincipal [" + tokenPrincipal + "].";
                    LOGGER.debug(msg, e);
                    listener.onResponse(AuthenticationResult.unsuccessful(msg, e));
                    return;
                }

                // Extract claims for UserRoleMapper.UserData and User constructors.
                final JWTClaimsSet jwtClaimsSet = jwtAuthenticationToken.getJwtClaimsSet();
                final Map<String, Object> jwtClaimsMap = jwtClaimsSet.getClaims();

                // Principal claim is mandatory
                final String principal = this.claimParserPrincipal.getClaimValue(jwtClaimsSet);
                final String messageAboutPrincipalClaim = "Realm ["
                    + super.name()
                    + "] got principal claim ["
                    + principal
                    + "] using parser ["
                    + this.claimParserPrincipal.getClaimName()
                    + "]. JWTClaimsSet is "
                    + jwtClaimsMap
                    + ".";
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
                        + "]. JWTClaimsSet is "
                        + jwtClaimsMap
                        + "."
                );

                // Metadata is optional
                final Map<String, Object> userMetadata = this.populateUserMetadata ? jwtClaimsMap : Map.of();
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
                                new CacheResult(result, endUserSignedJwt, clientAuthorizationSharedSecret, hasher)
                            );
                            this.counterCacheAdd.incrementAndGet();
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
                    authenticateSuccess = true; // authenticated successfully with authz roles lookup
                    return;
                }

                // Handle role mapping in JWT Realm.
                final AtomicBoolean roleMappingOk = new AtomicBoolean(false);
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
                        this.cache.put(tokenPrincipal, new CacheResult(result, endUserSignedJwt, clientAuthorizationSharedSecret, hasher));
                        this.counterCacheAdd.incrementAndGet();
                    }
                    listener.onResponse(result);
                    roleMappingOk.set(true); // authenticated successfully with authc role mapping
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
                authenticateSuccess = roleMappingOk.get(); // authenticated, but success depends on role mapping without an exception
            } else {
                final String className = (authenticationToken == null) ? "null" : authenticationToken.getClass().getCanonicalName();
                final String msg = "Realm [" + super.name() + "] does not support AuthenticationToken [" + className + "].";
                LOGGER.trace(msg);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
            }
        } finally {
            if (authenticateSuccess == false) {
                this.counterAuthenticateFail.incrementAndGet();
            } else {
                this.counterAuthenticateSuccess.incrementAndGet();
            }
        }
    }

    @Override
    public void usageStats(final ActionListener<Map<String, Object>> listener) {
        this.ensureInitialized();
        super.usageStats(ActionListener.wrap(usageStats -> {
            final LinkedHashMap<String, Object> token = new LinkedHashMap<>();
            token.put("miss", this.counterTokenFail.get());
            token.put("hit", this.counterTokenSuccess.get());
            usageStats.put("token", token);

            final LinkedHashMap<String, Object> authenticate = new LinkedHashMap<>();
            authenticate.put("miss", this.counterAuthenticateFail.get());
            authenticate.put("hit", this.counterAuthenticateSuccess.get());
            usageStats.put("authenticate", authenticate);

            final LinkedHashMap<String, Object> cache = new LinkedHashMap<>();
            cache.put("miss", this.counterCacheGetFail.get());
            cache.put("warn", this.counterCacheGetOkWarning.get());
            cache.put("hit", this.counterCacheGetOkNoWarning.get());
            cache.put("add", this.counterCacheAdd.get());
            cache.put("size", this.getCacheSize());
            usageStats.put("cache", cache);
            listener.onResponse(usageStats);
        }, listener::onFailure));
    }

    public int getCacheSize() {
        this.ensureInitialized();
        return (this.cache == null) ? -1 : this.cache.count();
    }

    public static class CacheResult {
        private final AuthenticationResult<User> authenticationResultUser;
        private final char[] hash; // even if tokenPrinciple matches, detect if JWT+SharedSecret changed

        public CacheResult(
            final AuthenticationResult<User> authenticationResultUser,
            final SecureString jwt,
            final SecureString clientSharedSecret,
            final Hasher hasher
        ) {
            assert authenticationResultUser != null : "AuthenticationResult must be non-null";
            assert authenticationResultUser.isAuthenticated() : "AuthenticationResult.isAuthenticated must be true";
            assert authenticationResultUser.getValue() != null : "AuthenticationResult.getValue=User must be non-null";
            assert jwt != null : "Cache key must be non-null";
            assert hasher != null : "Hasher must be non-null";
            this.authenticationResultUser = authenticationResultUser;
            this.hash = this.hash(jwt, clientSharedSecret, hasher);
        }

        public char[] hash(final SecureString jwt, final @Nullable SecureString clientSecret, final Hasher hasher) {
            return hasher.hash(JwtUtil.join("/", jwt, clientSecret));
        }

        public boolean verifySameCredentials(final SecureString jwt, final @Nullable SecureString clientSecret) {
            return Hasher.verifyHash(JwtUtil.join("/", jwt, clientSecret), this.hash);
        }

        public AuthenticationResult<User> get() {
            return this.authenticationResultUser;
        }
    }
}
