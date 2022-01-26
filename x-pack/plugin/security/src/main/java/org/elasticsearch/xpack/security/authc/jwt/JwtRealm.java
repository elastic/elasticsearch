/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.proc.DefaultJOSEObjectTypeVerifier;
import com.nimbusds.jose.proc.JOSEObjectTypeVerifier;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.ssl.SslConfiguration;
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

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class JwtRealm extends Realm implements CachingRealm, Releasable {

    private static final Logger LOGGER = LogManager.getLogger(JwtRealm.class);
    private static final JOSEObjectTypeVerifier<SecurityContext> JWT_HEADER_TYPE_VERIFIER = new DefaultJOSEObjectTypeVerifier<>(
        JOSEObjectType.JWT,
        null
    );

    // super constructor saves RealmConfig (use super.config). Contains: identifier, enabled, order, env, settings, threadContext

    // constructor saves parameters
    private final ThreadPool threadPool;
    private final SSLService sslService;
    private final UserRoleMapper userRoleMapper;
    private final ResourceWatcherService resourceWatcherService;

    // constructor looks up realm settings in super.config.settings()
    // JWT issuer settings
    private final String allowedIssuer;
    private final List<String> allowedSignatureAlgorithms;
    private final TimeValue allowedClockSkew;
    private final String jwkSetPath;
    private final SecureString jwkSetContentsHmac;
    private final List<String> allowedAudiences;
    // JWT end-user settings
    private final Boolean populateUserMetadata;
    private final ClaimParser principalAttribute;
    private final ClaimParser groupsAttribute;
    // JWT client settings
    private final String clientAuthorizationType;
    private final SecureString clientAuthorizationSharedSecret;
    // JWT cache settings
    private final TimeValue cacheTtl;
    private final Integer cacheMaxUsers;
    // Standard HTTP settings for outgoing connections to get JWT issuer jwkset_path
    private final TimeValue httpConnectTimeout;
    private final TimeValue httpConnectionReadTimeout;
    private final TimeValue httpSocketTimeout;
    private final Integer httpMaxConnections;
    private final Integer httpMaxEndpointConnections;
    // Standard HTTP proxy settings for outgoing connections to get JWT issuer jwkset_path
    private final String httpProxyScheme;
    private final int httpProxyPort;
    private final String httpProxyHost;

    // constructor derives members
    private final Hasher hasher;
    private final Cache<String, CacheResult> cache;
    private final CloseableHttpAsyncClient httpClient;
    private final JWKSet jwkSetHmac;
    private final JWKSet jwkSetPkc;

    // initialize sets this value, not the constructor, because all realms objects need to be constructed before linking any delegates
    private DelegatedAuthorizationSupport delegatedAuthorizationSupport;

    // stats
    private final AtomicLong counterTokenFail = new AtomicLong(0);
    private final AtomicLong counterTokenSuccess = new AtomicLong(0);
    private final AtomicLong counterAuthenticateFail = new AtomicLong(0);
    private final AtomicLong counterAuthenticateSuccess = new AtomicLong(0);
    private final AtomicLong counterCacheGetFail = new AtomicLong(0);
    private final AtomicLong counterCacheGetOkWarning = new AtomicLong(0);
    private final AtomicLong counterCacheGetOkNoWarning = new AtomicLong(0);
    private final AtomicLong counterCacheAdd = new AtomicLong(0);

    public JwtRealm(
        final RealmConfig realmConfig,
        final ThreadPool threadPool,
        final SSLService sslService,
        final UserRoleMapper userRoleMapper,
        final ResourceWatcherService resourceWatcherService
    ) throws SettingsException {
        // super constructor saves RealmConfig (use super.config). Contains: identifier, enabled, order, env, settings, threadContext
        super(realmConfig);

        // constructor saves parameters
        this.threadPool = threadPool;
        this.sslService = sslService;
        this.userRoleMapper = userRoleMapper;
        this.resourceWatcherService = resourceWatcherService;

        // constructor looks up realm settings in super.config.settings()
        // JWT issuer settings
        // RealmConfig realmConfig = super.config;
        this.allowedIssuer = realmConfig.getSetting(JwtRealmSettings.ALLOWED_ISSUER);
        this.allowedSignatureAlgorithms = realmConfig.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
        this.allowedClockSkew = realmConfig.getSetting(JwtRealmSettings.ALLOWED_CLOCK_SKEW);
        this.jwkSetPath = realmConfig.getSetting(JwtRealmSettings.JWKSET_PKC_PATH);
        this.jwkSetContentsHmac = realmConfig.getSetting(JwtRealmSettings.JWKSET_HMAC_CONTENTS);

        // JWT audience settings
        this.allowedAudiences = realmConfig.getSetting(JwtRealmSettings.ALLOWED_AUDIENCES);

        // JWT end-user settings
        this.principalAttribute = ClaimParser.forSetting(LOGGER, JwtRealmSettings.CLAIMS_PRINCIPAL, realmConfig, true);
        this.groupsAttribute = ClaimParser.forSetting(LOGGER, JwtRealmSettings.CLAIMS_GROUPS, realmConfig, false);
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
        final URI jwkSetPathPkcUri = JwtUtil.parseUriNoException(this.jwkSetPath);
        if (jwkSetPathPkcUri == null) {
            this.httpClient = null;
            jwkSetContentsPkc = JwtUtil.readFileContents(
                RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_PKC_PATH),
                this.jwkSetPath,
                super.config.env()
            );
        } else {
            this.httpClient = JwtRealm.createHttpClient(realmConfig, sslService);
            jwkSetContentsPkc = JwtUtil.readUrlContents(
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

    @Override
    public void initialize(final Iterable<Realm> allRealms, final XPackLicenseState xpackLicenseState) {
        if (this.delegatedAuthorizationSupport != null) {
            throw new IllegalStateException("Realm has already been initialized");
        }
        // extract list of realms referenced by super.config.settings() value for DelegatedAuthorizationSettings.AUTHZ_REALMS
        this.delegatedAuthorizationSupport = new DelegatedAuthorizationSupport(allRealms, super.config, xpackLicenseState);
    }

    @Override
    public boolean supports(final AuthenticationToken jwtAuthenticationToken) {
        this.ensureInitialized();
        return (jwtAuthenticationToken instanceof JwtAuthenticationToken);
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
                JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET,
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
                    JwtRealm.validateClientAuthorization(
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
                    JwtRealm.validateSignedJwt(
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
                final String principal = this.principalAttribute.getClaimValue(jwtClaimsSet);
                final String messageAboutPrincipalClaim = "Realm ["
                    + super.name()
                    + "] got principal claim ["
                    + principal
                    + "] using parser ["
                    + this.principalAttribute.getName()
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
                final List<String> groups = this.groupsAttribute.getClaimValues(jwtClaimsSet);
                LOGGER.trace(
                    "Realm ["
                        + super.name()
                        + "] principal ["
                        + principal
                        + "] got groups ["
                        + (groups == null ? "null" : String.join(",", groups))
                        + "] using parser ["
                        + (this.groupsAttribute.getName() == null ? "null" : this.groupsAttribute.getName())
                        + "]. JWTClaimsSet is "
                        + jwtClaimsMap
                        + "."
                );

                // DN, fullName, and email claims not supported
                final String dn = null;
                final String fullName = null;
                final String email = null;

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
                        this.cache.put(tokenPrincipal, new CacheResult(result, endUserSignedJwt, clientAuthorizationSharedSecret, hasher));
                        this.counterCacheAdd.incrementAndGet();
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
                        this.cache.invalidate(tokenPrincipal);
                        listener.onFailure(e);
                    }));
                    authenticateSuccess = true; // authenticated successfully with authz roles lookup
                    return;
                }

                // Handle role mapping in JWT Realm.
                final AtomicBoolean roleMappingOk = new AtomicBoolean(false);
                final UserRoleMapper.UserData userData = new UserRoleMapper.UserData(principal, dn, groups, userMetadata, super.config);
                this.userRoleMapper.resolveRoles(userData, ActionListener.wrap(setOfRoles -> {
                    // Intercept the role mapper listener response to log the resolved roles here. Empty is OK.
                    assert setOfRoles != null : "JWT role mapping should return non-null set of roles.";
                    final String[] roles = new TreeSet<>(setOfRoles).toArray(new String[setOfRoles.size()]);
                    LOGGER.debug(
                        "Realm ["
                            + super.name()
                            + "] principal ["
                            + principal
                            + "] dn ["
                            + dn
                            + "] groups ["
                            + (groups == null ? "null" : String.join(",", groups))
                            + "] metadata ["
                            + userMetadata
                            + "] got mapped roles ["
                            + Arrays.toString(roles)
                            + "]."
                    );
                    final User user = new User(principal, roles, fullName, email, userMetadata, true);
                    final AuthenticationResult<User> result = AuthenticationResult.success(user);
                    this.cache.put(tokenPrincipal, new CacheResult(result, endUserSignedJwt, clientAuthorizationSharedSecret, hasher));
                    this.counterCacheAdd.incrementAndGet();
                    listener.onResponse(result);
                    roleMappingOk.set(true); // authenticated successfully with authc role mapping
                }, e -> {
                    LOGGER.debug(
                        "Realm ["
                            + super.name()
                            + "] principal ["
                            + principal
                            + "] dn ["
                            + dn
                            + "] groups ["
                            + (groups == null ? "null" : String.join(",", groups))
                            + "] metadata ["
                            + userMetadata
                            + "] failed to get mapped roles.",
                        e
                    );
                    this.cache.invalidate(tokenPrincipal);
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

    public static boolean validateClientAuthorization(final String type, final SecureString expectedSecret, final SecureString actualSecret)
        throws Exception {
        switch (type) {
            case JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET:
                if (Strings.hasText(actualSecret) == false) {
                    throw new Exception("Rejected client authentication for type [" + type + "] due to no secret.");
                } else if (expectedSecret.equals(actualSecret) == false) {
                    throw new Exception("Rejected client authentication for type [" + type + "] due to secret mismatch.");
                }
                break;
            case JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION_TYPE_NONE:
            default:
                if (Strings.hasText(actualSecret)) {
                    throw new Exception("Rejected client authentication for type [" + type + "] due to present secret.");
                }
                break;
        }
        return false;
    }

    public static boolean validateSignedJwt(
        final SignedJWT signedJwt,
        final JWKSet jwkSet,
        final String allowedIssuer,
        final List<String> allowedAudiences,
        final List<String> allowedSignatureAlgorithms,
        final long allowedClockSkewSeconds
    ) throws Exception {
        final JWSHeader jwsHeader = signedJwt.getHeader();
        final JWSAlgorithm jwtHeaderAlgorithm = jwsHeader.getAlgorithm();
        final JOSEObjectType jwtHeaderType = jwsHeader.getType();
        final String jwsHeaderKeyID = jwsHeader.getKeyID();

        // Header "typ" must be "jwt" or null
        try {
            JWT_HEADER_TYPE_VERIFIER.verify(jwtHeaderType, null);
        } catch (Exception e) {
            throw new Exception("Invalid JWT type [" + jwtHeaderType + "].", e);
        }
        // Header "alg" must be in the allowed list
        if ((jwtHeaderAlgorithm == null) || (allowedSignatureAlgorithms.contains(jwtHeaderAlgorithm.getName()) == false)) {
            throw new Exception(
                "Rejected signature algorithm ["
                    + jwtHeaderAlgorithm
                    + "]. Allowed signature algorithms are ["
                    + String.join(",", allowedSignatureAlgorithms)
                    + "]"
            );
        }

        // Validate claims
        final JWTClaimsSet jwtClaimsSet = signedJwt.getJWTClaimsSet();
        final Date now = new Date();
        final Date iat = jwtClaimsSet.getIssueTime();
        final Date exp = jwtClaimsSet.getExpirationTime();
        final Date nbf = jwtClaimsSet.getIssueTime();
        final Date auth_time = jwtClaimsSet.getDateClaim("auth_time");
        final String issuer = jwtClaimsSet.getIssuer();
        final List<String> audiences = jwtClaimsSet.getAudience();
        LOGGER.info(
            "Validating JWT now="
                + now
                + ", typ="
                + jwtHeaderType
                + ", alg="
                + jwtHeaderAlgorithm
                + ", kid="
                + jwsHeaderKeyID
                + ", auth_time="
                + auth_time
                + ", iat="
                + iat
                + ", nbf="
                + nbf
                + ", exp="
                + exp
                + ", issuer="
                + issuer
                + ", audiences="
                + audiences
        );
        // Expected sequence of time claims: auth_time (opt), iat (req), nbf (opt), exp (req)
        JwtUtil.validateJwtAuthTime(allowedClockSkewSeconds, now, auth_time);
        JwtUtil.validateJwtIssuedAtTime(allowedClockSkewSeconds, now, iat);
        JwtUtil.validateJwtNotBeforeTime(allowedClockSkewSeconds, now, nbf);
        JwtUtil.validateJwtExpiredTime(allowedClockSkewSeconds, now, exp);

        if ((issuer == null) || (allowedIssuer.equals(issuer) == false)) {
            throw new Exception("Rejected issuer [" + issuer + "]. Allowed issuer is [" + allowedIssuer + "]");
        } else if ((audiences == null) || (allowedAudiences.stream().anyMatch(audiences::contains) == false)) {
            throw new Exception("Rejected audiences [" + audiences + "]. Allowed audiences are [" + allowedAudiences + "]");
        }

        // Filter list of JWKs by header (ex: "alg", "kid", "use")
        // final ImmutableJWKSet immutableJWKSet = new ImmutableJWKSet(jwkSet);
        // final JWSVerificationKeySelector jwsVerificationKeySelector = new JWSVerificationKeySelector(jwtHeaderAlgorithm,
        // immutableJWKSet);
        // final List<JWK> jwks;
        // try {
        // jwks = jwsVerificationKeySelector.selectJWSKeys(jwsHeader, null); // context for JWKSecurityContextJWKSet
        // } catch (Exception e) {
        // throw new Exception("Header did not match any JWKs due to an exception.", e);
        // }
        final List<JWK> jwks = jwkSet.getKeys();
        if (jwks.isEmpty()) {
            throw new Exception("Header did not match any JWKs.");
        }

        // Try each JWK to see if it can validate the SignedJWT signature
        boolean verifiedSignature = false;
        final Exception verifySignatureException = new Exception("JWT signature validation failed.");
        for (final JWK jwk : jwks) {
            try {
                final JWSVerifier jwsVerifier = JwtUtil.createJwsVerifier(jwk);
                if (verifiedSignature = signedJwt.verify(jwsVerifier)) {
                    break;
                }
            } catch (Exception e) {
                verifySignatureException.addSuppressed(e);
            }
        }
        if (verifiedSignature == false) {
            throw verifySignatureException;
        }

        // final ClientID clientId = null;
        // final Issuer expectedIssuer = new Issuer(this.allowedIssuer);
        // final IDTokenValidator idTokenValidator = new IDTokenValidator(expectedIssuer, clientId, jwsVerificationKeySelector, null);
        // idTokenValidator.setMaxClockSkew((int)this.allowedClockSkew.seconds());
        // try {
        // idTokenValidator.validate(signedJwt, null);
        // } catch (Exception e) {
        // e.printStackTrace();
        // }
        return false;
    }

    @Override
    public void expire(final String username) {
        this.ensureInitialized();
        if (this.cache != null) {
            LOGGER.trace("invalidating cache for user [{}] in realm [{}]", username, name());
            this.cache.invalidate(username);
        }
    }

    @Override
    public void expireAll() {
        this.ensureInitialized();
        if (this.cache != null) {
            LOGGER.trace("invalidating cache for all users in realm [{}]", name());
            this.cache.invalidateAll();
        }
    }

    @Override
    public void close() {
        this.ensureInitialized();
        this.expireAll();
    }

    @Override
    public void lookupUser(final String username, final ActionListener<User> listener) {
        this.ensureInitialized();
        listener.onResponse(null); // Run-As and Delegated Authorization are not supported
    }

    @Override
    public void usageStats(final ActionListener<Map<String, Object>> listener) {
        this.ensureInitialized();
        super.usageStats(ActionListener.wrap(usageStats -> {
            final LinkedHashMap<String, Object> token = new LinkedHashMap<>();
            token.put("miss", this.counterTokenFail.get());
            token.put("ok", this.counterTokenSuccess.get());
            usageStats.put("token", token);

            final LinkedHashMap<String, Object> authenticate = new LinkedHashMap<>();
            authenticate.put("miss", this.counterAuthenticateFail.get());
            authenticate.put("ok", this.counterAuthenticateSuccess.get());
            usageStats.put("authenticate", authenticate);

            final LinkedHashMap<String, Object> cache = new LinkedHashMap<>();
            authenticate.put("miss", this.counterCacheGetFail.get());
            authenticate.put("warn", this.counterCacheGetOkWarning.get());
            authenticate.put("ok", this.counterCacheGetOkNoWarning.get());
            authenticate.put("add", this.counterCacheAdd.get());
            authenticate.put("size", this.getCacheSize());
            usageStats.put("cache", cache);
            listener.onResponse(usageStats);
        }, listener::onFailure));
    }

    public void ensureInitialized() {
        if (this.delegatedAuthorizationSupport == null) {
            throw new IllegalStateException("Realm has not been initialized");
        }
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
            final boolean hashMatched = Hasher.verifyHash(JwtUtil.join("/", jwt, clientSecret), this.hash);
            return Boolean.valueOf(hashMatched); // tokenPrincipal matched, Boolean indicates if hash matched too
        }

        public AuthenticationResult<User> get() {
            return this.authenticationResultUser;
        }
    }

    /**
     * Creates a {@link CloseableHttpAsyncClient} that uses a {@link PoolingNHttpClientConnectionManager}
     */
    public static CloseableHttpAsyncClient createHttpClient(final RealmConfig realmConfig, final SSLService sslService) {
        try {
            SpecialPermission.check();
            return AccessController.doPrivileged((PrivilegedExceptionAction<CloseableHttpAsyncClient>) () -> {
                final String realmConfigPrefixSslSettings = RealmSettings.realmSslPrefix(realmConfig.identifier());
                final SslConfiguration elasticsearchSslConfig = sslService.getSSLConfiguration(realmConfigPrefixSslSettings);

                final int tcpConnectTimeoutMillis = (int) realmConfig.getSetting(JwtRealmSettings.HTTP_CONNECT_TIMEOUT).getMillis();
                final int tcpConnectionReadTimeoutSec = (int) realmConfig.getSetting(JwtRealmSettings.HTTP_CONNECTION_READ_TIMEOUT)
                    .getSeconds();
                final int tcpSocketTimeout = (int) realmConfig.getSetting(JwtRealmSettings.HTTP_SOCKET_TIMEOUT).getMillis();
                final int httpMaxEndpointConnections = realmConfig.getSetting(JwtRealmSettings.HTTP_MAX_ENDPOINT_CONNECTIONS);
                final int httpMaxConnections = realmConfig.getSetting(JwtRealmSettings.HTTP_MAX_CONNECTIONS);
                final String proxyScheme = realmConfig.getSetting(JwtRealmSettings.HTTP_PROXY_SCHEME);
                final String proxyAddress = realmConfig.getSetting(JwtRealmSettings.HTTP_PROXY_HOST);
                final int proxyPort = realmConfig.getSetting(JwtRealmSettings.HTTP_PROXY_PORT);

                final RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
                requestConfigBuilder.setConnectTimeout(tcpConnectTimeoutMillis)
                    .setConnectionRequestTimeout(tcpConnectionReadTimeoutSec)
                    .setSocketTimeout(tcpSocketTimeout);
                if (Strings.hasText(proxyAddress) && Strings.hasText(proxyScheme)) {
                    requestConfigBuilder.setProxy(new HttpHost(proxyAddress, proxyPort, proxyScheme));
                }
                final RegistryBuilder<SchemeIOSessionStrategy> sessionStrategyBuilder = RegistryBuilder.<SchemeIOSessionStrategy>create();
                //// final String realmConfigPrefixSslSettings = RealmSettings.realmSslPrefix(realmIdentifier);
                // final SSLContext sslContext = sslService.sslContext(elasticsearchSslConfig);
                // final HostnameVerifier hostnameVerifier = SSLService.getHostnameVerifier(elasticsearchSslConfig);
                // final SSLIOSessionStrategy sslIOSessionStrategy = new SSLIOSessionStrategy(sslContext, hostnameVerifier);
                final SSLIOSessionStrategy sslIOSessionStrategy = sslService.sslIOSessionStrategy(elasticsearchSslConfig);
                sessionStrategyBuilder.register("https", sslIOSessionStrategy);

                final PoolingNHttpClientConnectionManager httpClientConnectionManager = new PoolingNHttpClientConnectionManager(
                    new DefaultConnectingIOReactor(),
                    sessionStrategyBuilder.build()
                );
                httpClientConnectionManager.setDefaultMaxPerRoute(httpMaxEndpointConnections);
                httpClientConnectionManager.setMaxTotal(httpMaxConnections);

                final CloseableHttpAsyncClient httpAsyncClient = HttpAsyncClients.custom()
                    .setConnectionManager(httpClientConnectionManager)
                    .setDefaultRequestConfig(requestConfigBuilder.build())
                    .build();
                httpAsyncClient.start();
                return httpAsyncClient;
            });
        } catch (PrivilegedActionException e) {
            throw new IllegalStateException("Unable to create a HttpAsyncClient instance", e);
        }
    }

    /**
     * Use the HTTP Client to get URL content bytes up to N max bytes.
     * @param httpClient Configured HTTP/HTTPS client.
     * @param uri URI to download.
     * @param maxBytes Max bytes to read for the URI. Use Integer.MAX_VALUE to read all.
     * @return Byte array of the URI contents up to N max bytes.
     * @throws Exception Security exception or HTTP/HTTPS failure exception.
     */
    public static byte[] readBytes(final CloseableHttpAsyncClient httpClient, final URI uri, final int maxBytes) throws Exception {
        final PlainActionFuture<byte[]> plainActionFuture = PlainActionFuture.newFuture();
        try {
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                httpClient.execute(new HttpGet(uri), new FutureCallback<>() {
                    @Override
                    public void completed(final HttpResponse result) {
                        final HttpEntity entity = result.getEntity();
                        try (InputStream inputStream = entity.getContent()) {
                            plainActionFuture.onResponse(inputStream.readNBytes(maxBytes));
                        } catch (Exception e) {
                            plainActionFuture.onFailure(e);
                        }
                    }

                    @Override
                    public void failed(Exception e) {
                        plainActionFuture.onFailure(new ElasticsearchSecurityException("Get [" + uri + "] failed.", e));
                    }

                    @Override
                    public void cancelled() {
                        plainActionFuture.onFailure(new ElasticsearchSecurityException("Get [" + uri + "] was cancelled."));
                    }
                });
                return null;
            });
        } catch (Exception e) {
            throw new ElasticsearchSecurityException("Get [" + uri + "] failed.", e);
        }
        return plainActionFuture.actionGet();
    }
}
