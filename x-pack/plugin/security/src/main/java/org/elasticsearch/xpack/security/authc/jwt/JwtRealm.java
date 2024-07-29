/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.RotatableSecret;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
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
import org.elasticsearch.xpack.security.authc.support.ClaimParser;
import org.elasticsearch.xpack.security.authc.support.DelegatedAuthorizationSupport;
import org.elasticsearch.xpack.security.support.ReloadableSecurityComponent;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.join;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET;
import static org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings.CLIENT_AUTH_SHARED_SECRET_ROTATION_GRACE_PERIOD;

/**
 * JWT realms supports JWTs as bearer tokens for authenticating to Elasticsearch.
 * For security, it is recommended to authenticate the client too.
 */
public class JwtRealm extends Realm implements CachingRealm, ReloadableSecurityComponent, Releasable {

    private static final String LATEST_MALFORMED_JWT = "_latest_malformed_jwt";

    private static final Logger logger = LogManager.getLogger(JwtRealm.class);

    public static final String HEADER_END_USER_AUTHENTICATION = "Authorization";
    public static final String HEADER_CLIENT_AUTHENTICATION = "ES-Client-Authentication";
    public static final String HEADER_END_USER_AUTHENTICATION_SCHEME = "Bearer";

    private final Cache<BytesArray, ExpiringUser> jwtCache;
    private final CacheIteratorHelper<BytesArray, ExpiringUser> jwtCacheHelper;
    private final UserRoleMapper userRoleMapper;
    private final Boolean populateUserMetadata;
    private final ClaimParser claimParserPrincipal;
    private final ClaimParser claimParserGroups;
    private final ClaimParser claimParserDn;
    private final ClaimParser claimParserMail;
    private final ClaimParser claimParserName;
    private final JwtRealmSettings.ClientAuthenticationType clientAuthenticationType;
    private final RotatableSecret clientAuthenticationSharedSecret;
    private final JwtAuthenticator jwtAuthenticator;
    private final TimeValue allowedClockSkew;
    DelegatedAuthorizationSupport delegatedAuthorizationSupport = null;

    public JwtRealm(final RealmConfig realmConfig, final SSLService sslService, final UserRoleMapper userRoleMapper)
        throws SettingsException {
        super(realmConfig);
        this.userRoleMapper = userRoleMapper;
        this.userRoleMapper.clearRealmCacheOnChange(this);
        this.allowedClockSkew = realmConfig.getSetting(JwtRealmSettings.ALLOWED_CLOCK_SKEW);

        this.populateUserMetadata = realmConfig.getSetting(JwtRealmSettings.POPULATE_USER_METADATA);
        this.clientAuthenticationType = realmConfig.getSetting(JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE);
        this.clientAuthenticationSharedSecret = new RotatableSecret(
            realmConfig.getSetting(JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET)
        );
        // Validate Client Authentication settings. Throw SettingsException there was a problem.
        JwtUtil.validateClientAuthenticationSettings(
            RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE),
            this.clientAuthenticationType,
            RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET),
            this.clientAuthenticationSharedSecret
        );

        final TimeValue jwtCacheTtl = realmConfig.getSetting(JwtRealmSettings.JWT_CACHE_TTL);
        final int jwtCacheSize = realmConfig.getSetting(JwtRealmSettings.JWT_CACHE_SIZE);
        if (jwtCacheTtl.getNanos() > 0 && jwtCacheSize > 0) {
            this.jwtCache = CacheBuilder.<BytesArray, ExpiringUser>builder()
                .setExpireAfterWrite(jwtCacheTtl)
                .setMaximumWeight(jwtCacheSize)
                .build();
            this.jwtCacheHelper = new CacheIteratorHelper<>(this.jwtCache);
        } else {
            // TODO: log invalid cache settings
            this.jwtCache = null;
            this.jwtCacheHelper = null;
        }
        jwtAuthenticator = new JwtAuthenticator(realmConfig, sslService, this::expireAll);

        final Map<String, String> fallbackClaimNames = jwtAuthenticator.getFallbackClaimNames();

        this.claimParserPrincipal = ClaimParser.forSetting(
            logger,
            JwtRealmSettings.CLAIMS_PRINCIPAL,
            fallbackClaimNames,
            realmConfig,
            true
        );
        this.claimParserGroups = ClaimParser.forSetting(logger, JwtRealmSettings.CLAIMS_GROUPS, fallbackClaimNames, realmConfig, false);
        this.claimParserDn = ClaimParser.forSetting(logger, JwtRealmSettings.CLAIMS_DN, fallbackClaimNames, realmConfig, false);
        this.claimParserMail = ClaimParser.forSetting(logger, JwtRealmSettings.CLAIMS_MAIL, fallbackClaimNames, realmConfig, false);
        this.claimParserName = ClaimParser.forSetting(logger, JwtRealmSettings.CLAIMS_NAME, fallbackClaimNames, realmConfig, false);
    }

    /**
     * If X-pack licensing allows it, initialize delegated authorization support.
     * @param allRealms List of all realms containing authorization realms for this JWT realm.
     * @param xpackLicenseState X-pack license state.
     */
    @Override
    public void initialize(final Iterable<Realm> allRealms, final XPackLicenseState xpackLicenseState) {
        if (delegatedAuthorizationSupport != null) {
            throw new IllegalStateException("Realm " + name() + " has already been initialized");
        }
        // extract list of realms referenced by config.settings() value for DelegatedAuthorizationSettings.ROLES_REALMS
        delegatedAuthorizationSupport = new DelegatedAuthorizationSupport(allRealms, config, xpackLicenseState);
    }

    /**
     * Clean up JWT cache (if enabled).
     * Clean up HTTPS client cache (if enabled).
     */
    @Override
    public void close() {
        jwtAuthenticator.close();
    }

    @Override
    public void lookupUser(final String username, final ActionListener<User> listener) {
        ensureInitialized();
        listener.onResponse(null); // Run-As and Delegated Authorization lookups are not supported by JWT realms
    }

    @Override
    public void expire(final String username) {
        ensureInitialized();
        if (isCacheEnabled()) {
            logger.trace("Expiring JWT cache entries for realm [{}] principal=[{}]", name(), username);
            jwtCacheHelper.removeValuesIf(expiringUser -> expiringUser.user.principal().equals(username));
            logger.trace("Expired JWT cache entries for realm [{}] principal=[{}]", name(), username);
        }
    }

    @Override
    public void expireAll() {
        ensureInitialized();
        invalidateJwtCache();
    }

    @Override
    public AuthenticationToken token(final ThreadContext threadContext) {
        ensureInitialized();
        final SecureString userCredentials = JwtUtil.getHeaderValue(
            threadContext,
            JwtRealm.HEADER_END_USER_AUTHENTICATION,
            JwtRealm.HEADER_END_USER_AUTHENTICATION_SCHEME,
            false
        );
        SignedJWT signedJWT = parseSignedJWT(userCredentials, threadContext);
        if (signedJWT == null) {
            // this is not a valid JWT for ES realms, but custom realms can also consume the Bearer credentials scheme in their own format
            return null;
        }
        final SecureString clientCredentials = JwtUtil.getHeaderValue(
            threadContext,
            JwtRealm.HEADER_CLIENT_AUTHENTICATION,
            JwtRealmSettings.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME,
            true
        );
        return new JwtAuthenticationToken(signedJWT, JwtUtil.sha256(userCredentials), clientCredentials);
    }

    @Override
    public boolean supports(final AuthenticationToken jwtAuthenticationToken) {
        return (jwtAuthenticationToken instanceof JwtAuthenticationToken);
    }

    @Override
    public void authenticate(final AuthenticationToken authenticationToken, final ActionListener<AuthenticationResult<User>> listener) {
        ensureInitialized();
        if (authenticationToken instanceof JwtAuthenticationToken jwtAuthenticationToken) {
            final String tokenPrincipal = jwtAuthenticationToken.principal();

            // Authenticate client: If client authc off, fall through. Otherwise, only fall through if secret matched.
            final SecureString clientSecret = jwtAuthenticationToken.getClientAuthenticationSharedSecret();
            try {
                JwtUtil.validateClientAuthentication(
                    clientAuthenticationType,
                    clientAuthenticationSharedSecret,
                    clientSecret,
                    tokenPrincipal
                );
                logger.trace("Realm [{}] client authentication succeeded for token=[{}].", name(), tokenPrincipal);
            } catch (Exception e) {
                final String msg = "Realm [" + name() + "] client authentication failed for token=[" + tokenPrincipal + "].";
                logger.debug(msg, e);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, e));
                return; // FAILED (secret is missing or mismatched)
            }

            final BytesArray jwtCacheKey = isCacheEnabled()
                ? new BytesArray(new BytesRef(jwtAuthenticationToken.getUserCredentialsHash()), true)
                : null;
            if (jwtCacheKey != null) {
                final User cachedUser = tryAuthenticateWithCache(tokenPrincipal, jwtCacheKey);
                if (cachedUser != null) {
                    if (delegatedAuthorizationSupport.hasDelegation()) {
                        delegatedAuthorizationSupport.resolve(cachedUser.principal(), listener);
                    } else {
                        listener.onResponse(AuthenticationResult.success(cachedUser));
                    }
                    return;
                }
            }

            // Validate JWT: Extract JWT and claims set, and validate JWT.
            jwtAuthenticator.authenticate(jwtAuthenticationToken, ActionListener.wrap(claimsSet -> {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "Realm [{}] JWT validation success for token=[{}] with header [{}] and claimSet [{}]",
                        name(),
                        tokenPrincipal,
                        jwtAuthenticationToken.getSignedJWT().getHeader(),
                        jwtAuthenticationToken.getJWTClaimsSet()
                    );
                }
                processValidatedJwt(tokenPrincipal, jwtCacheKey, claimsSet, listener);
            }, ex -> {
                final String msg = "Realm ["
                    + name()
                    + "] JWT validation failed for token=["
                    + tokenPrincipal
                    + "] with header ["
                    + jwtAuthenticationToken.getSignedJWT().getHeader()
                    + "] and claimSet ["
                    + jwtAuthenticationToken.getJWTClaimsSet()
                    + "]";

                if (logger.isTraceEnabled()) {
                    logger.trace(msg, ex);
                } else {
                    logger.debug(msg + " Cause: " + ex.getMessage()); // only log the stack trace at trace level
                }
                // TODO: No point to continue to another realm if failure is ParseException
                listener.onResponse(AuthenticationResult.unsuccessful(msg, ex));
            }));

        } else {
            assert false : "should not happen";
            final String className = (authenticationToken == null) ? "null" : authenticationToken.getClass().getCanonicalName();
            final String msg = "Realm [" + name() + "] does not support AuthenticationToken [" + className + "].";
            logger.trace(msg);
            listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
        }
    }

    void ensureInitialized() {
        if (delegatedAuthorizationSupport == null) {
            throw new IllegalStateException("Realm has not been initialized");
        }
    }

    // Package private for testing
    RealmConfig getConfig() {
        return config;
    }

    // Package private for testing
    JwtAuthenticator getJwtAuthenticator() {
        return jwtAuthenticator;
    }

    private User tryAuthenticateWithCache(final String tokenPrincipal, final BytesArray jwtCacheKey) {
        final ExpiringUser expiringUser = jwtCache.get(jwtCacheKey);
        if (expiringUser == null) {
            logger.trace("Realm [" + name() + "] JWT cache miss token=[" + tokenPrincipal + "] key=[" + jwtCacheKey + "].");
        } else {
            final User user = expiringUser.user;
            final Date exp = expiringUser.exp; // claimsSet.getExpirationTime().getTime() + allowedClockSkew.getMillis()
            final String principal = user.principal();
            final Date now = new Date();
            final boolean cacheEntryNotExpired = now.getTime() < exp.getTime();
            logger.trace(
                "Realm [{}] JWT cache {} token=[{}] key=[{}] principal=[{}] exp=[{}] now=[{}].",
                name(),
                cacheEntryNotExpired ? "hit" : "exp",
                tokenPrincipal,
                jwtCacheKey,
                principal,
                exp,
                now
            );
            if (cacheEntryNotExpired) {
                return user;
            }
            // TODO: evict the entry
        }
        return null;
    }

    // package private for testing
    void processValidatedJwt(
        String tokenPrincipal,
        BytesArray jwtCacheKey,
        JWTClaimsSet claimsSet,
        ActionListener<AuthenticationResult<User>> listener
    ) {
        // At this point, JWT is validated. Parse the JWT claims using realm settings.
        final String principal = claimParserPrincipal.getClaimValue(claimsSet);
        if (Strings.hasText(principal) == false) {
            final String msg = "Realm ["
                + name()
                + "] no principal for token=["
                + tokenPrincipal
                + "] parser=["
                + claimParserPrincipal
                + "] claims=["
                + claimsSet
                + "].";
            logger.debug(msg);
            listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
            return;
        }

        // Roles listener: Log roles from delegated authz lookup or role mapping, and cache User if JWT cache is enabled.
        final ActionListener<AuthenticationResult<User>> logAndCacheListener = ActionListener.wrap(result -> {
            if (result.isAuthenticated()) {
                final User user = result.getValue();
                logger.trace(
                    () -> format("Realm [%s] roles resolved [%s] for principal=[%s].", name(), join(",", user.roles()), principal)
                );
                if (isCacheEnabled()) {
                    try (ReleasableLock ignored = jwtCacheHelper.acquireUpdateLock()) {
                        final long expWallClockMillis = claimsSet.getExpirationTime().getTime() + allowedClockSkew.getMillis();
                        jwtCache.put(jwtCacheKey, new ExpiringUser(result.getValue(), new Date(expWallClockMillis)));
                    }
                }
            }
            listener.onResponse(result);
        }, listener::onFailure);

        // Delegated role lookup or Role mapping: Use the above listener to log roles and cache User.
        if (delegatedAuthorizationSupport.hasDelegation()) {
            delegatedAuthorizationSupport.resolve(principal, logAndCacheListener);
            return;
        }

        // User metadata: If enabled, extract metadata from JWT claims set. Use it in UserRoleMapper.UserData and User constructors.
        final Map<String, Object> userMetadata = buildUserMetadata(claimsSet);

        // Role resolution: Handle role mapping in JWT Realm.
        final List<String> groups = claimParserGroups.getClaimValues(claimsSet);
        final String dn = claimParserDn.getClaimValue(claimsSet);
        final String mail = claimParserMail.getClaimValue(claimsSet);
        final String name = claimParserName.getClaimValue(claimsSet);
        final UserRoleMapper.UserData userData = new UserRoleMapper.UserData(principal, dn, groups, userMetadata, config);
        userRoleMapper.resolveRoles(userData, ActionListener.wrap(rolesSet -> {
            final User user = new User(principal, rolesSet.toArray(Strings.EMPTY_ARRAY), name, mail, userData.getMetadata(), true);
            logAndCacheListener.onResponse(AuthenticationResult.success(user));
        }, logAndCacheListener::onFailure));
    }

    @Override
    public void usageStats(final ActionListener<Map<String, Object>> listener) {
        ensureInitialized();
        super.usageStats(ActionListener.wrap(stats -> {
            stats.put("jwt.cache", Collections.singletonMap("size", isCacheEnabled() ? jwtCache.count() : -1));
            listener.onResponse(stats);
        }, listener::onFailure));
    }

    @Override
    public void reload(Settings settings) {
        final SecureString newClientSharedSecret = CLIENT_AUTHENTICATION_SHARED_SECRET.getConcreteSettingForNamespace(
            this.realmRef().getName()
        ).get(settings);

        JwtUtil.validateClientAuthenticationSettings(
            RealmSettings.getFullSettingKey(this.config, JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE),
            this.clientAuthenticationType,
            RealmSettings.getFullSettingKey(this.config, JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET),
            new RotatableSecret(newClientSharedSecret)
        );

        this.clientAuthenticationSharedSecret.rotate(
            newClientSharedSecret,
            config.getSetting(CLIENT_AUTH_SHARED_SECRET_ROTATION_GRACE_PERIOD)
        );
    }

    /**
     * Clean up JWT cache (if enabled).
     */
    private void invalidateJwtCache() {
        if (isCacheEnabled()) {
            try {
                logger.trace("Invalidating JWT cache for realm [{}]", name());
                try (ReleasableLock ignored = jwtCacheHelper.acquireForIterator()) {
                    jwtCache.invalidateAll();
                }
                logger.debug("Invalidated JWT cache for realm [{}]", name());
            } catch (Exception e) {
                // TODO: We should let the error bubble up instead of swallowing it
                logger.warn("Exception invalidating JWT cache for realm [" + name() + "]", e);
            }
        }
    }

    private boolean isCacheEnabled() {
        return jwtCache != null && jwtCacheHelper != null;
    }

    // package private for testing
    Cache<BytesArray, ExpiringUser> getJwtCache() {
        return jwtCache;
    }

    /**
     * Format and filter JWT contents as user metadata.
     * @param claimsSet Claims are supported. Claim keys are prefixed by "jwt_claim_".
     * @return Map of formatted and filtered values to be used as user metadata.
     */
    private Map<String, Object> buildUserMetadata(JWTClaimsSet claimsSet) {
        final HashMap<String, Object> metadata = new HashMap<>();
        metadata.put("jwt_token_type", jwtAuthenticator.getTokenType().value());
        if (populateUserMetadata) {
            claimsSet.getClaims()
                .entrySet()
                .stream()
                .filter(entry -> isAllowedTypeForClaim(entry.getValue()))
                .forEach(entry -> metadata.put("jwt_claim_" + entry.getKey(), entry.getValue()));
        }
        return Map.copyOf(metadata);
    }

    /**
     * Parses a {@link SignedJWT} from the provided {@param token}.
     * This internally, for the local thread, remembers the last **malformed** token parsed,
     * in order to avoid attempting to parse the same token multiple consecutive times (by different JWT realms in the chain).
     */
    private SignedJWT parseSignedJWT(SecureString token, ThreadContext threadContext) {
        if (Objects.equals(token, threadContext.getTransient(LATEST_MALFORMED_JWT))) {
            // already tried to parse this token and it didn't work
            return null;
        }
        SignedJWT signedJWT = JwtUtil.parseSignedJWT(token);
        if (signedJWT == null) {
            // this is a malformed JWT, update the latest malformed token reference
            threadContext.putTransient(LATEST_MALFORMED_JWT, token);
        }
        return signedJWT;
    }

    /**
     * JWTClaimsSet values are only allowed to be String, Boolean, Number, or Collection.
     * Collections are only allowed to contain String, Boolean, or Number.
     * Collections recursion is not allowed.
     * Maps are not allowed.
     * Nulls are not allowed.
     * @param value Claim value object.
     * @return True if the claim value is allowed, otherwise false.
     */
    private static boolean isAllowedTypeForClaim(final Object value) {
        return (value instanceof String
            || value instanceof Boolean
            || value instanceof Number
            || (value instanceof Collection
                && ((Collection<?>) value).stream().allMatch(e -> e instanceof String || e instanceof Boolean || e instanceof Number)));
    }

    // Cached authenticated users, and adjusted JWT expiration date (=exp+skew) for checking if the JWT expired before the cache entry
    record ExpiringUser(User user, Date exp) {
        ExpiringUser {
            Objects.requireNonNull(user, "User must not be null");
            Objects.requireNonNull(exp, "Expiration date must not be null");
        }
    }
}
