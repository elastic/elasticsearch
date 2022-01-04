/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
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

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JwtRealm extends Realm implements CachingRealm, Releasable {

    private static final Logger LOGGER = LogManager.getLogger(JwtRealm.class);

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
    private final TimeValue allowedClientSkew;
    private final String jwkSetPath;
    private final SecureString hmacSecretKey;
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
    private final String cacheHashAlgo;
    // Standard HTTP settings for outgoing connections to get JWT issuer jwkset_path
    private final TimeValue httpConnectTimeout;
    private final TimeValue httpConnectionReadTimeout;
    private final TimeValue httpSocketTimeout;
    private final Integer httpMaxConnections;
    private final Integer httpMaxEndpointConnections;

    // constructor derives members
    private final URL jwkSetPathUrl; // Non-null if jwkSetPath is set and starts with "https://"
    private final Path jwkSetPathObj; // Non-null if jwkSetPath is set and resolves to a local config file
    private final Cache<String, ListenableFuture<CachedAuthenticationSuccess>> cachedAuthenticationSuccesses;
    private final Hasher hasher;

    // initialize sets this value, not the constructor, because all realms objects need to be constructed before linking any delegates
    private DelegatedAuthorizationSupport delegatedRealms;

    public JwtRealm(
        final RealmConfig realmConfig,
        final ThreadPool threadPool,
        final SSLService sslService,
        final UserRoleMapper userRoleMapper,
        final ResourceWatcherService resourceWatcherService
    ) {
        // super constructor saves RealmConfig (use super.config). Contains: identifier, enabled, order, env, settings, threadContext
        super(realmConfig);

        // constructor saves parameters
        this.threadPool = threadPool;
        this.sslService = sslService;
        this.userRoleMapper = userRoleMapper;
        this.resourceWatcherService = resourceWatcherService;

        // constructor looks up realm settings in super.config.settings()
        // JWT issuer settings
        this.allowedIssuer = super.config.getSetting(JwtRealmSettings.ALLOWED_ISSUER);
        this.allowedSignatureAlgorithms = super.config.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
        this.allowedClientSkew = super.config.getSetting(JwtRealmSettings.ALLOWED_CLOCK_SKEW);
        this.jwkSetPath = super.config.getSetting(JwtRealmSettings.JWKSET_PATH);
        this.hmacSecretKey = super.config.getSetting(JwtRealmSettings.ISSUER_HMAC_SECRET_KEY);

        // JWT audience settings
        this.allowedAudiences = super.config.getSetting(JwtRealmSettings.ALLOWED_AUDIENCES);

        // JWT end-user settings
        this.principalAttribute = ClaimParser.forSetting(LOGGER, JwtRealmSettings.CLAIMS_PRINCIPAL, super.config, true);
        this.groupsAttribute = ClaimParser.forSetting(LOGGER, JwtRealmSettings.CLAIMS_GROUPS, super.config, false);
        this.populateUserMetadata = super.config.getSetting(JwtRealmSettings.POPULATE_USER_METADATA);

        // JWT client settings
        this.clientAuthorizationType = super.config.getSetting(JwtRealmSettings.CLIENT_AUTHORIZATION_TYPE);
        this.clientAuthorizationSharedSecret = super.config.getSetting(JwtRealmSettings.CLIENT_AUTHORIZATION_SHARED_SECRET);

        // JWT cache settings
        this.cacheTtl = super.config.getSetting(JwtRealmSettings.CACHE_TTL);
        this.cacheMaxUsers = super.config.getSetting(JwtRealmSettings.CACHE_MAX_USERS);
        this.cacheHashAlgo = super.config.getSetting(JwtRealmSettings.CACHE_HASH_ALGO);

        // Standard HTTP settings for outgoing connections to get JWT issuer jwkset_path
        this.httpConnectTimeout = super.config.getSetting(JwtRealmSettings.HTTP_CONNECT_TIMEOUT);
        this.httpConnectionReadTimeout = super.config.getSetting(JwtRealmSettings.HTTP_CONNECTION_READ_TIMEOUT);
        this.httpSocketTimeout = super.config.getSetting(JwtRealmSettings.HTTP_SOCKET_TIMEOUT);
        this.httpMaxConnections = super.config.getSetting(JwtRealmSettings.HTTP_MAX_CONNECTIONS);
        this.httpMaxEndpointConnections = super.config.getSetting(JwtRealmSettings.HTTP_MAX_ENDPOINT_CONNECTIONS);

        // constructor derives these members
        if (this.cacheTtl.getNanos() > 0) {
            this.cachedAuthenticationSuccesses = CacheBuilder.<String, ListenableFuture<CachedAuthenticationSuccess>>builder()
                .setExpireAfterWrite(this.cacheTtl)
                .setMaximumWeight(this.cacheMaxUsers)
                .build();
        } else {
            this.cachedAuthenticationSuccesses = null;
        }
        this.hasher = Hasher.resolve(this.cacheHashAlgo);

        // Validate Client Authorization Type and Client Authorization Credential format
        switch (this.clientAuthorizationType) {
            case JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET:
                // If type is "SharedSecret", the shared secret value must be set
                if (Strings.hasText(this.clientAuthorizationSharedSecret) == false) {
                    throw new SettingsException(
                        "Missing setting for ["
                            + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.CLIENT_AUTHORIZATION_SHARED_SECRET)
                            + "]. It is required when setting ["
                            + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.CLIENT_AUTHORIZATION_TYPE)
                            + "] is configured as ["
                            + JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET
                            + "]"
                    );
                }
                // If type is "SharedSecret", the shared secret value must Base64url-encoded
                try {
                    Base64.getUrlDecoder().decode(new String(this.hmacSecretKey.getChars()).getBytes(StandardCharsets.UTF_8));
                } catch (Exception e) {
                    throw new SettingsException(
                        "Base64Url-encoding is required for the Client Authorization Shared Secret ["
                            + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.CLIENT_AUTHORIZATION_SHARED_SECRET)
                            + "]"
                    );
                }
                break;
            case JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_NONE:
            default:
                // If type is "None", the shared secret value must not be set
                if (Strings.hasText(this.clientAuthorizationSharedSecret)) {
                    throw new SettingsException(
                        "Setting ["
                            + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.CLIENT_AUTHORIZATION_SHARED_SECRET)
                            + "] is not supported, because setting ["
                            + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.CLIENT_AUTHORIZATION_TYPE)
                            + "] is configured as ["
                            + JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_NONE
                            + "]"
                    );
                }
                break;
        }

        // Validate that at least one of JWT Set Path and HMAC Key Set are set. If HMAC Key Set, validate Base64Url-encoding.
        if (Strings.hasText(this.hmacSecretKey)) {
            try {
                Base64.getUrlDecoder().decode(new String(this.hmacSecretKey.getChars()).getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                throw new SettingsException(
                    "Base64Url-encoding is required for the Issuer HMAC Key  ["
                        + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.ISSUER_HMAC_SECRET_KEY)
                        + "]"
                );
            }
        } else if (Strings.hasText(this.jwkSetPath) == false) {
            throw new SettingsException(
                "At least one setting must be configured for ["
                    + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_PATH)
                    + "] or ["
                    + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.ISSUER_HMAC_SECRET_KEY)
                    + "]"
            );
        }
        // If JWK Set Path is configured, validate it is an HTTPS URL, or a local config file which exists and is non-empty.
        URL jwkSetPathUrlTemp = null;
        Path jwkSetPathObjTemp = null;
        if (Strings.hasText(this.jwkSetPath)) {
            if (this.jwkSetPath.startsWith("https://")) {
                try {
                    jwkSetPathUrlTemp = new URL(this.jwkSetPath);
                } catch (Exception e) {
                    throw new SettingsException(
                        "Invalid HTTPS URL for setting ["
                            + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_PATH)
                            + "]",
                        e
                    );
                }
            } else if (this.jwkSetPath.startsWith("http://")) {
                throw new SettingsException(
                    "Invalid JWK Set Path setting ["
                        + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_PATH)
                        + "]. HTTP not supported, use HTTPS."
                );
            } else {
                jwkSetPathObjTemp = realmConfig.env().configFile().resolve(this.jwkSetPath);
                try {
                    final String jwtSetPathContents = Files.readString(jwkSetPathObjTemp, StandardCharsets.UTF_8);
                    if (Strings.hasText(jwtSetPathContents) == false) {
                        throw new SettingsException(
                            "Empty JWK Set Path file for setting ["
                                + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_PATH)
                                + "]"
                        );
                    }
                } catch (SettingsException e) {
                    throw e; // re-throw inner SettingsException
                } catch (Exception e) {
                    throw new SettingsException(
                        "Invalid JWK Set Path setting ["
                            + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_PATH)
                            + "]",
                        e
                    );
                }
            }
        }
        this.jwkSetPathUrl = jwkSetPathUrlTemp;
        this.jwkSetPathObj = jwkSetPathObjTemp;

        // If Issuer HMAC Secret Key is set, at least one HMAC Signature Algorithm is required.
        // If at least one HMAC Signature Algorithm is set, Issuer HMAC Secret Key is required.
        final long countSecretKeySignatureAlgorithms = this.allowedSignatureAlgorithms.stream()
            .filter(JwtRealmSettings.SUPPORTED_SECRET_KEY_SIGNATURE_ALGORITHMS::contains)
            .count();
        if ((Strings.hasText(this.hmacSecretKey)) && (countSecretKeySignatureAlgorithms == 0)) {
            throw new SettingsException(
                "Issuer HMAC Key is configured in setting ["
                    + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.ISSUER_HMAC_SECRET_KEY)
                    + "], but no HMAC signature algorithms were found in setting ["
                    + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS)
                    + "]"
            );
        } else if ((countSecretKeySignatureAlgorithms > 0) && (Strings.hasText(this.hmacSecretKey) == false)) {
            throw new SettingsException(
                "HMAC signature algorithms were found in setting ["
                    + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS)
                    + "], but no Issuer HMAC Key is configured in setting ["
                    + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_PATH)
                    + "]"
            );
        }

        // If JWT Set Path is set, at least one Public Key Signature Algorithm is required.
        // If at least one Public Key Signature Algorithm is set, JWT Set Path is required.
        final long countPublicKeySignatureAlgorithms = this.allowedSignatureAlgorithms.stream()
            .filter(JwtRealmSettings.SUPPORTED_PUBLIC_KEY_SIGNATURE_ALGORITHMS::contains)
            .count();
        if ((Strings.hasText(this.jwkSetPath)) && (countPublicKeySignatureAlgorithms == 0)) {
            throw new SettingsException(
                "JWT Set Path is configured in setting ["
                    + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_PATH)
                    + "], but no public key signature algorithms were found in setting ["
                    + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS)
                    + "]"
            );
        } else if ((countPublicKeySignatureAlgorithms > 0) && (Strings.hasText(this.jwkSetPath) == false)) {
            throw new SettingsException(
                "Public key signature algorithms were found in setting ["
                    + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS)
                    + "], but no JWT Set Path is configured in setting ["
                    + RealmSettings.getFullSettingKey(super.config, JwtRealmSettings.JWKSET_PATH)
                    + "]"
            );
        }
    }

    @Override
    public void initialize(final Iterable<Realm> allRealms, final XPackLicenseState xpackLicenseState) {
        // initialize sets this value, not the constructor, because all realms objects need to be constructed before linking any delegates
        if (this.delegatedRealms == null) {
            this.delegatedRealms = new DelegatedAuthorizationSupport(allRealms, super.config, xpackLicenseState);
        }
        throw new IllegalStateException("Realm has already been initialized");
    }

    @Override
    public boolean supports(final AuthenticationToken jwtAuthenticationToken) {
        return (jwtAuthenticationToken instanceof JwtAuthenticationToken);
    }

    @Override
    public AuthenticationToken token(final ThreadContext threadContext) {
        final SecureString authorizationParameterValue = JwtRealm.getAuthorizationHeaderParameter(
            threadContext,
            JwtRealmSettings.HEADER_ENDUSER_AUTHORIZATION,
            JwtRealmSettings.HEADER_ENDUSER_AUTHORIZATION_SCHEME,
            false
        );
        if (authorizationParameterValue == null) {
            return null; // Could not find non-empty SchemeParameters in HTTP header "Authorization: Bearer <SchemeParameters>"
        }

        // Get all other possible parameters. A different JWT realm may do the actual authentication.
        final SecureString clientAuthorizationSharedSecretValue = JwtRealm.getAuthorizationHeaderParameter(
            threadContext,
            JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION,
            JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET,
            true
        );

        try {
            return new JwtAuthenticationToken(authorizationParameterValue, clientAuthorizationSharedSecretValue);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Could not construct JwtAuthenticationToken", e);
        }
    }

    @Override
    public void authenticate(final AuthenticationToken authenticationToken, final ActionListener<AuthenticationResult<User>> listener) {
        if (authenticationToken instanceof JwtAuthenticationToken jwtAuthenticationToken) {
            // TODO This method will be completed in a later PR

            // Filter steps (before any validation)
            // 1. Skip JWT if signature algorithm does not match any of the signature algorithms allowed by this realm.
            // 2. Skip JWT if issuer does not match the issuer allowed by this realm.
            // 3. Skip JWT if audience does not match any of the audiences allowed by this realm.

            // At this point, start validating. Trigger AuthenticationResult.unsuccessful() for any problem.

            // Cache lookup
            // 1. If present in cache, AuthenticationResult.successful().

            // Client Authorization
            switch (this.clientAuthorizationType) {
                case JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET:
                    if (this.clientAuthorizationSharedSecret.equals(jwtAuthenticationToken.getClientAuthorizationSharedSecret())) {
                        LOGGER.trace("Client authentication failed with SharedSecret in realm [{}]", super.name());
                        listener.onResponse(AuthenticationResult.unsuccessful("Client authentication required and failed", null));
                    }
                    LOGGER.trace("Client authentication succeeded with SharedSecret in realm [{}]", super.name());
                    break;
                case JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_NONE:
                default:
                    LOGGER.trace("Client authentication not required in realm [{}]", super.name());
                    break;
            }

            // JWT Authentication
            // 1. Verify signature (HMAC or RSA)
            // 2. If present verify nfb <= iat.
            // 3. If present verify nfb <= exp.
            // 4. If present verify iat <= exp.
            // 5. If present, verify nfb + allowedClockSkew < now.
            // 6. If present, verify iat + allowedClockSkew >= now.
            // 7. If present, verify exp + allowedClockSkew >= now.

            final String principal = "user1";
            final List<String> groups = List.of("group1");
            final String dn = null;
            final String name = null;
            final String mail = null;
            final Map<String, Object> userMetadata;
            if (this.populateUserMetadata) {
                userMetadata = Map.of("metadata1", "metadata1");
            } else {
                userMetadata = Map.of();
            }
            if (delegatedRealms.hasDelegation()) {
                delegatedRealms.resolve(principal, listener);
                return;
            }

            final UserRoleMapper.UserData userData = new UserRoleMapper.UserData(principal, dn, groups, userMetadata, super.config);
            this.userRoleMapper.resolveRoles(userData, ActionListener.wrap(roles -> {
                listener.onResponse(
                    AuthenticationResult.success(new User(principal, roles.toArray(Strings.EMPTY_ARRAY), name, mail, userMetadata, true))
                );
            }, listener::onFailure));
        } else {
            listener.onResponse(AuthenticationResult.notHandled());
        }
    }

    @Override
    public void expire(final String username) {
        if (this.cachedAuthenticationSuccesses != null) {
            LOGGER.trace("invalidating cache for user [{}] in realm [{}]", username, name());
            this.cachedAuthenticationSuccesses.invalidate(username);
        }
    }

    @Override
    public void expireAll() {
        if (this.cachedAuthenticationSuccesses != null) {
            LOGGER.trace("invalidating cache for all users in realm [{}]", name());
            this.cachedAuthenticationSuccesses.invalidateAll();
        }
    }

    @Override
    public void close() {
        this.expireAll();
    }

    @Override
    public void lookupUser(final String username, final ActionListener<User> listener) {
        listener.onResponse(null); // Run-As and Delegated Authorization are not supported
    }

    @Override
    public void usageStats(final ActionListener<Map<String, Object>> listener) {
        super.usageStats(ActionListener.wrap(stats -> {
            stats.put("cache", Collections.singletonMap("size", this.getCacheSize()));
            listener.onResponse(stats);
        }, listener::onFailure));
    }

    private int getCacheSize() {
        return (this.cachedAuthenticationSuccesses == null) ? -1 : this.cachedAuthenticationSuccesses.count();
    }

    private static class CachedAuthenticationSuccess {
        private final AuthenticationResult<User> authenticationResult; // required
        private final char[] jwtHash; // required (hash of JWT)
        private final char[] clientAuthorizationParameterHash; // optional (hash of SharedSecret or ClientCertificateChain)

        private CachedAuthenticationSuccess(
            final AuthenticationResult<User> authenticationResult,
            final @Nullable SecureString jwt,
            final @Nullable SecureString clientAuthorizationParameter,
            final Hasher hasher
        ) {
            assert authenticationResult != null : "AuthenticationResult must be non-null";
            assert authenticationResult.isAuthenticated() : "AuthenticationResult.isAuthenticated must be true";
            assert authenticationResult.getValue() != null : "AuthenticationResult.getValue=User must be non-null";
            assert jwt != null : "JWT must be non-null";
            this.authenticationResult = authenticationResult;
            this.jwtHash = hasher.hash(jwt);
            this.clientAuthorizationParameterHash = (clientAuthorizationParameter == null)
                ? null
                : hasher.hash(clientAuthorizationParameter);
        }

        private boolean verify(final SecureString jwt, final @Nullable SecureString clientAuthorizationParameter) {
            return (((jwt != null) && (this.jwtHash != null) && (Hasher.verifyHash(jwt, this.jwtHash))))
                && (((clientAuthorizationParameter == null) && (this.clientAuthorizationParameterHash == null))
                    || ((clientAuthorizationParameter != null)
                        && (this.clientAuthorizationParameterHash != null)
                        && (Hasher.verifyHash(clientAuthorizationParameter, this.clientAuthorizationParameterHash))));
        }
    }

    public static SecureString getAuthorizationHeaderParameter(
        final ThreadContext threadContext,
        final String headerName,
        final String schemeValue,
        final boolean ignoreCase
    ) {
        final String headerValue = threadContext.getHeader(headerName);
        if (Strings.hasText(headerValue)) {
            final String schemeValuePlusSpace = schemeValue + " ";
            if (headerValue.regionMatches(ignoreCase, 0, schemeValuePlusSpace, 0, schemeValuePlusSpace.length())) {
                final String trimmedSchemeParameters = headerValue.substring(schemeValuePlusSpace.length()).trim();
                if (Strings.hasText(trimmedSchemeParameters)) {
                    return new SecureString(trimmedSchemeParameters.toCharArray());
                }
            }
        }
        return null;
    }
}
