/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jwt.JWTClaimsSet;

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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
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
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
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
    private DelegatedAuthorizationSupport delegatedAuthorizationSupport;
    private boolean initialized = false;

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
        // RealmConfig realmConfig = super.config;
        this.allowedIssuer = realmConfig.getSetting(JwtRealmSettings.ALLOWED_ISSUER);
        this.allowedSignatureAlgorithms = realmConfig.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
        this.allowedClientSkew = realmConfig.getSetting(JwtRealmSettings.ALLOWED_CLOCK_SKEW);
        this.jwkSetPath = realmConfig.getSetting(JwtRealmSettings.JWKSET_PATH);
        this.hmacSecretKey = realmConfig.getSetting(JwtRealmSettings.ISSUER_HMAC_SECRET_KEY);

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
        this.cacheHashAlgo = realmConfig.getSetting(JwtRealmSettings.CACHE_HASH_ALGO);

        // Standard HTTP settings for outgoing connections to get JWT issuer jwkset_path
        this.httpConnectTimeout = realmConfig.getSetting(JwtRealmSettings.HTTP_CONNECT_TIMEOUT);
        this.httpConnectionReadTimeout = realmConfig.getSetting(JwtRealmSettings.HTTP_CONNECTION_READ_TIMEOUT);
        this.httpSocketTimeout = realmConfig.getSetting(JwtRealmSettings.HTTP_SOCKET_TIMEOUT);
        this.httpMaxConnections = realmConfig.getSetting(JwtRealmSettings.HTTP_MAX_CONNECTIONS);
        this.httpMaxEndpointConnections = realmConfig.getSetting(JwtRealmSettings.HTTP_MAX_ENDPOINT_CONNECTIONS);

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
        validateClientAuthorizationSettings(this.clientAuthorizationType, this.clientAuthorizationSharedSecret, super.config);

        // Validate that at least one of JWT Set Path and HMAC Key Set are set. If HMAC Key Set, validate Base64Url-encoding.
        validateIssuerCredentialSettings(super.config, this.hmacSecretKey, this.jwkSetPath, this.allowedSignatureAlgorithms);

        if (Strings.hasText(this.jwkSetPath)) {
            final Tuple<URL, Path> urlOrPath = validateJwkSetPathSetting(realmConfig, this.jwkSetPath);
            this.jwkSetPathUrl = urlOrPath.v1();
            this.jwkSetPathObj = urlOrPath.v2();
        } else {
            this.jwkSetPathUrl = null;
            this.jwkSetPathObj = null;
        }
    }

    // This logic is in a method for easy unit testing
    public static void validateClientAuthorizationSettings(
        final String clientAuthorizationType,
        final SecureString clientAuthorizationSharedSecret,
        final RealmConfig realmConfig
    ) throws SettingsException {
        switch (clientAuthorizationType) {
            case JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET:
                // If type is "SharedSecret", the shared secret value must be set
                if (Strings.hasText(clientAuthorizationSharedSecret) == false) {
                    throw new SettingsException(
                        "Missing setting for ["
                            + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHORIZATION_SHARED_SECRET)
                            + "]. It is required when setting ["
                            + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHORIZATION_TYPE)
                            + "] is configured as ["
                            + JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET
                            + "]"
                    );
                }
                // If type is "SharedSecret", the shared secret value must Base64url-encoded
                try {
                    Base64.getUrlDecoder().decode(clientAuthorizationSharedSecret.toString());
                } catch (Exception e) {
                    throw new SettingsException(
                        "Base64Url-encoding is required for the Client Authorization Shared Secret ["
                            + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHORIZATION_SHARED_SECRET)
                            + "]"
                    );
                }
                break;
            case JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_NONE:
            default:
                // If type is "None", the shared secret value must not be set
                if (Strings.hasText(clientAuthorizationSharedSecret)) {
                    throw new SettingsException(
                        "Setting ["
                            + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHORIZATION_SHARED_SECRET)
                            + "] is not supported, because setting ["
                            + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.CLIENT_AUTHORIZATION_TYPE)
                            + "] is configured as ["
                            + JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_NONE
                            + "]"
                    );
                }
                break;
        }
    }

    // This logic is in a method for easy unit testing
    public static void validateIssuerCredentialSettings(
        final RealmConfig realmConfig,
        final SecureString hmacSecretKey,
        final String jwkSetPath,
        final List<String> allowedSignatureAlgorithms
    ) throws SettingsException {
        final boolean hasHmacSecretKey = Strings.hasText(hmacSecretKey);
        final boolean hasJwkSetPath = Strings.hasText(jwkSetPath);

        // Validate HMAC or JWK Set are set, or both.
        if ((hasHmacSecretKey == false) && (hasJwkSetPath == false)) {
            throw new SettingsException(
                "At least one setting is required for settings ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.ISSUER_HMAC_SECRET_KEY)
                    + "] or ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.JWKSET_PATH)
                    + "]"
            );
        }

        // Validate HMAC SecretKey Base64Url-encoding is OK, and decoded value is non-empty
        if (hasHmacSecretKey) {
            byte[] decodedHmacSecretKeyBytes = null;
            try {
                decodedHmacSecretKeyBytes = Base64.getUrlDecoder().decode(hmacSecretKey.toString());
            } catch (Exception e) {
                throw new SettingsException(
                    "Base64Url decoding failed for setting ["
                        + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.ISSUER_HMAC_SECRET_KEY)
                        + "]",
                    e
                );
            }
            if (decodedHmacSecretKeyBytes.length == 0) {
                throw new SettingsException(
                    "Base64Url decoded value is empty for setting ["
                        + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.ISSUER_HMAC_SECRET_KEY)
                        + "]"
                );
            }
            Arrays.fill(decodedHmacSecretKeyBytes, (byte) 0); // clear decoded secret key
        }

        // Validate JWK Set Path is HTTPS URL or local file. If local file, validate it exists and is non-empty.
        if (hasJwkSetPath) {
            final Tuple<URL, Path> urlOrPath = validateJwkSetPathSetting(realmConfig, jwkSetPath);
        }

        // If Issuer HMAC Secret Key is set, at least one HMAC Signature Algorithm is required.
        // If at least one HMAC Signature Algorithm is set, Issuer HMAC Secret Key is required.
        final boolean anySecretKeySignatureAlgorithms = allowedSignatureAlgorithms.stream()
            .anyMatch(JwtRealmSettings.SUPPORTED_SECRET_KEY_SIGNATURE_ALGORITHMS::contains);
        if (hasHmacSecretKey && (anySecretKeySignatureAlgorithms == false)) {
            throw new SettingsException(
                "Issuer HMAC Secret Key is configured in setting ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.ISSUER_HMAC_SECRET_KEY)
                    + "], but no HMAC signature algorithms were found in setting ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS)
                    + "]"
            );
        } else if ((anySecretKeySignatureAlgorithms) && (hasHmacSecretKey == false)) {
            throw new SettingsException(
                "HMAC signature algorithms were found in setting ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS)
                    + "], but no Issuer HMAC Secret Key is configured in setting ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.JWKSET_PATH)
                    + "]"
            );
        }

        // If JWT Set Path is set, at least one Public Key Signature Algorithm is required.
        // If at least one Public Key Signature Algorithm is set, JWT Set Path is required.
        final boolean anyPublicKeySignatureAlgorithms = allowedSignatureAlgorithms.stream()
            .anyMatch(JwtRealmSettings.SUPPORTED_PUBLIC_KEY_SIGNATURE_ALGORITHMS::contains);
        if (hasJwkSetPath && (anyPublicKeySignatureAlgorithms == false)) {
            throw new SettingsException(
                "JWT Set Path is configured in setting ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.JWKSET_PATH)
                    + "], but no public key signature algorithms were found in setting ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS)
                    + "]"
            );
        } else if ((anyPublicKeySignatureAlgorithms) && (hasJwkSetPath == false)) {
            throw new SettingsException(
                "Public key signature algorithms were found in setting ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS)
                    + "], but no JWT Set Path is configured in setting ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.JWKSET_PATH)
                    + "]"
            );
        }
    }

    public static Tuple<URL, Path> validateJwkSetPathSetting(final RealmConfig realmConfig, final String jwkSetPath) {
        if (jwkSetPath == null) {
            return null;
        }
        // Suppress any URL parsing exception. If needed, it will be added to a SettingsException at the end.
        Exception urlException = null;
        if (jwkSetPath.startsWith("http://")) {
            urlException = new Exception(
                "HTTP URL ["
                    + jwkSetPath
                    + "] is not allowed for setting ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.JWKSET_PATH)
                    + "]. Must use HTTPS or local file."
            );
        } else if (jwkSetPath.startsWith("https://")) {
            try {
                return new Tuple<>(new URL(jwkSetPath), null); // RETURN URL AS NON-NULL AND PATH AS NULL
            } catch (Exception e) {
                LOGGER.trace(
                    "HTTPS URL ["
                        + jwkSetPath
                        + "] parsing failed for setting ["
                        + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.JWKSET_PATH)
                        + "]",
                    e
                );
                urlException = e;
            }
        } else {
            urlException = new Exception(
                "URL ["
                    + jwkSetPath
                    + "] is not valid for setting ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.JWKSET_PATH)
                    + "]. Must use HTTPS or local file."
            );
        }
        // Suppress any Path parsing exception. If needed, it will be added to a SettingsException at the end.
        Exception pathException = null;
        try {
            // Use separate lines, in case any are null. That allows NPE stack trace line number to show exactly which one is null.
            final Environment environment = realmConfig.env();
            final Path directoryPath = environment.configFile();
            final Path filePath = directoryPath.resolve(jwkSetPath);
            // Check file exists, accessible, and non-empty. TODO Maybe return parsed contents here instead of later in a watcher thread.
            final String fileContents = Files.readString(filePath, StandardCharsets.UTF_8);
            if (Strings.hasText(fileContents) == false) {
                throw new Exception(
                    "Empty file ["
                        + jwkSetPath
                        + "] for setting ["
                        + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.JWKSET_PATH)
                        + "]"
                );
            }
            return new Tuple<>(null, filePath); // RETURN URL AS NULL AND PATH AS NON-NULL (i.e. URL Exception is discarded)
        } catch (Exception e) {
            pathException = new Exception(
                "Error loading file ["
                    + jwkSetPath
                    + "] for setting ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.JWKSET_PATH)
                    + "]",
                e
            );
        }
        // Throw SettingsException with the two suppressed exceptions.
        final SettingsException settingsException = new SettingsException(
            "Invalid value [" + jwkSetPath + "] for setting " + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.JWKSET_PATH)
        );
        settingsException.addSuppressed(urlException);
        settingsException.addSuppressed(pathException);
        throw settingsException;
    }

    private void ensureExpectedValueForInitialized(final boolean expectedValue) {
        if (this.initialized != expectedValue) {
            if (expectedValue) {
                throw new IllegalStateException("Realm has not been initialized");
            } else {
                throw new IllegalStateException("Realm has already been initialized");
            }
        }
    }

    @Override
    public void initialize(final Iterable<Realm> allRealms, final XPackLicenseState xpackLicenseState) {
        this.ensureExpectedValueForInitialized(false);
        // extract list of realms referenced by super.config.settings() value for DelegatedAuthorizationSettings.AUTHZ_REALMS
        this.delegatedAuthorizationSupport = new DelegatedAuthorizationSupport(allRealms, super.config, xpackLicenseState);
        this.initialized = true;
    }

    @Override
    public boolean supports(final AuthenticationToken jwtAuthenticationToken) {
        this.ensureExpectedValueForInitialized(true);
        return (jwtAuthenticationToken instanceof JwtAuthenticationToken);
    }

    @Override
    public AuthenticationToken token(final ThreadContext threadContext) {
        this.ensureExpectedValueForInitialized(true);
        final SecureString authorizationParameterValue = JwtRealm.getHeaderSchemeParameters(
            threadContext,
            JwtRealmSettings.HEADER_ENDUSER_AUTHORIZATION,
            JwtRealmSettings.HEADER_ENDUSER_AUTHORIZATION_SCHEME,
            false
        );
        if (authorizationParameterValue == null) {
            return null; // Could not find non-empty SchemeParameters in HTTP header "Authorization: Bearer <SchemeParameters>"
        }

        // Get all other possible parameters. A different JWT realm may do the actual authentication.
        final SecureString clientAuthorizationSharedSecretValue = JwtRealm.getHeaderSchemeParameters(
            threadContext,
            JwtRealmSettings.HEADER_CLIENT_AUTHORIZATION,
            JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET,
            true
        );

        return new JwtAuthenticationToken(authorizationParameterValue, clientAuthorizationSharedSecretValue);
    }

    @Override
    public void authenticate(final AuthenticationToken authenticationToken, final ActionListener<AuthenticationResult<User>> listener) {
        this.ensureExpectedValueForInitialized(true);
        if (authenticationToken instanceof JwtAuthenticationToken jwtAuthenticationToken) {
            LOGGER.trace("Realm [{}] supports JwtAuthenticationToken.", super.name());
            final JWSHeader jwsHeader = jwtAuthenticationToken.getJwsHeader();
            final JWTClaimsSet jwtClaimsSet = jwtAuthenticationToken.getJwtClaimsSet();
            final String clientAuthorizationSharedSecret = jwtAuthenticationToken.getClientAuthorizationSharedSecret().toString();

            // Filter steps (before any validation)

            // 1. Skip JWT if signature algorithm does not match any of the signature algorithms allowed by this realm.
            final JWSAlgorithm jwsSignatureAlgorithm = jwsHeader.getAlgorithm();
            if ((jwsSignatureAlgorithm == null) || (this.allowedSignatureAlgorithms.contains(jwsSignatureAlgorithm.getName()) == false)) {
                final String msg = String.format(
                    Locale.ROOT,
                    "Realm [%s] does not allow signature algorithm [%s]. Allowed signature algorithms are %s.",
                    super.name(),
                    jwsSignatureAlgorithm,
                    this.allowedSignatureAlgorithms
                );
                LOGGER.debug(msg);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
                return;
            }
            LOGGER.debug(
                "Realm [{}] allows signature algorithm [{}]. Allowed signature algorithms are {}.",
                super.name(),
                jwsSignatureAlgorithm,
                this.allowedSignatureAlgorithms
            );

            // 2. Skip JWT if issuer does not match the issuer allowed by this realm.
            final String jwtIssuer = jwtClaimsSet.getIssuer();
            if ((jwtIssuer == null) || (this.allowedIssuer.contains(jwtIssuer) == false)) {
                final String msg = String.format(
                    Locale.ROOT,
                    "Realm [%s] does not allow issuer [%s]. Allowed issuer is [%s].",
                    super.name(),
                    jwtIssuer,
                    this.allowedIssuer
                );
                LOGGER.debug(msg);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
                return;
            }
            LOGGER.debug("Realm [{}] allows issuer [{}]. Allowed issuer is [{}].", super.name(), jwtIssuer, this.allowedIssuer);

            // 3. Skip JWT if audience does not match any of the audiences allowed by this realm.
            final List<String> jwtAudiences = jwtClaimsSet.getAudience();
            if ((jwtAudiences == null) || (this.allowedAudiences.stream().anyMatch(jwtAudiences::contains) == false)) {
                final String msg = String.format(
                    Locale.ROOT,
                    "Realm [%s] does not allow audiences %s. Allowed audiences are %s.",
                    super.name(),
                    jwtAudiences,
                    this.allowedAudiences
                );
                LOGGER.debug(msg);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
                return;
            }
            LOGGER.debug("Realm [{}] allows audiences {}. Allowed audiences are {}.", super.name(), jwtAudiences, this.allowedAudiences);

            // TODO The implementation of JWT authentication will be completed in a later PR
            // At this point, this is the right realm to do validation. Trigger AuthenticationResult.unsuccessful() if any problems found.

            // Cache lookup
            // 1. If present in cache, AuthenticationResult.successful().

            // JWT Authentication
            // 1. Verify signature (HMAC or RSA)
            // 2. If present verify nfb <= iat.
            // 3. If present verify nfb <= exp.
            // 4. If present verify iat <= exp.
            // 5. If present, verify nfb + allowedClockSkew < now.
            // 6. If present, verify iat + allowedClockSkew >= now.
            // 7. If present, verify exp + allowedClockSkew >= now.

            // Client Authorization
            switch (this.clientAuthorizationType) {
                case JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET:
                    if (this.clientAuthorizationSharedSecret.equals(clientAuthorizationSharedSecret) == false) {
                        final String msg = String.format(
                            Locale.ROOT,
                            "Realm [%s] client authentication failed for [%s].",
                            Locale.ROOT,
                            super.name(),
                            this.clientAuthorizationType
                        );
                        LOGGER.debug(msg);
                        listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
                        return;
                    }
                    LOGGER.debug("Realm [{}] client authentication succeeded for [{}].", super.name(), this.clientAuthorizationType);
                    break;
                case JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_NONE:
                default:
                    LOGGER.debug("Realm [{}] client authentication skipped for [{}].", super.name(), this.clientAuthorizationType);
                    break;
            }

            final String jwtPrincipal = this.principalAttribute.getClaimValue(jwtClaimsSet);
            final String msg1 = String.format(
                Locale.ROOT,
                "Realm [%s] got principal [%s] from claim [%s] and parser [%s]. JWTClaimsSet is [%s].",
                super.name(),
                jwtPrincipal,
                this.principalAttribute.getName(),
                this.principalAttribute.getParser().toString(),
                jwtClaimsSet.toString()
            );
            LOGGER.debug(msg1);
            if (jwtPrincipal == null) {
                listener.onResponse(AuthenticationResult.unsuccessful(msg1, null));
                return;
            }
            final List<String> jwtGroups = this.groupsAttribute.getClaimValues(jwtClaimsSet);
            final String msg2 = String.format(
                Locale.ROOT,
                "Realm [%s] principal [%s] got groups [%s] from claim [%s] and parser [%s]. JWTClaimsSet is [%s].",
                super.name(),
                jwtPrincipal,
                jwtGroups,
                (this.groupsAttribute.getName() == null ? "null" : this.groupsAttribute.getName().toString()),
                (this.groupsAttribute.getParser() == null ? "null" : this.groupsAttribute.getParser().toString()),
                jwtClaimsSet.toString()
            );
            LOGGER.debug(msg2);
            final String jwtDn = null; // JWT realm settings does not support claims.dn
            final String jwtFullName = null; // JWT realm settings does not support claims.name
            final String jwtEmail = null; // JWT realm settings does not support claims.mail
            final Map<String, Object> userMetadata;
            if (this.populateUserMetadata) {
                final String msg3 = String.format(
                    Locale.ROOT,
                    "Realm [%s] principal [%s] got user metadata from JWTClaimsSet [%s].",
                    super.name(),
                    jwtPrincipal,
                    jwtClaimsSet.toString()
                );
                LOGGER.debug(msg3);
                userMetadata = jwtClaimsSet.getClaims();
            } else {
                final String msg3 = String.format(
                    Locale.ROOT,
                    "Realm [%s] principal [%s] ignored user metadata from JWTClaimsSet [%s].",
                    super.name(),
                    jwtPrincipal,
                    jwtClaimsSet.toString()
                );
                LOGGER.debug(msg3);
                userMetadata = Map.of();
            }
            if (this.delegatedAuthorizationSupport.hasDelegation()) {
                this.delegatedAuthorizationSupport.resolve(jwtPrincipal, listener);
                final String msg4 = String.format(
                    Locale.ROOT,
                    "Realm [%s] principal [%s] got roles [%s] from authz realms %s.",
                    super.name(),
                    jwtPrincipal,
                    "",
                    this.delegatedAuthorizationSupport.toString()
                ); // TODO Print authz realm name
                LOGGER.debug(msg4);
                return;
            }

            final UserRoleMapper.UserData userData = new UserRoleMapper.UserData(
                jwtPrincipal,
                jwtDn,
                jwtGroups,
                userMetadata,
                super.config
            );
            this.userRoleMapper.resolveRoles(userData, ActionListener.wrap(rolesSet -> {
                final String[] roles = rolesSet.toArray(new String[rolesSet.size()]);
                final String msg4 = String.format(
                    Locale.ROOT,
                    "Realm [%s] principal [%s] got roles [%s] via role mapping.",
                    super.name(),
                    jwtPrincipal,
                    roles
                );
                LOGGER.debug(msg4);
                final User user = new User(jwtPrincipal, roles, jwtFullName, jwtEmail, userMetadata, true);
                listener.onResponse(AuthenticationResult.success(user));
            }, listener::onFailure));
        } else {
            final String msg = String.format(
                Locale.ROOT,
                "Realm [%s] does not support AuthenticationToken [%s].",
                super.name(),
                (authenticationToken == null ? "null" : authenticationToken.getClass().getSimpleName())
            );
            LOGGER.debug(msg);
            listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
        }
    }

    @Override
    public void expire(final String username) {
        this.ensureExpectedValueForInitialized(true);
        if (this.cachedAuthenticationSuccesses != null) {
            LOGGER.trace("invalidating cache for user [{}] in realm [{}]", username, name());
            this.cachedAuthenticationSuccesses.invalidate(username);
        }
    }

    @Override
    public void expireAll() {
        this.ensureExpectedValueForInitialized(true);
        if (this.cachedAuthenticationSuccesses != null) {
            LOGGER.trace("invalidating cache for all users in realm [{}]", name());
            this.cachedAuthenticationSuccesses.invalidateAll();
        }
    }

    @Override
    public void close() {
        this.ensureExpectedValueForInitialized(true);
        this.expireAll();
    }

    @Override
    public void lookupUser(final String username, final ActionListener<User> listener) {
        this.ensureExpectedValueForInitialized(true);
        listener.onResponse(null); // Run-As and Delegated Authorization are not supported
    }

    @Override
    public void usageStats(final ActionListener<Map<String, Object>> listener) {
        this.ensureExpectedValueForInitialized(true);
        super.usageStats(ActionListener.wrap(stats -> {
            stats.put("cache", Collections.singletonMap("size", this.getCacheSize()));
            listener.onResponse(stats);
        }, listener::onFailure));
    }

    private int getCacheSize() {
        this.ensureExpectedValueForInitialized(true);
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

    public static SecureString getHeaderSchemeParameters(
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
