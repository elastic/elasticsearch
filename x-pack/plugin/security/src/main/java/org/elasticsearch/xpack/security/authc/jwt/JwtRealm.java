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
import com.nimbusds.oauth2.sdk.auth.Secret;

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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

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
        final String cacheHashAlgo = realmConfig.getSetting(JwtRealmSettings.CACHE_HASH_ALGO);

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
        this.hasher = Hasher.resolve(cacheHashAlgo);

        // Validate Client Authorization Type and Client Authorization Credential format
        validateClientAuthorizationSettings(this.clientAuthorizationType, this.clientAuthorizationSharedSecret, super.config);

        // Validate and parse JWT Set Path
        final Tuple<URL, Path> urlOrPath = validateJwkSetPathSetting(realmConfig, this.jwkSetPath);
        this.jwkSetPathUrl = (urlOrPath == null) ? null : urlOrPath.v1();
        this.jwkSetPathObj = (urlOrPath == null) ? null : urlOrPath.v2();

        // Validate that at least one of JWT Set Path and HMAC Key Set are set. If HMAC Key Set, validate Base64Url-encoding.
        validateIssuerCredentialSettings(super.config, this.hmacSecretKey, this.jwkSetPath, this.allowedSignatureAlgorithms);
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
                            + "] is ["
                            + JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET
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
                            + "] is ["
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
                "At least one setting is required for ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.ISSUER_HMAC_SECRET_KEY)
                    + "] or ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.JWKSET_PATH)
                    + "]"
            );
        }

        // Validate HMAC SecretKey Base64Url-encoding is OK, and decoded value is non-empty
        if (hasHmacSecretKey) {
            try {
                final Secret decodedHmacSecretKeyBytes = new Secret(hmacSecretKey.toString());
                Arrays.fill(decodedHmacSecretKeyBytes.getValueBytes(), (byte) 0); // clear decoded secret key
            } catch (Exception e) {
                throw new SettingsException(
                    "Validation failed for setting ["
                        + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.ISSUER_HMAC_SECRET_KEY)
                        + "]",
                    e
                );
            }
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
        if (Strings.hasText(jwkSetPath) == false) {
            return null;
        }
        // If HTTPS URL parsing succeeds then return right away, otherwise save the exception and fall through to next step.
        final Exception urlException;
        if (jwkSetPath.startsWith("https://")) {
            try {
                return new Tuple<>(new URL(jwkSetPath), null); // RETURN URL AS NON-NULL AND PATH AS NULL
            } catch (Exception e) {
                LOGGER.debug(
                    "HTTPS URL ["
                        + jwkSetPath
                        + "] parsing failed for setting ["
                        + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.JWKSET_PATH)
                        + "].",
                    e
                );
                urlException = e;
            }
        } else {
            urlException = new Exception(
                "Parse URL not attempted for ["
                    + jwkSetPath
                    + "] for setting ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.JWKSET_PATH)
                    + "]. Only HTTPS URL or local file are supported."
            );
        }
        // If local file parsing succeeds then return right away, otherwise save the exception and fall through to next step.
        final Exception pathException;
        try {
            // Use separate lines of code, in case any are null. That allows NPE stack trace line number to show exactly which one is null.
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
                        + "]."
                );
            }
            return new Tuple<>(null, filePath); // RETURN URL AS NULL AND PATH AS NON-NULL (i.e. URL Exception is discarded)
        } catch (Exception e) {
            pathException = new Exception(
                "Error loading local file ["
                    + jwkSetPath
                    + "] for setting ["
                    + RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.JWKSET_PATH)
                    + "].",
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

    @Override
    public void initialize(final Iterable<Realm> allRealms, final XPackLicenseState xpackLicenseState) {
        if (this.delegatedAuthorizationSupport != null) {
            throw new IllegalStateException("Realm has already been initialized");
        }
        // extract list of realms referenced by super.config.settings() value for DelegatedAuthorizationSettings.AUTHZ_REALMS
        this.delegatedAuthorizationSupport = new DelegatedAuthorizationSupport(allRealms, super.config, xpackLicenseState);
    }

    private void ensureInitialized() {
        if (this.delegatedAuthorizationSupport == null) {
            throw new IllegalStateException("Realm has not been initialized");
        }
    }

    @Override
    public boolean supports(final AuthenticationToken jwtAuthenticationToken) {
        this.ensureInitialized();
        return (jwtAuthenticationToken instanceof JwtAuthenticationToken);
    }

    @Override
    public AuthenticationToken token(final ThreadContext threadContext) {
        this.ensureInitialized();
        final SecureString authorizationParameterValue = JwtRealm.getHeaderSchemeParameters(
            threadContext,
            JwtRealmSettings.HEADER_END_USER_AUTHORIZATION,
            JwtRealmSettings.HEADER_END_USER_AUTHORIZATION_SCHEME,
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
        this.ensureInitialized();
        if (authenticationToken instanceof JwtAuthenticationToken jwtAuthenticationToken) {
            final String tokenPrincipal = jwtAuthenticationToken.principal();
            LOGGER.trace("Realm [{}] received JwtAuthenticationToken for tokenPrincipal [{}].", super.name(), tokenPrincipal);
            final JWSHeader jwsHeader = jwtAuthenticationToken.getJwsHeader();
            final JWTClaimsSet jwtClaimsSet = jwtAuthenticationToken.getJwtClaimsSet();
            final String issuerClaim = jwtAuthenticationToken.getIssuerClaim();
            final List<String> audiencesClaim = jwtAuthenticationToken.getAudiencesClaim();
            final Map<String, Object> jwtClaimsMap = jwtClaimsSet.getClaims();
            final SecureString clientAuthorizationSharedSecret = jwtAuthenticationToken.getClientAuthorizationSharedSecret();
            final String clientAuthorizationSharedSecretString = (clientAuthorizationSharedSecret == null)
                ? null
                : clientAuthorizationSharedSecret.toString();

            // Filter steps (before any validation)

            // 1. Skip JWT if issuer does not match the issuer allowed by this realm.
            if ((issuerClaim == null) || (this.allowedIssuer.equals(issuerClaim) == false)) {
                final String msg = String.format(
                    Locale.ROOT,
                    "Realm [%s] did not allow issuer [%s] for tokenPrincipal [%s]. Allowed issuer is [%s].",
                    super.name(),
                    issuerClaim,
                    tokenPrincipal,
                    this.allowedIssuer
                );
                LOGGER.debug(msg);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
                return;
            }
            LOGGER.trace(
                "Realm [{}] allowed issuer [{}] for tokenPrincipal [{}]. Allowed issuer is [{}].",
                super.name(),
                issuerClaim,
                tokenPrincipal,
                this.allowedIssuer
            );

            // 2. Skip JWT if audience does not match any of the audiences allowed by this realm.
            if ((audiencesClaim == null) || (this.allowedAudiences.stream().anyMatch(audiencesClaim::contains) == false)) {
                final String msg = String.format(
                    Locale.ROOT,
                    "Realm [%s] did not allow audiences [%s] for tokenPrincipal [%s]. Allowed audiences are [%s].",
                    super.name(),
                    (audiencesClaim == null ? "null" : String.join(",", audiencesClaim)),
                    tokenPrincipal,
                    String.join(",", this.allowedAudiences)
                );
                LOGGER.debug(msg);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
                return;
            }
            LOGGER.trace(
                "Realm [{}] allowed at least one audience [{}] for tokenPrincipal [{}]. Allowed audiences are [{}].",
                super.name(),
                String.join(",", audiencesClaim),
                tokenPrincipal,
                String.join(",", this.allowedAudiences)
            );

            // 3. Skip JWT if signature algorithm does not match any of the signature algorithms allowed by this realm.
            final JWSAlgorithm jwsSignatureAlgorithm = jwsHeader.getAlgorithm();
            if ((jwsSignatureAlgorithm == null) || (this.allowedSignatureAlgorithms.contains(jwsSignatureAlgorithm.getName()) == false)) {
                final String msg = String.format(
                    Locale.ROOT,
                    "Realm [%s] did not allow signature algorithm [%s] for tokenPrincipal [%s]. Allowed signature algorithms are [%s].",
                    super.name(),
                    jwsSignatureAlgorithm,
                    tokenPrincipal,
                    String.join(",", this.allowedSignatureAlgorithms)
                );
                LOGGER.debug(msg);
                listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
                return;
            }
            LOGGER.trace(
                "Realm [{}] allowed signature algorithm [{}] for tokenPrincipal [{}]. Allowed signature algorithms are [{}].",
                super.name(),
                jwsSignatureAlgorithm,
                tokenPrincipal,
                String.join(",", this.allowedSignatureAlgorithms)
            );

            // TODO The implementation of JWT authentication will be completed in a later PR
            // At this point, this is the right realm to do validation. Trigger AuthenticationResult.unsuccessful() if any problems found.

            // Cache lookup
            // 1. If present in cache, AuthenticationResult.successful().

            // Client Authorization
            switch (this.clientAuthorizationType) {
                case JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_SHARED_SECRET:
                    if (Strings.hasText(clientAuthorizationSharedSecretString) == false) {
                        final String msg = String.format(
                            Locale.ROOT,
                            "Realm [%s] client authentication [%s] failed for tokenPrincipal [%s] because request header is missing.",
                            super.name(),
                            this.clientAuthorizationType,
                            tokenPrincipal
                        );
                        LOGGER.debug(msg);
                        listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
                        return;
                    } else if (this.clientAuthorizationSharedSecret.toString().equals(clientAuthorizationSharedSecretString) == false) {
                        final String msg = String.format(
                            Locale.ROOT,
                            "Realm [%s] client authentication [%s] failed for tokenPrincipal [%s] because request header did not match.",
                            super.name(),
                            this.clientAuthorizationType,
                            tokenPrincipal
                        );
                        LOGGER.debug(msg);
                        listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
                        return;
                    }
                    LOGGER.trace(
                        "Realm [{}] client authentication [{}] succeeded for tokenPrincipal [{}] because request header matched.",
                        super.name(),
                        this.clientAuthorizationType,
                        tokenPrincipal
                    );
                    break;
                case JwtRealmSettings.SUPPORTED_CLIENT_AUTHORIZATION_TYPE_NONE:
                default:
                    if (Strings.hasText(clientAuthorizationSharedSecretString)) {
                        final String msg = String.format(
                            Locale.ROOT,
                            "Realm [%s] client authentication [%s] failed for tokenPrincipal [%s] because request header is present.",
                            super.name(),
                            this.clientAuthorizationType,
                            tokenPrincipal
                        );
                        LOGGER.debug(msg);
                        listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
                        return;
                    }
                    LOGGER.trace(
                        "Realm [{}] client authentication [{}] succeeded for tokenPrincipal [{}] because request header is not present.",
                        super.name(),
                        this.clientAuthorizationType,
                        tokenPrincipal
                    );
                    break;
            }

            // At this point, stop using tokenPrincipal. Extract the principal claim specified in the realm, and use that for logging.

            // JWT Authentication
            // 1. Verify signature (HMAC or RSA)
            // 2. If present verify nfb <= iat.
            // 3. If present verify nfb <= exp.
            // 4. If present verify iat <= exp.
            // 5. If present, verify nfb + allowedClockSkew < now.
            // 6. If present, verify iat + allowedClockSkew >= now.
            // 7. If present, verify exp + allowedClockSkew >= now.

            // Extract claims into principal, groups, dn, fullName, email, and metadata.

            // Principal is mandatory
            final String jwtPrincipal = this.principalAttribute.getClaimValue(jwtClaimsSet);
            final String messageAboutPrincipalClaim = String.format(
                Locale.ROOT,
                "Realm [%s] got principal claim [%s] using parser [%s]. JWTClaimsSet is %s.",
                super.name(),
                jwtPrincipal,
                this.principalAttribute.getName(),
                jwtClaimsMap
            );
            if (jwtPrincipal == null) {
                LOGGER.debug(messageAboutPrincipalClaim);
                listener.onResponse(AuthenticationResult.unsuccessful(messageAboutPrincipalClaim, null));
                return;
            } else {
                LOGGER.trace(messageAboutPrincipalClaim);
            }

            // Groups is optional
            final List<String> jwtGroups = this.groupsAttribute.getClaimValues(jwtClaimsSet);
            LOGGER.trace(
                String.format(
                    Locale.ROOT,
                    "Realm [%s] principal [%s] got groups [%s] using parser [%s]. JWTClaimsSet is %s.",
                    super.name(),
                    jwtPrincipal,
                    (jwtGroups == null ? "null" : String.join(",", jwtGroups)),
                    (this.groupsAttribute.getName() == null ? "null" : this.groupsAttribute.getName()),
                    jwtClaimsMap
                )
            );

            // DN, fullName, and email not supported by JWT realm. Pass nulls to UserRoleMapper.UserData and User constructors.
            final String jwtDn = null;
            final String jwtFullName = null;
            final String jwtEmail = null;

            // Metadata is optional
            final Map<String, Object> userMetadata = this.populateUserMetadata ? jwtClaimsMap : Map.of();
            LOGGER.trace(
                String.format(
                    Locale.ROOT,
                    "Realm [%s] principal [%s] populateUserMetadata [%s] got metadata [%s] from JWTClaimsSet.",
                    super.name(),
                    jwtPrincipal,
                    this.populateUserMetadata,
                    userMetadata
                )
            );

            // Delegate authorization to other realms. If enabled, do lookup here and return. Don't fall through.
            if (this.delegatedAuthorizationSupport.hasDelegation()) {
                final String delegatedAuthorizationSupportDetails = this.delegatedAuthorizationSupport.toString();
                this.delegatedAuthorizationSupport.resolve(jwtPrincipal, ActionListener.wrap(authenticationResultUser -> {
                    // Intercept the delegated authorization listener response to log the resolved roles here. Empty is OK.
                    assert authenticationResultUser != null : "JWT delegated authz should return a non-null AuthenticationResult<User>";
                    final User user = authenticationResultUser.getValue();
                    assert user != null : "JWT delegated authz should return a non-null User";
                    final String[] roles = user.roles();
                    assert roles != null : "JWT delegated authz should return non-null Roles";
                    LOGGER.debug(
                        String.format(
                            Locale.ROOT,
                            "Realm [%s] principal [%s] got lookup roles [%s] via delegated authorization [%s]",
                            super.name(),
                            jwtPrincipal,
                            Arrays.toString(roles),
                            delegatedAuthorizationSupportDetails
                        )
                    );
                    listener.onResponse(authenticationResultUser);
                }, e -> {
                    LOGGER.debug(
                        String.format(
                            Locale.ROOT,
                            "Realm [%s] principal [%s] failed to get lookup roles via delegated authorization [%s]",
                            super.name(),
                            jwtPrincipal,
                            delegatedAuthorizationSupportDetails
                        ),
                        e
                    );
                    listener.onFailure(e);
                }));
                return;
            }

            // Handle role mapping in JWT Realm. Realm settings decided what claims to use here for principal, groups, dn, and
            // metadata.
            final UserRoleMapper.UserData userData = new UserRoleMapper.UserData(
                jwtPrincipal,
                jwtDn,
                jwtGroups,
                userMetadata,
                super.config
            );
            this.userRoleMapper.resolveRoles(userData, ActionListener.wrap(setOfRoles -> {
                // Intercept the role mapper listener response to log the resolved roles here. Empty is OK.
                assert setOfRoles != null : "JWT role mapping should return non-null set of roles.";
                final String[] roles = new TreeSet<>(setOfRoles).toArray(new String[setOfRoles.size()]);
                LOGGER.debug(
                    String.format(
                        Locale.ROOT,
                        "Realm [%s] principal [%s] dn [%s] groups [%s] metadata [%s] got mapped roles [%s].",
                        super.name(),
                        jwtPrincipal,
                        jwtDn,
                        (jwtGroups == null ? "null" : String.join(",", jwtGroups)),
                        userMetadata,
                        Arrays.toString(roles)
                    )
                );
                final User user = new User(jwtPrincipal, roles, jwtFullName, jwtEmail, userMetadata, true);
                listener.onResponse(AuthenticationResult.success(user));
            }, e -> {
                LOGGER.debug(
                    String.format(
                        Locale.ROOT,
                        "Realm [%s] principal [%s] dn [%s] groups [%s] metadata [%s] failed to get mapped roles.",
                        super.name(),
                        jwtPrincipal,
                        jwtDn,
                        (jwtGroups == null ? "null" : String.join(",", jwtGroups)),
                        userMetadata
                    ),
                    e
                );
                listener.onFailure(e);
            }));
        } else {
            final String msg = String.format(
                Locale.ROOT,
                "Realm [%s] does not support AuthenticationToken [%s].",
                super.name(),
                (authenticationToken == null ? "null" : authenticationToken.getClass().getCanonicalName())
            );
            LOGGER.trace(msg);
            listener.onResponse(AuthenticationResult.unsuccessful(msg, null));
        }
    }

    @Override
    public void expire(final String username) {
        this.ensureInitialized();
        if (this.cachedAuthenticationSuccesses != null) {
            LOGGER.trace("invalidating cache for user [{}] in realm [{}]", username, name());
            this.cachedAuthenticationSuccesses.invalidate(username);
        }
    }

    @Override
    public void expireAll() {
        this.ensureInitialized();
        if (this.cachedAuthenticationSuccesses != null) {
            LOGGER.trace("invalidating cache for all users in realm [{}]", name());
            this.cachedAuthenticationSuccesses.invalidateAll();
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
        super.usageStats(ActionListener.wrap(stats -> {
            stats.put("cache", Collections.singletonMap("size", this.getCacheSize()));
            listener.onResponse(stats);
        }, listener::onFailure));
    }

    private int getCacheSize() {
        this.ensureInitialized();
        return (this.cachedAuthenticationSuccesses == null) ? -1 : this.cachedAuthenticationSuccesses.count();
    }

    private static class CachedAuthenticationSuccess {
        private final String cacheKey;
        private final AuthenticationResult<User> authenticationResult;

        private CachedAuthenticationSuccess(final @Nullable String cacheKey, final AuthenticationResult<User> authenticationResult) {
            assert cacheKey != null : "Cache key must be non-null";
            assert authenticationResult != null : "AuthenticationResult must be non-null";
            assert authenticationResult.isAuthenticated() : "AuthenticationResult.isAuthenticated must be true";
            assert authenticationResult.getValue() != null : "AuthenticationResult.getValue=User must be non-null";
            this.cacheKey = cacheKey;
            this.authenticationResult = authenticationResult;
        }

        private boolean verify(final SecureString lookupKey) {
            return this.cacheKey.equals(lookupKey.toString());
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
