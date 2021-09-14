/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.pki;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslTrustConfig;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SslSettingsLoader;
import org.elasticsearch.xpack.security.authc.BytesKey;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.support.DelegatedAuthorizationSupport;
import org.elasticsearch.xpack.security.authc.support.mapper.CompositeRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.MessageDigest;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PkiRealm extends Realm implements CachingRealm {

    public static final String PKI_CERT_HEADER_NAME = "__SECURITY_CLIENT_CERTIFICATE";

    // For client based cert validation, the auth type must be specified but UNKNOWN is an acceptable value
    private static final String AUTH_TYPE = "UNKNOWN";

    // the lock is used in an odd manner; when iterating over the cache we cannot have modifiers other than deletes using
    // the iterator but when not iterating we can modify the cache without external locking. When making normal modifications to the cache
    // the read lock is obtained so that we can allow concurrent modifications; however when we need to iterate over the keys or values of
    // the cache the write lock must obtained to prevent any modifications
    private final ReleasableLock readLock;
    private final ReleasableLock writeLock;

    {
        final ReadWriteLock iterationLock = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(iterationLock.readLock());
        writeLock = new ReleasableLock(iterationLock.writeLock());
    }

    private final X509TrustManager trustManager;
    private final Pattern principalPattern;
    private final UserRoleMapper roleMapper;
    private final Cache<BytesKey, User> cache;
    private DelegatedAuthorizationSupport delegatedRealms;
    private final boolean delegationEnabled;

    public PkiRealm(RealmConfig config, ResourceWatcherService watcherService, NativeRoleMappingStore nativeRoleMappingStore) {
        this(config, new CompositeRoleMapper(config, watcherService, nativeRoleMappingStore));
    }

    // pkg private for testing
    PkiRealm(RealmConfig config, UserRoleMapper roleMapper) {
        super(config);
        this.delegationEnabled = config.getSetting(PkiRealmSettings.DELEGATION_ENABLED_SETTING);
        this.trustManager = trustManagers(config);
        this.principalPattern = config.getSetting(PkiRealmSettings.USERNAME_PATTERN_SETTING);
        this.roleMapper = roleMapper;
        this.roleMapper.refreshRealmOnChange(this);
        this.cache = CacheBuilder.<BytesKey, User>builder()
                .setExpireAfterWrite(config.getSetting(PkiRealmSettings.CACHE_TTL_SETTING))
                .setMaximumWeight(config.getSetting(PkiRealmSettings.CACHE_MAX_USERS_SETTING))
                .build();
        this.delegatedRealms = null;
        validateAuthenticationDelegationConfiguration(config);
    }

    @Override
    public void initialize(Iterable<Realm> realms, XPackLicenseState licenseState) {
        if (delegatedRealms != null) {
            throw new IllegalStateException("Realm has already been initialized");
        }
        delegatedRealms = new DelegatedAuthorizationSupport(realms, config, licenseState);
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return token instanceof X509AuthenticationToken;
    }

    @Override
    public X509AuthenticationToken token(ThreadContext context) {
        Object pkiHeaderValue = context.getTransient(PKI_CERT_HEADER_NAME);
        if (pkiHeaderValue == null) {
            return null;
        }
        assert pkiHeaderValue instanceof X509Certificate[];
        X509Certificate[] certificates = (X509Certificate[]) pkiHeaderValue;
        if (certificates.length == 0) {
            return null;
        }
        X509AuthenticationToken token = new X509AuthenticationToken(certificates);
        // the following block of code maintains BWC:
        // When constructing the token object we only return it if the Subject DN of the certificate can be parsed by at least one PKI
        // realm. We then consider the parsed Subject DN as the "principal" even though it is potentially incorrect because when several
        // realms are installed the one that first parses the principal might not be the one that finally authenticates (does trusted chain
        // validation). In this case the principal should be set by the realm that completes the authentication. But in the common case,
        // where a single PKI realm is configured, there is no risk of eagerly parsing the principal before authentication and it also
        // maintains BWC.
        String parsedPrincipal = getPrincipalFromSubjectDN(principalPattern, token, logger);
        if (parsedPrincipal == null) {
            return null;
        }
        token.setPrincipal(parsedPrincipal);
        // end BWC code block
        return token;
    }

    @Override
    public void authenticate(AuthenticationToken authToken, ActionListener<AuthenticationResult> listener) {
        assert delegatedRealms != null : "Realm has not been initialized correctly";
        X509AuthenticationToken token = (X509AuthenticationToken) authToken;
        try {
            final BytesKey fingerprint = computeTokenFingerprint(token);
            User user = cache.get(fingerprint);
            if (user != null) {
                logger.debug((Supplier<?>) () -> new ParameterizedMessage("Using cached authentication for DN [{}], as principal [{}]",
                        token.dn(), user.principal()));
                if (delegatedRealms.hasDelegation()) {
                    delegatedRealms.resolve(user.principal(), listener);
                } else {
                    listener.onResponse(AuthenticationResult.success(user));
                }
            } else if (false == delegationEnabled && token.isDelegated()) {
                listener.onResponse(AuthenticationResult.unsuccessful("Realm does not permit delegation for " + token.dn(), null));
            } else if (false == isCertificateChainTrusted(token)) {
                listener.onResponse(AuthenticationResult.unsuccessful("Certificate for " + token.dn() + " is not trusted", null));
            } else {
                // parse the principal again after validating the cert chain, and do not rely on the token.principal one, because that could
                // be set by a different realm that failed trusted chain validation. We SHOULD NOT parse the principal BEFORE this step, but
                // we do it for BWC purposes. Changing this is a breaking change.
                final String principal = getPrincipalFromSubjectDN(principalPattern, token, logger);
                if (principal == null) {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage(
                            "the extracted principal after cert chain validation, from DN [{}], using pattern [{}] is null", token.dn(),
                            principalPattern.toString()));
                    listener.onResponse(AuthenticationResult.unsuccessful("Could not parse principal from Subject DN " + token.dn(), null));
                } else {
                    final ActionListener<AuthenticationResult> cachingListener = ActionListener.wrap(result -> {
                        if (result.isAuthenticated()) {
                            try (ReleasableLock ignored = readLock.acquire()) {
                                cache.put(fingerprint, result.getUser());
                            }
                        }
                        listener.onResponse(result);
                    }, listener::onFailure);
                    if (false == principal.equals(token.principal())) {
                        logger.debug((Supplier<?>) () -> new ParameterizedMessage(
                                "the extracted principal before [{}] and after [{}] cert chain validation, for DN [{}], are different",
                                token.principal(), principal, token.dn()));
                    }
                    if (delegatedRealms.hasDelegation()) {
                        delegatedRealms.resolve(principal, cachingListener);
                    } else {
                        buildUser(token, principal, cachingListener);
                    }
                }
            }
        } catch (CertificateEncodingException e) {
            listener.onResponse(AuthenticationResult.unsuccessful("Certificate for " + token.dn() + " has encoding issues", e));
        }
    }

    private void buildUser(X509AuthenticationToken token, String principal, ActionListener<AuthenticationResult> listener) {
        final Map<String, Object> metadata;
        if (token.isDelegated()) {
            metadata = Map.of("pki_dn", token.dn(),
                    "pki_delegated_by_user", token.getDelegateeAuthentication().getUser().principal(),
                    "pki_delegated_by_realm", token.getDelegateeAuthentication().getAuthenticatedBy().getName());
        } else {
            metadata = Map.of("pki_dn", token.dn());
        }
        final UserRoleMapper.UserData userData = new UserRoleMapper.UserData(principal, token.dn(), Set.of(), metadata, config);
        roleMapper.resolveRoles(userData, ActionListener.wrap(roles -> {
            final User computedUser = new User(principal, roles.toArray(new String[roles.size()]), null, null, metadata, true);
            listener.onResponse(AuthenticationResult.success(computedUser));
        }, listener::onFailure));
    }

    @Override
    public void lookupUser(String username, ActionListener<User> listener) {
        listener.onResponse(null);
    }

    static String getPrincipalFromSubjectDN(Pattern principalPattern, X509AuthenticationToken token, Logger logger) {
        String dn = token.credentials()[0].getSubjectX500Principal().toString();
        Matcher matcher = principalPattern.matcher(dn);
        if (false == matcher.find()) {
            logger.debug((Supplier<?>) () -> new ParameterizedMessage("could not extract principal from DN [{}] using pattern [{}]", dn,
                    principalPattern.toString()));
            return null;
        }
        String principal = matcher.group(1);
        if (Strings.isNullOrEmpty(principal)) {
            logger.debug((Supplier<?>) () -> new ParameterizedMessage("the extracted principal from DN [{}] using pattern [{}] is empty",
                    dn, principalPattern.toString()));
            return null;
        }
        return principal;
    }

    private boolean isCertificateChainTrusted(X509AuthenticationToken token) {
        if (trustManager == null) {
            // No extra trust managers specified
            // If the token is NOT delegated then it is authenticated, because the certificate chain has been validated by the TLS channel.
            // Otherwise, if the token is delegated, then it cannot be authenticated without a trustManager
            return token.isDelegated() == false;
        } else {
            try {
                trustManager.checkClientTrusted(token.credentials(), AUTH_TYPE);
                return true;
            } catch (CertificateException e) {
                if (logger.isTraceEnabled()) {
                    logger.trace("failed certificate validation for Subject DN [" + token.dn() + "]", e);
                } else if (logger.isDebugEnabled()) {
                    logger.debug("failed certificate validation for Subject DN [{}]", token.dn());
                }
            }
            return false;
        }
    }

    private X509TrustManager trustManagers(RealmConfig realmConfig) {
        final SslConfiguration sslConfiguration = SslSettingsLoader.load(
            realmConfig.settings(),
            RealmSettings.realmSettingPrefix(realmConfig.identifier()),
            realmConfig.env()
        );
        final SslTrustConfig trustConfig = sslConfiguration.getTrustConfig();
        if (trustConfig.isSystemDefault()) {
            return null;
        }
        final X509ExtendedTrustManager trustManager = trustConfig.createTrustManager();
        if (trustManager.getAcceptedIssuers().length == 0) {
            logger.warn("PKI Realm [{}] uses trust configuration [{}] which has no accepted certificate issuers", this, trustConfig);
        }
        return trustManager;
    }

    @Override
    public void expire(String username) {
        try (ReleasableLock ignored = writeLock.acquire()) {
            Iterator<User> userIterator = cache.values().iterator();
            while (userIterator.hasNext()) {
                if (userIterator.next().principal().equals(username)) {
                    userIterator.remove();
                    // do not break since there is no guarantee username is unique in this realm
                }
            }
        }
    }

    @Override
    public void expireAll() {
        try (ReleasableLock ignored = readLock.acquire()) {
            cache.invalidateAll();
        }
    }

    @Override
    public void usageStats(ActionListener<Map<String, Object>> listener) {
        super.usageStats(ActionListener.wrap(stats -> {
            stats.put("has_truststore", trustManager != null);
            stats.put("has_authorization_realms", delegatedRealms != null && delegatedRealms.hasDelegation());
            stats.put("has_default_username_pattern", PkiRealmSettings.DEFAULT_USERNAME_PATTERN.equals(principalPattern.pattern()));
            stats.put("is_authentication_delegated", delegationEnabled);
            listener.onResponse(stats);
        }, listener::onFailure));
    }

    private void validateAuthenticationDelegationConfiguration(RealmConfig config) {
        if (delegationEnabled) {
            List<String> exceptionMessages = new ArrayList<>(2);
            if (this.trustManager == null) {
                exceptionMessages.add("a trust configuration ("
                        + config.getConcreteSetting(PkiRealmSettings.CAPATH_SETTING).getKey() + " or "
                        + config.getConcreteSetting(PkiRealmSettings.TRUST_STORE_PATH).getKey() + ")");
            }
            if (false == TokenService.isTokenServiceEnabled(config.settings())) {
                exceptionMessages.add("that the token service be also enabled ("
                        + XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey() + ")");
            }
            if (false == exceptionMessages.isEmpty()) {
                String message = "PKI realms with delegation enabled require " + exceptionMessages.get(0);
                if (exceptionMessages.size() == 2) {
                    message = message + " and " + exceptionMessages.get(1);
                }
                throw new IllegalStateException(message);
            }
        }
    }

    static BytesKey computeTokenFingerprint(X509AuthenticationToken token) throws CertificateEncodingException {
        MessageDigest digest = MessageDigests.sha256();
        for (X509Certificate certificate : token.credentials()) {
            digest.update(certificate.getEncoded());
        }
        return new BytesKey(digest.digest());
    }
}
