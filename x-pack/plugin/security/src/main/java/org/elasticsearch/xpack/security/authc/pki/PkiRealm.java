/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.pki;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.security.authc.BytesKey;
import org.elasticsearch.xpack.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.security.authc.support.DelegatedAuthorizationSupport;
import org.elasticsearch.xpack.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.CompositeRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import javax.net.ssl.X509TrustManager;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    public PkiRealm(RealmConfig config, ResourceWatcherService watcherService, NativeRoleMappingStore nativeRoleMappingStore) {
        this(config, new CompositeRoleMapper(config, watcherService, nativeRoleMappingStore));
    }

    // pkg private for testing
    PkiRealm(RealmConfig config, UserRoleMapper roleMapper) {
        super(config);
        this.trustManager = trustManagers(config);
        this.principalPattern = config.getSetting(PkiRealmSettings.USERNAME_PATTERN_SETTING);
        this.roleMapper = roleMapper;
        this.roleMapper.refreshRealmOnChange(this);
        this.cache = CacheBuilder.<BytesKey, User>builder()
                .setExpireAfterWrite(config.getSetting(PkiRealmSettings.CACHE_TTL_SETTING))
                .setMaximumWeight(config.getSetting(PkiRealmSettings.CACHE_MAX_USERS_SETTING))
                .build();
        this.delegatedRealms = null;
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
        return token(context.getTransient(PKI_CERT_HEADER_NAME), principalPattern, logger);
    }

    @Override
    public void authenticate(AuthenticationToken authToken, ActionListener<AuthenticationResult> listener) {
        assert delegatedRealms != null : "Realm has not been initialized correctly";
        X509AuthenticationToken token = (X509AuthenticationToken) authToken;
        try {
            final BytesKey fingerprint = computeFingerprint(token.credentials()[0]);
            User user = cache.get(fingerprint);
            if (user != null) {
                if (delegatedRealms.hasDelegation()) {
                    delegatedRealms.resolve(token.principal(), listener);
                } else {
                    listener.onResponse(AuthenticationResult.success(user));
                }
            } else if (isCertificateChainTrusted(trustManager, token, logger) == false) {
                listener.onResponse(AuthenticationResult.unsuccessful("Certificate for " + token.dn() + " is not trusted", null));
            } else {
                final ActionListener<AuthenticationResult> cachingListener = ActionListener.wrap(result -> {
                    if (result.isAuthenticated()) {
                        try (ReleasableLock ignored = readLock.acquire()) {
                            cache.put(fingerprint, result.getUser());
                        }
                    }
                    listener.onResponse(result);
                }, listener::onFailure);
                if (delegatedRealms.hasDelegation()) {
                    delegatedRealms.resolve(token.principal(), cachingListener);
                } else {
                    this.buildUser(token, cachingListener);
                }
            }
        } catch (CertificateEncodingException e) {
            listener.onResponse(AuthenticationResult.unsuccessful("Certificate for " + token.dn() + " has encoding issues", e));
        }
    }

    private void buildUser(X509AuthenticationToken token, ActionListener<AuthenticationResult> listener) {
        final Map<String, Object> metadata = Collections.singletonMap("pki_dn", token.dn());
        final UserRoleMapper.UserData userData = new UserRoleMapper.UserData(token.principal(),
                token.dn(), Collections.emptySet(), metadata, this.config);
        roleMapper.resolveRoles(userData, ActionListener.wrap(roles -> {
            final User computedUser =
                    new User(token.principal(), roles.toArray(new String[roles.size()]), null, null, metadata, true);
            listener.onResponse(AuthenticationResult.success(computedUser));
        }, listener::onFailure));
    }

    @Override
    public void lookupUser(String username, ActionListener<User> listener) {
        listener.onResponse(null);
    }

    static X509AuthenticationToken token(Object pkiHeaderValue, Pattern principalPattern, Logger logger) {
        if (pkiHeaderValue == null) {
            return null;
        }

        assert pkiHeaderValue instanceof X509Certificate[];
        X509Certificate[] certificates = (X509Certificate[]) pkiHeaderValue;
        if (certificates.length == 0) {
            return null;
        }

        String dn = certificates[0].getSubjectX500Principal().toString();
        Matcher matcher = principalPattern.matcher(dn);
        if (!matcher.find()) {
            if (logger.isDebugEnabled()) {
                logger.debug("certificate authentication succeeded for [{}] but could not extract principal from DN", dn);
            }
            return null;
        }

        String principal = matcher.group(1);
        if (Strings.isNullOrEmpty(principal)) {
            if (logger.isDebugEnabled()) {
                logger.debug("certificate authentication succeeded for [{}] but extracted principal was empty", dn);
            }
            return null;
        }
        return new X509AuthenticationToken(certificates, principal, dn);
    }

    static boolean isCertificateChainTrusted(X509TrustManager trustManager, X509AuthenticationToken token, Logger logger) {
        if (trustManager != null) {
            try {
                trustManager.checkClientTrusted(token.credentials(), AUTH_TYPE);
                return true;
            } catch (CertificateException e) {
                if (logger.isTraceEnabled()) {
                    logger.trace((Supplier<?>)
                            () -> new ParameterizedMessage("failed certificate validation for principal [{}]", token.principal()), e);
                } else if (logger.isDebugEnabled()) {
                    logger.debug("failed certificate validation for principal [{}]", token.principal());
                }
            }
            return false;
        }

        // No extra trust managers specified, so at this point we can be considered authenticated.
        return true;
    }

    X509TrustManager trustManagers(RealmConfig realmConfig) {
        final List<String> certificateAuthorities = realmConfig.hasSetting(PkiRealmSettings.CAPATH_SETTING) ?
                realmConfig.getSetting(PkiRealmSettings.CAPATH_SETTING) : null;
        String truststorePath = realmConfig.getSetting(PkiRealmSettings.TRUST_STORE_PATH).orElse(null);
        if (truststorePath == null && certificateAuthorities == null) {
            return null;
        } else if (truststorePath != null && certificateAuthorities != null) {
            final String pathKey = RealmSettings.getFullSettingKey(realmConfig, PkiRealmSettings.TRUST_STORE_PATH);
            final String caKey = RealmSettings.getFullSettingKey(realmConfig, PkiRealmSettings.CAPATH_SETTING);
            throw new IllegalArgumentException("[" + pathKey + "] and [" + caKey + "] cannot be used at the same time");
        } else if (truststorePath != null) {
            final X509TrustManager trustManager = trustManagersFromTruststore(truststorePath, realmConfig);
            if (trustManager.getAcceptedIssuers().length == 0) {
                logger.warn("PKI Realm {} uses truststore {} which has no accepted certificate issuers", this, truststorePath);
            }
            return trustManager;
        }
        final X509TrustManager trustManager = trustManagersFromCAs(certificateAuthorities, realmConfig.env());
        if (trustManager.getAcceptedIssuers().length == 0) {
            logger.warn("PKI Realm {} uses CAs {} with no accepted certificate issuers", this, certificateAuthorities);
        }
        return trustManager;
    }

    private static X509TrustManager trustManagersFromTruststore(String truststorePath, RealmConfig realmConfig) {
        if (realmConfig.hasSetting(PkiRealmSettings.TRUST_STORE_PASSWORD) == false
                && realmConfig.hasSetting(PkiRealmSettings.LEGACY_TRUST_STORE_PASSWORD) == false) {
            throw new IllegalArgumentException("Neither [" +
                    RealmSettings.getFullSettingKey(realmConfig, PkiRealmSettings.TRUST_STORE_PASSWORD) + "] or [" +
                    RealmSettings.getFullSettingKey(realmConfig, PkiRealmSettings.LEGACY_TRUST_STORE_PASSWORD)
                    + "] is configured");
        }
        try (SecureString password = realmConfig.getSetting(PkiRealmSettings.TRUST_STORE_PASSWORD)) {
            String trustStoreAlgorithm = realmConfig.getSetting(PkiRealmSettings.TRUST_STORE_ALGORITHM);
            String trustStoreType = SSLConfigurationSettings.getKeyStoreType(
                    realmConfig.getConcreteSetting(PkiRealmSettings.TRUST_STORE_TYPE), realmConfig.settings(),
                    truststorePath);
            try {
                return CertParsingUtils.trustManager(truststorePath, trustStoreType, password.getChars(), trustStoreAlgorithm, realmConfig
                    .env());
            } catch (Exception e) {
                throw new IllegalArgumentException("failed to load specified truststore", e);
            }
        }
    }

    private static X509TrustManager trustManagersFromCAs(List<String> certificateAuthorities, Environment env) {
        assert certificateAuthorities != null;
        try {
            Certificate[] certificates = CertParsingUtils.readCertificates(certificateAuthorities, env);
            return CertParsingUtils.trustManager(certificates);
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load certificate authorities for PKI realm", e);
        }
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

    private static BytesKey computeFingerprint(X509Certificate certificate) throws CertificateEncodingException {
        MessageDigest digest = MessageDigests.sha256();
        digest.update(certificate.getEncoded());
        return new BytesKey(digest.digest());
    }
}
