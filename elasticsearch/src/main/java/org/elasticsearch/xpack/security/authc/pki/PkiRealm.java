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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.ssl.CertUtils;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.support.DnRoleMapper;

import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.security.Security.setting;
import static org.elasticsearch.xpack.XPackSettings.HTTP_SSL_ENABLED;
import static org.elasticsearch.xpack.XPackSettings.TRANSPORT_SSL_ENABLED;

public class PkiRealm extends Realm {

    public static final String PKI_CERT_HEADER_NAME = "__SECURITY_CLIENT_CERTIFICATE";
    public static final String TYPE = "pki";
    public static final String DEFAULT_USERNAME_PATTERN = "CN=(.*?)(?:,|$)";

    // For client based cert validation, the auth type must be specified but UNKNOWN is an acceptable value
    public static final String AUTH_TYPE = "UNKNOWN";

    private final X509TrustManager trustManager;
    private final Pattern principalPattern;
    private final DnRoleMapper roleMapper;


    public PkiRealm(RealmConfig config, ResourceWatcherService watcherService, SSLService sslService) {
        this(config, new DnRoleMapper(TYPE, config, watcherService, null), sslService);
    }

    // pkg private for testing
    PkiRealm(RealmConfig config, DnRoleMapper roleMapper, SSLService sslService) {
        super(TYPE, config);
        this.trustManager = trustManagers(config);
        this.principalPattern = Pattern.compile(config.settings().get("username_pattern", DEFAULT_USERNAME_PATTERN),
                Pattern.CASE_INSENSITIVE);
        this.roleMapper = roleMapper;
        checkSSLEnabled(config, sslService);
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
    public User authenticate(AuthenticationToken authToken) {
        X509AuthenticationToken token = (X509AuthenticationToken)authToken;
        if (isCertificateChainTrusted(trustManager, token, logger) == false) {
            return null;
        }

        Set<String> roles = roleMapper.resolveRoles(token.dn(), Collections.<String>emptyList());
        return new User(token.principal(), roles.toArray(new String[roles.size()]));
    }

    @Override
    public User lookupUser(String username) {
        return null;
    }

    @Override
    public boolean userLookupSupported() {
        return false;
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

    static X509TrustManager trustManagers(RealmConfig realmConfig) {
        final Settings settings = realmConfig.settings();
        final Environment env = realmConfig.env();
        String[] certificateAuthorities = settings.getAsArray("certificate_authorities", null);
        String truststorePath = settings.get("truststore.path");
        if (truststorePath == null && certificateAuthorities == null) {
            return null;
        } else if (truststorePath != null && certificateAuthorities != null) {
            final String settingPrefix = Realms.REALMS_GROUPS_SETTINGS.getKey() + realmConfig.name() + ".";
            throw new IllegalArgumentException("[" + settingPrefix + "truststore.path] and [" + settingPrefix + "certificate_authorities]" +
                    " cannot be used at the same time");
        } else if (truststorePath != null) {
            return trustManagersFromTruststore(realmConfig);
        }
        return trustManagersFromCAs(settings, env);
    }

    private static X509TrustManager trustManagersFromTruststore(RealmConfig realmConfig) {
        final Settings settings = realmConfig.settings();
        String truststorePath = settings.get("truststore.path");
        String password = settings.get("truststore.password");
        if (password == null) {
            final String settingPrefix = Realms.REALMS_GROUPS_SETTINGS.getKey() + realmConfig.name() + ".";
            throw new IllegalArgumentException("[" + settingPrefix + "truststore.password] is not configured");
        }

        String trustStoreAlgorithm = settings.get("truststore.algorithm", System.getProperty("ssl.TrustManagerFactory.algorithm",
                TrustManagerFactory.getDefaultAlgorithm()));
        try {
            return CertUtils.trustManager(truststorePath, password, trustStoreAlgorithm, realmConfig.env());
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to load specified truststore", e);
        }
    }

    private static X509TrustManager trustManagersFromCAs(Settings settings, Environment env) {
        String[] certificateAuthorities = settings.getAsArray("certificate_authorities", null);
        assert certificateAuthorities != null;
        try {
            Certificate[] certificates = CertUtils.readCertificates(Arrays.asList(certificateAuthorities), env);
            return CertUtils.trustManager(certificates);
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load certificate authorities for PKI realm", e);
        }
    }

    /**
     * Checks to see if both SSL and Client authentication are enabled on at least one network communication layer. If
     * not an exception will be thrown
     *
     * @param config this realm's configuration
     * @param sslService the SSLService to use for ssl configurations
     */
    static void checkSSLEnabled(RealmConfig config, SSLService sslService) {
        Settings settings = config.globalSettings();

        // HTTP
        final boolean httpSsl = HTTP_SSL_ENABLED.get(settings);
        Settings httpSSLSettings = SSLService.getHttpTransportSSLSettings(settings);
        final boolean httpClientAuth = sslService.isSSLClientAuthEnabled(httpSSLSettings);
        if (httpSsl && httpClientAuth) {
            return;
        }

        // Default Transport
        final boolean ssl = TRANSPORT_SSL_ENABLED.get(settings);
        final Settings transportSSLSettings = settings.getByPrefix(setting("transport.ssl."));
        final boolean clientAuthEnabled = sslService.isSSLClientAuthEnabled(transportSSLSettings);
        if (ssl && clientAuthEnabled) {
            return;
        }

        // Transport Profiles
        Map<String, Settings> groupedSettings = settings.getGroups("transport.profiles.");
        for (Map.Entry<String, Settings> entry : groupedSettings.entrySet()) {
            Settings profileSettings = entry.getValue().getByPrefix(Security.settingPrefix());
            if (SecurityNetty4Transport.PROFILE_SSL_SETTING.get(profileSettings)
                    && sslService.isSSLClientAuthEnabled(
                    SecurityNetty4Transport.profileSslSettings(profileSettings), transportSSLSettings)) {
                return;
            }
        }

        throw new IllegalStateException("PKI realm [" + config.name() + "] is enabled but cannot be used as neither HTTP or Transport " +
                "has SSL with client authentication enabled");
    }
}
