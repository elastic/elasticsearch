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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.ssl.CertUtils;
import org.elasticsearch.xpack.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.RealmSettings;
import org.elasticsearch.xpack.security.authc.support.DnRoleMapper;

import javax.net.ssl.X509TrustManager;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.XPackSettings.HTTP_SSL_ENABLED;
import static org.elasticsearch.xpack.security.Security.setting;

public class PkiRealm extends Realm {

    public static final String PKI_CERT_HEADER_NAME = "__SECURITY_CLIENT_CERTIFICATE";
    public static final String TYPE = "pki";

    static final String DEFAULT_USERNAME_PATTERN = "CN=(.*?)(?:,|$)";
    private static final Setting<Pattern> USERNAME_PATTERN_SETTING = new Setting<>("username_pattern", DEFAULT_USERNAME_PATTERN,
            s -> Pattern.compile(s, Pattern.CASE_INSENSITIVE), Setting.Property.NodeScope);
    private static final SSLConfigurationSettings SSL_SETTINGS = SSLConfigurationSettings.withoutPrefix();

    // For client based cert validation, the auth type must be specified but UNKNOWN is an acceptable value
    public static final String AUTH_TYPE = "UNKNOWN";

    private final X509TrustManager trustManager;
    private final Pattern principalPattern;
    private final DnRoleMapper roleMapper;


    public PkiRealm(RealmConfig config, ResourceWatcherService watcherService, SSLService sslService) {
        this(config, new DnRoleMapper(TYPE, config, watcherService), sslService);
    }

    // pkg private for testing
    PkiRealm(RealmConfig config, DnRoleMapper roleMapper, SSLService sslService) {
        super(TYPE, config);
        this.trustManager = trustManagers(config);
        this.principalPattern = USERNAME_PATTERN_SETTING.get(config.settings());
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
    public void authenticate(AuthenticationToken authToken, ActionListener<User> listener) {
        X509AuthenticationToken token = (X509AuthenticationToken)authToken;
        if (isCertificateChainTrusted(trustManager, token, logger) == false) {
            listener.onResponse(null);
        } else {
            Set<String> roles = roleMapper.resolveRoles(token.dn(), Collections.<String>emptyList());
            listener.onResponse(new User(token.principal(), roles.toArray(new String[roles.size()])));
        }
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

    static X509TrustManager trustManagers(RealmConfig realmConfig) {
        final Settings settings = realmConfig.settings();
        final Environment env = realmConfig.env();
        String[] certificateAuthorities = settings.getAsArray(SSL_SETTINGS.caPaths.getKey(), null);
        String truststorePath = SSL_SETTINGS.truststorePath.get(settings).orElse(null);
        if (truststorePath == null && certificateAuthorities == null) {
            return null;
        } else if (truststorePath != null && certificateAuthorities != null) {
            final String pathKey = RealmSettings.getFullSettingKey(realmConfig, SSL_SETTINGS.truststorePath);
            final String caKey = RealmSettings.getFullSettingKey(realmConfig, SSL_SETTINGS.caPaths);
            throw new IllegalArgumentException("[" + pathKey + "] and [" + caKey + "] cannot be used at the same time");
        } else if (truststorePath != null) {
            return trustManagersFromTruststore(truststorePath, realmConfig);
        }
        return trustManagersFromCAs(settings, env);
    }

    private static X509TrustManager trustManagersFromTruststore(String truststorePath, RealmConfig realmConfig) {
        final Settings settings = realmConfig.settings();
        String password = SSL_SETTINGS.truststorePassword.get(settings).orElseThrow(() -> new IllegalArgumentException(
                "[" + RealmSettings.getFullSettingKey(realmConfig, SSL_SETTINGS.truststorePassword) + "] is not configured"
        ));
        String trustStoreAlgorithm = SSL_SETTINGS.truststoreAlgorithm.get(settings);
        try {
            return CertUtils.trustManager(truststorePath, password, trustStoreAlgorithm, realmConfig.env());
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to load specified truststore", e);
        }
    }

    private static X509TrustManager trustManagersFromCAs(Settings settings, Environment env) {
        String[] certificateAuthorities = settings.getAsArray(SSL_SETTINGS.caPaths.getKey(), null);
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
    // TODO move this to a Bootstrap check!
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
        final Settings transportSSLSettings = settings.getByPrefix(setting("transport.ssl."));
        final boolean clientAuthEnabled = sslService.isSSLClientAuthEnabled(transportSSLSettings);
        if (clientAuthEnabled) {
            return;
        }

        // Transport Profiles
        Map<String, Settings> groupedSettings = settings.getGroups("transport.profiles.");
        for (Map.Entry<String, Settings> entry : groupedSettings.entrySet()) {
            Settings profileSettings = entry.getValue().getByPrefix(Security.settingPrefix());
            if (sslService.isSSLClientAuthEnabled(SecurityNetty4Transport.profileSslSettings(profileSettings), transportSSLSettings)) {
                return;
            }
        }

        throw new IllegalStateException("PKI realm [" + config.name() + "] is enabled but cannot be used as neither HTTP or Transport " +
                "has SSL with client authentication enabled");
    }

    /**
     * @return The {@link Setting setting configuration} for this realm type
     */
    public static Set<Setting<?>> getSettings() {
        Set<Setting<?>> settings = new HashSet<>();
        settings.add(USERNAME_PATTERN_SETTING);

        settings.add(SSL_SETTINGS.truststorePath);
        settings.add(SSL_SETTINGS.truststorePassword);
        settings.add(SSL_SETTINGS.truststoreAlgorithm);
        settings.add(SSL_SETTINGS.caPaths);

        DnRoleMapper.getSettings(settings);

        return settings;
    }
}
