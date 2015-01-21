/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.primitives.Ints;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.shield.ShieldSettingsException;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Map;

/**
 * This service houses the private key and trust managers needed for SSL/TLS negotiation.  It is the central place to
 * get SSLEngines and SocketFactories.
 */
public class SSLService extends AbstractComponent {

    static final String[] DEFAULT_CIPHERS = new String[]{ "TLS_RSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA" };
    static final String[] DEFAULT_SUPPORTED_PROTOCOLS = new String[] {"TLSv1", "TLSv1.1", "TLSv1.2"};
    static final TimeValue DEFAULT_SESSION_CACHE_TIMEOUT = TimeValue.timeValueHours(24);

    private Map<String, SSLContext> sslContexts = ConcurrentCollections.newConcurrentMap();

    @Inject
    public SSLService(Settings settings) {
        super(settings);
    }

    /**
     * @return An SSLSocketFactory (for client-side SSL handshaking)
     */
    public SSLSocketFactory getSSLSocketFactory() {
        return getSslContext(ImmutableSettings.EMPTY).getSocketFactory();
    }

    public String[] supportedProtocols() {
        return componentSettings.getAsArray("supported_protocols", DEFAULT_SUPPORTED_PROTOCOLS);
    }

    public String[] ciphers() {
        return componentSettings.getAsArray("ciphers", DEFAULT_CIPHERS);
    }

    public SSLEngine createSSLEngine() {
        return createSSLEngine(ImmutableSettings.EMPTY);
    }

    public SSLEngine createSSLEngine(Settings settings) {
        return createSSLEngine(settings, null, -1);
    }

    public SSLEngine createSSLEngine(Settings settings, String host, int port) {
        String[] ciphers = settings.getAsArray("ciphers", ciphers());
        String[] supportedProtocols = settings.getAsArray("supported_protocols", supportedProtocols());
        return createSSLEngine(getSslContext(settings), ciphers, supportedProtocols, host, port);
    }

    public SSLContext getSslContext() {
        return getSslContext(ImmutableSettings.EMPTY);
    }

    private SSLContext getSslContext(Settings settings) {
        String keyStorePath = settings.get("keystore.path", componentSettings.get("keystore.path", System.getProperty("javax.net.ssl.keyStore")));
        String keyStorePassword = settings.get("keystore.password", componentSettings.get("keystore.password", System.getProperty("javax.net.ssl.keyStorePassword")));
        String keyStoreAlgorithm = settings.get("keystore.algorithm", componentSettings.get("keystore.algorithm", System.getProperty("ssl.KeyManagerFactory.algorithm", KeyManagerFactory.getDefaultAlgorithm())));
        String keyPassword = settings.get("keystore.key_password", componentSettings.get("keystore.key_password", keyStorePassword));

        String trustStorePath = settings.get("truststore.path", componentSettings.get("truststore.path", System.getProperty("javax.net.ssl.trustStore")));
        String trustStorePassword = settings.get("truststore.password", componentSettings.get("truststore.password", System.getProperty("javax.net.ssl.trustStorePassword")));
        String trustStoreAlgorithm = settings.get("truststore.algorithm", componentSettings.get("truststore.algorithm", System.getProperty("ssl.TrustManagerFactory.algorithm", TrustManagerFactory.getDefaultAlgorithm())));

        if (trustStorePath == null) {
            //the keystore will also be the truststore
            trustStorePath = keyStorePath;
            trustStorePassword = keyStorePassword;
        }

        if (keyStorePath == null) {
            throw new ShieldSettingsException("no keystore configured");
        }
        if (keyStorePassword == null) {
            throw new ShieldSettingsException("no keystore password configured");
        }

        //protocols supported: https://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#SSLContext
        String sslProtocol = componentSettings.get("protocol", "TLS");

        // no need for a complex key, same path + protocol define about reusability of a SSLContext
        // also no need for pwd verification. If it worked before, it will work again
        String key = keyStorePath + trustStorePath + sslProtocol;
        SSLContext sslContext = sslContexts.get(key);
        if (sslContext == null) {
            int sessionCacheSize = settings.getAsInt("session.cache_size", componentSettings.getAsInt("session.cache_size", 1000));
            TimeValue sessionCacheTimeout = settings.getAsTime("session.cache_timeout", componentSettings.getAsTime("session.cache_timeout", DEFAULT_SESSION_CACHE_TIMEOUT));
            logger.debug("using keystore[{}], key_algorithm[{}], truststore[{}], truststore_algorithm[{}], tls_protocol[{}], session_cache_size[{}], session_cache_timeout[{}]",
                keyStorePath, keyStoreAlgorithm, trustStorePath, trustStoreAlgorithm, sslProtocol, sessionCacheSize, sessionCacheTimeout);

            TrustManagerFactory trustFactory = getTrustFactory(trustStorePath, trustStorePassword, trustStoreAlgorithm);
            KeyManagerFactory keyManagerFactory = createKeyManagerFactory(keyStorePath, keyStorePassword, keyStoreAlgorithm, keyPassword);
            sslContext = createSslContext(keyManagerFactory, trustFactory, sslProtocol, sessionCacheSize, sessionCacheTimeout);
            sslContexts.put(key, sslContext);
        } else {
            logger.trace("found keystore[{}], truststore[{}], tls_protocol[{}] in SSL context cache, reusing", keyStorePath, trustStorePath, sslProtocol);
        }

        return sslContext;
    }

    private SSLEngine createSSLEngine(SSLContext sslContext, String[] ciphers, String[] supportedProtocols, String host, int port) {
        SSLEngine sslEngine = sslContext.createSSLEngine(host, port);
        try {
            sslEngine.setEnabledCipherSuites(ciphers);
        } catch (Throwable t) {
            throw new ElasticsearchSSLException("failed loading cipher suites [" + Arrays.asList(ciphers) + "]", t);
        }

        try {
            sslEngine.setEnabledProtocols(supportedProtocols);
        } catch (IllegalArgumentException e) {
            throw new ElasticsearchSSLException("failed setting supported protocols [" + Arrays.asList(supportedProtocols) + "]", e);
        }
        return sslEngine;
    }

    private KeyManagerFactory createKeyManagerFactory(String keyStore, String keyStorePassword, String keyStoreAlgorithm, String keyPassword) {
        try (FileInputStream in = new FileInputStream(keyStore)) {
            // Load KeyStore
            KeyStore ks = KeyStore.getInstance("jks");
            ks.load(in, keyStorePassword.toCharArray());

            // Initialize KeyManagerFactory
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(keyStoreAlgorithm);
            kmf.init(ks, keyPassword.toCharArray());
            return kmf;
        } catch (Exception e) {
            throw new ElasticsearchSSLException("failed to initialize a KeyManagerFactory", e);
        }
    }

    private SSLContext createSslContext(KeyManagerFactory keyManagerFactory, TrustManagerFactory trustFactory, String sslProtocol, int sessionCacheSize, TimeValue sessionCacheTimeout) {
        // Initialize sslContext
        try {
            SSLContext sslContext = SSLContext.getInstance(sslProtocol);
            sslContext.init(keyManagerFactory.getKeyManagers(), trustFactory.getTrustManagers(), null);
            sslContext.getServerSessionContext().setSessionCacheSize(sessionCacheSize);
            sslContext.getServerSessionContext().setSessionTimeout(Ints.checkedCast(sessionCacheTimeout.seconds()));
            return sslContext;
        } catch (Exception e) {
            throw new ElasticsearchSSLException("failed to initialize the SSLContext", e);
        }
    }

    private TrustManagerFactory getTrustFactory(String trustStore, String trustStorePassword, String trustStoreAlgorithm) {
        try (FileInputStream in = new FileInputStream(trustStore)) {
            // Load TrustStore
            KeyStore ks = KeyStore.getInstance("jks");
            ks.load(in, trustStorePassword == null ? null : trustStorePassword.toCharArray());

            // Initialize a trust manager factory with the trusted store
            TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(trustStoreAlgorithm);
            trustFactory.init(ks);
            return trustFactory;
        } catch (Exception e) {
            throw new ElasticsearchException("failed to initialize a TrustManagerFactory", e);
        }
    }
}
