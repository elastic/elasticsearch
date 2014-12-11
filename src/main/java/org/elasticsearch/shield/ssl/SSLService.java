/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
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

    private Map<String, SSLContext> sslContexts = Maps.newHashMapWithExpectedSize(3);

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

    public SSLEngine createSSLEngine() {
        return createSSLEngine(ImmutableSettings.EMPTY);
    }

    public SSLEngine createSSLEngine(Settings settings) {
        String[] ciphers = settings.getAsArray("ciphers", componentSettings.getAsArray("ciphers", DEFAULT_CIPHERS));
        return createSSLEngine(getSslContext(settings), ciphers);
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
            throw new ShieldSettingsException("No keystore configured");
        }
        if (keyStorePassword == null) {
            throw new ShieldSettingsException("No keystore password configured");
        }

        //protocols supported: https://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#SSLContext
        String sslProtocol = componentSettings.get("protocol", "TLS");

        // no need for a complex key, same path + protocol define about reusability of a SSLContext
        // also no need for pwd verification. If it worked before, it will work again
        String key = keyStorePath + trustStorePath + sslProtocol;
        SSLContext sslContext = sslContexts.get(key);
        if (sslContext == null) {
            logger.debug("using keyStore[{}], keyAlgorithm[{}], trustStore[{}], truststoreAlgorithm[{}], TLS protocol[{}]",
                keyStorePath, keyStoreAlgorithm, trustStorePath, trustStoreAlgorithm, sslProtocol);

            TrustManagerFactory trustFactory = getTrustFactory(trustStorePath, trustStorePassword, trustStoreAlgorithm);
            KeyManagerFactory keyManagerFactory = createKeyManagerFactory(keyStorePath, keyStorePassword, keyStoreAlgorithm, keyPassword);
            sslContext = createSslContext(keyManagerFactory, trustFactory, sslProtocol);
            sslContexts.put(key, sslContext);
        } else {
            logger.trace("Found keystore[{}], trustStore[{}], TLS protocol[{}] in SSL context cache, reusing", keyStorePath, trustStorePath, sslProtocol);
        }

        return sslContext;
    }

    private SSLEngine createSSLEngine(SSLContext sslContext, String[] ciphers) {
        SSLEngine sslEngine = sslContext.createSSLEngine();
        try {
            sslEngine.setEnabledCipherSuites(ciphers);
        } catch (Throwable t) {
            throw new ElasticsearchSSLException("Error loading cipher suites [" + Arrays.asList(ciphers) + "]", t);
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
            throw new ElasticsearchSSLException("Failed to initialize a KeyManagerFactory", e);
        }
    }

    private SSLContext createSslContext(KeyManagerFactory keyManagerFactory, TrustManagerFactory trustFactory, String sslProtocol) {
        // Initialize sslContext
        try {
            SSLContext sslContext = SSLContext.getInstance(sslProtocol);
            sslContext.init(keyManagerFactory.getKeyManagers(), trustFactory.getTrustManagers(), null);
            return sslContext;
        } catch (Exception e) {
            throw new ElasticsearchSSLException("Failed to initialize the SSLContext", e);
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
            throw new ElasticsearchException("Failed to initialize a TrustManagerFactory", e);
        }
    }
}
