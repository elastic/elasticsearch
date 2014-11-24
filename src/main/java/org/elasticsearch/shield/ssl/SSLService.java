/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.ShieldSettingsException;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Arrays;

/**
 * This service houses the private key and trust managers needed for SSL/TLS negotiation.  It is the central place to
 * get SSLEngines and SocketFactories.
 */
public class SSLService extends AbstractComponent {

    static final String[] DEFAULT_CIPHERS = new String[]{ "TLS_RSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA" };

    private final TrustManagerFactory trustFactory;
    private final KeyManagerFactory keyManagerFactory;
    private final String sslProtocol;
    private final SSLContext sslContext;
    private final String[] ciphers;

    @Inject
    public SSLService(Settings settings) {
        super(settings);

        String keyStorePath = componentSettings.get("keystore.path", System.getProperty("javax.net.ssl.keyStore"));
        String keyStorePassword = componentSettings.get("keystore.password", System.getProperty("javax.net.ssl.keyStorePassword"));
        String keyStoreAlgorithm = componentSettings.get("keystore.algorithm", System.getProperty("ssl.KeyManagerFactory.algorithm", KeyManagerFactory.getDefaultAlgorithm()));

        String trustStorePath = componentSettings.get("truststore.path", System.getProperty("javax.net.ssl.trustStore"));
        String trustStorePassword = componentSettings.get("truststore.password", System.getProperty("javax.net.ssl.trustStorePassword"));
        String trustStoreAlgorithm = componentSettings.get("truststore.algorithm", System.getProperty("ssl.TrustManagerFactory.algorithm", TrustManagerFactory.getDefaultAlgorithm()));

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

        this.ciphers = componentSettings.getAsArray("ciphers", DEFAULT_CIPHERS);
        //protocols supported: https://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#SSLContext
        this.sslProtocol = componentSettings.get("protocol", "TLS");

        logger.debug("using keyStore[{}], keyAlgorithm[{}], trustStore[{}], truststoreAlgorithm[{}], ciphersuites[{}], TLS protocol[{}]",
                keyStorePath, keyStoreAlgorithm, trustStorePath, trustStoreAlgorithm, ciphers, sslProtocol);

        this.trustFactory = getTrustFactory(trustStorePath, trustStorePassword, trustStoreAlgorithm);
        this.keyManagerFactory = createKeyManagerFactory(keyStorePath, keyStorePassword, keyStoreAlgorithm);
        this.sslContext = createSslContext(trustFactory);
    }

    /**
     * @return An SSLSocketFactory (for client-side SSL handshaking)
     */
    public SSLSocketFactory getSSLSocketFactory() {
        return sslContext.getSocketFactory();
    }

    /**
     * This engine is configured with a trust manager and a keystore that should have only one private key.
     * Four possible usages for elasticsearch exist:
     * Node-to-Node outbound:
     * - sslEngine.setUseClientMode(true)
     * Node-to-Node inbound:
     * - sslEngine.setUseClientMode(false)
     * - sslEngine.setNeedClientAuth(true)
     * Client-to-Node:
     * - sslEngine.setUseClientMode(true)
     * Http Client-to-Node (inbound):
     * - sslEngine.setUserClientMode(false)
     * - sslEngine.setNeedClientAuth(false)
     */
    public SSLEngine createSSLEngine() {
        return createSSLEngine(this.sslContext);
    }

    public SSLEngine createSSLEngineWithTruststore(Settings settings) {
        String trustStore = settings.get("truststore.path", System.getProperty("javax.net.ssl.trustStore"));
        String trustStorePassword = settings.get("truststore.password", System.getProperty("javax.net.ssl.trustStorePassword"));
        String trustStoreAlgorithm = settings.get("truststore.algorithm", System.getProperty("ssl.TrustManagerFactory.algorithm", TrustManagerFactory.getDefaultAlgorithm()));

        // no truststore or password, return regular ssl engine
        if (trustStore == null || trustStorePassword == null) {
            return createSSLEngine();
        }

        TrustManagerFactory trustFactory = getTrustFactory(trustStore, trustStorePassword, trustStoreAlgorithm);
        SSLContext customTruststoreSSLContext = createSslContext(trustFactory);
        return createSSLEngine(customTruststoreSSLContext);
    }

    private SSLEngine createSSLEngine(SSLContext sslContext) {
        SSLEngine sslEngine = sslContext.createSSLEngine();
        try {
            sslEngine.setEnabledCipherSuites(ciphers);
        } catch (Throwable t) {
            throw new ElasticsearchSSLException("Error loading cipher suites [" + Arrays.asList(ciphers) + "]", t);
        }
        return sslEngine;
    }

    private KeyManagerFactory createKeyManagerFactory(String keyStore, String keyStorePassword, String keyStoreAlgorithm) {
        try (FileInputStream in = new FileInputStream(keyStore)) {
            // Load KeyStore
            KeyStore ks = KeyStore.getInstance("jks");
            ks.load(in, keyStorePassword.toCharArray());

            // Initialize KeyManagerFactory
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(keyStoreAlgorithm);
            kmf.init(ks, keyStorePassword.toCharArray());
            return kmf;
        } catch (Exception e) {
            throw new ElasticsearchSSLException("Failed to initialize a KeyManagerFactory", e);
        }
    }

    private SSLContext createSslContext(TrustManagerFactory trustFactory) {
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
