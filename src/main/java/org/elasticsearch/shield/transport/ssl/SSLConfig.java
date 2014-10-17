/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Arrays;

/**
 *
 */
public class SSLConfig {

    private static final ESLogger logger = Loggers.getLogger(SSLConfig.class);
    static final String[] DEFAULT_CIPHERS = new String[] { "TLS_RSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA" };
    private final boolean clientAuth;

    private SSLContext sslContext;
    private String[] ciphers;

    public SSLConfig(Settings componentSettings, Settings defaultSettings, boolean defaultRequireClientAuth) {
        this.clientAuth = componentSettings.getAsBoolean("require.client.auth", defaultSettings.getAsBoolean("require.client.auth", defaultRequireClientAuth));
        TrustManager[] trustManagers = null;
        if (clientAuth) {
            trustManagers = new SSLTrustConfig(componentSettings, defaultSettings).getTrustManagers();
        }

        String keyStore = componentSettings.get("keystore", defaultSettings.get("keystore", System.getProperty("javax.net.ssl.keyStore")));
        String keyStorePassword = componentSettings.get("keystore_password", defaultSettings.get("keystore_password", System.getProperty("javax.net.ssl.keyStorePassword")));
        String keyStoreAlgorithm = componentSettings.get("keystore_algorithm", defaultSettings.get("keystore_algorithm", System.getProperty("ssl.KeyManagerFactory.algorithm")));
        this.ciphers = componentSettings.getAsArray("ciphers", defaultSettings.getAsArray("ciphers", DEFAULT_CIPHERS));

        if (keyStore == null) {
            throw new ElasticsearchException("SSL Enabled, but keystore unconfigured");
        }

        if (keyStoreAlgorithm == null) {
            keyStoreAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
        }

        logger.debug("using keyStore[{}], keyAlgorithm[{}], ", keyStore, keyStoreAlgorithm);

        if (!new File(keyStore).exists()) {
            throw new ElasticsearchSSLException("Keystore at path ["+ keyStore +"] does not exist");
        }

        KeyStore ks = null;
        KeyManagerFactory kmf = null;
        try (FileInputStream in = new FileInputStream(keyStore)){
            // Load KeyStore
            ks = KeyStore.getInstance("jks");
            ks.load(in, keyStorePassword.toCharArray());

            // Initialize KeyManagerFactory
            kmf = KeyManagerFactory.getInstance(keyStoreAlgorithm);
            kmf.init(ks, keyStorePassword.toCharArray());
        } catch (Exception e) {
            throw new ElasticsearchSSLException("Failed to initialize a KeyManagerFactory", e);
        }

        // Initialize sslContext
        try {
            String algorithm = componentSettings.get("context_algorithm", defaultSettings.get("shield.ssl.context_algorithm", "TLS"));
            sslContext = SSLContext.getInstance(algorithm);
            sslContext.init(kmf.getKeyManagers(), trustManagers, null);
        } catch (Exception e) {
            throw new ElasticsearchSSLException("Failed to initialize the SSLContext", e);
        }

    }

   public SSLEngine createSSLEngine() {
       SSLEngine sslEngine = sslContext.createSSLEngine();
       try {
           sslEngine.setEnabledCipherSuites(ciphers);
       } catch (Throwable t) {
           throw new ElasticsearchSSLException("Error loading cipher suites ["+Arrays.asList(ciphers)+"]", t);
       }
       sslEngine.setNeedClientAuth(clientAuth);
       return sslEngine;
   }

}
