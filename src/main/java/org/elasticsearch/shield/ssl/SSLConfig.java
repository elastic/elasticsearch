/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
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
    // TODO removing the second one results in fails, need to verify the differences, maybe per JVM?
    public static final String[] DEFAULT_CIPHERS = new String[] { "TLS_RSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA" };
    private final boolean clientAuth;

    private SSLContext sslContext;
    private String[] ciphers;

    public SSLConfig(Settings componentSettings) {
        this(componentSettings, ImmutableSettings.EMPTY);
    }

    public SSLConfig(Settings componentSettings, Settings settings) {
        this.clientAuth = componentSettings.getAsBoolean("client.auth", settings.getAsBoolean("shield.ssl.client.auth", true));
        String keyStore = componentSettings.get("keystore", settings.get("shield.ssl.keystore", System.getProperty("javax.net.ssl.keyStore")));
        String keyStorePassword = componentSettings.get("keystore_password", settings.get("shield.ssl.keystore_password", System.getProperty("javax.net.ssl.keyStorePassword")));
        String keyStoreAlgorithm = componentSettings.get("keystore_algorithm", settings.get("shield.ssl.keystore_algorithm", System.getProperty("ssl.KeyManagerFactory.algorithm")));
        String trustStore = componentSettings.get("truststore", settings.get("shield.ssl.truststore", System.getProperty("javax.net.ssl.trustStore")));
        String trustStorePassword = componentSettings.get("truststore_password", settings.get("shield.ssl.truststore_password", System.getProperty("javax.net.ssl.trustStorePassword")));
        String trustStoreAlgorithm = componentSettings.get("truststore_algorithm", settings.get("shield.ssl.truststore_algorithm", System.getProperty("ssl.TrustManagerFactory.algorithm")));
        this.ciphers = componentSettings.getAsArray("ciphers", settings.getAsArray("shield.ssl.ciphers", DEFAULT_CIPHERS));

        if (keyStore == null) {
            throw new ElasticsearchException("SSL Enabled, but keystore unconfigured");
        }

        if (trustStore == null) {
            throw new ElasticsearchException("SSL Enabled, but truststore unconfigured");
        }

        if (keyStoreAlgorithm == null) {
            keyStoreAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
        }

        if (trustStoreAlgorithm == null) {
            trustStoreAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
        }

        logger.debug("using keyStore[{}], keyAlgorithm[{}], trustStore[{}], trustAlgorithm[{}]", keyStore, keyStoreAlgorithm, trustStore, trustStoreAlgorithm);

        if (!new File(keyStore).exists()) {
            throw new ElasticsearchSSLException("Keystore at path ["+ keyStore +"] does not exist");
        }

        if (!new File(trustStore).exists()) {
            throw new ElasticsearchSSLException("Truststore at path ["+ keyStore +"] does not exist");
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

        TrustManager[] trustManagers = null;
        try (FileInputStream in = new FileInputStream(trustStore)) {
            // Load TrustStore
            ks.load(in, trustStorePassword.toCharArray());

            // Initialize a trust manager factory with the trusted store
            TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(trustStoreAlgorithm);
            trustFactory.init(ks);

            // Retrieve the trust managers from the factory
            trustManagers = trustFactory.getTrustManagers();
        } catch (Exception e) {
            throw new ElasticsearchException("Failed to initialize a TrustManagerFactory", e);
        }

        // Initialize sslContext
        try {
            String algorithm = componentSettings.get("context_algorithm", settings.get("shield.ssl.context_algorithm", "TLS"));
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
