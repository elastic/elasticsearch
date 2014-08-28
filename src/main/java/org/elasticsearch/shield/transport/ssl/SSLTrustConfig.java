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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;

/**
 *
 */
public class SSLTrustConfig {

    private static final ESLogger logger = Loggers.getLogger(SSLTrustConfig.class);
    private final TrustManager[] trustManagers;
    private final String sslContextAlgorithm;

    private SSLContext sslContext;

    public SSLTrustConfig(Settings componentSettings, Settings defaultSettings) {
        this.sslContextAlgorithm = componentSettings.get("context_algorithm", defaultSettings.get("shield.ssl.context_algorithm", "TLS"));
        String trustStore = componentSettings.get("truststore", defaultSettings.get("truststore", System.getProperty("javax.net.ssl.trustStore")));
        String trustStorePassword = componentSettings.get("truststore_password", defaultSettings.get("truststore_password", System.getProperty("javax.net.ssl.trustStorePassword")));
        String trustStoreAlgorithm = componentSettings.get("truststore_algorithm", defaultSettings.get("truststore_algorithm", System.getProperty("ssl.TrustManagerFactory.algorithm")));

        if (trustStore == null) {
            throw new ElasticsearchException("SSL Enabled, but truststore unconfigured");
        }

        if (trustStoreAlgorithm == null) {
            trustStoreAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
        }

        logger.debug("using trustStore[{}], trustAlgorithm[{}]", trustStore, trustStoreAlgorithm);

        if (!new File(trustStore).exists()) {
            throw new ElasticsearchSSLException("Truststore at path ["+ trustStore +"] does not exist");
        }

        try (FileInputStream in = new FileInputStream(trustStore)) {
            // Load TrustStore
            KeyStore ks = KeyStore.getInstance("jks");
            ks.load(in, trustStorePassword == null ? null : trustStorePassword.toCharArray());

            // Initialize a trust manager factory with the trusted store
            TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(trustStoreAlgorithm);
            trustFactory.init(ks);

            // Retrieve the trust managers from the factory
            trustManagers = trustFactory.getTrustManagers();
        } catch (Exception e) {
            throw new ElasticsearchException("Failed to initialize a TrustManagerFactory", e);
        }
    }

    public SSLSocketFactory createSSLSocketFactory() {
        // Initialize sslContext
        try {
            sslContext = SSLContext.getInstance(sslContextAlgorithm);
            sslContext.init(null, trustManagers, null);
        } catch (Exception e) {
            throw new ElasticsearchSSLException("Failed to initialize the SSLContext", e);
        }
        return sslContext.getSocketFactory();
    }

    public TrustManager[] getTrustManagers() {
        return trustManagers;
    }
}
