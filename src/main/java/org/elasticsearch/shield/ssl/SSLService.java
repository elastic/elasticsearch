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
import org.elasticsearch.shield.authc.ldap.LdapModule;
import org.elasticsearch.shield.authc.ldap.LdapSslSocketFactory;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Arrays;

/**
 * This service houses the private key and trust managers needed for SSL/TLS negotiation.  It is the central place to
 * get SSLEngines and SocketFactories.
 */
public class SSLService extends AbstractComponent {
    static final String[] DEFAULT_CIPHERS = new String[] { "TLS_RSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA" };
    private final TrustManagerFactory trustFactory;
    private final SSLContext sslContext;
    private final String[] ciphers;

    @Inject
    public SSLService(Settings settings) {
        super(settings);

        String keyStore = componentSettings.get("keystore", System.getProperty("javax.net.ssl.keyStore"));
        String keyStorePassword = componentSettings.get("keystore_password", System.getProperty("javax.net.ssl.keyStorePassword"));
        String keyStoreAlgorithm = componentSettings.get("keystore_algorithm", System.getProperty("ssl.KeyManagerFactory.algorithm", KeyManagerFactory.getDefaultAlgorithm()));

        String trustStore = componentSettings.get("truststore", System.getProperty("javax.net.ssl.trustStore"));
        String trustStorePassword = componentSettings.get("truststore_password", System.getProperty("javax.net.ssl.trustStorePassword"));
        String trustStoreAlgorithm = componentSettings.get("truststore_algorithm", System.getProperty("ssl.TrustManagerFactory.algorithm", TrustManagerFactory.getDefaultAlgorithm()));

        if (trustStore == null) {
            //the keystore will also be the truststore
            trustStore = keyStore;
            trustStorePassword = keyStorePassword;
        }

        if (keyStore == null) {
            throw new ShieldSettingsException("No keystore configured");
        }
        if (keyStorePassword == null) {
            throw new ShieldSettingsException("No keystore password configured");
        }

        this.ciphers = componentSettings.getAsArray("ciphers", DEFAULT_CIPHERS);
        //protocols supported: https://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#SSLContext
        String sslProtocol = componentSettings.get("protocol", "TLS");

        logger.debug("using keyStore[{}], keyAlgorithm[{}], trustStore[{}], truststoreAlgorithm[{}], ciphersuites[{}], TLS protocol[{}]",
                keyStore, keyStoreAlgorithm, trustStore, trustStoreAlgorithm, ciphers, sslProtocol);

        try (FileInputStream in = new FileInputStream(trustStore)) {
            // Load TrustStore
            KeyStore ks = KeyStore.getInstance("jks");
            ks.load(in, trustStorePassword == null ? null : trustStorePassword.toCharArray());

            // Initialize a trust manager factory with the trusted store
            trustFactory = TrustManagerFactory.getInstance(trustStoreAlgorithm);
            trustFactory.init(ks);
        } catch (Exception e) {
            throw new ElasticsearchException("Failed to initialize a TrustManagerFactory", e);
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
            sslContext = SSLContext.getInstance(sslProtocol);
            sslContext.init(kmf.getKeyManagers(), trustFactory.getTrustManagers(), null);
        } catch (Exception e) {
            throw new ElasticsearchSSLException("Failed to initialize the SSLContext", e);
        }
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
     *    - sslEngine.setUseClientMode(true)
     * Node-to-Node inbound:
     *    - sslEngine.setUseClientMode(false)
     *    - sslEngine.setNeedClientAuth(true)
     * Client-to-Node:
     *    - sslEngine.setUseClientMode(true)
     * Node-from-Client (inbound):
     *    - sslEngine.setUserClientMode(false)
     *    - sslEngine.setNeedClientAuth(false)
     * @return
     */
    public SSLEngine createSSLEngine() {
        SSLEngine sslEngine = sslContext.createSSLEngine();
        try {
            sslEngine.setEnabledCipherSuites(ciphers);
        } catch (Throwable t) {
            throw new ElasticsearchSSLException("Error loading cipher suites ["+ Arrays.asList(ciphers)+"]", t);
        }
        return sslEngine;
    }

    public static boolean isSSLEnabled(Settings settings) {
        return settings.getAsBoolean("shield.transport.ssl", false) ||
                settings.getAsBoolean("shield.http.ssl", false) ||
                (LdapSslSocketFactory.secureUrls(settings.getAsArray("shield.authc.ldap.url")) &&
                        LdapModule.enabled(settings));
    }
}
