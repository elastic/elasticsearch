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
import org.elasticsearch.shield.authc.support.ldap.LdapSslSocketFactory;

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
    public static final String SHIELD_TRANSPORT_SSL = "shield.transport.ssl";
    public static final String SHIELD_HTTP_SSL = "shield.http.ssl";
    public static final String SHIELD_AUTHC_LDAP_URL = "shield.authc.ldap.url";
    private final SSLContext sslContext;
    private final String[] ciphers;

    @Inject
    public SSLService(Settings settings) {
        super(settings);

        String keyStorePath = componentSettings.get("keystore", System.getProperty("javax.net.ssl.keyStore"));
        String keyStorePassword = componentSettings.get("keystore_password", System.getProperty("javax.net.ssl.keyStorePassword"));
        String keyStoreAlgorithm = componentSettings.get("keystore_algorithm", System.getProperty("ssl.KeyManagerFactory.algorithm", KeyManagerFactory.getDefaultAlgorithm()));

        String trustStorePath = componentSettings.get("truststore", System.getProperty("javax.net.ssl.trustStore"));
        String trustStorePassword = componentSettings.get("truststore_password", System.getProperty("javax.net.ssl.trustStorePassword"));
        String trustStoreAlgorithm = componentSettings.get("truststore_algorithm", System.getProperty("ssl.TrustManagerFactory.algorithm", TrustManagerFactory.getDefaultAlgorithm()));

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
        String sslProtocol = componentSettings.get("protocol", "TLS");

        logger.debug("using keyStore[{}], keyAlgorithm[{}], trustStore[{}], truststoreAlgorithm[{}], ciphersuites[{}], TLS protocol[{}]",
                keyStorePath, keyStoreAlgorithm, trustStorePath, trustStoreAlgorithm, ciphers, sslProtocol);

        final TrustManagerFactory trustFactory;
        try (FileInputStream in = new FileInputStream(trustStorePath)) {
            // Load TrustStore
            KeyStore trustStore = KeyStore.getInstance("jks");
            trustStore.load(in, trustStorePassword == null ? null : trustStorePassword.toCharArray());

            // Initialize a trust manager factory with the trusted store
            trustFactory = TrustManagerFactory.getInstance(trustStoreAlgorithm);
            trustFactory.init(trustStore);
        } catch (Exception e) {
            throw new ElasticsearchException("Failed to initialize a TrustManagerFactory", e);
        }

        KeyStore keyStore;
        KeyManagerFactory keyManagerFactory;
        try (FileInputStream in = new FileInputStream(keyStorePath)){
            // Load KeyStore
            keyStore = KeyStore.getInstance("jks");
            keyStore.load(in, keyStorePassword.toCharArray());

            // Initialize KeyManagerFactory
            keyManagerFactory = KeyManagerFactory.getInstance(keyStoreAlgorithm);
            keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());

        } catch (Exception e) {
            throw new ElasticsearchSSLException("Failed to initialize a KeyManagerFactory", e);
        }

        // Initialize sslContext
        try {
            sslContext = SSLContext.getInstance(sslProtocol);
            sslContext.init(keyManagerFactory.getKeyManagers(), trustFactory.getTrustManagers(), null);
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
     * Http Client-to-Node (inbound):
     *    - sslEngine.setUserClientMode(false)
     *    - sslEngine.setNeedClientAuth(false)
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
        return settings.getAsBoolean(SHIELD_TRANSPORT_SSL, false) ||
                settings.getAsBoolean(SHIELD_HTTP_SSL, false) ||
                (LdapSslSocketFactory.secureUrls(settings.getAsArray(SHIELD_AUTHC_LDAP_URL)) &&
                        LdapModule.enabled(settings));
    }
}
