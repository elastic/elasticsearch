/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.cache.LoadingCache;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.primitives.Ints;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.UncheckedExecutionException;
import org.elasticsearch.shield.ShieldSettingsException;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * This service houses the private key and trust managers needed for SSL/TLS negotiation.  It is the central place to
 * get SSLEngines and SocketFactories.
 */
public abstract class AbstractSSLService extends AbstractComponent {

    static final String[] DEFAULT_CIPHERS = new String[] { "TLS_RSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA" };
    static final String[] DEFAULT_SUPPORTED_PROTOCOLS = new String[] { "TLSv1", "TLSv1.1", "TLSv1.2" };
    static final TimeValue DEFAULT_SESSION_CACHE_TIMEOUT = TimeValue.timeValueHours(24);
    static final int DEFAULT_SESSION_CACHE_SIZE = 1000;
    static final String DEFAULT_PROTOCOL = "TLS";

    protected LoadingCache<SSLSettings, SSLContext> sslContexts = CacheBuilder.newBuilder().build(new SSLContextCacheLoader());

    public AbstractSSLService(Settings settings) {
        super(settings);
    }

    /**
     * @return A SSLSocketFactory (for client-side SSL handshaking)
     */
    public SSLSocketFactory sslSocketFactory() {
        return sslContext(ImmutableSettings.EMPTY).getSocketFactory();
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
        return createSSLEngine(sslContext(settings), ciphers, supportedProtocols, host, port);
    }

    public SSLContext sslContext() {
        return sslContext(ImmutableSettings.EMPTY);
    }

    protected SSLContext sslContext(Settings settings) {
        SSLSettings sslSettings = sslSettings(settings);
        try {
            return sslContexts.getUnchecked(sslSettings);
        } catch (UncheckedExecutionException e) {
            // Unwrap ElasticsearchSSLException
            if (e.getCause() instanceof ElasticsearchSSLException) {
                throw (ElasticsearchSSLException) e.getCause();
            } else {
                throw new ElasticsearchSSLException("failed to load SSLContext", e);
            }
        }
    }

    protected abstract SSLSettings sslSettings(Settings customSettings);

    SSLEngine createSSLEngine(SSLContext sslContext, String[] ciphers, String[] supportedProtocols, String host, int port) {
        SSLEngine sslEngine = sslContext.createSSLEngine(host, port);
        try {
            sslEngine.setEnabledCipherSuites(supportedCiphers(sslEngine.getSupportedCipherSuites(), ciphers));
        } catch (ElasticsearchException e) {
            throw e;
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

    String[] supportedCiphers(String[] supportedCiphers, String[] requestedCiphers) {
        List<String> requestedCiphersList = new ArrayList<>(requestedCiphers.length);
        List<String> unsupportedCiphers = new LinkedList<>();
        boolean found;
        for (String requestedCipher : requestedCiphers) {
            found = false;
            for (String supportedCipher : supportedCiphers) {
                if (supportedCipher.equals(requestedCipher)) {
                    found = true;
                    requestedCiphersList.add(requestedCipher);
                    break;
                }
            }

            if (!found) {
                unsupportedCiphers.add(requestedCipher);
            }
        }

        if (requestedCiphersList.isEmpty()) {
            throw new ShieldSettingsException("none of the ciphers [" + Arrays.asList(requestedCiphers) + "] are supported by this JVM");
        }

        if (!unsupportedCiphers.isEmpty()) {
            logger.error("unsupported ciphers [{}] were requested but cannot be used in this JVM. If you are trying to use ciphers\n" +
                    "with a key length greater than 128 bits on an Oracle JVM, you will need to install the unlimited strength\n" +
                    "JCE policy files. Additionally, please ensure the PKCS11 provider is enabled for your JVM.", unsupportedCiphers);
        }

        return requestedCiphersList.toArray(new String[requestedCiphersList.size()]);
    }

    private class SSLContextCacheLoader extends CacheLoader<SSLSettings, SSLContext> {

        @Override
        public SSLContext load(SSLSettings sslSettings) throws Exception {
            if (logger.isDebugEnabled()) {
                logger.debug("using keystore[{}], key_algorithm[{}], truststore[{}], truststore_algorithm[{}], tls_protocol[{}], session_cache_size[{}], session_cache_timeout[{}]",
                        sslSettings.keyStorePath, sslSettings.keyStoreAlgorithm, sslSettings.trustStorePath, sslSettings.trustStoreAlgorithm, sslSettings.sslProtocol, sslSettings.sessionCacheSize, sslSettings.sessionCacheTimeout);
            }

            TrustManager[] trustManagers = trustManagers(sslSettings.trustStorePath, sslSettings.trustStorePassword, sslSettings.trustStoreAlgorithm);
            KeyManager[] keyManagers = keyManagers(sslSettings.keyStorePath, sslSettings.keyStorePassword, sslSettings.keyStoreAlgorithm, sslSettings.keyPassword);
            return createSslContext(keyManagers, trustManagers, sslSettings.sslProtocol, sslSettings.sessionCacheSize, sslSettings.sessionCacheTimeout);
        }


        private KeyManager[] keyManagers(String keyStore, String keyStorePassword, String keyStoreAlgorithm, String keyPassword) {
            if (keyStore == null) {
                return null;
            }

            try {
                // Load KeyStore
                KeyStore ks = readKeystore(keyStore, keyStorePassword);

                // Initialize KeyManagerFactory
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(keyStoreAlgorithm);
                kmf.init(ks, keyPassword.toCharArray());
                return kmf.getKeyManagers();
            } catch (Exception e) {
                throw new ElasticsearchSSLException("failed to initialize a KeyManagerFactory", e);
            }
        }

        private SSLContext createSslContext(KeyManager[] keyManagers, TrustManager[] trustManagers, String sslProtocol, int sessionCacheSize, TimeValue sessionCacheTimeout) {
            // Initialize sslContext
            try {
                SSLContext sslContext = SSLContext.getInstance(sslProtocol);
                sslContext.init(keyManagers, trustManagers, null);
                sslContext.getServerSessionContext().setSessionCacheSize(sessionCacheSize);
                sslContext.getServerSessionContext().setSessionTimeout(Ints.checkedCast(sessionCacheTimeout.seconds()));
                return sslContext;
            } catch (Exception e) {
                throw new ElasticsearchSSLException("failed to initialize the SSLContext", e);
            }
        }

        private TrustManager[] trustManagers(String trustStorePath, String trustStorePassword, String trustStoreAlgorithm) {
            try {
                // Load TrustStore
                KeyStore ks = null;
                if (trustStorePath != null) {
                    ks = readKeystore(trustStorePath, trustStorePassword);
                }

                // Initialize a trust manager factory with the trusted store
                TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(trustStoreAlgorithm);
                trustFactory.init(ks);
                return trustFactory.getTrustManagers();
            } catch (Exception e) {
                throw new ElasticsearchSSLException("failed to initialize a TrustManagerFactory", e);
            }
        }

        private KeyStore readKeystore(String path, String password) throws Exception {
            try (FileInputStream in = new FileInputStream(path)) {
                // Load TrustStore
                KeyStore ks = KeyStore.getInstance("jks");
                assert password != null;
                ks.load(in, password.toCharArray());
                return ks;
            }
        }

    }

    static class SSLSettings {

        private static final ESLogger logger = Loggers.getLogger(SSLSettings.class);

        String keyStorePath;
        String keyStorePassword;
        String keyStoreAlgorithm;
        String keyPassword;
        String trustStorePath;
        String trustStorePassword;
        String trustStoreAlgorithm;
        String sslProtocol;
        int sessionCacheSize;
        TimeValue sessionCacheTimeout;

        SSLSettings(Settings settings, Settings componentSettings) {
            keyStorePath = settings.get("keystore.path", componentSettings.get("keystore.path", System.getProperty("javax.net.ssl.keyStore")));
            keyStorePassword = settings.get("keystore.password", componentSettings.get("keystore.password", System.getProperty("javax.net.ssl.keyStorePassword")));
            keyStoreAlgorithm = settings.get("keystore.algorithm", componentSettings.get("keystore.algorithm", System.getProperty("ssl.KeyManagerFactory.algorithm", KeyManagerFactory.getDefaultAlgorithm())));
            keyPassword = settings.get("keystore.key_password", componentSettings.get("keystore.key_password", keyStorePassword));

            // Truststore settings
            trustStorePath = settings.get("truststore.path", componentSettings.get("truststore.path", System.getProperty("javax.net.ssl.trustStore")));
            trustStorePassword = settings.get("truststore.password", componentSettings.get("truststore.password", System.getProperty("javax.net.ssl.trustStorePassword")));
            trustStoreAlgorithm = settings.get("truststore.algorithm", componentSettings.get("truststore.algorithm", System.getProperty("ssl.TrustManagerFactory.algorithm", TrustManagerFactory.getDefaultAlgorithm())));

            sslProtocol = settings.get("protocol", componentSettings.get("protocol", DEFAULT_PROTOCOL));
            sessionCacheSize = settings.getAsInt("session.cache_size", componentSettings.getAsInt("session.cache_size", DEFAULT_SESSION_CACHE_SIZE));
            sessionCacheTimeout = settings.getAsTime("session.cache_timeout", componentSettings.getAsTime("session.cache_timeout", DEFAULT_SESSION_CACHE_TIMEOUT));

            if (trustStorePath == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("no truststore defined. using keystore [{}] as truststore", keyStorePath);
                }
                trustStorePath = keyStorePath;
                trustStorePassword = keyStorePassword;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SSLSettings that = (SSLSettings) o;

            if (keyStorePath != null ? !keyStorePath.equals(that.keyStorePath) : that.keyStorePath != null) {
                return false;
            }
            if (sslProtocol != null ? !sslProtocol.equals(that.sslProtocol) : that.sslProtocol != null) {
                return false;
            }
            if (trustStorePath != null ? !trustStorePath.equals(that.trustStorePath) : that.trustStorePath != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = keyStorePath != null ? keyStorePath.hashCode() : 0;
            result = 31 * result + (trustStorePath != null ? trustStorePath.hashCode() : 0);
            result = 31 * result + (sslProtocol != null ? sslProtocol.hashCode() : 0);
            return result;
        }
    }
}
