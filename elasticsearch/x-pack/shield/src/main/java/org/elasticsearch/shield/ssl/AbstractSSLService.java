/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This service houses the private key and trust managers needed for SSL/TLS negotiation.  It is the central place to
 * get SSLEngines and SocketFactories.
 */
public abstract class AbstractSSLService extends AbstractComponent {

    public static final String CIPHERS_SETTING = "shield.ssl.ciphers";
    public static final String SUPPORTED_PROTOCOLS_SETTING = "shield.ssl.supported_protocols";

    public static final String[] DEFAULT_SUPPORTED_PROTOCOLS = new String[]{"TLSv1", "TLSv1.1", "TLSv1.2"};

    static final String[] DEFAULT_CIPHERS = new String[]{"TLS_RSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"};
    static final TimeValue DEFAULT_SESSION_CACHE_TIMEOUT = TimeValue.timeValueHours(24);
    static final int DEFAULT_SESSION_CACHE_SIZE = 1000;
    static final String DEFAULT_PROTOCOL = "TLSv1.2";

    private final ConcurrentHashMap<SSLSettings, SSLContext> sslContexts = new ConcurrentHashMap<>();
    private final SSLContextCacheLoader cacheLoader = new SSLContextCacheLoader();
    protected Environment env;

    public AbstractSSLService(Settings settings, Environment environment) {
        super(settings);
        this.env = environment;
    }

    /**
     * @return A SSLSocketFactory (for client-side SSL handshaking)
     */
    public SSLSocketFactory sslSocketFactory() {
        SSLSocketFactory socketFactory = sslContext().getSocketFactory();
        return new ShieldSSLSocketFactory(socketFactory, supportedProtocols(), supportedCiphers(socketFactory.getSupportedCipherSuites(),
                ciphers()));
    }

    public String[] supportedProtocols() {
        return settings.getAsArray(SUPPORTED_PROTOCOLS_SETTING, DEFAULT_SUPPORTED_PROTOCOLS);
    }

    public String[] ciphers() {
        return settings.getAsArray(CIPHERS_SETTING, DEFAULT_CIPHERS);
    }

    public SSLEngine createSSLEngine() {
        return createSSLEngine(Settings.EMPTY);
    }

    public SSLEngine createSSLEngine(Settings settings) {
        return createSSLEngine(settings, null, -1);
    }

    public SSLEngine createSSLEngine(Settings settings, String host, int port) {
        String[] ciphers = settings.getAsArray(CIPHERS_SETTING, ciphers());
        String[] supportedProtocols = settings.getAsArray(SUPPORTED_PROTOCOLS_SETTING, supportedProtocols());
        return createSSLEngine(sslContext(settings), ciphers, supportedProtocols, host, port);
    }

    public SSLContext sslContext() {
        return sslContext(Settings.EMPTY);
    }

    protected SSLContext sslContext(Settings settings) {
        SSLSettings sslSettings = sslSettings(settings);
        return sslContexts.computeIfAbsent(sslSettings, (theSettings) ->
                cacheLoader.load(theSettings));
    }

    /**
     * @return The list of sensitive settings. (these settings shouldnot be exposed via rest API for example)
     */
    public static String[] sensitiveSettings() {
        return new String[]{
                CIPHERS_SETTING,
                SUPPORTED_PROTOCOLS_SETTING,
                "protocol",
                "session.cache_size",
                "session.cache_timeout",
                "keystore.path",
                "keystore.password",
                "keystore.algorithm",
                "keystore.key_password",
                "truststore.path",
                "truststore.password",
                "truststore.algorithm"
        };
    }

    protected abstract SSLSettings sslSettings(Settings customSettings);

    SSLEngine createSSLEngine(SSLContext sslContext, String[] ciphers, String[] supportedProtocols, String host, int port) {
        SSLEngine sslEngine = sslContext.createSSLEngine(host, port);
        try {
            sslEngine.setEnabledCipherSuites(supportedCiphers(sslEngine.getSupportedCipherSuites(), ciphers));
        } catch (ElasticsearchException e) {
            throw e;
        } catch (Throwable t) {
            throw new IllegalArgumentException("failed loading cipher suites [" + Arrays.asList(ciphers) + "]", t);
        }

        try {
            sslEngine.setEnabledProtocols(supportedProtocols);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("failed setting supported protocols [" + Arrays.asList(supportedProtocols) + "]", e);
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
            throw new IllegalArgumentException("none of the ciphers [" + Arrays.asList(requestedCiphers) + "] are supported by this JVM");
        }

        if (!unsupportedCiphers.isEmpty()) {
            logger.error("unsupported ciphers [{}] were requested but cannot be used in this JVM. If you are trying to use ciphers\n" +
                    "with a key length greater than 128 bits on an Oracle JVM, you will need to install the unlimited strength\n" +
                    "JCE policy files. Additionally, please ensure the PKCS11 provider is enabled for your JVM.", unsupportedCiphers);
        }

        return requestedCiphersList.toArray(new String[requestedCiphersList.size()]);
    }

    protected Path resolvePath(String location) {
        return env.configFile().resolve(location);
    }

    private class SSLContextCacheLoader {

        public SSLContext load(SSLSettings sslSettings) {
            if (logger.isDebugEnabled()) {
                logger.debug("using keystore[{}], key_algorithm[{}], truststore[{}], truststore_algorithm[{}], tls_protocol[{}], " +
                        "session_cache_size[{}], session_cache_timeout[{}]",
                        sslSettings.keyStorePath, sslSettings.keyStoreAlgorithm, sslSettings.trustStorePath,
                        sslSettings.trustStoreAlgorithm, sslSettings.sslProtocol, sslSettings.sessionCacheSize,
                        sslSettings.sessionCacheTimeout);
            }

            TrustManager[] trustManagers = trustManagers(sslSettings.trustStorePath, sslSettings.trustStorePassword,
                    sslSettings.trustStoreAlgorithm);
            KeyManager[] keyManagers = keyManagers(sslSettings.keyStorePath, sslSettings.keyStorePassword, sslSettings.keyStoreAlgorithm,
                    sslSettings.keyPassword);
            return createSslContext(keyManagers, trustManagers, sslSettings.sslProtocol, sslSettings.sessionCacheSize,
                    sslSettings.sessionCacheTimeout);
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
                throw new ElasticsearchException("failed to initialize a KeyManagerFactory", e);
            }
        }

        private SSLContext createSslContext(KeyManager[] keyManagers, TrustManager[] trustManagers, String sslProtocol,
                                            int sessionCacheSize, TimeValue sessionCacheTimeout) {
            // Initialize sslContext
            try {
                SSLContext sslContext = SSLContext.getInstance(sslProtocol);
                sslContext.init(keyManagers, trustManagers, null);
                sslContext.getServerSessionContext().setSessionCacheSize(sessionCacheSize);
                sslContext.getServerSessionContext().setSessionTimeout(Math.toIntExact(sessionCacheTimeout.seconds()));
                return sslContext;
            } catch (Exception e) {
                throw new ElasticsearchException("failed to initialize the SSLContext", e);
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
                throw new ElasticsearchException("failed to initialize a TrustManagerFactory", e);
            }
        }

        private KeyStore readKeystore(String path, String password) throws Exception {
            try (InputStream in = Files.newInputStream(resolvePath(path))) {
                // Load TrustStore
                KeyStore ks = KeyStore.getInstance("jks");
                assert password != null;
                ks.load(in, password.toCharArray());
                return ks;
            }
        }

    }

    public static class SSLSettings {

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

        SSLSettings(Settings settings, Settings sslServiceSettings) {
            keyStorePath = settings.get("keystore.path", sslServiceSettings.get("shield.ssl.keystore.path",
                    System.getProperty("javax.net.ssl.keyStore")));
            keyStorePassword = settings.get("keystore.password", sslServiceSettings.get("shield.ssl.keystore.password",
                    System.getProperty("javax.net.ssl.keyStorePassword")));
            keyStoreAlgorithm = settings.get("keystore.algorithm", sslServiceSettings.get("shield.ssl.keystore.algorithm",
                    System.getProperty("ssl.KeyManagerFactory.algorithm", KeyManagerFactory.getDefaultAlgorithm())));
            keyPassword = settings.get("keystore.key_password",
                    sslServiceSettings.get("shield.ssl.keystore.key_password", keyStorePassword));

            // Truststore settings
            trustStorePath = settings.get("truststore.path", sslServiceSettings.get("shield.ssl.truststore.path",
                    System.getProperty("javax.net.ssl.trustStore")));
            trustStorePassword = settings.get("truststore.password", sslServiceSettings.get("shield.ssl.truststore.password",
                    System.getProperty("javax.net.ssl.trustStorePassword")));
            trustStoreAlgorithm = settings.get("truststore.algorithm", sslServiceSettings.get("shield.ssl.truststore.algorithm",
                    System.getProperty("ssl.TrustManagerFactory.algorithm", TrustManagerFactory.getDefaultAlgorithm())));

            sslProtocol = settings.get("protocol", sslServiceSettings.get("shield.ssl.protocol", DEFAULT_PROTOCOL));
            sessionCacheSize = settings.getAsInt("session.cache_size",
                    sslServiceSettings.getAsInt("shield.ssl.session.cache_size", DEFAULT_SESSION_CACHE_SIZE));
            sessionCacheTimeout = settings.getAsTime("session.cache_timeout",
                    sslServiceSettings.getAsTime("shield.ssl.session.cache_timeout", DEFAULT_SESSION_CACHE_TIMEOUT));

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

    /**
     * This socket factory set the protocols and ciphers on each SSLSocket after it is created
     */
    static class ShieldSSLSocketFactory extends SSLSocketFactory {

        private final SSLSocketFactory delegate;
        private final String[] supportedProtocols;
        private final String[] ciphers;

        ShieldSSLSocketFactory(SSLSocketFactory delegate, String[] supportedProtocols, String[] ciphers) {
            this.delegate = delegate;
            this.supportedProtocols = supportedProtocols;
            this.ciphers = ciphers;
        }

        @Override
        public String[] getDefaultCipherSuites() {
            return ciphers;
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return delegate.getSupportedCipherSuites();
        }

        @Override
        public Socket createSocket() throws IOException {
            SSLSocket sslSocket = (SSLSocket) delegate.createSocket();
            configureSSLSocket(sslSocket);
            return sslSocket;
        }

        @Override
        public Socket createSocket(Socket socket, String host, int port, boolean autoClose) throws IOException {
            SSLSocket sslSocket = (SSLSocket) delegate.createSocket(socket, host, port, autoClose);
            configureSSLSocket(sslSocket);
            return sslSocket;
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException {
            SSLSocket sslSocket = (SSLSocket) delegate.createSocket(host, port);
            configureSSLSocket(sslSocket);
            return sslSocket;
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
            SSLSocket sslSocket = (SSLSocket) delegate.createSocket(host, port, localHost, localPort);
            configureSSLSocket(sslSocket);
            return sslSocket;
        }

        @Override
        public Socket createSocket(InetAddress host, int port) throws IOException {
            SSLSocket sslSocket = (SSLSocket) delegate.createSocket(host, port);
            configureSSLSocket(sslSocket);
            return sslSocket;
        }

        @Override
        public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
            SSLSocket sslSocket = (SSLSocket) delegate.createSocket(address, port, localAddress, localPort);
            configureSSLSocket(sslSocket);
            return sslSocket;
        }

        private void configureSSLSocket(SSLSocket socket) {
            socket.setEnabledProtocols(supportedProtocols);
            socket.setEnabledCipherSuites(ciphers);
        }
    }
}
