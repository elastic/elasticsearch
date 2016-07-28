/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.ssl.SSLConfiguration.Custom;
import org.elasticsearch.xpack.security.ssl.SSLConfiguration.Global;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This service houses the private key and trust managers needed for SSL/TLS negotiation.  It is the central place to
 * get SSLEngines and SocketFactories.
 */
public abstract class AbstractSSLService extends AbstractComponent {

    private final SSLContextCacheLoader cacheLoader = new SSLContextCacheLoader();
    private final ConcurrentHashMap<SSLConfiguration, SSLContext> sslContexts = new ConcurrentHashMap<>();

    protected final SSLConfiguration globalSSLConfiguration;
    protected final Environment env;

    private Listener listener = Listener.NOOP;

    AbstractSSLService(Settings settings, Environment environment, Global globalSSLConfiguration) {
        super(settings);
        this.env = environment;
        this.globalSSLConfiguration = globalSSLConfiguration;
    }

    public String[] supportedProtocols() {
        return globalSSLConfiguration.supportedProtocols().toArray(Strings.EMPTY_ARRAY);
    }

    public String[] ciphers() {
        return globalSSLConfiguration.ciphers().toArray(Strings.EMPTY_ARRAY);
    }

    public SSLSocketFactory sslSocketFactory(Settings settings) {
        return sslSocketFactory(sslContext(settings));
    }

    protected SSLSocketFactory sslSocketFactory(SSLContext sslContext) {
        SSLSocketFactory socketFactory = sslContext.getSocketFactory();
        return new SecuritySSLSocketFactory(socketFactory, supportedProtocols(),
                supportedCiphers(socketFactory.getSupportedCipherSuites(), ciphers(), false));
    }

    public SSLEngine createSSLEngine() {
        return createSSLEngine(globalSSLConfiguration, null, -1);
    }

    public SSLEngine createSSLEngine(Settings settings) {
        return createSSLEngine(settings, null, -1);
    }

    public SSLEngine createSSLEngine(Settings settings, String host, int port) {
        if (settings.isEmpty()) {
            return createSSLEngine(globalSSLConfiguration, host, port);
        }
        return createSSLEngine(sslConfiguration(settings), host, port);
    }

    public SSLEngine createSSLEngine(SSLConfiguration configuration, String host, int port) {
        return createSSLEngine(sslContext(configuration),
                configuration.ciphers().toArray(Strings.EMPTY_ARRAY),
                configuration.supportedProtocols().toArray(Strings.EMPTY_ARRAY),
                host, port);
    }

    public SSLContext sslContext() {
        return sslContext(globalSSLConfiguration);
    }

    public SSLContext sslContext(Settings settings) {
        if (settings.isEmpty()) {
            return sslContext();
        }

        SSLConfiguration sslConfiguration = sslConfiguration(settings);
        return sslContext(sslConfiguration);
    }

    protected SSLContext sslContext(SSLConfiguration sslConfiguration) {
        return sslContexts.computeIfAbsent(sslConfiguration, cacheLoader::load);
    }

    protected SSLConfiguration sslConfiguration(Settings customSettings) {
        return new Custom(customSettings, globalSSLConfiguration);
    }

    protected abstract void validateSSLConfiguration(SSLConfiguration configuration);

    SSLEngine createSSLEngine(SSLContext sslContext, String[] ciphers, String[] supportedProtocols, String host, int port) {
        SSLEngine sslEngine = sslContext.createSSLEngine(host, port);
        try {
            sslEngine.setEnabledCipherSuites(supportedCiphers(sslEngine.getSupportedCipherSuites(), ciphers, false));
        } catch (ElasticsearchException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("failed loading cipher suites [" + Arrays.asList(ciphers) + "]", e);
        }

        try {
            sslEngine.setEnabledProtocols(supportedProtocols);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("failed setting supported protocols [" + Arrays.asList(supportedProtocols) + "]", e);
        }
        return sslEngine;
    }

    String[] supportedCiphers(String[] supportedCiphers, String[] requestedCiphers, boolean log) {
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
            throw new IllegalArgumentException("none of the ciphers " + Arrays.asList(requestedCiphers) + " are supported by this JVM");
        }

        if (log && !unsupportedCiphers.isEmpty()) {
            logger.error("unsupported ciphers [{}] were requested but cannot be used in this JVM, however there are supported ciphers " +
                    "that will be used [{}]. If you are trying to use ciphers with a key length greater than 128 bits on an Oracle JVM, " +
                    "you will need to install the unlimited strength JCE policy files.", unsupportedCiphers, requestedCiphersList);
        }

        return requestedCiphersList.toArray(new String[requestedCiphersList.size()]);
    }

    /**
     * Sets the listener to the value provided. Must not be {@code null}
     */
    void setListener(Listener listener) {
        this.listener = Objects.requireNonNull(listener);
    }

    /**
     * Returns the existing {@link SSLContext} for the configuration or {@code null}
     */
    SSLContext getSSLContext(SSLConfiguration sslConfiguration) {
        return sslContexts.get(sslConfiguration);
    }

    /**
     * Accessor to the loaded ssl configuration objects at the current point in time. This is useful for testing
     */
    Collection<SSLConfiguration> getLoadedSSLConfigurations() {
        return Collections.unmodifiableSet(new HashSet<>(sslContexts.keySet()));
    }

    private class SSLContextCacheLoader {

        public SSLContext load(SSLConfiguration sslConfiguration) {
            validateSSLConfiguration(sslConfiguration);
            if (logger.isDebugEnabled()) {
                logger.debug("using ssl settings [{}]", sslConfiguration);
            }

            TrustManager[] trustManagers = sslConfiguration.trustConfig().trustManagers(env);
            KeyManager[] keyManagers = sslConfiguration.keyConfig().keyManagers(env);
            SSLContext sslContext = createSslContext(keyManagers, trustManagers, sslConfiguration.protocol(),
                    sslConfiguration.sessionCacheSize(), sslConfiguration.sessionCacheTimeout());

            // check the supported ciphers and log them here
            supportedCiphers(sslContext.getSupportedSSLParameters().getCipherSuites(),
                    sslConfiguration.ciphers().toArray(Strings.EMPTY_ARRAY), true);
            listener.onSSLContextLoaded(sslConfiguration);
            return sslContext;
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
    }

    /**
     * This socket factory set the protocols and ciphers on each SSLSocket after it is created
     */
    static class SecuritySSLSocketFactory extends SSLSocketFactory {

        private final SSLSocketFactory delegate;
        private final String[] supportedProtocols;
        private final String[] ciphers;

        SecuritySSLSocketFactory(SSLSocketFactory delegate, String[] supportedProtocols, String[] ciphers) {
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

    interface Listener {
        /**
         * Called after a new SSLContext has been created
         * @param sslConfiguration the configuration used to create the SSLContext
         */
        void onSSLContextLoaded(SSLConfiguration sslConfiguration);

        Listener NOOP = (s) -> {};
    }
}
