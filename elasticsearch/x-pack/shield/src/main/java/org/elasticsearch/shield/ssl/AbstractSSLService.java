/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.ssl.SSLConfiguration.Custom;
import org.elasticsearch.shield.ssl.SSLConfiguration.Global;
import org.elasticsearch.shield.ssl.TrustConfig.Reloadable.Listener;
import org.elasticsearch.watcher.ResourceWatcherService;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This service houses the private key and trust managers needed for SSL/TLS negotiation.  It is the central place to
 * get SSLEngines and SocketFactories.
 */
public abstract class AbstractSSLService extends AbstractComponent {

    private final ConcurrentHashMap<SSLConfiguration, SSLContext> sslContexts = new ConcurrentHashMap<>();
    private final SSLContextCacheLoader cacheLoader = new SSLContextCacheLoader();

    protected SSLConfiguration globalSSLConfiguration;
    protected Environment env;
    protected ResourceWatcherService resourceWatcherService;

    public AbstractSSLService(Settings settings, Environment environment, Global globalSSLConfiguration,
                              ResourceWatcherService resourceWatcherService) {
        super(settings);
        this.env = environment;
        this.globalSSLConfiguration = globalSSLConfiguration;
        this.resourceWatcherService = resourceWatcherService;
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
        return new ShieldSSLSocketFactory(socketFactory, supportedProtocols(),
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

    private class SSLContextCacheLoader {

        public SSLContext load(SSLConfiguration sslConfiguration) {
            validateSSLConfiguration(sslConfiguration);
            if (logger.isDebugEnabled()) {
                logger.debug("using ssl settings [{}]", sslConfiguration);
            }

            ConfigRefreshListener configRefreshListener = new ConfigRefreshListener(sslConfiguration);
            TrustManager[] trustManagers = sslConfiguration.trustConfig().trustManagers(env, resourceWatcherService, configRefreshListener);
            KeyManager[] keyManagers = sslConfiguration.keyConfig().keyManagers(env, resourceWatcherService, configRefreshListener);
            SSLContext sslContext = createSslContext(keyManagers, trustManagers, sslConfiguration.protocol(),
                    sslConfiguration.sessionCacheSize(), sslConfiguration.sessionCacheTimeout());

            // check the supported ciphers and log them here
            supportedCiphers(sslContext.getSupportedSSLParameters().getCipherSuites(),
                    sslConfiguration.ciphers().toArray(Strings.EMPTY_ARRAY), true);
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

    class ConfigRefreshListener implements Listener {

        private final SSLConfiguration sslConfiguration;

        ConfigRefreshListener(SSLConfiguration sslConfiguration) {
            this.sslConfiguration = sslConfiguration;
        }

        @Override
        public void onReload() {
            SSLContext context = sslContexts.get(sslConfiguration);
            if (context != null) {
                invalidateSessions(context.getClientSessionContext());
                invalidateSessions(context.getServerSessionContext());
            }
        }

        void invalidateSessions(SSLSessionContext sslSessionContext) {
            Enumeration<byte[]> sessionIds = sslSessionContext.getIds();
            while (sessionIds.hasMoreElements()) {
                byte[] sessionId = sessionIds.nextElement();
                sslSessionContext.getSession(sessionId).invalidate();
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("failed to load updated ssl context for [{}]", e, sslConfiguration);
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
