/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.security.ssl.SSLConfiguration.Custom;
import org.elasticsearch.xpack.security.ssl.SSLConfiguration.Global;
import org.elasticsearch.xpack.security.transport.netty4.SecurityNetty4HttpServerTransport;
import org.elasticsearch.xpack.security.transport.netty4.SecurityNetty4Transport;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.xpack.security.Security.setting;
import static org.elasticsearch.xpack.security.Security.settingPrefix;
import static org.elasticsearch.xpack.security.authc.Realms.REALMS_GROUPS_SETTINGS;
import static org.elasticsearch.xpack.security.transport.netty4.SecurityNetty4Transport.SSL_SETTING;

/**
 * Provides access to {@link SSLEngine} and {@link SSLSocketFactory} objects based on a provided configuration. All
 * configurations loaded by this service must be configured on construction.
 */
public class SSLService extends AbstractComponent {

    private final Map<SSLConfiguration, SSLContextHolder> sslContexts;
    private final SSLConfiguration globalSSLConfiguration;
    private final Environment env;

    public SSLService(Settings settings, Environment environment) {
        super(settings);
        this.env = environment;
        this.globalSSLConfiguration = new Global(settings);
        this.sslContexts = loadSSLConfigurations();
    }

    /**
     * Create a new {@link SSLSocketFactory} based on the provided settings. The settings are used to identify the ssl configuration that
     * should be used to create the socket factory. The socket factory will also properly configure the ciphers and protocols on each
     * socket that is created
     * @param settings the settings used to identify the ssl configuration, typically under a *.ssl. prefix. An empty settings will return
     *                 a factory created from the default configuration
     * @return {@link SSLSocketFactory}
     */
    public SSLSocketFactory sslSocketFactory(Settings settings) {
        SSLConfiguration sslConfiguration = sslConfiguration(settings);
        SSLSocketFactory socketFactory = sslContext(sslConfiguration).getSocketFactory();
        return new SecuritySSLSocketFactory(socketFactory, sslConfiguration.supportedProtocols().toArray(Strings.EMPTY_ARRAY),
                supportedCiphers(socketFactory.getSupportedCipherSuites(), sslConfiguration.ciphers(), false));
    }

    /**
     * Creates an {@link SSLEngine} based on the provided settings. The settings are used to identify the ssl configuration that should be
     * used to create the engine. This SSLEngine cannot be used for hostname verification since the engine will not be created with the
     * host and port. This method is useful to obtain an SSLEngine that will be used for server connections or client connections that
     * will not use hostname verification.
     * @param settings the settings used to identify the ssl configuration, typically under a *.ssl. prefix. An empty settings will return
     *                 a SSLEngine created from the default configuration
     * @return {@link SSLEngine}
     */
    public SSLEngine createSSLEngine(Settings settings) {
        return createSSLEngine(settings, null, -1);
    }

    /**
     * Creates an {@link SSLEngine} based on the provided settings. The settings are used to identify the ssl configuration that should be
     * used to create the engine. This SSLEngine can be used for a connection that requires hostname verification assuming the provided
     * host and port are correct. The SSLEngine created by this method is most useful for clients with hostname verificaton enabled
     * @param settings the settings used to identify the ssl configuration, typically under a *.ssl. prefix. An empty settings will return
     *                 a SSLEngine created from the default configuration
     * @param host the host of the remote endpoint. If using hostname verification, this should match what is in the remote endpoint's
     *             certificate
     * @param port the port of the remote endpoint
     * @return {@link SSLEngine}
     */
    public SSLEngine createSSLEngine(Settings settings, String host, int port) {
        SSLConfiguration configuration = sslConfiguration(settings);
        SSLContext sslContext = sslContext(configuration);
        SSLEngine sslEngine = sslContext.createSSLEngine(host, port);
        String[] ciphers = supportedCiphers(sslEngine.getSupportedCipherSuites(), configuration.ciphers(), false);
        try {
            sslEngine.setEnabledCipherSuites(ciphers);
        } catch (ElasticsearchException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("failed loading cipher suites " + Arrays.toString(ciphers), e);
        }

        String[] supportedProtocols = configuration.supportedProtocols().toArray(Strings.EMPTY_ARRAY);
        try {
            sslEngine.setEnabledProtocols(supportedProtocols);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("failed setting supported protocols " + Arrays.toString(supportedProtocols), e);
        }
        return sslEngine;
    }

    /**
     * Returns whether the provided settings results in a valid configuration that can be used for server connections
     */
    public boolean isConfigurationValidForServerUsage(Settings settings) {
        SSLConfiguration sslConfiguration = sslConfiguration(settings);
        return sslConfiguration.keyConfig() != KeyConfig.NONE;
    }

    /**
     * Returns the {@link SSLContext} for the global configuration. Mainly used for testing
     */
    SSLContext sslContext() {
        return sslContextHolder(globalSSLConfiguration).sslContext();
    }

    /**
     * Returns the {@link SSLContext} for the configuration
     */
    SSLContext sslContext(SSLConfiguration configuration) {
        return sslContextHolder(configuration).sslContext();
    }

    /**
     * Returns the existing {@link SSLContextHolder} for the configuration
     * @throws IllegalArgumentException if not found
     */
    SSLContextHolder sslContextHolder(SSLConfiguration sslConfiguration) {
        SSLContextHolder holder = sslContexts.get(sslConfiguration);
        if (holder == null) {
            throw new IllegalArgumentException("did not find a SSLContext for [" + sslConfiguration.toString() + "]");
        }
        return holder;
    }

    /**
     * Returns the existing {@link SSLConfiguration} for the given settings
     * @param settings the settings for the ssl configuration
     * @return the ssl configuration for the provided settings. If the settings are empty, the global configuration is returned
     */
    SSLConfiguration sslConfiguration(Settings settings) {
        if (settings.isEmpty()) {
            return globalSSLConfiguration;
        }
        return new Custom(settings, globalSSLConfiguration);
    }

    /**
     * Accessor to the loaded ssl configuration objects at the current point in time. This is useful for testing
     */
    Collection<SSLConfiguration> getLoadedSSLConfigurations() {
        return Collections.unmodifiableSet(new HashSet<>(sslContexts.keySet()));
    }

    /**
     * Returns the intersection of the supported ciphers with the requested ciphers. This method will also optionally log if unsupported
     * ciphers were requested.
     * @throws IllegalArgumentException if no supported ciphers are in the requested ciphers
     */
    String[] supportedCiphers(String[] supportedCiphers, List<String> requestedCiphers, boolean log) {
        List<String> supportedCiphersList = new ArrayList<>(requestedCiphers.size());
        List<String> unsupportedCiphers = new LinkedList<>();
        boolean found;
        for (String requestedCipher : requestedCiphers) {
            found = false;
            for (String supportedCipher : supportedCiphers) {
                if (supportedCipher.equals(requestedCipher)) {
                    found = true;
                    supportedCiphersList.add(requestedCipher);
                    break;
                }
            }

            if (!found) {
                unsupportedCiphers.add(requestedCipher);
            }
        }

        if (supportedCiphersList.isEmpty()) {
            throw new IllegalArgumentException("none of the ciphers " + Arrays.toString(requestedCiphers.toArray())
                    + " are supported by this JVM");
        }

        if (log && !unsupportedCiphers.isEmpty()) {
            logger.error("unsupported ciphers [{}] were requested but cannot be used in this JVM, however there are supported ciphers " +
                    "that will be used [{}]. If you are trying to use ciphers with a key length greater than 128 bits on an Oracle JVM, " +
                    "you will need to install the unlimited strength JCE policy files.", unsupportedCiphers, supportedCiphersList);
        }

        return supportedCiphersList.toArray(new String[supportedCiphersList.size()]);
    }

    /**
     * Creates an {@link SSLContext} based on the provided configuration
     * @param sslConfiguration the configuration to use for context creation
     * @return the created SSLContext
     */
    private SSLContextHolder createSslContext(SSLConfiguration sslConfiguration) {
        if (logger.isDebugEnabled()) {
            logger.debug("using ssl settings [{}]", sslConfiguration);
        }
        ReloadableTrustManager trustManager =
                new ReloadableTrustManager(sslConfiguration.trustConfig().createTrustManager(env), sslConfiguration.trustConfig());
        ReloadableX509KeyManager keyManager =
                new ReloadableX509KeyManager(sslConfiguration.keyConfig().createKeyManager(env), sslConfiguration.keyConfig());

        // Initialize sslContext
        try {
            SSLContext sslContext = SSLContext.getInstance(sslConfiguration.protocol());
            sslContext.init(new X509ExtendedKeyManager[] { keyManager }, new X509ExtendedTrustManager[] { trustManager }, null);
            sslContext.getServerSessionContext().setSessionCacheSize(sslConfiguration.sessionCacheSize());
            sslContext.getServerSessionContext().setSessionTimeout(Math.toIntExact(sslConfiguration.sessionCacheTimeout().seconds()));

            // check the supported ciphers and log them here to prevent spamming logs on every call
            supportedCiphers(sslContext.getSupportedSSLParameters().getCipherSuites(), sslConfiguration.ciphers(), true);

            return new SSLContextHolder(sslContext, trustManager, keyManager);
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new ElasticsearchException("failed to initialize the SSLContext", e);
        }
    }

    /**
     * Parses the settings to load all SSLConfiguration objects that will be used.
     */
    private Map<SSLConfiguration, SSLContextHolder> loadSSLConfigurations() {
        Map<SSLConfiguration, SSLContextHolder> sslConfigurations = new HashMap<>();
        validateSSLConfiguration(globalSSLConfiguration);
        sslConfigurations.put(globalSSLConfiguration, createSslContext(globalSSLConfiguration));
        List<Settings> sslSettings = new ArrayList<>();
        sslSettings.addAll(getTransportSSLSettings(settings));
        sslSettings.addAll(getRealmsSSLSettings(settings));
        sslSettings.addAll(getHttpSSLSettings(settings));
        for (Settings settings : sslSettings) {
            SSLConfiguration sslConfiguration = new Custom(settings, globalSSLConfiguration);
            validateSSLConfiguration(sslConfiguration);
            if (sslConfigurations.containsKey(sslConfiguration) == false) {
                sslConfigurations.put(sslConfiguration, createSslContext(sslConfiguration));
            }
        }
        return Collections.unmodifiableMap(sslConfigurations);
    }

    private void validateSSLConfiguration(SSLConfiguration configuration) {
        configuration.keyConfig().validate();
        configuration.trustConfig().validate();
    }

    /**
     * This socket factory wraps an existing SSLSocketFactory and sets the protocols and ciphers on each SSLSocket after it is created. This
     * is needed even though the SSLContext is configured properly as the configuration does not flow down to the sockets created by the
     * SSLSocketFactory obtained from the SSLContext.
     */
    private static class SecuritySSLSocketFactory extends SSLSocketFactory {

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

    /**
     * Wraps a trust manager to delegate to. If the trust material needs to be reloaded, then the delegate will be switched after
     * reloading
     */
    final class ReloadableTrustManager extends X509ExtendedTrustManager {

        private volatile X509ExtendedTrustManager trustManager;
        private final TrustConfig trustConfig;

        ReloadableTrustManager(X509ExtendedTrustManager trustManager, TrustConfig trustConfig) {
            this.trustManager = trustManager == null ? new EmptyX509TrustManager() : trustManager;
            this.trustConfig = trustConfig;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
            trustManager.checkClientTrusted(x509Certificates, s, socket);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
            trustManager.checkServerTrusted(x509Certificates, s, socket);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
            trustManager.checkClientTrusted(x509Certificates, s, sslEngine);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
            trustManager.checkServerTrusted(x509Certificates, s, sslEngine);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            trustManager.checkClientTrusted(x509Certificates, s);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            trustManager.checkServerTrusted(x509Certificates, s);
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return trustManager.getAcceptedIssuers();
        }

        void reload() {
            X509ExtendedTrustManager loadedTrustManager = trustConfig.createTrustManager(env);
            if (loadedTrustManager == null) {
                this.trustManager = new EmptyX509TrustManager();
            } else {
                this.trustManager = loadedTrustManager;
            }
        }

        X509ExtendedTrustManager getTrustManager() {
            return trustManager;
        }
    }

    /**
     * Wraps a key manager and delegates all calls to it. When the key material needs to be reloaded, then the delegate is swapped after
     * a new one has been loaded
     */
    final class ReloadableX509KeyManager extends X509ExtendedKeyManager {

        private volatile X509ExtendedKeyManager keyManager;
        private final KeyConfig keyConfig;

        ReloadableX509KeyManager(X509ExtendedKeyManager keyManager, KeyConfig keyConfig) {
            this.keyManager = keyManager == null ? new EmptyKeyManager() : keyManager;
            this.keyConfig = keyConfig;
        }

        @Override
        public String[] getClientAliases(String s, Principal[] principals) {
            return keyManager.getClientAliases(s, principals);
        }

        @Override
        public String chooseClientAlias(String[] strings, Principal[] principals, Socket socket) {
            return keyManager.chooseClientAlias(strings, principals, socket);
        }

        @Override
        public String[] getServerAliases(String s, Principal[] principals) {
            return keyManager.getServerAliases(s, principals);
        }

        @Override
        public String chooseServerAlias(String s, Principal[] principals, Socket socket) {
            return keyManager.chooseServerAlias(s, principals, socket);
        }

        @Override
        public X509Certificate[] getCertificateChain(String s) {
            return keyManager.getCertificateChain(s);
        }

        @Override
        public PrivateKey getPrivateKey(String s) {
            return keyManager.getPrivateKey(s);
        }

        @Override
        public String chooseEngineClientAlias(String[] strings, Principal[] principals, SSLEngine engine) {
            return keyManager.chooseEngineClientAlias(strings, principals, engine);
        }

        @Override
        public String chooseEngineServerAlias(String s, Principal[] principals, SSLEngine engine) {
            return keyManager.chooseEngineServerAlias(s, principals, engine);
        }

        void reload() {
            X509ExtendedKeyManager loadedKeyManager = keyConfig.createKeyManager(env);
            if (loadedKeyManager == null) {
                this.keyManager = new EmptyKeyManager();
            } else {
                this.keyManager = loadedKeyManager;
            }
        }

        // pkg-private accessor for testing
        X509ExtendedKeyManager getKeyManager() {
            return keyManager;
        }
    }

    /**
     * A struct for holding the SSLContext and the backing key manager and trust manager
     */
    static final class SSLContextHolder {

        private final SSLContext context;
        private final ReloadableTrustManager trustManager;
        private final ReloadableX509KeyManager keyManager;

        SSLContextHolder(SSLContext context, ReloadableTrustManager trustManager, ReloadableX509KeyManager keyManager) {
            this.context = context;
            this.trustManager = trustManager;
            this.keyManager = keyManager;
        }

        SSLContext sslContext() {
            return context;
        }

        ReloadableX509KeyManager keyManager() {
            return keyManager;
        }

        ReloadableTrustManager trustManager() {
            return trustManager;
        }

        synchronized void reload() {
            trustManager.reload();
            keyManager.reload();
            invalidateSessions(context.getClientSessionContext());
            invalidateSessions(context.getServerSessionContext());
        }

        /**
         * Invalidates the sessions in the provided {@link SSLSessionContext}
         */
        private static void invalidateSessions(SSLSessionContext sslSessionContext) {
            Enumeration<byte[]> sessionIds = sslSessionContext.getIds();
            while (sessionIds.hasMoreElements()) {
                byte[] sessionId = sessionIds.nextElement();
                sslSessionContext.getSession(sessionId).invalidate();
            }
        }
    }

    /**
     * This is an empty key manager that is used in case a loaded key manager is null
     */
    private static final class EmptyKeyManager extends X509ExtendedKeyManager {

        @Override
        public String[] getClientAliases(String s, Principal[] principals) {
            return new String[0];
        }

        @Override
        public String chooseClientAlias(String[] strings, Principal[] principals, Socket socket) {
            return null;
        }

        @Override
        public String[] getServerAliases(String s, Principal[] principals) {
            return new String[0];
        }

        @Override
        public String chooseServerAlias(String s, Principal[] principals, Socket socket) {
            return null;
        }

        @Override
        public X509Certificate[] getCertificateChain(String s) {
            return new X509Certificate[0];
        }

        @Override
        public PrivateKey getPrivateKey(String s) {
            return null;
        }
    }

    /**
     * This is an empty trust manager that is used in case a loaded trust manager is null
     */
    private static final class EmptyX509TrustManager extends X509ExtendedTrustManager {

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(Global.CIPHERS_SETTING);
        settings.add(Global.SUPPORTED_PROTOCOLS_SETTING);
        settings.add(Global.KEYSTORE_PATH_SETTING);
        settings.add(Global.KEYSTORE_PASSWORD_SETTING);
        settings.add(Global.KEYSTORE_ALGORITHM_SETTING);
        settings.add(Global.KEYSTORE_KEY_PASSWORD_SETTING);
        settings.add(Global.KEY_PATH_SETTING);
        settings.add(Global.KEY_PASSWORD_SETTING);
        settings.add(Global.CERT_SETTING);
        settings.add(Global.TRUSTSTORE_PATH_SETTING);
        settings.add(Global.TRUSTSTORE_PASSWORD_SETTING);
        settings.add(Global.TRUSTSTORE_ALGORITHM_SETTING);
        settings.add(Global.PROTOCOL_SETTING);
        settings.add(Global.SESSION_CACHE_SIZE_SETTING);
        settings.add(Global.SESSION_CACHE_TIMEOUT_SETTING);
        settings.add(Global.CA_PATHS_SETTING);
        settings.add(Global.INCLUDE_JDK_CERTS_SETTING);
    }

    private static List<Settings> getRealmsSSLSettings(Settings settings) {
        List<Settings> sslSettings = new ArrayList<>();
        Settings realmsSettings = REALMS_GROUPS_SETTINGS.get(settings);
        for (String name : realmsSettings.names()) {
            Settings realmSSLSettings = realmsSettings.getAsSettings(name).getByPrefix("ssl.");
            if (realmSSLSettings.isEmpty() == false) {
                sslSettings.add(realmSSLSettings);
            }
        }
        return sslSettings;
    }

    private static List<Settings> getTransportSSLSettings(Settings settings) {
        List<Settings> sslSettings = new ArrayList<>();
        Map<String, Settings> profiles = TransportSettings.TRANSPORT_PROFILES_SETTING.get(settings).getAsGroups(true);
        for (Entry<String, Settings> entry : profiles.entrySet()) {
            Settings profileSettings = entry.getValue();
            final boolean profileSsl = SecurityNetty4Transport.profileSSL(profileSettings, SSL_SETTING.get(settings));
            if (profileSsl && profileSettings.isEmpty() == false) {
                sslSettings.add(profileSettings.getByPrefix(settingPrefix()));
            }
        }
        return sslSettings;
    }

    private static List<Settings> getHttpSSLSettings(Settings settings) {
        if (SecurityNetty4HttpServerTransport.SSL_SETTING.get(settings)) {
            return Collections.singletonList(settings.getByPrefix(setting("http.ssl.")));
        }
        return Collections.emptyList();
    }
}
