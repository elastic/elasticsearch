/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.ssl.cert.CertificateInfo;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
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
import java.util.Optional;
import java.util.Set;

/**
 * Provides access to {@link SSLEngine} and {@link SSLSocketFactory} objects based on a provided configuration. All
 * configurations loaded by this service must be configured on construction.
 */
public class SSLService extends AbstractComponent {

    private final Map<SSLConfiguration, SSLContextHolder> sslContexts;
    private final SSLConfiguration globalSSLConfiguration;
    private final SetOnce<SSLConfiguration> transportSSLConfiguration = new SetOnce<>();
    private final Environment env;

    /**
     * Create a new SSLService that parses the settings for the ssl contexts that need to be created, creates them, and then caches them
     * for use later
     */
    public SSLService(Settings settings, Environment environment) {
        super(settings);
        this.env = environment;
        this.globalSSLConfiguration = new SSLConfiguration(settings.getByPrefix(XPackSettings.GLOBAL_SSL_PREFIX));
        this.sslContexts = loadSSLConfigurations();
    }

    private SSLService(Settings settings, Environment environment, SSLConfiguration globalSSLConfiguration,
                       Map<SSLConfiguration, SSLContextHolder> sslContexts) {
        super(settings);
        this.env = environment;
        this.globalSSLConfiguration = globalSSLConfiguration;
        this.sslContexts = sslContexts;
    }

    /**
     * Creates a new SSLService that supports dynamic creation of SSLContext instances. Instances created by this service will not be
     * cached and will not be monitored for reloading. This dynamic server does have access to the cached and monitored instances that
     * have been created during initialization
     */
    public SSLService createDynamicSSLService() {
        return new SSLService(settings, env, globalSSLConfiguration, sslContexts) {

            @Override
            Map<SSLConfiguration, SSLContextHolder> loadSSLConfigurations() {
                // we don't need to load anything...
                return Collections.emptyMap();
            }

            /**
             * Returns the existing {@link SSLContextHolder} for the configuration
             * @throws IllegalArgumentException if not found
             */
            @Override
            SSLContextHolder sslContextHolder(SSLConfiguration sslConfiguration) {
                SSLContextHolder holder = sslContexts.get(sslConfiguration);
                if (holder == null) {
                    // normally we'd throw here but let's create a new one that is not cached and will not be monitored for changes!
                    holder = createSslContext(sslConfiguration);
                }
                return holder;
            }
        };
    }

    /**
     * Create a new {@link SSLIOSessionStrategy} based on the provided settings. The settings are used to identify the SSL configuration
     * that should be used to create the context.
     *
     * @param settings the settings used to identify the ssl configuration, typically under a *.ssl. prefix. An empty settings will return
     *                 a context created from the default configuration
     * @return Never {@code null}.
     */
    public SSLIOSessionStrategy sslIOSessionStrategy(Settings settings) {
        SSLConfiguration config = sslConfiguration(settings);
        SSLContext sslContext = sslContext(config);
        String[] ciphers = supportedCiphers(sslParameters(sslContext).getCipherSuites(), config.cipherSuites(), false);
        String[] supportedProtocols = config.supportedProtocols().toArray(Strings.EMPTY_ARRAY);
        HostnameVerifier verifier;

        if (config.verificationMode().isHostnameVerificationEnabled()) {
            verifier = SSLIOSessionStrategy.getDefaultHostnameVerifier();
        } else {
            verifier = NoopHostnameVerifier.INSTANCE;
        }

        return sslIOSessionStrategy(sslContext, supportedProtocols, ciphers, verifier);
    }

    /**
     * The {@link SSLParameters} that are associated with the {@code sslContext}.
     * <p>
     * This method exists to simplify testing since {@link SSLContext#getSupportedSSLParameters()} is {@code final}.
     *
     * @param sslContext The SSL context for the current SSL settings
     * @return Never {@code null}.
     */
    SSLParameters sslParameters(SSLContext sslContext) {
        return sslContext.getSupportedSSLParameters();
    }

    /**
     * This method only exists to simplify testing of {@link #sslIOSessionStrategy(Settings)} because {@link SSLIOSessionStrategy} does
     * not expose any of the parameters that you give it.
     *
     * @param sslContext SSL Context used to handle SSL / TCP requests
     * @param protocols Supported protocols
     * @param ciphers Supported ciphers
     * @param verifier Hostname verifier
     * @return Never {@code null}.
     */
    SSLIOSessionStrategy sslIOSessionStrategy(SSLContext sslContext, String[] protocols, String[] ciphers, HostnameVerifier verifier) {
        return new SSLIOSessionStrategy(sslContext, protocols, ciphers, verifier);
    }

    /**
     * Create a new {@link SSLSocketFactory} based on the provided settings. The settings are used to identify the ssl configuration that
     * should be used to create the socket factory. The socket factory will also properly configure the ciphers and protocols on each
     * socket that is created
     * @param settings the settings used to identify the ssl configuration, typically under a *.ssl. prefix. An empty settings will return
     *                 a factory created from the default configuration
     * @return Never {@code null}.
     */
    public SSLSocketFactory sslSocketFactory(Settings settings) {
        SSLConfiguration sslConfiguration = sslConfiguration(settings);
        SSLSocketFactory socketFactory = sslContext(sslConfiguration).getSocketFactory();
        return new SecuritySSLSocketFactory(socketFactory, sslConfiguration.supportedProtocols().toArray(Strings.EMPTY_ARRAY),
                supportedCiphers(socketFactory.getSupportedCipherSuites(), sslConfiguration.cipherSuites(), false));
    }

    /**
     * Creates an {@link SSLEngine} based on the provided settings. The settings are used to identify the ssl configuration that should be
     * used to create the engine. This SSLEngine cannot be used for hostname verification since the engine will not be created with the
     * host and port. This method is useful to obtain an SSLEngine that will be used for server connections or client connections that
     * will not use hostname verification.
     * @param settings the settings used to identify the ssl configuration, typically under a *.ssl. prefix. An empty settings will return
     *                 a SSLEngine created from the default configuration
     * @param fallbackSettings the settings that should be used for the fallback of the SSLConfiguration. Using {@link Settings#EMPTY}
     *                         results in a fallback to the global configuration
     * @return {@link SSLEngine}
     */
    public SSLEngine createSSLEngine(Settings settings, Settings fallbackSettings) {
        return createSSLEngine(settings, fallbackSettings, null, -1);
    }

    /**
     * Creates an {@link SSLEngine} based on the provided settings. The settings are used to identify the ssl configuration that should be
     * used to create the engine. This SSLEngine can be used for a connection that requires hostname verification assuming the provided
     * host and port are correct. The SSLEngine created by this method is most useful for clients with hostname verification enabled
     * @param settings the settings used to identify the ssl configuration, typically under a *.ssl. prefix. An empty settings will return
     *                 a SSLEngine created from the default configuration
     * @param fallbackSettings the settings that should be used for the fallback of the SSLConfiguration. Using {@link Settings#EMPTY}
     *                         results in a fallback to the global configuration
     * @param host the host of the remote endpoint. If using hostname verification, this should match what is in the remote endpoint's
     *             certificate
     * @param port the port of the remote endpoint
     * @return {@link SSLEngine}
     */
    public SSLEngine createSSLEngine(Settings settings, Settings fallbackSettings, String host, int port) {
        SSLConfiguration configuration = sslConfiguration(settings, fallbackSettings);
        return createSSLEngine(configuration, host, port);
    }

    /**
     * Creates an {@link SSLEngine} based on the provided configuration. This SSLEngine can be used for a connection that requires
     * hostname verification assuming the provided
     * host and port are correct. The SSLEngine created by this method is most useful for clients with hostname verification enabled
     * @param configuration the ssl configuration
     * @param host the host of the remote endpoint. If using hostname verification, this should match what is in the remote endpoint's
     *             certificate
     * @param port the port of the remote endpoint
     * @return {@link SSLEngine}
     * @see #sslConfiguration(Settings, Settings)
     */
    public SSLEngine createSSLEngine(SSLConfiguration configuration, String host, int port) {
        SSLContext sslContext = sslContext(configuration);
        SSLEngine sslEngine = sslContext.createSSLEngine(host, port);
        String[] ciphers = supportedCiphers(sslEngine.getSupportedCipherSuites(), configuration.cipherSuites(), false);
        String[] supportedProtocols = configuration.supportedProtocols().toArray(Strings.EMPTY_ARRAY);
        SSLParameters parameters = new SSLParameters(ciphers, supportedProtocols);
        if (configuration.verificationMode().isHostnameVerificationEnabled() && host != null) {
            // By default, a SSLEngine will not perform hostname verification. In order to perform hostname verification
            // we need to specify a EndpointIdentificationAlgorithm. We use the HTTPS algorithm to prevent against
            // man in the middle attacks for all of our connections.
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
        }
        // we use the cipher suite order so that we can prefer the ciphers we set first in the list
        parameters.setUseCipherSuitesOrder(true);
        configuration.sslClientAuth().configure(parameters);

        // many SSLEngine options can be configured using either SSLParameters or direct methods on the engine itself, but there is one
        // tricky aspect; if you set a value directly on the engine and then later set the SSLParameters the value set directly on the
        // engine will be overwritten by the value in the SSLParameters
        sslEngine.setSSLParameters(parameters);
        return sslEngine;
    }

    /**
     * Returns whether the provided settings results in a valid configuration that can be used for server connections
     * @param sslConfiguration the configuration to check
     */
    public boolean isConfigurationValidForServerUsage(SSLConfiguration sslConfiguration) {
        return sslConfiguration.keyConfig() != KeyConfig.NONE;
    }

    /**
     * Indicates whether client authentication is enabled for a particular configuration
     * @param settings the settings used to identify the ssl configuration, typically under a *.ssl. prefix. The global configuration
     *                 will be used for fallback
     */
    public boolean isSSLClientAuthEnabled(Settings settings) {
        return isSSLClientAuthEnabled(settings, Settings.EMPTY);
    }

    /**
     * Indicates whether client authentication is enabled for a particular configuration
     * @param settings the settings used to identify the ssl configuration, typically under a *.ssl. prefix
     * @param fallback the settings that should be used for the fallback of the SSLConfiguration. Using {@link Settings#EMPTY}
     *                 results in a fallback to the global configuration
     */
    public boolean isSSLClientAuthEnabled(Settings settings, Settings fallback) {
        SSLConfiguration sslConfiguration = sslConfiguration(settings, fallback);
        return isSSLClientAuthEnabled(sslConfiguration);
    }

    /**
     * Indicates whether client authentication is enabled for a particular configuration
     */
    public boolean isSSLClientAuthEnabled(SSLConfiguration sslConfiguration) {
        return sslConfiguration.sslClientAuth().enabled();
    }

    /**
     * Returns the {@link VerificationMode} that is specified in the settings (or the default)
     * @param settings the settings used to identify the ssl configuration, typically under a *.ssl. prefix
     * @param fallback the settings that should be used for the fallback of the SSLConfiguration. Using {@link Settings#EMPTY}
     *                 results in a fallback to the global configuration
     */
    public VerificationMode getVerificationMode(Settings settings, Settings fallback) {
        SSLConfiguration sslConfiguration = sslConfiguration(settings, fallback);
        return sslConfiguration.verificationMode();
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
        return new SSLConfiguration(settings, globalSSLConfiguration);
    }

    /**
     * Returns the existing {@link SSLConfiguration} for the given settings and applies the provided fallback settings instead of the global
     * configuration
     * @param settings the settings for the ssl configuration
     * @param fallbackSettings the settings that should be used for the fallback of the SSLConfiguration. Using {@link Settings#EMPTY}
     *                         results in a fallback to the global configuration
     * @return the ssl configuration for the provided settings. If the settings are empty, the global configuration is returned
     */
    public SSLConfiguration sslConfiguration(Settings settings, Settings fallbackSettings) {
        if (settings.isEmpty() && fallbackSettings.isEmpty()) {
            return globalSSLConfiguration;
        }
        SSLConfiguration fallback = sslConfiguration(fallbackSettings);
        return new SSLConfiguration(settings, fallback);
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
        X509ExtendedTrustManager trustManager = sslConfiguration.trustConfig().createTrustManager(env);
        X509ExtendedKeyManager keyManager = sslConfiguration.keyConfig().createKeyManager(env);
        return createSslContext(keyManager, trustManager, sslConfiguration);
    }

    /**
     * Creates an {@link SSLContext} based on the provided configuration and trust/key managers
     * @param sslConfiguration the configuration to use for context creation
     * @param keyManager the key manager to use
     * @param trustManager the trust manager to use
     * @return the created SSLContext
     */
    private SSLContextHolder createSslContext(X509ExtendedKeyManager keyManager, X509ExtendedTrustManager trustManager,
                                              SSLConfiguration sslConfiguration) {
        // Initialize sslContext
        try {
            SSLContext sslContext = SSLContext.getInstance(sslContextAlgorithm(sslConfiguration.supportedProtocols()));
            sslContext.init(new X509ExtendedKeyManager[] { keyManager }, new X509ExtendedTrustManager[] { trustManager }, null);

            // check the supported ciphers and log them here to prevent spamming logs on every call
            supportedCiphers(sslContext.getSupportedSSLParameters().getCipherSuites(), sslConfiguration.cipherSuites(), true);

            return new SSLContextHolder(sslContext, sslConfiguration);
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new ElasticsearchException("failed to initialize the SSLContext", e);
        }
    }

    /**
     * Parses the settings to load all SSLConfiguration objects that will be used.
     */
    Map<SSLConfiguration, SSLContextHolder> loadSSLConfigurations() {
        Map<SSLConfiguration, SSLContextHolder> sslConfigurations = new HashMap<>();
        sslConfigurations.put(globalSSLConfiguration, createSslContext(globalSSLConfiguration));

        final Settings transportSSLSettings = settings.getByPrefix(XPackSettings.TRANSPORT_SSL_PREFIX);
        List<Settings> sslSettingsList = new ArrayList<>();
        sslSettingsList.add(getHttpTransportSSLSettings(settings));
        sslSettingsList.add(settings.getByPrefix("xpack.http.ssl."));
        sslSettingsList.addAll(getRealmsSSLSettings(settings));
        sslSettingsList.addAll(getMonitoringExporterSettings(settings));

        sslSettingsList.forEach((sslSettings) ->
                sslConfigurations.computeIfAbsent(new SSLConfiguration(sslSettings, globalSSLConfiguration), this::createSslContext));

        // transport is special because we want to use a auto-generated key when there isn't one
        final SSLConfiguration transportSSLConfiguration = new SSLConfiguration(transportSSLSettings, globalSSLConfiguration);
        this.transportSSLConfiguration.set(transportSSLConfiguration);
        List<Settings> profileSettings = getTransportProfileSSLSettings(settings);
        sslConfigurations.computeIfAbsent(transportSSLConfiguration, this::createSslContext);
        profileSettings.forEach((profileSetting) ->
            sslConfigurations.computeIfAbsent(new SSLConfiguration(profileSetting, transportSSLConfiguration), this::createSslContext));
        return Collections.unmodifiableMap(sslConfigurations);
    }


    /**
     * Returns information about each certificate that is referenced by any SSL configuration.
     * This includes certificates used for identity (with a private key) and those used for trust, but excludes
     * certificates that are provided by the JRE.
     * Due to the nature of KeyStores, this may include certificates that are available, but never used
     * such as a CA certificate that is no longer in use, or a server certificate for an unrelated host.
     * @see TrustConfig#certificates(Environment)
     */
    public Set<CertificateInfo> getLoadedCertificates() throws GeneralSecurityException, IOException {
        Set<CertificateInfo> certificates = new HashSet<>();
        for (SSLConfiguration config : this.getLoadedSSLConfigurations()) {
            certificates.addAll(config.getDefinedCertificates(env));
        }
        return certificates;
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
            SSLSocket sslSocket = createWithPermissions(delegate::createSocket);
            configureSSLSocket(sslSocket);
            return sslSocket;
        }

        @Override
        public Socket createSocket(Socket socket, String host, int port, boolean autoClose) throws IOException {
            SSLSocket sslSocket = createWithPermissions(() -> delegate.createSocket(socket, host, port, autoClose));
            configureSSLSocket(sslSocket);
            return sslSocket;
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException {
            SSLSocket sslSocket = createWithPermissions(() -> delegate.createSocket(host, port));
            configureSSLSocket(sslSocket);
            return sslSocket;
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
            SSLSocket sslSocket = createWithPermissions(() ->  delegate.createSocket(host, port, localHost, localPort));
            configureSSLSocket(sslSocket);
            return sslSocket;
        }

        @Override
        public Socket createSocket(InetAddress host, int port) throws IOException {
            SSLSocket sslSocket = createWithPermissions(() ->  delegate.createSocket(host, port));
            configureSSLSocket(sslSocket);
            return sslSocket;
        }

        @Override
        public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
            SSLSocket sslSocket = createWithPermissions(() -> delegate.createSocket(address, port, localAddress, localPort));
            configureSSLSocket(sslSocket);
            return sslSocket;
        }

        private void configureSSLSocket(SSLSocket socket) {
            SSLParameters parameters = new SSLParameters(ciphers, supportedProtocols);
            // we use the cipher suite order so that we can prefer the ciphers we set first in the list
            parameters.setUseCipherSuitesOrder(true);
            socket.setSSLParameters(parameters);
        }

        private static SSLSocket createWithPermissions(CheckedSupplier<Socket, IOException> supplier) throws IOException {
            return (SSLSocket) SocketAccess.doPrivileged(supplier);
        }
    }


    final class SSLContextHolder {
        private volatile SSLContext context;
        private final KeyConfig keyConfig;
        private final TrustConfig trustConfig;
        private final SSLConfiguration sslConfiguration;

        SSLContextHolder(SSLContext context, SSLConfiguration sslConfiguration) {
            this.context = context;
            this.sslConfiguration = sslConfiguration;
            this.keyConfig = sslConfiguration.keyConfig();
            this.trustConfig = sslConfiguration.trustConfig();
        }

        SSLContext sslContext() {
            return context;
        }

        /**
         * Invalidates the sessions in the provided {@link SSLSessionContext}
         */
        private void invalidateSessions(SSLSessionContext sslSessionContext) {
            Enumeration<byte[]> sessionIds = sslSessionContext.getIds();
            while (sessionIds.hasMoreElements()) {
                byte[] sessionId = sessionIds.nextElement();
                sslSessionContext.getSession(sessionId).invalidate();
            }
        }

        synchronized void reload() {
            invalidateSessions(context.getClientSessionContext());
            invalidateSessions(context.getServerSessionContext());
            reloadSslContext();
        }

        private void reloadSslContext() {
            try {
                X509ExtendedKeyManager loadedKeyManager = Optional.ofNullable(keyConfig.createKeyManager(env)).
                    orElse(getEmptyKeyManager());
                X509ExtendedTrustManager loadedTrustManager = Optional.ofNullable(trustConfig.createTrustManager(env)).
                    orElse(getEmptyTrustManager());
                SSLContext loadedSslContext = SSLContext.getInstance(sslContextAlgorithm(sslConfiguration.supportedProtocols()));
                loadedSslContext.init(new X509ExtendedKeyManager[]{loadedKeyManager},
                    new X509ExtendedTrustManager[]{loadedTrustManager}, null);
                supportedCiphers(loadedSslContext.getSupportedSSLParameters().getCipherSuites(), sslConfiguration.cipherSuites(), false);
                this.context = loadedSslContext;
            } catch (GeneralSecurityException | IOException e) {
                throw new ElasticsearchException("failed to initialize the SSLContext", e);
            }
        }

        X509ExtendedKeyManager getEmptyKeyManager() throws GeneralSecurityException, IOException {
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null, null);
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, null);
            return (X509ExtendedKeyManager) keyManagerFactory.getKeyManagers()[0];
        }

        X509ExtendedTrustManager getEmptyTrustManager() throws GeneralSecurityException, IOException {
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null, null);
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("X509");
            trustManagerFactory.init(keyStore);
            return (X509ExtendedTrustManager) trustManagerFactory.getTrustManagers()[0];
        }
    }

    private static List<Settings> getRealmsSSLSettings(Settings settings) {
        List<Settings> sslSettings = new ArrayList<>();
        Settings realmsSettings = settings.getByPrefix(SecurityField.setting("authc.realms."));
        for (String name : realmsSettings.names()) {
            Settings realmSSLSettings = realmsSettings.getAsSettings(name).getByPrefix("ssl.");
            if (realmSSLSettings.isEmpty() == false) {
                sslSettings.add(realmSSLSettings);
            }
        }
        return sslSettings;
    }

    private static List<Settings> getTransportProfileSSLSettings(Settings settings) {
        List<Settings> sslSettings = new ArrayList<>();
        Map<String, Settings> profiles = settings.getGroups("transport.profiles.", true);
        for (Entry<String, Settings> entry : profiles.entrySet()) {
            Settings profileSettings = entry.getValue().getByPrefix("xpack.security.ssl.");
            if (profileSettings.isEmpty() == false) {
                sslSettings.add(profileSettings);
            }
        }
        return sslSettings;
    }

    public static Settings getHttpTransportSSLSettings(Settings settings) {
        Settings httpSSLSettings = settings.getByPrefix(XPackSettings.HTTP_SSL_PREFIX);
        if (httpSSLSettings.isEmpty()) {
            return httpSSLSettings;
        }

        Settings.Builder builder = Settings.builder().put(httpSSLSettings);
        if (builder.get("client_authentication") == null) {
            builder.put("client_authentication", XPackSettings.HTTP_CLIENT_AUTH_DEFAULT);
        }
        return builder.build();
    }

    private static List<Settings> getMonitoringExporterSettings(Settings settings) {
        List<Settings> sslSettings = new ArrayList<>();
        Map<String, Settings> exportersSettings = settings.getGroups("xpack.monitoring.exporters.");
        for (Entry<String, Settings> entry : exportersSettings.entrySet()) {
            Settings exporterSSLSettings = entry.getValue().getByPrefix("ssl.");
            if (exporterSSLSettings.isEmpty() == false) {
                sslSettings.add(exporterSSLSettings);
            }
        }
        return sslSettings;
    }

    /**
     * Maps the supported protocols to an appropriate ssl context algorithm. We make an attempt to use the "best" algorithm when
     * possible. The names in this method are taken from the
     * <a href="http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html">JCA Standard Algorithm Name
     * Documentation for Java 8</a>.
     */
    private static String sslContextAlgorithm(List<String> supportedProtocols) {
        if (supportedProtocols.isEmpty()) {
            return "TLSv1.2";
        }

        String algorithm = "SSL";
        for (String supportedProtocol : supportedProtocols) {
            switch (supportedProtocol) {
                case "TLSv1.2":
                    return "TLSv1.2";
                case "TLSv1.1":
                    if ("TLSv1.2".equals(algorithm) == false) {
                        algorithm = "TLSv1.1";
                    }
                    break;
                case "TLSv1":
                    switch (algorithm) {
                        case "TLSv1.2":
                        case "TLSv1.1":
                            break;
                        default:
                            algorithm = "TLSv1";
                    }
                    break;
                case "SSLv3":
                    switch (algorithm) {
                        case "SSLv2":
                        case "SSL":
                            algorithm = "SSLv3";
                    }
                    break;
                case "SSLv2":
                case "SSLv2Hello":
                    break;
                default:
                    throw new IllegalArgumentException("found unexpected value in supported protocols: " + supportedProtocol);
            }
        }
        return algorithm;
    }
}
