/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.ssl.cert.CertificateInfo;
import org.elasticsearch.xpack.core.watcher.WatcherField;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Provides access to {@link SSLEngine} and {@link SSLSocketFactory} objects based on a provided configuration. All
 * configurations loaded by this service must be configured on construction.
 */
public class SSLService {

    private static final Logger logger = LogManager.getLogger(SSLService.class);
    /**
     * An ordered map of protocol algorithms to SSLContext algorithms. The map is ordered from most
     * secure to least secure. The names in this map are taken from the
     * <a href="https://docs.oracle.com/en/java/javase/11/docs/specs/security/standard-names.html#sslcontext-algorithms">
     * Java Security Standard Algorithm Names Documentation for Java 11</a>.
     */
    private static final Map<String, String> ORDERED_PROTOCOL_ALGORITHM_MAP;
    static {
        LinkedHashMap<String, String> protocolAlgorithmMap = new LinkedHashMap<>();
        protocolAlgorithmMap.put("TLSv1.3", "TLSv1.3");
        protocolAlgorithmMap.put("TLSv1.2", "TLSv1.2");
        protocolAlgorithmMap.put("TLSv1.1", "TLSv1.1");
        protocolAlgorithmMap.put("TLSv1", "TLSv1");
        protocolAlgorithmMap.put("SSLv3", "SSLv3");
        protocolAlgorithmMap.put("SSLv2", "SSL");
        protocolAlgorithmMap.put("SSLv2Hello", "SSL");
        ORDERED_PROTOCOL_ALGORITHM_MAP = Collections.unmodifiableMap(protocolAlgorithmMap);
    }

    private final Settings settings;

    /**
     * This is a mapping from "context name" (in general use, the name of a setting key)
     * to a configuration.
     * This allows us to easily answer the question "What is the configuration for ssl in realm XYZ?"
     * Multiple "context names" may map to the same configuration (either by object-identity or by object-equality).
     * For example "xpack.http.ssl" may exist as a name in this map and have the global ssl configuration as a value
     */
    private final Map<String, SSLConfiguration> sslConfigurations;

    /**
     * A mapping from a SSLConfiguration to a pre-built context.
     * <p>
     * This is managed separately to the {@link #sslConfigurations} map, so that a single configuration (by object equality)
     * always maps to the same {@link SSLContextHolder}, even if it is being used within a different context-name.
     */
    private final Map<SSLConfiguration, SSLContextHolder> sslContexts;
    private final SetOnce<SSLConfiguration> transportSSLConfiguration = new SetOnce<>();
    private final Environment env;

    /**
     * Create a new SSLService that parses the settings for the ssl contexts that need to be created, creates them, and then caches them
     * for use later
     */
    public SSLService(Settings settings, Environment environment) {
        this.settings = settings;
        this.env = environment;
        this.sslConfigurations = new HashMap<>();
        this.sslContexts = loadSSLConfigurations();
    }

    private SSLService(Settings settings, Environment environment, Map<String, SSLConfiguration> sslConfigurations,
                       Map<SSLConfiguration, SSLContextHolder> sslContexts) {
        this.settings = settings;
        this.env = environment;
        this.sslConfigurations = sslConfigurations;
        this.sslContexts = sslContexts;
    }

    /**
     * Creates a new SSLService that supports dynamic creation of SSLContext instances. Instances created by this service will not be
     * cached and will not be monitored for reloading. This dynamic server does have access to the cached and monitored instances that
     * have been created during initialization
     */
    public SSLService createDynamicSSLService() {
        return new SSLService(settings, env, sslConfigurations, sslContexts) {

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
     * @deprecated This method will fail if the SSL configuration uses a {@link org.elasticsearch.common.settings.SecureSetting} but the
     * {@link org.elasticsearch.common.settings.SecureSettings} have been closed. Use {@link #getSSLConfiguration(String)}
     * and {@link #sslIOSessionStrategy(SSLConfiguration)} (Deprecated, but not removed because monitoring uses dynamic SSL settings)
     */
    @Deprecated
    public SSLIOSessionStrategy sslIOSessionStrategy(Settings settings) {
        SSLConfiguration config = sslConfiguration(settings);
        return sslIOSessionStrategy(config);
    }

    public SSLIOSessionStrategy sslIOSessionStrategy(SSLConfiguration config) {
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
     * @param protocols  Supported protocols
     * @param ciphers    Supported ciphers
     * @param verifier   Hostname verifier
     * @return Never {@code null}.
     */
    SSLIOSessionStrategy sslIOSessionStrategy(SSLContext sslContext, String[] protocols, String[] ciphers, HostnameVerifier verifier) {
        return new SSLIOSessionStrategy(sslContext, protocols, ciphers, verifier);
    }

    /**
     * Create a new {@link SSLSocketFactory} based on the provided configuration.
     * The socket factory will also properly configure the ciphers and protocols on each socket that is created
     * @param configuration The SSL configuration to use. Typically obtained from {@link #getSSLConfiguration(String)}
     * @return Never {@code null}.
     */
    public SSLSocketFactory sslSocketFactory(SSLConfiguration configuration) {
        final SSLContextHolder contextHolder = sslContextHolder(configuration);
        SSLSocketFactory socketFactory = contextHolder.sslContext().getSocketFactory();
        final SecuritySSLSocketFactory securitySSLSocketFactory = new SecuritySSLSocketFactory(
            () -> contextHolder.sslContext().getSocketFactory(),
            configuration.supportedProtocols().toArray(Strings.EMPTY_ARRAY),
            supportedCiphers(socketFactory.getSupportedCipherSuites(), configuration.cipherSuites(), false));
        contextHolder.addReloadListener(securitySSLSocketFactory::reload);
        return securitySSLSocketFactory;
    }

    /**
     * Creates an {@link SSLEngine} based on the provided configuration. This SSLEngine can be used for a connection that requires
     * hostname verification assuming the provided
     * host and port are correct. The SSLEngine created by this method is most useful for clients with hostname verification enabled
     *
     * @param configuration the ssl configuration
     * @param host          the host of the remote endpoint. If using hostname verification, this should match what is in the remote
     *                      endpoint's certificate
     * @param port          the port of the remote endpoint
     * @return {@link SSLEngine}
     * @see #getSSLConfiguration(String)
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
     *
     * @param sslConfiguration the configuration to check
     */
    public boolean isConfigurationValidForServerUsage(SSLConfiguration sslConfiguration) {
        Objects.requireNonNull(sslConfiguration, "SSLConfiguration cannot be null");
        return sslConfiguration.keyConfig() != KeyConfig.NONE;
    }

    /**
     * Indicates whether client authentication is enabled for a particular configuration
     */
    public boolean isSSLClientAuthEnabled(SSLConfiguration sslConfiguration) {
        Objects.requireNonNull(sslConfiguration, "SSLConfiguration cannot be null");
        return sslConfiguration.sslClientAuth().enabled();
    }

    /**
     * Returns the {@link SSLContext} for the configuration. Mainly used for testing
     */
    public SSLContext sslContext(SSLConfiguration configuration) {
        return sslContextHolder(configuration).sslContext();
    }

    /**
     * Returns the existing {@link SSLContextHolder} for the configuration
     *
     * @throws IllegalArgumentException if not found
     */
    SSLContextHolder sslContextHolder(SSLConfiguration sslConfiguration) {
        Objects.requireNonNull(sslConfiguration, "SSL Configuration cannot be null");
        SSLContextHolder holder = sslContexts.get(sslConfiguration);
        if (holder == null) {
            throw new IllegalArgumentException("did not find a SSLContext for [" + sslConfiguration.toString() + "]");
        }
        return holder;
    }

    /**
     * Returns the existing {@link SSLConfiguration} for the given settings
     *
     * @param settings the settings for the ssl configuration
     * @return the ssl configuration for the provided settings
     */
    public SSLConfiguration sslConfiguration(Settings settings) {
        return new SSLConfiguration(settings);
    }

    public Set<String> getTransportProfileContextNames() {
        return Collections.unmodifiableSet(this.sslConfigurations
            .keySet().stream()
            .filter(k -> k.startsWith("transport.profiles."))
            .collect(Collectors.toSet()));
    }

    /**
     * Accessor to the loaded ssl configuration objects at the current point in time. This is useful for testing
     */
    Collection<SSLConfiguration> getLoadedSSLConfigurations() {
        return Set.copyOf(sslContexts.keySet());
    }

    /**
     * Returns the intersection of the supported ciphers with the requested ciphers. This method will also optionally log if unsupported
     * ciphers were requested.
     *
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
     *
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
     *
     * @param sslConfiguration the configuration to use for context creation
     * @param keyManager       the key manager to use
     * @param trustManager     the trust manager to use
     * @return the created SSLContext
     */
    private SSLContextHolder createSslContext(X509ExtendedKeyManager keyManager, X509ExtendedTrustManager trustManager,
                                              SSLConfiguration sslConfiguration) {
        // Initialize sslContext
        try {
            SSLContext sslContext = SSLContext.getInstance(sslContextAlgorithm(sslConfiguration.supportedProtocols()));
            sslContext.init(new X509ExtendedKeyManager[]{keyManager}, new X509ExtendedTrustManager[]{trustManager}, null);

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
        Map<SSLConfiguration, SSLContextHolder> sslContextHolders = new HashMap<>();

        Map<String, Settings> sslSettingsMap = new HashMap<>();
        sslSettingsMap.put(XPackSettings.HTTP_SSL_PREFIX, getHttpTransportSSLSettings(settings));
        sslSettingsMap.put("xpack.http.ssl", settings.getByPrefix("xpack.http.ssl."));
        sslSettingsMap.putAll(getRealmsSSLSettings(settings));
        sslSettingsMap.putAll(getMonitoringExporterSettings(settings));
        sslSettingsMap.put(WatcherField.EMAIL_NOTIFICATION_SSL_PREFIX, settings.getByPrefix(WatcherField.EMAIL_NOTIFICATION_SSL_PREFIX));

        sslSettingsMap.forEach((key, sslSettings) -> loadConfiguration(key, sslSettings, sslContextHolders));

        final Settings transportSSLSettings = settings.getByPrefix(XPackSettings.TRANSPORT_SSL_PREFIX);
        final SSLConfiguration transportSSLConfiguration =
            loadConfiguration(XPackSettings.TRANSPORT_SSL_PREFIX, transportSSLSettings, sslContextHolders);
        this.transportSSLConfiguration.set(transportSSLConfiguration);
        Map<String, Settings> profileSettings = getTransportProfileSSLSettings(settings);
        profileSettings.forEach((key, profileSetting) -> loadConfiguration(key, profileSetting, sslContextHolders));

        return Collections.unmodifiableMap(sslContextHolders);
    }

    private SSLConfiguration loadConfiguration(String key, Settings settings, Map<SSLConfiguration, SSLContextHolder> contextHolders) {
        if (key.endsWith(".")) {
            // Drop trailing '.' so that any exception messages are consistent
            key = key.substring(0, key.length() - 1);
        }
        try {
            final SSLConfiguration configuration = new SSLConfiguration(settings);
            storeSslConfiguration(key, configuration);
            contextHolders.computeIfAbsent(configuration, this::createSslContext);
            return configuration;
        } catch (Exception e) {
            throw new ElasticsearchSecurityException("failed to load SSL configuration [{}]", e, key);
        }
    }

    private void storeSslConfiguration(String key, SSLConfiguration configuration) {
        if (key.endsWith(".")) {
            key = key.substring(0, key.length() - 1);
        }
        sslConfigurations.put(key, configuration);
    }


    /**
     * Returns information about each certificate that is referenced by any SSL configuration.
     * This includes certificates used for identity (with a private key) and those used for trust, but excludes
     * certificates that are provided by the JRE.
     * Due to the nature of KeyStores, this may include certificates that are available, but never used
     * such as a CA certificate that is no longer in use, or a server certificate for an unrelated host.
     *
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

        private final Supplier<SSLSocketFactory> delegateSupplier;
        private final String[] supportedProtocols;
        private final String[] ciphers;

        private volatile SSLSocketFactory delegate;

        SecuritySSLSocketFactory(Supplier<SSLSocketFactory> delegateSupplier, String[] supportedProtocols, String[] ciphers) {
            this.delegateSupplier = delegateSupplier;
            this.delegate = this.delegateSupplier.get();
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
            SSLSocket sslSocket = createWithPermissions(() -> delegate.createSocket(host, port, localHost, localPort));
            configureSSLSocket(sslSocket);
            return sslSocket;
        }

        @Override
        public Socket createSocket(InetAddress host, int port) throws IOException {
            SSLSocket sslSocket = createWithPermissions(() -> delegate.createSocket(host, port));
            configureSSLSocket(sslSocket);
            return sslSocket;
        }

        @Override
        public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
            SSLSocket sslSocket = createWithPermissions(() -> delegate.createSocket(address, port, localAddress, localPort));
            configureSSLSocket(sslSocket);
            return sslSocket;
        }

        public void reload() {
            final SSLSocketFactory newDelegate = delegateSupplier.get();
            this.delegate = newDelegate;
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
        private final List<Runnable> reloadListeners;

        SSLContextHolder(SSLContext context, SSLConfiguration sslConfiguration) {
            this.context = context;
            this.sslConfiguration = sslConfiguration;
            this.keyConfig = sslConfiguration.keyConfig();
            this.trustConfig = sslConfiguration.trustConfig();
            this.reloadListeners = new ArrayList<>();
        }

        SSLContext sslContext() {
            return context;
        }

        synchronized void reload() {
            invalidateSessions(context.getClientSessionContext());
            invalidateSessions(context.getServerSessionContext());
            reloadSslContext();
            this.reloadListeners.forEach(Runnable::run);
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

        public void addReloadListener(Runnable listener) {
            this.reloadListeners.add(listener);
        }
    }

    /**
     * Invalidates the sessions in the provided {@link SSLSessionContext}
     */
    static void invalidateSessions(SSLSessionContext sslSessionContext) {
        Enumeration<byte[]> sessionIds = sslSessionContext.getIds();
        while (sessionIds.hasMoreElements()) {
            byte[] sessionId = sessionIds.nextElement();
            SSLSession session = sslSessionContext.getSession(sessionId);
            // a SSLSession could be null as there is no lock while iterating, the session cache
            // could have evicted a value, the session could be timed out, or the session could
            // have already been invalidated, which removes the value from the session cache in the
            // sun implementation
            if (session != null) {
                session.invalidate();
            }
        }
    }

    /**
     * @return A map of Settings prefix to Settings object
     */
    private static Map<String, Settings> getRealmsSSLSettings(Settings settings) {
        final Map<String, Settings> sslSettings = new HashMap<>();
        final String prefix = "xpack.security.authc.realms.";
        final Map<String, Settings> settingsByRealmType = settings.getGroups(prefix);
        settingsByRealmType.forEach((realmType, typeSettings) -> {
                final Optional<String> nonDottedSetting = typeSettings.keySet().stream().filter(k -> k.indexOf('.') == -1).findAny();
                if (nonDottedSetting.isPresent()) {
                    logger.warn("Skipping any SSL configuration from realm [{}{}] because the key [{}] is not in the correct format",
                        prefix, realmType, nonDottedSetting.get());
                } else {
                    typeSettings.getAsGroups().forEach((realmName, realmSettings) -> {
                        Settings realmSSLSettings = realmSettings.getByPrefix("ssl.");
                        // Put this even if empty, so that the name will be mapped to the global SSL configuration
                        sslSettings.put(prefix + realmType + "." + realmName + ".ssl", realmSSLSettings);
                    });
                }
            }
        );
        return sslSettings;
    }

    private static Map<String, Settings> getTransportProfileSSLSettings(Settings settings) {
        Map<String, Settings> sslSettings = new HashMap<>();
        Map<String, Settings> profiles = settings.getGroups("transport.profiles.", true);
        for (Entry<String, Settings> entry : profiles.entrySet()) {
            Settings profileSettings = entry.getValue().getByPrefix("xpack.security.ssl.");
            sslSettings.put("transport.profiles." + entry.getKey() + ".xpack.security.ssl", profileSettings);
        }
        return sslSettings;
    }

    private Settings getHttpTransportSSLSettings(Settings settings) {
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

    public SSLConfiguration getHttpTransportSSLConfiguration() {
        return getSSLConfiguration(XPackSettings.HTTP_SSL_PREFIX);
    }

    private static Map<String, Settings> getMonitoringExporterSettings(Settings settings) {
        Map<String, Settings> sslSettings = new HashMap<>();
        Map<String, Settings> exportersSettings = settings.getGroups("xpack.monitoring.exporters.");
        for (Entry<String, Settings> entry : exportersSettings.entrySet()) {
            Settings exporterSSLSettings = entry.getValue().getByPrefix("ssl.");
            // Put this even if empty, so that the name will be mapped to the global SSL configuration
            sslSettings.put("xpack.monitoring.exporters." + entry.getKey() + ".ssl", exporterSSLSettings);
        }
        return sslSettings;
    }

    public SSLConfiguration getSSLConfiguration(String contextName) {
        if (contextName.endsWith(".")) {
            contextName = contextName.substring(0, contextName.length() - 1);
        }
        final SSLConfiguration configuration = sslConfigurations.get(contextName);
        if (configuration == null) {
            logger.warn("Cannot find SSL configuration for context {}. Known contexts are: {}", contextName,
                Strings.collectionToCommaDelimitedString(sslConfigurations.keySet()));
        }
        return configuration;
    }

    /**
     * Maps the supported protocols to an appropriate ssl context algorithm. We make an attempt to use the "best" algorithm when
     * possible. The names in this method are taken from the
     * <a href="https://docs.oracle.com/en/java/javase/11/docs/specs/security/standard-names.html#sslcontext-algorithms">Java Security
     * Standard Algorithm Names Documentation for Java 11</a>.
     */
    private static String sslContextAlgorithm(List<String> supportedProtocols) {
        if (supportedProtocols.isEmpty()) {
            throw new IllegalArgumentException("no SSL/TLS protocols have been configured");
        }
        for (Entry<String, String> entry : ORDERED_PROTOCOL_ALGORITHM_MAP.entrySet()) {
            if (supportedProtocols.contains(entry.getKey())) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("no supported SSL/TLS protocol was found in the configured supported protocols: "
            + supportedProtocols);
    }
}
