/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.IOSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.DiagnosticTrustManager;
import org.elasticsearch.common.ssl.KeyStoreUtil;
import org.elasticsearch.common.ssl.SslConfigException;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslDiagnostics;
import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.common.ssl.SslTrustConfig;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.ssl.cert.CertificateInfo;
import org.elasticsearch.xpack.core.watcher.WatcherField;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS;

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
        if (DEFAULT_SUPPORTED_PROTOCOLS.contains("TLSv1.3")) {
            protocolAlgorithmMap.put("TLSv1.3", "TLSv1.3");
        }
        protocolAlgorithmMap.put("TLSv1.2", "TLSv1.2");
        protocolAlgorithmMap.put("TLSv1.1", "TLSv1.1");
        protocolAlgorithmMap.put("TLSv1", "TLSv1");
        protocolAlgorithmMap.put("SSLv3", "SSLv3");
        protocolAlgorithmMap.put("SSLv2", "SSL");
        protocolAlgorithmMap.put("SSLv2Hello", "SSL");
        ORDERED_PROTOCOL_ALGORITHM_MAP = Collections.unmodifiableMap(protocolAlgorithmMap);
    }

    private static final Setting<Boolean> DIAGNOSE_TRUST_EXCEPTIONS_SETTING = Setting.boolSetting(
        "xpack.security.ssl.diagnose.trust", true, Setting.Property.NodeScope);

    private final Environment env;
    private final Settings settings;
    private final boolean diagnoseTrustExceptions;

    /**
     * This is a mapping from "context name" (in general use, the name of a setting key)
     * to a configuration.
     * This allows us to easily answer the question "What is the configuration for ssl in realm XYZ?"
     * Multiple "context names" may map to the same configuration (either by object-identity or by object-equality).
     * For example "xpack.http.ssl" may exist as a name in this map and have the global ssl configuration as a value
     */
    private final Map<String, SslConfiguration> sslConfigurations;

    /**
     * A mapping from an SslConfiguration to a pre-built context.
     * <p>
     * This is managed separately to the {@link #sslConfigurations} map, so that a single configuration (by object equality)
     * always maps to the same {@link SSLContextHolder}, even if it is being used within a different context-name.
     */
    private final Map<SslConfiguration, SSLContextHolder> sslContexts;

    /**
     * Create a new SSLService that parses the settings for the ssl contexts that need to be created, creates them, and then caches them
     * for use later
     */
    public SSLService(Environment environment) {
        this(environment, getSSLConfigurations(environment, environment.settings()));
    }

    /**
     * Create a new SSLService using the provided {@link SslConfiguration} instances. The ssl
     * contexts created from these configurations will be cached.
     */
    public SSLService(Environment environment, Map<String, SslConfiguration> sslConfigurations) {
        this.env = environment;
        this.settings = env.settings();
        this.diagnoseTrustExceptions = DIAGNOSE_TRUST_EXCEPTIONS_SETTING.get(environment.settings());
        this.sslConfigurations = sslConfigurations;
        this.sslContexts = loadSslConfigurations(this.sslConfigurations);
    }

    @Deprecated
    public SSLService(Settings settings, Environment environment) {
        this.env = environment;
        this.settings = env.settings();
        this.diagnoseTrustExceptions = DIAGNOSE_TRUST_EXCEPTIONS_SETTING.get(settings);
        this.sslConfigurations = getSSLConfigurations(env, this.settings);
        this.sslContexts = loadSslConfigurations(this.sslConfigurations);
    }

    private SSLService(Environment environment, Map<String, SslConfiguration> sslConfigurations,
                       Map<SslConfiguration, SSLContextHolder> sslContexts) {
        this.env = environment;
        this.settings = env.settings();
        this.diagnoseTrustExceptions = DIAGNOSE_TRUST_EXCEPTIONS_SETTING.get(environment.settings());
        this.sslConfigurations = sslConfigurations;
        this.sslContexts = sslContexts;
    }

    /**
     * Creates a new SSLService that supports dynamic creation of SSLContext instances. Instances created by this service will not be
     * cached and will not be monitored for reloading. This dynamic server does have access to the cached and monitored instances that
     * have been created during initialization
     */
    public SSLService createDynamicSSLService() {
        return new SSLService(env, sslConfigurations, sslContexts) {

            @Override
            Map<SslConfiguration, SSLContextHolder> loadSslConfigurations(Map<String, SslConfiguration> sslConfigurations) {
                // we don't need to load anything...
                return Collections.emptyMap();
            }

            /**
             * Returns the existing {@link SSLContextHolder} for the configuration
             * @throws IllegalArgumentException if not found
             */
            @Override
            SSLContextHolder sslContextHolder(SslConfiguration sslConfiguration) {
                SSLContextHolder holder = sslContexts.get(sslConfiguration);
                if (holder == null) {
                    // normally we'd throw here but let's create a new one that is not cached and will not be monitored for changes!
                    holder = createSslContext(sslConfiguration);
                }
                return holder;
            }
        };
    }

    public static void registerSettings(List<Setting<?>> settingList) {
        settingList.add(DIAGNOSE_TRUST_EXCEPTIONS_SETTING);
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
     * and {@link #sslIOSessionStrategy(SslConfiguration)} (Deprecated, but not removed because monitoring uses dynamic SSL settings)
     */
    @Deprecated
    public SSLIOSessionStrategy sslIOSessionStrategy(Settings settings) {
        SslConfiguration config = sslConfiguration(settings);
        return sslIOSessionStrategy(config);
    }

    public SSLIOSessionStrategy sslIOSessionStrategy(SslConfiguration config) {
        SSLContext sslContext = sslContext(config);
        String[] ciphers = supportedCiphers(sslParameters(sslContext).getCipherSuites(), config.getCipherSuites(), false);
        String[] supportedProtocols = config.getSupportedProtocols().toArray(Strings.EMPTY_ARRAY);
        HostnameVerifier verifier;

        if (config.getVerificationMode().isHostnameVerificationEnabled()) {
            verifier = SSLIOSessionStrategy.getDefaultHostnameVerifier();
        } else {
            verifier = NoopHostnameVerifier.INSTANCE;
        }

        final SSLIOSessionStrategy strategy = sslIOSessionStrategy(sslContext, supportedProtocols, ciphers, verifier);
        return strategy;
    }

    public static HostnameVerifier getHostnameVerifier(SslConfiguration sslConfiguration) {
        if (sslConfiguration.getVerificationMode().isHostnameVerificationEnabled()) {
            return new DefaultHostnameVerifier();
        } else {
            return NoopHostnameVerifier.INSTANCE;
        }
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
        return new SSLIOSessionStrategy(sslContext, protocols, ciphers, verifier) {
            @Override
            protected void verifySession(HttpHost host, IOSession iosession, SSLSession session) throws SSLException {
                if (verifier.verify(host.getHostName(), session) == false) {
                    final Certificate[] certs = session.getPeerCertificates();
                    final X509Certificate x509 = (X509Certificate) certs[0];
                    final X500Principal x500Principal = x509.getSubjectX500Principal();
                    final String altNames = Strings.collectionToCommaDelimitedString(SslDiagnostics.describeValidHostnames(x509));
                    throw new SSLPeerUnverifiedException(LoggerMessageFormat.format("Expected SSL certificate to be valid for host [{}]," +
                            " but it is only valid for subject alternative names [{}] and subject [{}]",
                        new Object[]{host.getHostName(), altNames, x500Principal.toString()}));
                }
            }
        };
    }

    /**
     * Create a new {@link SSLSocketFactory} based on the provided configuration.
     * The socket factory will also properly configure the ciphers and protocols on each socket that is created
     *
     * @param configuration The SSL configuration to use. Typically obtained from {@link #getSSLConfiguration(String)}
     * @return Never {@code null}.
     */
    public SSLSocketFactory sslSocketFactory(SslConfiguration configuration) {
        final SSLContextHolder contextHolder = sslContextHolder(configuration);
        SSLSocketFactory socketFactory = contextHolder.sslContext().getSocketFactory();
        final SecuritySSLSocketFactory securitySSLSocketFactory = new SecuritySSLSocketFactory(
            () -> contextHolder.sslContext().getSocketFactory(),
            configuration.getSupportedProtocols().toArray(Strings.EMPTY_ARRAY),
            supportedCiphers(socketFactory.getSupportedCipherSuites(), configuration.getCipherSuites(), false));
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
    public SSLEngine createSSLEngine(SslConfiguration configuration, String host, int port) {
        SSLContext sslContext = sslContext(configuration);
        SSLEngine sslEngine = sslContext.createSSLEngine(host, port);
        String[] ciphers = supportedCiphers(sslEngine.getSupportedCipherSuites(), configuration.getCipherSuites(), false);
        String[] supportedProtocols = configuration.getSupportedProtocols().toArray(Strings.EMPTY_ARRAY);
        SSLParameters parameters = new SSLParameters(ciphers, supportedProtocols);
        if (configuration.getVerificationMode().isHostnameVerificationEnabled() && host != null) {
            // By default, an SSLEngine will not perform hostname verification. In order to perform hostname verification
            // we need to specify a EndpointIdentificationAlgorithm. We use the HTTPS algorithm to prevent against
            // man in the middle attacks for all of our connections.
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
        }
        // we use the cipher suite order so that we can prefer the ciphers we set first in the list
        parameters.setUseCipherSuitesOrder(true);
        configuration.getClientAuth().configure(parameters);

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
    public boolean isConfigurationValidForServerUsage(SslConfiguration sslConfiguration) {
        Objects.requireNonNull(sslConfiguration, "SslConfiguration cannot be null");
        return sslConfiguration.getKeyConfig().hasKeyMaterial();
    }

    /**
     * Indicates whether client authentication is enabled for a particular configuration
     */
    public boolean isSSLClientAuthEnabled(SslConfiguration sslConfiguration) {
        Objects.requireNonNull(sslConfiguration, "SslConfiguration cannot be null");
        return sslConfiguration.getClientAuth().enabled();
    }

    /**
     * Returns the {@link SSLContext} for the configuration. Mainly used for testing
     */
    public SSLContext sslContext(SslConfiguration configuration) {
        return sslContextHolder(configuration).sslContext();
    }

    public void reloadSSLContext(SslConfiguration configuration) {
        sslContextHolder(configuration).reload();
    }

    /**
     * Returns the existing {@link SSLContextHolder} for the configuration
     *
     * @throws IllegalArgumentException if not found
     */
    SSLContextHolder sslContextHolder(SslConfiguration sslConfiguration) {
        Objects.requireNonNull(sslConfiguration, "SSL Configuration cannot be null");
        SSLContextHolder holder = sslContexts.get(sslConfiguration);
        if (holder == null) {
            logger.info("Cannot find SSL context [{}], available contexts are [{}]", sslConfiguration, sslContexts.keySet());
            throw new IllegalArgumentException("did not find an SSLContext for [" + sslConfiguration.toString() + "]");
        }
        return holder;
    }

    /**
     * Returns the existing {@link SslConfiguration} for the given settings
     *
     * @param settings the settings for the ssl configuration
     * @return the ssl configuration for the provided settings
     */
    public SslConfiguration sslConfiguration(Settings settings) {
        return SslSettingsLoader.load(settings, null, env);
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
    Collection<SslConfiguration> getLoadedSslConfigurations() {
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

            if (found == false) {
                unsupportedCiphers.add(requestedCipher);
            }
        }

        if (supportedCiphersList.isEmpty()) {
            throw new SslConfigException(
                "none of the ciphers " + Arrays.toString(requestedCiphers.toArray()) + " are supported by this JVM");
        }

        if (log && unsupportedCiphers.isEmpty() == false) {
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
    private SSLContextHolder createSslContext(SslConfiguration sslConfiguration) {
        if (logger.isDebugEnabled()) {
            logger.debug("using ssl settings [{}]", sslConfiguration);
        }
        X509ExtendedTrustManager trustManager = sslConfiguration.getTrustConfig().createTrustManager();
        X509ExtendedKeyManager keyManager = sslConfiguration.getKeyConfig().createKeyManager();
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
                                              SslConfiguration sslConfiguration) {
        trustManager = wrapWithDiagnostics(trustManager, sslConfiguration);
        // Initialize sslContext
        try {
            SSLContext sslContext = SSLContext.getInstance(sslContextAlgorithm(sslConfiguration.getSupportedProtocols()));
            sslContext.init(new X509ExtendedKeyManager[]{keyManager}, new X509ExtendedTrustManager[]{trustManager}, null);

            // check the supported ciphers and log them here to prevent spamming logs on every call
            supportedCiphers(sslContext.getSupportedSSLParameters().getCipherSuites(), sslConfiguration.getCipherSuites(), true);

            return new SSLContextHolder(sslContext, sslConfiguration);
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new ElasticsearchException("failed to initialize the SSLContext", e);
        }
    }

    X509ExtendedTrustManager wrapWithDiagnostics(X509ExtendedTrustManager trustManager, SslConfiguration configuration) {
        if (diagnoseTrustExceptions && trustManager instanceof DiagnosticTrustManager == false) {
            final Logger diagnosticLogger = LogManager.getLogger(DiagnosticTrustManager.class);
            // A single configuration might be used in many place, if there are multiple, we just list "shared" because
            // that is better than the alternatives. Just listing would be misleading (it might not be the right one)
            // but listing all of them would be confusing (e.g. some might be the default realms)
            // This needs to be a supplier (deferred evaluation) because we might load more configurations after this context is built.
            final Supplier<String> contextName = () -> {
                final List<String> names = sslConfigurations.entrySet().stream()
                    .filter(e -> e.getValue().equals(configuration))
                    .limit(2) // we only need to distinguishing between 0/1/many
                    .map(Entry::getKey)
                    .collect(Collectors.toUnmodifiableList());
                final String name;
                switch (names.size()) {
                    case 0:
                        name = "(unknown)";
                        break;
                    case 1:
                        name = names.get(0);
                        break;
                    default:
                        name = "(shared)";
                        break;
                }
                return name + " (with trust configuration: " + configuration.getTrustConfig() + ")";
            };
            trustManager = new DiagnosticTrustManager(trustManager, contextName, diagnosticLogger::warn);
        }
        return trustManager;
    }

    public static Map<String, SslConfiguration> getSSLConfigurations(Environment env) {
        return getSSLConfigurations(env, env.settings());
    }

    private static Map<String, SslConfiguration> getSSLConfigurations(Environment env, Settings settings) {
        final Map<String, Settings> sslSettingsMap = getSSLSettingsMap(settings);
        final Map<String, SslConfiguration> sslConfigurationMap = new HashMap<>(sslSettingsMap.size());
        sslSettingsMap.forEach((key, sslSettings) -> {
            if (key.endsWith(".")) {
                // Drop trailing '.' so that any exception messages are consistent
                key = key.substring(0, key.length() - 1);
            }
            try {
                sslConfigurationMap.put(key, SslSettingsLoader.load(sslSettings, null, env, getKeyStoreFilter(key)));
            } catch (SslConfigException e) {
                throw new ElasticsearchSecurityException("failed to load SSL configuration [{}] - {}", e, key, e.getMessage());
            }
        });
        return Collections.unmodifiableMap(sslConfigurationMap);
    }

    private static Function<KeyStore, KeyStore> getKeyStoreFilter(String sslContext) {
        if (sslContext.equals("xpack.security.http.ssl")) {
            final Function<GeneralSecurityException, RuntimeException> exceptionHandler = e -> new ElasticsearchSecurityException(
                "Cannot process keystore for SSL configuration [" + sslContext + "] - " + e.getMessage(),
                e
            );
            final Predicate<KeyStoreUtil.KeyStoreEntry> isCA = e -> e.getX509Certificate().getBasicConstraints() >= 0;
            return ks -> {
                final AtomicInteger keyCount = new AtomicInteger(0);
                final AtomicInteger caCount = new AtomicInteger(0);
                KeyStoreUtil.stream(ks, exceptionHandler).filter(e -> e.isKeyEntry()).forEach(e -> {
                    keyCount.incrementAndGet();
                    if (isCA.test(e)) {
                        caCount.incrementAndGet();
                    }
                });
                if (keyCount.get() <= 1) {
                    // There's only 1 key in the keystore - don't filter it
                    return ks;
                }
                if (caCount.get() > 0 && caCount.get() < keyCount.get()) {
                    // There are both CAs & non-CAs in the keystore, filter out the CAs (they're probably there to support enrollment)
                    return KeyStoreUtil.filter(ks, e -> e.isKeyEntry() && isCA.test(e) == false);
                } else {
                    return ks;
                }
            };
        }
        return null;
    }

    static Map<String, Settings> getSSLSettingsMap(Settings settings) {
        final Map<String, Settings> sslSettingsMap = new HashMap<>();
        sslSettingsMap.put(XPackSettings.HTTP_SSL_PREFIX, getHttpTransportSSLSettings(settings));
        sslSettingsMap.put("xpack.http.ssl", settings.getByPrefix("xpack.http.ssl."));
        sslSettingsMap.putAll(getRealmsSSLSettings(settings));
        sslSettingsMap.putAll(getMonitoringExporterSettings(settings));
        sslSettingsMap.put(WatcherField.EMAIL_NOTIFICATION_SSL_PREFIX, settings.getByPrefix(WatcherField.EMAIL_NOTIFICATION_SSL_PREFIX));
        sslSettingsMap.put(XPackSettings.TRANSPORT_SSL_PREFIX, settings.getByPrefix(XPackSettings.TRANSPORT_SSL_PREFIX));
        sslSettingsMap.putAll(getTransportProfileSSLSettings(settings));
        return Collections.unmodifiableMap(sslSettingsMap);
    }

    /**
     * Parses the settings to load all SslConfiguration objects that will be used.
     */
    Map<SslConfiguration, SSLContextHolder> loadSslConfigurations(Map<String, SslConfiguration> sslConfigurationMap) {
        final Map<SslConfiguration, SSLContextHolder> sslContextHolders = new HashMap<>(sslConfigurationMap.size());
        sslConfigurationMap.forEach((key, sslConfiguration) -> {
            try {
                sslContextHolders.computeIfAbsent(sslConfiguration, this::createSslContext);
            } catch (SslConfigException e) {
                throw new ElasticsearchSecurityException("failed to load SSL configuration [{}] - {}", e, key, e.getMessage());
            } catch (Exception e) {
                throw new ElasticsearchSecurityException("failed to load SSL configuration [{}] - {}", e, key, e);
            }
        });

        for (String context : List.of("xpack.security.transport.ssl", "xpack.security.http.ssl")) {
            validateServerConfiguration(context);
        }

        return Collections.unmodifiableMap(sslContextHolders);
    }

    private void validateServerConfiguration(String prefix) {
        assert prefix.endsWith(".ssl");
        SslConfiguration configuration = getSSLConfiguration(prefix);
        final String enabledSetting = prefix + ".enabled";
        if (settings.getAsBoolean(enabledSetting, false)) {
            // Client Authentication _should_ be required, but if someone turns it off, then this check is no longer relevant
            final SSLConfigurationSettings configurationSettings = SSLConfigurationSettings.withPrefix(prefix + ".", true);
            if (isConfigurationValidForServerUsage(configuration) == false) {
                throw new ElasticsearchSecurityException(
                    "invalid SSL configuration for "
                        + prefix
                        + " - server ssl configuration requires a key and certificate, but these have not been configured; "
                        + "you must set either ["
                        + configurationSettings.x509KeyPair.keystorePath.getKey()
                        + "], or both ["
                        + configurationSettings.x509KeyPair.keyPath.getKey()
                        + "] and ["
                        + configurationSettings.x509KeyPair.certificatePath.getKey()
                        + "]"
                );
            }
        } else if (settings.hasValue(enabledSetting) == false) {
            final List<String> sslSettingNames = settings.keySet().stream()
                .filter(s -> s.startsWith(prefix))
                .sorted()
                .collect(Collectors.toUnmodifiableList());
            if (sslSettingNames.isEmpty() == false) {
                throw new ElasticsearchSecurityException("invalid configuration for " + prefix + " - [" + enabledSetting +
                    "] is not set, but the following settings have been configured in elasticsearch.yml : [" +
                    Strings.collectionToCommaDelimitedString(sslSettingNames) + "]");
            }
        }
    }


    /**
     * Returns information about each certificate that is referenced by any SSL configuration.
     * This includes certificates used for identity (with a private key) and those used for trust, but excludes
     * certificates that are provided by the JRE.
     * Due to the nature of KeyStores, this may include certificates that are available, but never used
     * such as a CA certificate that is no longer in use, or a server certificate for an unrelated host.
     *
     * @see SslTrustConfig#getConfiguredCertificates()
     */
    public Collection<CertificateInfo> getLoadedCertificates() throws GeneralSecurityException, IOException {
        return this.getLoadedSslConfigurations().stream()
            .map(SslConfiguration::getConfiguredCertificates)
            .flatMap(Collection::stream)
            .map(cert -> new CertificateInfo(
                cert.getPath(), cert.getFormat(), cert.getAlias(), cert.hasPrivateKey(), cert.getCertificate()
            ))
            .collect(Sets.toUnmodifiableSortedSet());
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
        private final SslKeyConfig keyConfig;
        private final SslTrustConfig trustConfig;
        private final SslConfiguration sslConfiguration;
        private final List<Runnable> reloadListeners;

        SSLContextHolder(SSLContext context, SslConfiguration sslConfiguration) {
            this.context = context;
            this.sslConfiguration = sslConfiguration;
            this.keyConfig = sslConfiguration.getKeyConfig();
            this.trustConfig = sslConfiguration.getTrustConfig();
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
                X509ExtendedKeyManager loadedKeyManager = keyConfig.createKeyManager();
                X509ExtendedTrustManager loadedTrustManager = trustConfig.createTrustManager();
                loadedTrustManager = wrapWithDiagnostics(loadedTrustManager, sslConfiguration);

                SSLContext loadedSslContext = SSLContext.getInstance(sslContextAlgorithm(sslConfiguration.getSupportedProtocols()));
                loadedSslContext.init(new X509ExtendedKeyManager[]{loadedKeyManager},
                    new X509ExtendedTrustManager[]{loadedTrustManager}, null);
                supportedCiphers(loadedSslContext.getSupportedSSLParameters().getCipherSuites(), sslConfiguration.getCipherSuites(), false);
                this.context = loadedSslContext;
            } catch (GeneralSecurityException e) {
                throw new ElasticsearchException("failed to initialize the SSLContext", e);
            }
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
            // an SSLSession could be null as there is no lock while iterating, the session cache
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

    private static Settings getHttpTransportSSLSettings(Settings settings) {
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

    public SslConfiguration getHttpTransportSSLConfiguration() {
        return getSSLConfiguration(XPackSettings.HTTP_SSL_PREFIX);
    }

    public SslConfiguration getTransportSSLConfiguration() {
        return getSSLConfiguration(XPackSettings.TRANSPORT_SSL_PREFIX);
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

    public SslConfiguration getSSLConfiguration(String contextName) {
        if (contextName.endsWith(".")) {
            contextName = contextName.substring(0, contextName.length() - 1);
        }
        final SslConfiguration configuration = sslConfigurations.get(contextName);
        if (configuration == null) {
            logger.warn("Cannot find SSL configuration for context {}. Known contexts are: {}", contextName,
                Strings.collectionToCommaDelimitedString(sslConfigurations.keySet()));
        } else {
            logger.debug("SSL configuration [{}] is [{}]", contextName, configuration);
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
