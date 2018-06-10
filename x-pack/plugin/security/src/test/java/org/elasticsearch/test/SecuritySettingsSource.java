/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.discovery.ClusterDiscoveryConfiguration;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.support.HasherFactory;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static org.apache.lucene.util.LuceneTestCase.createTempFile;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.security.test.SecurityTestUtils.writeFile;

/**
 * {@link org.elasticsearch.test.NodeConfigurationSource} subclass that allows to set all needed settings for x-pack security.
 * Unicast discovery is configured through {@link org.elasticsearch.test.discovery.ClusterDiscoveryConfiguration.UnicastZen},
 * also x-pack is installed with all the needed configuration and files.
 * To avoid conflicts, every cluster should have its own instance of this class as some configuration files need to be created.
 */
public class SecuritySettingsSource extends ClusterDiscoveryConfiguration.UnicastZen {

    public static final Settings DEFAULT_SETTINGS = Settings.EMPTY;

    public static final String TEST_USER_NAME = "test_user";
    public static final String HASHING_ALGORITHM = randomFrom("pbkdf2", "bcrypt");
    private static final Hasher hasher = HasherFactory.getHasher(HASHING_ALGORITHM);
    public static final String TEST_PASSWORD_HASHED =
        new String(hasher.hash(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())));
    public static final String TEST_ROLE = "user";
    public static final String TEST_SUPERUSER = "test_superuser";

    public static final String DEFAULT_TRANSPORT_CLIENT_ROLE = "transport_client";
    public static final String DEFAULT_TRANSPORT_CLIENT_USER_NAME = "test_trans_client_user";

    public static final String CONFIG_STANDARD_USER =
            TEST_USER_NAME + ":" + TEST_PASSWORD_HASHED + "\n" +
            DEFAULT_TRANSPORT_CLIENT_USER_NAME + ":" + TEST_PASSWORD_HASHED + "\n" +
            TEST_SUPERUSER + ":" + TEST_PASSWORD_HASHED + "\n";

    public static final String CONFIG_STANDARD_USER_ROLES =
            TEST_ROLE + ":" + TEST_USER_NAME + "," + DEFAULT_TRANSPORT_CLIENT_USER_NAME + "\n" +
            DEFAULT_TRANSPORT_CLIENT_ROLE + ":" + DEFAULT_TRANSPORT_CLIENT_USER_NAME + "\n" +
            "superuser:" + TEST_SUPERUSER + "\n";

    public static final String CONFIG_ROLE_ALLOW_ALL =
            TEST_ROLE + ":\n" +
                    "  cluster: [ ALL ]\n" +
                    "  indices:\n" +
                    "    - names: '*'\n" +
                    "      privileges: [ ALL ]\n";

    private final Path parentFolder;
    private final String subfolderPrefix;
    private final boolean sslEnabled;
    private final boolean hostnameVerificationEnabled;
    private final boolean usePEM;

    /**
     * Creates a new {@link org.elasticsearch.test.NodeConfigurationSource} for the security configuration.//UsersTool
     *
     * @param numOfNodes the number of nodes for proper unicast configuration (can be more than actually available)
     * @param sslEnabled whether ssl is enabled
     * @param parentFolder the parent folder that will contain all of the configuration files that need to be created
     * @param scope the scope of the test that is requiring an instance of SecuritySettingsSource
     */
    public SecuritySettingsSource(int numOfNodes, boolean sslEnabled, Path parentFolder, Scope scope) {
        super(numOfNodes, DEFAULT_SETTINGS);
        this.parentFolder = parentFolder;
        this.subfolderPrefix = scope.name();
        this.sslEnabled = sslEnabled;
        this.hostnameVerificationEnabled = randomBoolean();
        this.usePEM = randomBoolean();

    }

    Path nodePath(final int nodeOrdinal) {
        return parentFolder.resolve(subfolderPrefix + "-" + nodeOrdinal);
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        final Path home = nodePath(nodeOrdinal);
        final Path xpackConf = home.resolve("config");
        try {
            Files.createDirectories(xpackConf);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        writeFile(xpackConf, "roles.yml", configRoles());
        writeFile(xpackConf, "users", configUsers());
        writeFile(xpackConf, "users_roles", configUsersRoles());

        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(XPackSettings.SECURITY_ENABLED.getKey(), true)
                //TODO: for now isolate security tests from watcher & monitoring (randomize this later)
                .put(XPackSettings.WATCHER_ENABLED.getKey(), false)
                .put(XPackSettings.MONITORING_ENABLED.getKey(), false)
                .put(XPackSettings.AUDIT_ENABLED.getKey(), randomBoolean())
                .put(LoggingAuditTrail.HOST_ADDRESS_SETTING.getKey(), randomBoolean())
                .put(LoggingAuditTrail.HOST_NAME_SETTING.getKey(), randomBoolean())
                .put(LoggingAuditTrail.NODE_NAME_SETTING.getKey(), randomBoolean())
                .put("xpack.security.authc.realms.file.type", FileRealmSettings.TYPE)
                .put("xpack.security.authc.realms.file.order", 0)
                .put("xpack.security.authc.realms.index.type", NativeRealmSettings.TYPE)
            .put("xpack.security.authc.realms.index.order", "1")
            .put("xpack.security.authc.password_hashing.algorithm", HASHING_ALGORITHM);
        addNodeSSLSettings(builder);
        return builder.build();
    }

    @Override
    public Path nodeConfigPath(int nodeOrdinal) {
        return nodePath(nodeOrdinal).resolve("config");
    }

    @Override
    public Settings transportClientSettings() {
        Settings superSettings = super.transportClientSettings();
        Settings.Builder builder = Settings.builder().put(superSettings);
        addClientSSLSettings(builder, "");
        addDefaultSecurityTransportType(builder, superSettings);

        if (randomBoolean()) {
            builder.put(SecurityField.USER_SETTING.getKey(),
                    transportClientUsername() + ":" + new String(transportClientPassword().getChars()));
        } else {
            builder.put(ThreadContext.PREFIX + ".Authorization", basicAuthHeaderValue(transportClientUsername(),
                    transportClientPassword()));
        }
        return builder.build();
    }

    protected void addDefaultSecurityTransportType(Settings.Builder builder, Settings settings) {
        if (NetworkModule.TRANSPORT_TYPE_SETTING.exists(settings) == false) {
            builder.put(NetworkModule.TRANSPORT_TYPE_SETTING.getKey(), SecurityField.NAME4);
        }
    }


    @Override
    public Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateSecurity.class, Netty4Plugin.class, ReindexPlugin.class, CommonAnalysisPlugin.class);
    }

    @Override
    public Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(XPackClientPlugin.class, Netty4Plugin.class, ReindexPlugin.class, CommonAnalysisPlugin.class);
    }

    protected String configUsers() {
        return CONFIG_STANDARD_USER;
    }

    protected String configUsersRoles() {
        return CONFIG_STANDARD_USER_ROLES;
    }

    protected String configRoles() {
        return CONFIG_ROLE_ALLOW_ALL;
    }

    protected String nodeClientUsername() {
        return TEST_USER_NAME;
    }

    protected SecureString nodeClientPassword() {
        return new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray());
    }

    protected String transportClientUsername() {
        return DEFAULT_TRANSPORT_CLIENT_USER_NAME;
    }

    protected SecureString transportClientPassword() {
        return new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray());
    }

    private void addNodeSSLSettings(Settings.Builder builder) {
        if (sslEnabled) {
            if (usePEM) {
                addSSLSettingsForPEMFiles(builder, "",
                        "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem", "testnode",
                        "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
                        Arrays.asList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-client-profile.crt",
                                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/active-directory-ca.crt",
                                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt",
                                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/openldap.crt",
                                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"),
                        sslEnabled, hostnameVerificationEnabled, false);

            } else {
                addSSLSettingsForStore(builder, "", "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks",
                        "testnode", sslEnabled, hostnameVerificationEnabled, false);
            }
        } else if (randomBoolean()) {
            builder.put(XPackSettings.TRANSPORT_SSL_ENABLED.getKey(), false);
        }
    }

    public void addClientSSLSettings(Settings.Builder builder, String prefix) {
        if (usePEM) {
            addSSLSettingsForPEMFiles(builder, prefix,
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.pem", "testclient",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt",
                Arrays.asList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
                              "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt"),
                    sslEnabled, hostnameVerificationEnabled, true);
        } else {
            addSSLSettingsForStore(builder, prefix, "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.jks",
                "testclient", sslEnabled, hostnameVerificationEnabled, true);
        }
    }

    /**
     * Returns the configuration settings given the location of a certificate and its password
     *
     * @param resourcePathToStore the location of the keystore or truststore
     * @param password the password
     */
    public static void addSSLSettingsForStore(Settings.Builder builder, String resourcePathToStore, String password) {
        addSSLSettingsForStore(builder, "", resourcePathToStore, password, true, true, true);
    }

    private static void addSSLSettingsForStore(Settings.Builder builder, String prefix, String resourcePathToStore, String password,
                                               boolean sslEnabled, boolean hostnameVerificationEnabled,
                                               boolean transportClient) {
        Path store = resolveResourcePath(resourcePathToStore);

        if (transportClient == false) {
            builder.put(prefix + "xpack.security.http.ssl.enabled", false);
        }
        builder.put(XPackSettings.TRANSPORT_SSL_ENABLED.getKey(), sslEnabled);

        builder.put(prefix + "xpack.ssl.verification_mode", hostnameVerificationEnabled ? "full" : "certificate");
        builder.put(prefix + "xpack.ssl.keystore.path", store);
        if (transportClient) {
            // continue using insecure settings for clients until we figure out what to do there...
            builder.put(prefix + "xpack.ssl.keystore.password", password);
        } else {
            addSecureSettings(builder, secureSettings ->
                secureSettings.setString(prefix + "xpack.ssl.keystore.secure_password", password));
        }

        if (randomBoolean()) {
            builder.put(prefix + "xpack.ssl.truststore.path", store);
            if (transportClient) {
                // continue using insecure settings for clients until we figure out what to do there...
                builder.put(prefix + "xpack.ssl.truststore.password", password);
            } else {
                addSecureSettings(builder, secureSettings ->
                    secureSettings.setString(prefix + "xpack.ssl.truststore.secure_password", password));
            }
        }
    }

    private static void addSSLSettingsForPEMFiles(Settings.Builder builder, String prefix, String keyPath, String password,
                                                  String certificatePath, List<String> trustedCertificates, boolean sslEnabled,
                                                  boolean hostnameVerificationEnabled, boolean transportClient) {

        if (transportClient == false) {
            builder.put(prefix + "xpack.security.http.ssl.enabled", false);
        }
        builder.put(XPackSettings.TRANSPORT_SSL_ENABLED.getKey(), sslEnabled);

        builder.put(prefix + "xpack.ssl.verification_mode", hostnameVerificationEnabled ? "full" : "certificate");
        builder.put(prefix + "xpack.ssl.key", resolveResourcePath(keyPath))
                .put(prefix + "xpack.ssl.certificate", resolveResourcePath(certificatePath));
        if (transportClient) {
            // continue using insecure settings for clients until we figure out what to do there...
            builder.put(prefix + "xpack.ssl.key_passphrase", password);
        } else {
            addSecureSettings(builder, secureSettings ->
                secureSettings.setString(prefix + "xpack.ssl.secure_key_passphrase", password));
        }

        if (trustedCertificates.isEmpty() == false) {
            builder.put(prefix + "xpack.ssl.certificate_authorities",
                    Strings.arrayToCommaDelimitedString(resolvePathsToString(trustedCertificates)));
        }
    }

    public static void addSecureSettings(Settings.Builder builder, Consumer<MockSecureSettings> settingsSetter) {
        SecureSettings secureSettings = builder.getSecureSettings();
        if (secureSettings instanceof MockSecureSettings) {
            settingsSetter.accept((MockSecureSettings) secureSettings);
        } else if (secureSettings == null) {
            MockSecureSettings mockSecureSettings = new MockSecureSettings();
            settingsSetter.accept(mockSecureSettings);
            builder.setSecureSettings(mockSecureSettings);
        } else {
            throw new AssertionError("Test settings builder must contain MockSecureSettings, " +
                "but has [" + secureSettings.getClass().getName() + "]");
        }
    }

    private static String[] resolvePathsToString(List<String> resourcePaths) {
        List<String> resolvedPaths = new ArrayList<>(resourcePaths.size());
        for (String resource : resourcePaths) {
            resolvedPaths.add(resolveResourcePath(resource).toString());
        }
        return resolvedPaths.toArray(new String[resolvedPaths.size()]);
    }

    private static Path resolveResourcePath(String resourcePathToStore) {
        try {
            Path path = createTempFile();
            try (InputStream resourceInput = SecuritySettingsSource.class.getResourceAsStream(resourcePathToStore)) {
                Files.copy(resourceInput, path, StandardCopyOption.REPLACE_EXISTING);
            }
            return path;
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to resolve resource (Path=[{}])", e, resourcePathToStore);
        }
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }
}
