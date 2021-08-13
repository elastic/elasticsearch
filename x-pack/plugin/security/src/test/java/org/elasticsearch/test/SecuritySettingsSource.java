/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.test;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static org.apache.lucene.util.LuceneTestCase.createTempFile;
import static org.elasticsearch.test.ESTestCase.inFipsJvm;
import static org.elasticsearch.test.SecurityIntegTestCase.getFastStoredHashAlgoForTests;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD;
import static org.elasticsearch.xpack.security.test.SecurityTestUtils.writeFile;

/**
 * {@link org.elasticsearch.test.NodeConfigurationSource} subclass that allows to set all needed settings for x-pack security.
 * X-pack is installed with all the needed configuration and files.
 * To avoid conflicts, every cluster should have its own instance of this class as some configuration files need to be created.
 */
public class SecuritySettingsSource extends NodeConfigurationSource {

    public static final String TEST_USER_NAME = "test_user";
    public static final Hasher HASHER = getFastStoredHashAlgoForTests();
    public static final String TEST_PASSWORD_HASHED =
        new String(HASHER.hash(new SecureString(TEST_PASSWORD.toCharArray())));
    public static final String TEST_ROLE = "user";
    public static final String TEST_SUPERUSER = "test_superuser";
    public static final RequestOptions SECURITY_REQUEST_OPTIONS = RequestOptions.DEFAULT.toBuilder()
        .addHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString((TEST_USER_NAME + ":" + TEST_PASSWORD).getBytes(StandardCharsets.UTF_8)))
        .build();

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
                    "      allow_restricted_indices: true\n" +
                    "      privileges: [ ALL ]\n";

    private final Path parentFolder;
    private final String subfolderPrefix;
    private final boolean sslEnabled;
    private final boolean hostnameVerificationEnabled;
    private final boolean usePEM;

    /**
     * Creates a new {@link org.elasticsearch.test.NodeConfigurationSource} for the security configuration.
     *
     * @param sslEnabled whether ssl is enabled
     * @param parentFolder the parent folder that will contain all of the configuration files that need to be created
     * @param scope the scope of the test that is requiring an instance of SecuritySettingsSource
     */
    public SecuritySettingsSource(boolean sslEnabled, Path parentFolder, Scope scope) {
        this.parentFolder = parentFolder;
        this.subfolderPrefix = scope.name();
        this.sslEnabled = sslEnabled;
        this.hostnameVerificationEnabled = randomBoolean();
        // Use PEM instead of JKS stores so that we can run these in a FIPS 140 JVM
        if (inFipsJvm()) {
            this.usePEM = true;
        } else {
            this.usePEM = randomBoolean();
        }
    }

    Path nodePath(final int nodeOrdinal) {
        return parentFolder.resolve(subfolderPrefix + "-" + nodeOrdinal);
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
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
        writeFile(xpackConf, "operator_users.yml", configOperatorUsers());
        writeFile(xpackConf, "service_tokens", configServiceTokens());

        Settings.Builder builder = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), home)
                .put(XPackSettings.SECURITY_ENABLED.getKey(), true)
                .put(NetworkModule.TRANSPORT_TYPE_KEY, randomBoolean() ? SecurityField.NAME4 : SecurityField.NIO)
                .put(NetworkModule.HTTP_TYPE_KEY, randomBoolean() ? SecurityField.NAME4 : SecurityField.NIO)
                //TODO: for now isolate security tests from watcher (randomize this later)
                .put(XPackSettings.WATCHER_ENABLED.getKey(), false)
                .put(XPackSettings.AUDIT_ENABLED.getKey(), randomBoolean())
                .put(LoggingAuditTrail.EMIT_HOST_ADDRESS_SETTING.getKey(), randomBoolean())
                .put(LoggingAuditTrail.EMIT_HOST_NAME_SETTING.getKey(), randomBoolean())
                .put(LoggingAuditTrail.EMIT_NODE_NAME_SETTING.getKey(), randomBoolean())
                .put(LoggingAuditTrail.EMIT_NODE_ID_SETTING.getKey(), randomBoolean())
                .put("xpack.security.authc.realms." + FileRealmSettings.TYPE + ".file.order", 0)
                .put("xpack.security.authc.realms." + NativeRealmSettings.TYPE + ".index.order", "1")
                .put("xpack.license.self_generated.type", "trial");
        addNodeSSLSettings(builder);
        return builder.build();
    }

    @Override
    public Path nodeConfigPath(int nodeOrdinal) {
        return nodePath(nodeOrdinal).resolve("config");
    }

    protected void addDefaultSecurityTransportType(Settings.Builder builder, Settings settings) {
        if (NetworkModule.TRANSPORT_TYPE_SETTING.exists(settings) == false) {
            builder.put(NetworkModule.TRANSPORT_TYPE_SETTING.getKey(), SecurityField.NAME4);
        }
    }


    @Override
    public Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateSecurity.class, Netty4Plugin.class, ReindexPlugin.class, CommonAnalysisPlugin.class,
            InternalSettingsPlugin.class);
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

    protected String configOperatorUsers() {
        // By default, no operator user is configured
        return "";
    }

    protected String configServiceTokens() {
        return "";
    }

    protected String nodeClientUsername() {
        return TEST_USER_NAME;
    }

    protected SecureString nodeClientPassword() {
        return new SecureString(TEST_PASSWORD.toCharArray());
    }

    public static void addSSLSettingsForNodePEMFiles(Settings.Builder builder, String prefix, boolean hostnameVerificationEnabled) {
        addSSLSettingsForPEMFiles(builder, prefix,
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem", "testnode",
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
            Arrays.asList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-client-profile.crt",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/active-directory-ca.crt",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/openldap.crt",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_ec.crt"),
            hostnameVerificationEnabled);
    }

    private void addNodeSSLSettings(Settings.Builder builder) {
        if (sslEnabled) {
            builder.put("xpack.security.transport.ssl.enabled", true);
            if (usePEM) {
                addSSLSettingsForNodePEMFiles(builder, "xpack.security.transport.", hostnameVerificationEnabled);
            } else {
                addSSLSettingsForStore(builder, "xpack.security.transport.",
                    "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks", "testnode",
                    hostnameVerificationEnabled);
            }
        } else if (randomBoolean()) {
            builder.put(XPackSettings.TRANSPORT_SSL_ENABLED.getKey(), false);
        }
    }

    public void addClientSSLSettings(Settings.Builder builder, String prefix) {
        builder.put("xpack.security.transport.ssl.enabled", sslEnabled);
        if (usePEM) {
            addSSLSettingsForPEMFiles(builder, prefix,
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.pem", "testclient",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt",
                Arrays.asList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
                    "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_ec.crt",
                    "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt"),
                hostnameVerificationEnabled);
        } else {
            addSSLSettingsForStore(builder, prefix, "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.jks",
                "testclient", hostnameVerificationEnabled);
        }
    }

    /**
     * Returns the configuration settings given the location of a certificate and its password
     *
     * @param resourcePathToStore the location of the keystore or truststore
     * @param password the password
     */
    public static void addSSLSettingsForStore(Settings.Builder builder, String resourcePathToStore, String password, String prefix) {
        addSSLSettingsForStore(builder, prefix, resourcePathToStore, password, true);
    }

    private static void addSSLSettingsForStore(Settings.Builder builder, String prefix, String resourcePathToStore, String password,
                                               boolean hostnameVerificationEnabled) {
        Path store = resolveResourcePath(resourcePathToStore);
        builder.put(prefix + "ssl.verification_mode", hostnameVerificationEnabled ? "full" : "certificate");
        builder.put(prefix + "ssl.keystore.path", store);
        final String finalPrefix = prefix;
        addSecureSettings(builder, secureSettings ->
            secureSettings.setString(finalPrefix + "ssl.keystore.secure_password", password));

        if (randomBoolean()) {
            builder.put(prefix + "ssl.truststore.path", store);
            addSecureSettings(builder, secureSettings ->
                secureSettings.setString(finalPrefix + "ssl.truststore.secure_password", password));
        }
    }

    /**
     * Returns the SSL related configuration settings given the location of a key and certificate and the location
     * of the PEM certificates to be trusted
     *
     * @param keyPath             The path to the Private key to be used for SSL
     * @param password            The password with which the private key is protected
     * @param certificatePath     The path to the PEM formatted Certificate encapsulating the public key that corresponds
     *                            to the Private Key specified in {@code keyPath}. Will be presented to incoming
     *                            SSL connections.
     * @param trustedCertificates A list of PEM formatted certificates that will be trusted.
     */
    public static void addSSLSettingsForPEMFiles(Settings.Builder builder, String keyPath, String password,
                                                 String certificatePath, List<String> trustedCertificates) {
        addSSLSettingsForPEMFiles(builder, "", keyPath, password, certificatePath, trustedCertificates, true);
    }

    /**
     * Returns the SSL related configuration settings given the location of a key and certificate and the location
     * of the PEM certificates to be trusted
     *
     * @param keyPath             The path to the Private key to be used for SSL
     * @param password            The password with which the private key is protected
     * @param certificatePath     The path to the PEM formatted Certificate encapsulating the public key that corresponds
     *                            to the Private Key specified in {@code keyPath}. Will be presented to incoming
     *                            SSL connections.
     * @param prefix              The settings prefix to use before ssl setting names
     * @param trustedCertificates A list of PEM formatted certificates that will be trusted.
     */
    public static void addSSLSettingsForPEMFiles(Settings.Builder builder, String keyPath, String password,
                                                 String certificatePath, String prefix, List<String> trustedCertificates) {
        addSSLSettingsForPEMFiles(builder, prefix, keyPath, password, certificatePath, trustedCertificates, true);
    }

    private static void addSSLSettingsForPEMFiles(Settings.Builder builder, String prefix, String keyPath, String password,
                                                  String certificatePath, List<String> trustedCertificates,
                                                  boolean hostnameVerificationEnabled) {
        if (prefix.equals("")) {
            prefix = "xpack.security.transport.";
        }
        builder.put(prefix + "ssl.verification_mode", hostnameVerificationEnabled ? "full" : "certificate");
        builder.put(prefix + "ssl.key", resolveResourcePath(keyPath))
                .put(prefix + "ssl.certificate", resolveResourcePath(certificatePath));
        final String finalPrefix = prefix;
        addSecureSettings(builder, secureSettings ->
            secureSettings.setString(finalPrefix + "ssl.secure_key_passphrase", password));

        if (trustedCertificates.isEmpty() == false) {
            builder.put(prefix + "ssl.certificate_authorities",
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
