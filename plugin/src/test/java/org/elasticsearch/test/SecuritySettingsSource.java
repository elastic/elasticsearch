/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.discovery.ClusterDiscoveryConfiguration;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealm;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.security.test.SecurityTestUtils.writeFile;

/**
 * {@link org.elasticsearch.test.NodeConfigurationSource} subclass that allows to set all needed settings for x-pack security.
 * Unicast discovery is configured through {@link org.elasticsearch.test.discovery.ClusterDiscoveryConfiguration.UnicastZen},
 * also x-pack is installed with all the needed configuration and files.
 * To avoid conflicts, every cluster should have its own instance of this class as some configuration files need to be created.
 */
public class SecuritySettingsSource extends ClusterDiscoveryConfiguration.UnicastZen {

    public static final Settings DEFAULT_SETTINGS = Settings.EMPTY;

    public static final String DEFAULT_USER_NAME = "test_user";
    public static final String DEFAULT_PASSWORD = "changeme";
    public static final String DEFAULT_PASSWORD_HASHED = new String(Hasher.BCRYPT.hash(new SecuredString(DEFAULT_PASSWORD.toCharArray())));
    public static final String DEFAULT_ROLE = "user";

    public static final String DEFAULT_TRANSPORT_CLIENT_ROLE = "transport_client";
    public static final String DEFAULT_TRANSPORT_CLIENT_USER_NAME = "test_trans_client_user";

    public static final String CONFIG_STANDARD_USER =
            DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD_HASHED + "\n" +
            DEFAULT_TRANSPORT_CLIENT_USER_NAME + ":" + DEFAULT_PASSWORD_HASHED + "\n";

    public static final String CONFIG_STANDARD_USER_ROLES =
            DEFAULT_ROLE + ":" + DEFAULT_USER_NAME + "," + DEFAULT_TRANSPORT_CLIENT_USER_NAME + "\n" +
            DEFAULT_TRANSPORT_CLIENT_ROLE + ":" + DEFAULT_TRANSPORT_CLIENT_USER_NAME+ "\n";

    public static final String CONFIG_ROLE_ALLOW_ALL =
            DEFAULT_ROLE + ":\n" +
                    "  cluster: [ ALL ]\n" +
                    "  indices:\n" +
                    "    - names: '*'\n" +
                    "      privileges: [ ALL ]\n";

    private final Path parentFolder;
    private final String subfolderPrefix;
    private final byte[] systemKey;
    private final boolean useGeneratedSSLConfig;
    private final boolean hostnameVerificationEnabled;

    /**
     * Creates a new {@link org.elasticsearch.test.NodeConfigurationSource} for the security configuration.
     *
     * @param numOfNodes the number of nodes for proper unicast configuration (can be more than actually available)
     * @param useGeneratedSSLConfig whether ssl key/cert should be auto-generated
     * @param parentFolder the parent folder that will contain all of the configuration files that need to be created
     * @param scope the scope of the test that is requiring an instance of SecuritySettingsSource
     */
    public SecuritySettingsSource(int numOfNodes, boolean useGeneratedSSLConfig, Path parentFolder, Scope scope) {
        this(numOfNodes, useGeneratedSSLConfig, generateKey(), parentFolder, scope);
    }

    /**
     * Creates a new {@link org.elasticsearch.test.NodeConfigurationSource} for the security configuration.
     *
     * @param numOfNodes the number of nodes for proper unicast configuration (can be more than actually available)
     * @param useGeneratedSSLConfig whether ssl key/cert should be auto-generated
     * @param systemKey the system key that all of the nodes will use to sign messages
     * @param parentFolder the parent folder that will contain all of the configuration files that need to be created
     * @param scope the scope of the test that is requiring an instance of SecuritySettingsSource
     */
    public SecuritySettingsSource(int numOfNodes, boolean useGeneratedSSLConfig, byte[] systemKey, Path parentFolder, Scope scope) {
        super(numOfNodes, DEFAULT_SETTINGS);
        this.systemKey = systemKey;
        this.parentFolder = parentFolder;
        this.subfolderPrefix = scope.name();
        this.useGeneratedSSLConfig = useGeneratedSSLConfig;
        this.hostnameVerificationEnabled = randomBoolean();
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        Path home = SecurityTestUtils.createFolder(parentFolder, subfolderPrefix + "-" + nodeOrdinal);
        Path xpackConf = SecurityTestUtils.createFolder(home.resolve("config"), XPackPlugin.NAME);
        writeFile(xpackConf, "system_key", systemKey);
        writeFile(xpackConf, "users", configUsers());
        writeFile(xpackConf, "users_roles", configUsersRoles());
        writeFile(xpackConf, "roles.yml", configRoles());

        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(Environment.PATH_CONF_SETTING.getKey(), home.resolve("config"))
                //TODO: for now isolate security tests from watcher & monitoring (randomize this later)
                .put(XPackSettings.WATCHER_ENABLED.getKey(), false)
                .put(XPackSettings.MONITORING_ENABLED.getKey(), false)
                .put(XPackSettings.AUDIT_ENABLED.getKey(), randomBoolean())
                .put(LoggingAuditTrail.HOST_ADDRESS_SETTING.getKey(), randomBoolean())
                .put(LoggingAuditTrail.HOST_NAME_SETTING.getKey(), randomBoolean())
                .put(LoggingAuditTrail.NODE_NAME_SETTING.getKey(), randomBoolean())
                .put("xpack.security.authc.realms.file.type", FileRealm.TYPE)
                .put("xpack.security.authc.realms.file.order", 0)
                .put("xpack.security.authc.realms.index.type", NativeRealm.TYPE)
                .put("xpack.security.authc.realms.index.order", "1")
                .put(getNodeSSLSettings());
        return builder.build();
    }

    @Override
    public Settings transportClientSettings() {
        Settings.Builder builder = Settings.builder().put(super.transportClientSettings())
                .put(getClientSSLSettings());
        if (randomBoolean()) {
            builder.put(Security.USER_SETTING.getKey(),
                    transportClientUsername() + ":" + new String(transportClientPassword().internalChars()));
        } else {
            builder.put(ThreadContext.PREFIX + ".Authorization", basicAuthHeaderValue(transportClientUsername(),
                    transportClientPassword()));
        }
        return builder.build();
    }

    @Override
    public Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(xpackPluginClass(),
                Netty4Plugin.class);
    }

    @Override
    public Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
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
        return DEFAULT_USER_NAME;
    }

    protected SecuredString nodeClientPassword() {
        return new SecuredString(DEFAULT_PASSWORD.toCharArray());
    }

    protected String transportClientUsername() {
        return DEFAULT_TRANSPORT_CLIENT_USER_NAME;
    }

    protected SecuredString transportClientPassword() {
        return new SecuredString(DEFAULT_PASSWORD.toCharArray());
    }

    protected byte[] systemKey() {
        return systemKey;
    }

    protected Class<? extends XPackPlugin> xpackPluginClass() {
        return XPackPlugin.class;
    }

    private static byte[] generateKey() {
        try {
            return CryptoService.generateKey();
        } catch (Exception e) {
            throw new ElasticsearchException("exception while generating the system key", e);
        }
    }

    public Settings getNodeSSLSettings() {
        if (randomBoolean()) {
            return getSSLSettingsForPEMFiles("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem", "testnode",
                    "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
                    Arrays.asList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-client-profile.crt",
                            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/active-directory-ca.crt",
                            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt",
                            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/openldap.crt",
                            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"),
                    useGeneratedSSLConfig, hostnameVerificationEnabled, false);
        }
        return getSSLSettingsForStore("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks", "testnode",
                useGeneratedSSLConfig, hostnameVerificationEnabled, false);
    }

    public Settings getClientSSLSettings() {
        if (randomBoolean()) {
            return getSSLSettingsForPEMFiles("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.pem", "testclient",
                    "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt",
                    Arrays.asList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
                            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt"),
                    useGeneratedSSLConfig, hostnameVerificationEnabled, true);
        }

        return getSSLSettingsForStore("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.jks", "testclient",
                useGeneratedSSLConfig, hostnameVerificationEnabled, true);
    }

    /**
     * Returns the configuration settings given the location of a certificate and its password
     *
     * @param resourcePathToStore the location of the keystore or truststore
     * @param password the password
     * @return the configuration settings
     */
    public static Settings getSSLSettingsForStore(String resourcePathToStore, String password) {
        return getSSLSettingsForStore(resourcePathToStore, password, false, true, true);
    }

    private static Settings getSSLSettingsForStore(String resourcePathToStore, String password, boolean useGeneratedSSLConfig,
                                                   boolean hostnameVerificationEnabled, boolean transportClient) {
        Path store = resolveResourcePath(resourcePathToStore);

        Settings.Builder builder = Settings.builder();

        if (transportClient == false) {
            builder.put("xpack.security.http.ssl.enabled", false);
        }

        builder.put("xpack.ssl.verification_mode", hostnameVerificationEnabled ? "full" : "certificate");
        if (useGeneratedSSLConfig == false) {
            builder.put("xpack.ssl.keystore.path", store)
                    .put("xpack.ssl.keystore.password", password);
        }

        if (useGeneratedSSLConfig == false && randomBoolean()) {
            builder.put("xpack.ssl.truststore.path", store)
                    .put("xpack.ssl.truststore.password", password);
        }
        return builder.build();
    }

    private static Settings getSSLSettingsForPEMFiles(String keyPath, String password, String certificatePath,
                                            List<String> trustedCertificates, boolean useGeneratedSSLConfig,
                                            boolean hostnameVerificationEnabled, boolean transportClient) {
        Settings.Builder builder = Settings.builder();

        if (transportClient == false) {
            builder.put("xpack.security.http.ssl.enabled", false);
        }

        builder.put("xpack.ssl.verification_mode", hostnameVerificationEnabled ? "full" : "certificate");
        if (useGeneratedSSLConfig == false) {
            builder.put("xpack.ssl.key", resolveResourcePath(keyPath))
                    .put("xpack.ssl.key_passphrase", password)
                    .put("xpack.ssl.certificate", resolveResourcePath(certificatePath));

            if (trustedCertificates.isEmpty() == false) {
                builder.put("xpack.ssl.certificate_authorities",
                        Strings.arrayToCommaDelimitedString(resolvePathsToString(trustedCertificates)));
            }
        }
        return builder.build();
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
            Path path = PathUtils.get(SecuritySettingsSource.class.getResource(resourcePathToStore).toURI());
            if (Files.notExists(path)) {
                throw new ElasticsearchException("path does not exist: " + path);
            }
            return path;
        } catch (URISyntaxException e) {
            throw new ElasticsearchException("exception while reading the store", e);
        }
    }
}
