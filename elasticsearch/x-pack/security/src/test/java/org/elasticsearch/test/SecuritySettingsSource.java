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
import org.elasticsearch.transport.NettyPlugin;
import org.elasticsearch.xpack.monitoring.Monitoring;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealm;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.audit.AuditTrailModule;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;
import org.elasticsearch.xpack.security.transport.netty.SecurityNettyHttpServerTransport;
import org.elasticsearch.xpack.security.transport.netty.SecurityNettyTransport;
import org.elasticsearch.test.discovery.ClusterDiscoveryConfiguration;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.XPackPlugin;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.security.test.SecurityTestUtils.writeFile;

/**
 * {@link org.elasticsearch.test.NodeConfigurationSource} subclass that allows to set all needed settings for x-pack security.
 * Unicast discovery is configured through {@link org.elasticsearch.test.discovery.ClusterDiscoveryConfiguration.UnicastZen},
 * also x-pack is installed with all the needed configuration and files.
 * To avoid conflicts, every cluster should have its own instance of this class as some configuration files need to be created.
 */
public class SecuritySettingsSource extends ClusterDiscoveryConfiguration.UnicastZen {

    public static final Settings DEFAULT_SETTINGS = Settings.builder()
            .put("node.mode", "network")
            .build();

    public static final String DEFAULT_USER_NAME = "test_user";
    public static final String DEFAULT_PASSWORD = "changeme";
    public static final String DEFAULT_PASSWORD_HASHED = new String(Hasher.BCRYPT.hash(new SecuredString(DEFAULT_PASSWORD.toCharArray())));
    public static final String DEFAULT_ROLE = "user";

    public static final String DEFAULT_TRANSPORT_CLIENT_ROLE = "trans_client_user";
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
                    "      privileges: [ ALL ]\n" +
            DEFAULT_TRANSPORT_CLIENT_ROLE + ":\n" +
                    "  cluster:\n" +
                    "    - transport_client";

    private final Path parentFolder;
    private final String subfolderPrefix;
    private final byte[] systemKey;
    private final boolean sslTransportEnabled;
    private final boolean hostnameVerificationEnabled;
    private final boolean hostnameVerificationResolveNameEnabled;

    /**
     * Creates a new {@link org.elasticsearch.test.NodeConfigurationSource} for the security configuration.
     *
     * @param numOfNodes the number of nodes for proper unicast configuration (can be more than actually available)
     * @param sslTransportEnabled whether ssl should be enabled on the transport layer or not
     * @param parentFolder the parent folder that will contain all of the configuration files that need to be created
     * @param scope the scope of the test that is requiring an instance of SecuritySettingsSource
     */
    public SecuritySettingsSource(int numOfNodes, boolean sslTransportEnabled, Path parentFolder, Scope scope) {
        this(numOfNodes, sslTransportEnabled, generateKey(), parentFolder, scope);
    }

    /**
     * Creates a new {@link org.elasticsearch.test.NodeConfigurationSource} for the security configuration.
     *
     * @param numOfNodes the number of nodes for proper unicast configuration (can be more than actually available)
     * @param sslTransportEnabled whether ssl should be enabled on the transport layer or not
     * @param systemKey the system key that all of the nodes will use to sign messages
     * @param parentFolder the parent folder that will contain all of the configuration files that need to be created
     * @param scope the scope of the test that is requiring an instance of SecuritySettingsSource
     */
    public SecuritySettingsSource(int numOfNodes, boolean sslTransportEnabled, byte[] systemKey, Path parentFolder, Scope scope) {
        super(numOfNodes, DEFAULT_SETTINGS);
        this.systemKey = systemKey;
        this.parentFolder = parentFolder;
        this.subfolderPrefix = scope.name();
        this.sslTransportEnabled = sslTransportEnabled;
        this.hostnameVerificationEnabled = randomBoolean();
        this.hostnameVerificationResolveNameEnabled = randomBoolean();
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        Path folder = SecurityTestUtils.createFolder(parentFolder, subfolderPrefix + "-" + nodeOrdinal);
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal))

                //TODO: for now isolate security tests from watcher & monitoring (randomize this later)
                .put(XPackPlugin.featureEnabledSetting(Watcher.NAME), false)
                .put(XPackPlugin.featureEnabledSetting(Monitoring.NAME), false)
                .put(AuditTrailModule.ENABLED_SETTING.getKey(), randomBoolean())
                .put(LoggingAuditTrail.HOST_ADDRESS_SETTING.getKey(), randomBoolean())
                .put(LoggingAuditTrail.HOST_NAME_SETTING.getKey(), randomBoolean())
                .put(LoggingAuditTrail.NODE_NAME_SETTING.getKey(), randomBoolean())
                .put(CryptoService.FILE_SETTING.getKey(), writeFile(folder, "system_key", systemKey))
                .put("xpack.security.authc.realms.file.type", FileRealm.TYPE)
                .put("xpack.security.authc.realms.file.order", 0)
                .put("xpack.security.authc.realms.file.files.users", writeFile(folder, "users", configUsers()))
                .put("xpack.security.authc.realms.file.files.users_roles", writeFile(folder, "users_roles", configUsersRoles()))
                .put("xpack.security.authc.realms.index.type", NativeRealm.TYPE)
                .put("xpack.security.authc.realms.index.order", "1")
                .put(FileRolesStore.ROLES_FILE_SETTING.getKey(), writeFile(folder, "roles.yml", configRoles()))
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
        return Arrays.asList(xpackPluginClass(), NettyPlugin.class);
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
                    Collections.singletonList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"),
                    Arrays.asList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-client-profile.crt",
                            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/active-directory-ca.crt",
                            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt",
                            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/openldap.crt",
                            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"),
                    sslTransportEnabled, hostnameVerificationEnabled, hostnameVerificationResolveNameEnabled, false);
        }
        return getSSLSettingsForStore("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks", "testnode",
                sslTransportEnabled, hostnameVerificationEnabled, hostnameVerificationResolveNameEnabled, false);
    }

    public Settings getClientSSLSettings() {
        if (randomBoolean()) {
            return getSSLSettingsForPEMFiles("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.pem", "testclient",
                    Collections.singletonList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt"),
                    Arrays.asList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
                            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt"),
                    sslTransportEnabled, hostnameVerificationEnabled, hostnameVerificationResolveNameEnabled, true);
        }

        return getSSLSettingsForStore("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.jks", "testclient",
                sslTransportEnabled, hostnameVerificationEnabled, hostnameVerificationResolveNameEnabled, true);
    }

    /**
     * Returns the configuration settings given the location of a certificate and its password
     *
     * @param resourcePathToStore the location of the keystore or truststore
     * @param password the password
     * @return the configuration settings
     */
    public static Settings getSSLSettingsForStore(String resourcePathToStore, String password) {
        return getSSLSettingsForStore(resourcePathToStore, password, true, true, true, true);
    }

    private static Settings getSSLSettingsForStore(String resourcePathToStore, String password, boolean sslTransportEnabled,
                                                   boolean hostnameVerificationEnabled, boolean hostnameVerificationResolveNameEnabled,
                                                   boolean transportClient) {
        Path store = resolveResourcePath(resourcePathToStore);

        final String sslEnabledSetting =
                randomFrom(SecurityNettyTransport.SSL_SETTING.getKey(), SecurityNettyTransport.DEPRECATED_SSL_SETTING.getKey());
        Settings.Builder builder = Settings.builder().put(sslEnabledSetting, sslTransportEnabled);

        if (transportClient == false) {
            builder.put(SecurityNettyHttpServerTransport.SSL_SETTING.getKey(), false);
        }

        if (sslTransportEnabled) {
            builder.put("xpack.security.ssl.keystore.path", store)
                    .put("xpack.security.ssl.keystore.password", password)
                    .put(SecurityNettyTransport.HOSTNAME_VERIFICATION_SETTING.getKey(), hostnameVerificationEnabled)
                    .put(SecurityNettyTransport.HOSTNAME_VERIFICATION_RESOLVE_NAME_SETTING.getKey(),
                            hostnameVerificationResolveNameEnabled);
        }

        if (sslTransportEnabled && randomBoolean()) {
            builder.put("xpack.security.ssl.truststore.path", store)
                    .put("xpack.security.ssl.truststore.password", password);
        }
        return builder.build();
    }

    private static Settings getSSLSettingsForPEMFiles(String keyPath, String password, List<String> certificateFiles,
                                            List<String> trustedCertificates, boolean sslTransportEnabled,
                                            boolean hostnameVerificationEnabled, boolean hostnameVerificationResolveNameEnabled,
                                            boolean transportClient) {
        Settings.Builder builder = Settings.builder();
        final String sslEnabledSetting =
                randomFrom(SecurityNettyTransport.SSL_SETTING.getKey(), SecurityNettyTransport.DEPRECATED_SSL_SETTING.getKey());
        builder.put(sslEnabledSetting, sslTransportEnabled);

        if (transportClient == false) {
            builder.put(SecurityNettyHttpServerTransport.SSL_SETTING.getKey(), false);
        }

        if (sslTransportEnabled) {
            builder.put("xpack.security.ssl.key.path", resolveResourcePath(keyPath))
                    .put("xpack.security.ssl.key.password", password)
                    .put("xpack.security.ssl.cert", Strings.arrayToCommaDelimitedString(resolvePathsToString(certificateFiles)))
                    .put(randomFrom(SecurityNettyTransport.HOSTNAME_VERIFICATION_SETTING.getKey(),
                            SecurityNettyTransport.DEPRECATED_HOSTNAME_VERIFICATION_SETTING.getKey()), hostnameVerificationEnabled)
                    .put(SecurityNettyTransport.HOSTNAME_VERIFICATION_RESOLVE_NAME_SETTING.getKey(),
                            hostnameVerificationResolveNameEnabled);

            if (trustedCertificates.isEmpty() == false) {
                builder.put("xpack.security.ssl.ca", Strings.arrayToCommaDelimitedString(resolvePathsToString(trustedCertificates)));
            }
        }
        return builder.build();
    }

    static String[] resolvePathsToString(List<String> resourcePaths) {
        List<String> resolvedPaths = new ArrayList<>(resourcePaths.size());
        for (String resource : resourcePaths) {
            resolvedPaths.add(resolveResourcePath(resource).toString());
        }
        return resolvedPaths.toArray(new String[resolvedPaths.size()]);
    }

    static Path resolveResourcePath(String resourcePathToStore) {
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
