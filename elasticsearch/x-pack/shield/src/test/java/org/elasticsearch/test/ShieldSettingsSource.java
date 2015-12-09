/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.crypto.InternalCryptoService;
import org.elasticsearch.shield.test.ShieldTestUtils;
import org.elasticsearch.shield.transport.netty.ShieldNettyHttpServerTransport;
import org.elasticsearch.shield.transport.netty.ShieldNettyTransport;
import org.elasticsearch.test.discovery.ClusterDiscoveryConfiguration;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.shield.test.ShieldTestUtils.writeFile;

/**
 * {@link org.elasticsearch.test.NodeConfigurationSource} subclass that allows to set all needed settings for shield.
 * Unicast discovery is configured through {@link org.elasticsearch.test.discovery.ClusterDiscoveryConfiguration.UnicastZen},
 * also shield is installed with all the needed configuration and files.
 * To avoid conflicts, every cluster should have its own instance of this class as some configuration files need to be created.
 */
public class ShieldSettingsSource extends ClusterDiscoveryConfiguration.UnicastZen {

    public static final Settings DEFAULT_SETTINGS = settingsBuilder()
            .put("node.mode", "network")
            .put("plugins.load_classpath_plugins", false)
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
                    "  cluster: ALL\n" +
                    "  indices:\n" +
                    "    '*': ALL\n" +
            DEFAULT_TRANSPORT_CLIENT_ROLE + ":\n" +
                    "  cluster:\n" +
                    "    - cluster:monitor/nodes/info\n" +
                    "    - cluster:monitor/state";

    private final Path parentFolder;
    private final String subfolderPrefix;
    private final byte[] systemKey;
    private final boolean sslTransportEnabled;
    private final boolean hostnameVerificationEnabled;
    private final boolean hostnameVerificationResolveNameEnabled;

    /**
     * Creates a new {@link org.elasticsearch.test.NodeConfigurationSource} for the shield configuration.
     *
     * @param numOfNodes the number of nodes for proper unicast configuration (can be more than actually available)
     * @param sslTransportEnabled whether ssl should be enabled on the transport layer or not
     * @param parentFolder the parent folder that will contain all of the configuration files that need to be created
     * @param scope the scope of the test that is requiring an instance of ShieldSettingsSource
     */
    public ShieldSettingsSource(int numOfNodes, boolean sslTransportEnabled, Path parentFolder, ESIntegTestCase.Scope scope) {
        this(numOfNodes, sslTransportEnabled, generateKey(), parentFolder, scope);
    }

    /**
     * Creates a new {@link org.elasticsearch.test.NodeConfigurationSource} for the shield configuration.
     *
     * @param numOfNodes the number of nodes for proper unicast configuration (can be more than actually available)
     * @param sslTransportEnabled whether ssl should be enabled on the transport layer or not
     * @param systemKey the system key that all of the nodes will use to sign messages
     * @param parentFolder the parent folder that will contain all of the configuration files that need to be created
     * @param scope the scope of the test that is requiring an instance of ShieldSettingsSource
     */
    public ShieldSettingsSource(int numOfNodes, boolean sslTransportEnabled, byte[] systemKey, Path parentFolder, ESIntegTestCase.Scope scope) {
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
        Path folder = ShieldTestUtils.createFolder(parentFolder, subfolderPrefix + "-" + nodeOrdinal);
        Settings.Builder builder = settingsBuilder().put(super.nodeSettings(nodeOrdinal))

                //TODO: for now isolate shield tests from watcher & marvel (randomize this later)
                .put("watcher.enabled", false)
                .put("marvel.enabled", false)

                .put("shield.audit.enabled", randomBoolean())
                .put("shield.audit.logfile.prefix.emit_node_host_address", randomBoolean())
                .put("shield.audit.logfile.prefix.emit_node_host_name", randomBoolean())
                .put("shield.audit.logfile.prefix.emit_node_name", randomBoolean())
                .put(InternalCryptoService.FILE_SETTING, writeFile(folder, "system_key", systemKey))
                .put("shield.authc.realms.esusers.type", ESUsersRealm.TYPE)
                .put("shield.authc.realms.esusers.order", 0)
                .put("shield.authc.realms.esusers.files.users", writeFile(folder, "users", configUsers()))
                .put("shield.authc.realms.esusers.files.users_roles", writeFile(folder, "users_roles", configUsersRoles()))
                .put("shield.authz.store.files.roles", writeFile(folder, "roles.yml", configRoles()))
                // Test framework sometimes randomily selects the 'index' or 'none' cache and that makes the
                // validation in ShieldPlugin fail.
                .put(IndexModule.QUERY_CACHE_TYPE, ShieldPlugin.OPT_OUT_QUERY_CACHE)
                .put(getNodeSSLSettings());

        setUser(builder, nodeClientUsername(), nodeClientPassword());

        return builder.build();
    }

    @Override
    public Settings transportClientSettings() {
        Settings.Builder builder = settingsBuilder().put(super.transportClientSettings())
                .put(getClientSSLSettings());
        setUser(builder, transportClientUsername(), transportClientPassword());
        return builder.build();
    }

    @Override
    public Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(xpackPluginClass());
    }

    @Override
    public Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.<Class<? extends Plugin>>singletonList(xpackPluginClass());
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

    private void setUser(Settings.Builder builder, String username, SecuredString password) {
        if (randomBoolean()) {
            builder.put(Headers.PREFIX + "." + UsernamePasswordToken.BASIC_AUTH_HEADER, basicAuthHeaderValue(username, password));
        } else {
            builder.put("shield.user", username + ":" + new String(password.internalChars()));
        }
    }

    private static byte[] generateKey() {
        try {
            return InternalCryptoService.generateKey();
        } catch (Exception e) {
            throw new ElasticsearchException("exception while generating the system key", e);
        }
    }

    public Settings getNodeSSLSettings() {
        return getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks", "testnode", sslTransportEnabled, hostnameVerificationEnabled, hostnameVerificationResolveNameEnabled);
    }

    public Settings getClientSSLSettings() {
        return getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient.jks", "testclient", sslTransportEnabled, hostnameVerificationEnabled, hostnameVerificationResolveNameEnabled);
    }

    /**
     * Returns the configuration settings given the location of a certificate and its password
     *
     * @param resourcePathToStore the location of the keystore or truststore
     * @param password the password
     * @return the configuration settings
     */
    public static Settings getSSLSettingsForStore(String resourcePathToStore, String password) {
        return getSSLSettingsForStore(resourcePathToStore, password, true, true, true);
    }

    private static Settings getSSLSettingsForStore(String resourcePathToStore, String password, boolean sslTransportEnabled, boolean hostnameVerificationEnabled, boolean hostnameVerificationResolveNameEnabled) {
        Path store;
        try {
            store = PathUtils.get(ShieldSettingsSource.class.getResource(resourcePathToStore).toURI());
        } catch (URISyntaxException e) {
            throw new ElasticsearchException("exception while reading the store", e);
        }

        if (Files.notExists(store)) {
            throw new ElasticsearchException("store path doesn't exist");
        }

        Settings.Builder builder = settingsBuilder()
                .put("shield.transport.ssl", sslTransportEnabled)
                .put(ShieldNettyHttpServerTransport.HTTP_SSL_SETTING, false);

        if (sslTransportEnabled) {
            builder.put("shield.ssl.keystore.path", store)
                    .put("shield.ssl.keystore.password", password)
                    .put(ShieldNettyTransport.HOSTNAME_VERIFICATION_SETTING, hostnameVerificationEnabled)
                    .put(ShieldNettyTransport.HOSTNAME_VERIFICATION_RESOLVE_NAME_SETTING, hostnameVerificationResolveNameEnabled);
        }

        if (sslTransportEnabled && randomBoolean()) {
            builder.put("shield.ssl.truststore.path", store)
                    .put("shield.ssl.truststore.password", password);
        }
        return builder.build();
    }
}
