/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.test;

import com.google.common.base.Charsets;
import com.google.common.net.InetAddresses;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.os.OsUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.plugin.ShieldPlugin;
import org.elasticsearch.shield.transport.netty.NettySecuredTransport;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportModule;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;


@Ignore
@AbstractRandomizedTest.Integration
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public abstract class ShieldIntegrationTest extends ElasticsearchIntegrationTest {

    protected static final String DEFAULT_USER_NAME = "test_user";
    protected static final String DEFAULT_PASSWORD = "changeme";
    protected static final String DEFAULT_ROLE = "user";

    protected static final String DEFAULT_TRANSPORT_CLIENT_ROLE = "trans_client_user";
    protected static final String DEFAULT_TRANSPORT_CLIENT_USER_NAME = "test_trans_client_user";

    public static final String CONFIG_IPFILTER_ALLOW_ALL = "allow: all\n";
    public static final String CONFIG_STANDARD_USER =
            DEFAULT_USER_NAME + ":{plain}" + DEFAULT_PASSWORD + "\n" +
            DEFAULT_TRANSPORT_CLIENT_USER_NAME + ":{plain}" + DEFAULT_PASSWORD + "\n";

    public static final String CONFIG_STANDARD_USER_ROLES =
            DEFAULT_ROLE + ":" + DEFAULT_USER_NAME + "," + DEFAULT_TRANSPORT_CLIENT_USER_NAME + "\n" +
            DEFAULT_TRANSPORT_CLIENT_ROLE + ":" + DEFAULT_TRANSPORT_CLIENT_USER_NAME+ "\n";

    public static final String CONFIG_ROLE_ALLOW_ALL =
            DEFAULT_ROLE + ":\n" +
            "  cluster: ALL\n" +
            "  indices:\n" +
            "    '*': ALL\n" +
            "transport_client:\n" +
            "  cluster:\n" +
            "    - cluster:monitor/nodes/info\n" +
            "    - cluster:monitor/state";

    @ClassRule
    public static TemporaryFolder tmpFolder = new TemporaryFolder();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        File folder = newFolder();

        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put("discovery.zen.ping.multicast.enabled", false)
                .put("discovery.type", "zen")
                .put("node.mode", "network")
                .put("plugin.types", ShieldPlugin.class.getName())
                .put("shield.authc.esusers.files.users", writeFile(folder, "users", configUsers()))
                .put("shield.authc.esusers.files.users_roles", writeFile(folder, "users_roles", configUsersRoles()))
                .put("shield.authz.store.files.roles", writeFile(folder, "roles.yml", configRole()))
                .put("shield.transport.n2n.ip_filter.file", writeFile(folder, "ip_filter.yml", CONFIG_IPFILTER_ALLOW_ALL))
                .put(getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks", "testnode"))
                .put("shield.audit.enabled", true)
                .put("plugins.load_classpath_plugins", false);

        setUser(builder);

        if (OsUtils.MAC) {
            builder.put("network.host", randomBoolean() ? "127.0.0.1" : "::1");
        }

        return builder.build();
    }

    protected String configRole() {
        return CONFIG_ROLE_ALLOW_ALL;
    }

    protected String configUsers() {
        return CONFIG_STANDARD_USER;
    }

    protected String configUsersRoles() {
        return CONFIG_STANDARD_USER_ROLES;
    }

    @Override
    protected Settings transportClientSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .put("shield.user", getClientUsername() + ":" + getClientPassword())
                .put(TransportModule.TRANSPORT_TYPE_KEY, NettySecuredTransport.class.getName())
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false)
                .put("plugin.types", ShieldPlugin.class.getName())
                .put("node.mode", "network")
                .put(getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient.jks", "testclient"));

        setUser(builder);

        return builder.build();
    }

    protected void setUser(ImmutableSettings.Builder settings) {
        if (randomBoolean()) {
            settings.put("request.headers.Authorization", basicAuthHeaderValue(getClientUsername(), getClientPassword()));
        } else {
            settings.put("shield.user", getClientUsername() + ":" + new String(getClientPassword().internalChars()));
        }
    }

    protected String writeFile(File folder, String name, String content) {
        return writeFile(folder, name, content.getBytes(Charsets.UTF_8));
    }

    protected String writeFile(File folder, String name, byte[] content) {
        Path file = folder.toPath().resolve(name);
        try {
            Streams.copy(content, file.toFile());
        } catch (IOException e) {
            throw new ElasticsearchException("Error writing file in test", e);
        }
        return file.toFile().getAbsolutePath();
    }

    protected String getUnicastHostAddress() {
        TransportAddress transportAddress = internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress();
        assertThat(transportAddress, instanceOf(InetSocketTransportAddress.class));
        InetSocketTransportAddress address = (InetSocketTransportAddress) transportAddress;
        return InetAddresses.toAddrString(address.address().getAddress()) + ":" + address.address().getPort();
    }

    protected String getClientUsername() {
        return DEFAULT_TRANSPORT_CLIENT_USER_NAME;
    }

    protected SecuredString getClientPassword() {
        return new SecuredString(DEFAULT_PASSWORD.toCharArray());
    }

    protected Settings getSSLSettingsForStore(String resourcePathToStore, String password) {
        File store;
        try {
            store = new File(getClass().getResource(resourcePathToStore).toURI());
            assertThat(store.exists(), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ImmutableSettings.Builder builder = settingsBuilder()
                .put("shield.transport.ssl", true)
                .put("shield.ssl.keystore.path", store.getPath())
                .put("shield.ssl.keystore.password", password)
                .put("shield.http.ssl", true);

        if (randomBoolean()) {
            builder.put("shield.ssl.truststore.path", store.getPath())
                   .put("shield.ssl.truststore.password", password);
        }

        return builder.build();
    }

    protected File newFolder() {
        try {
            return tmpFolder.newFolder();
        } catch (IOException ioe) {
            logger.error("could not create temporary folder", ioe);
            fail("could not create temporary folder");
            return null;
        }
    }

    protected void assertGreenClusterState(Client client) {
        ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().get();
        assertNoTimeout(clusterHealthResponse);
        assertThat(clusterHealthResponse.getStatus(), is(ClusterHealthStatus.GREEN));
    }
}
