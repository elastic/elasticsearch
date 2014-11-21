/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.SysGlobals;
import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.TestGroup;
import com.google.common.base.Charsets;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.os.OsUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.key.InternalKeyService;
import org.elasticsearch.shield.plugin.ShieldPlugin;
import org.elasticsearch.shield.transport.netty.NettySecuredTransport;
import org.elasticsearch.test.rest.ElasticsearchRestTests;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.transport.TransportModule;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class ShieldRestTests extends ElasticsearchRestTests {

    public static final int CHILD_JVM_ID = Integer.parseInt(System.getProperty(SysGlobals.CHILDVM_SYSPROP_JVM_ID, "0"));
    public static final int BASE_PORT = 33000 + CHILD_JVM_ID * 100;
    public static final String BASE_PORT_RANGE = BASE_PORT + "-" + (BASE_PORT+10) ;

    protected static final boolean ENABLE_TRANSPORT_SSL = true;
    protected static final boolean SHIELD_AUDIT_ENABLED = false;

    protected static final String DEFAULT_USER_NAME = "test_user";
    protected static final String DEFAULT_PASSWORD = "changeme";
    protected static final String DEFAULT_ROLE = "user";

    public static final String CONFIG_IPFILTER_ALLOW_ALL = "allow: all\n";
    public static final String CONFIG_STANDARD_USER = DEFAULT_USER_NAME + ":{plain}" + DEFAULT_PASSWORD + "\n";
    public static final String CONFIG_STANDARD_USER_ROLES = DEFAULT_ROLE + ":" + DEFAULT_USER_NAME+ "\n";
    public static final String CONFIG_ROLE_ALLOW_ALL = DEFAULT_ROLE + ":\n" +
            "  cluster: ALL\n" +
            "  indices:\n" +
            "    '*': ALL\n";

    static {

        TestGroup testGroup = Rest.class.getAnnotation(TestGroup.class);
        String sysProperty = TestGroup.Utilities.getSysProperty(Rest.class);
        boolean enabled;
        try {
            enabled = RandomizedTest.systemPropertyAsBoolean(sysProperty, testGroup.enabled());
        } catch (IllegalArgumentException e) {
            // Ignore malformed system property, disable the group if malformed though.
            enabled = false;
        }

        //customize the global cluster only if rest tests are enabled
        //not perfect but good enough as REST tests are supposed to be run only separately on CI
        if (enabled) {
            final byte[] key;
            try {
                key = InternalKeyService.generateKey();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            InternalTestCluster.DEFAULT_SETTINGS_SOURCE = new SettingsSource() {

                @Override
                public Settings node(int nodeOrdinal) {
                    File store;
                    try {
                        store = new File(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks").toURI());
                        assertThat(store.exists(), is(true));
                    } catch (Exception e) {
                        throw new ElasticsearchException("Error reading test node cert", e);
                    }
                    String password = "testnode";

                    File folder = createFolder();

                    String keyFile = writeFile(folder, "system_key", key);

                    ImmutableSettings.Builder builder = ImmutableSettings.builder()
                            .put(InternalKeyService.FILE_SETTING, keyFile)
                            .put("request.headers.Authorization", basicAuthHeaderValue(DEFAULT_USER_NAME, SecuredStringTests.build(DEFAULT_PASSWORD)))
                            .put("discovery.zen.ping.multicast.enabled", false)
                            .put("discovery.type", "zen")
                            .put("node.mode", "network")
                            .put("plugin.types", ShieldPlugin.class.getName())
                            .put("shield.authc.esusers.files.users", createFile(folder, "users", CONFIG_STANDARD_USER))
                            .put("shield.authc.esusers.files.users_roles", createFile(folder, "users_roles", CONFIG_STANDARD_USER_ROLES))
                            .put("shield.authz.store.files.roles", createFile(folder, "roles.yml", CONFIG_ROLE_ALLOW_ALL))
                            .put("shield.transport.n2n.ip_filter.file", createFile(folder, "ip_filter.yml", CONFIG_IPFILTER_ALLOW_ALL))
                            .put("shield.transport.ssl", ENABLE_TRANSPORT_SSL)
                            .put("shield.ssl.keystore.path", store.getPath())
                            .put("shield.ssl.keystore.password", password)
                            .put("shield.ssl.truststore.path", store.getPath())
                            .put("shield.ssl.truststore.password", password)
                            .put("shield.http.ssl", false)
                            .put("transport.tcp.port", BASE_PORT_RANGE)
                            .putArray("discovery.zen.ping.unicast.hosts", "127.0.0.1:" + BASE_PORT, "127.0.0.1:" + (BASE_PORT + 1), "127.0.0.1:" + (BASE_PORT + 2), "127.0.0.1:" + (BASE_PORT + 3))
                            .put("shield.audit.enabled", SHIELD_AUDIT_ENABLED);

                    builder.put("network.host", "127.0.0.1");
                    if (OsUtils.MAC) {
                        builder.put("network.host", randomBoolean() ? "127.0.0.1" : "::1");
                    }

                    return builder.build();
                }

                @Override
                public Settings transportClient() {
                    File store;
                    String password = "testclient";
                    try {
                        store = new File(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient.jks").toURI());
                        assertThat(store.exists(), is(true));
                    } catch (Exception e) {
                        throw new ElasticsearchException("Error reading test client cert", e);
                    }

                    File folder = createFolder();

                    return ImmutableSettings.builder()
                            .put("request.headers.Authorization", basicAuthHeaderValue(DEFAULT_USER_NAME, SecuredStringTests.build(DEFAULT_PASSWORD)))
                            .put(TransportModule.TRANSPORT_TYPE_KEY, NettySecuredTransport.class.getName())
                            .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false)
                            .put("node.mode", "network")
                            .put("shield.transport.n2n.ip_filter.file", createFile(folder, "ip_filter.yml", CONFIG_IPFILTER_ALLOW_ALL))
                            .put("shield.transport.ssl", ENABLE_TRANSPORT_SSL)
                            .put("shield.ssl.keystore.path", store.getPath())
                            .put("shield.ssl.keystore.password", password)
                            .put("shield.ssl.truststore.path", store.getPath())
                            .put("shield.ssl.truststore.password", password)
                            .put("cluster.name", internalCluster().getClusterName())
                            .build();
                }
            };
        }
    }

    @ClassRule
    public static TemporaryFolder tmpFolder = new TemporaryFolder();

    @AfterClass
    public static void cleanup() {
        tmpFolder = null;
    }

    public ShieldRestTests(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected Settings restClientSettings() {
        return ImmutableSettings.builder()
                .put("request.headers.Authorization", basicAuthHeaderValue(DEFAULT_USER_NAME, SecuredStringTests.build(DEFAULT_PASSWORD))).build();
    }

    /* static helper methods for the global test class */
    static File createFolder() {
        try {
            return tmpFolder.newFolder();
        } catch (IOException ioe) {
            fail("could not create temporary folder");
            return null;
        }
    }

    static String createFile(File folder, String name, String content) {
        return writeFile(folder, name, content.getBytes(Charsets.UTF_8));
    }

    static String writeFile(File folder, String name, byte[] content) {
        Path file = folder.toPath().resolve(name);
        try {
            Streams.copy(content, file.toFile());
        } catch (IOException e) {
            throw new ElasticsearchException("Error writing file in test", e);
        }
        return file.toFile().getAbsolutePath();
    }
}
