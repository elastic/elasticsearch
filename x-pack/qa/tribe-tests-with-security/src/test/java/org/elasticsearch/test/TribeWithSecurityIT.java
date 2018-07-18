/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;


import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.action.role.GetRolesResponse;
import org.elasticsearch.xpack.core.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.INTERNAL_SECURITY_INDEX;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.arrayContaining;

public class TribeWithSecurityIT extends SecurityIntegTestCase {

    private static TestCluster cluster2;
    private static TestCluster tribeNode;
    private static Hasher hasher;

    @BeforeClass
    public static void init() {
        hasher = Hasher.resolve("bcrypt");
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (cluster2 == null) {
            cluster2 = buildExternalCluster(System.getProperty("tests.cluster2"));
        }
        if (tribeNode == null) {
            tribeNode = buildExternalCluster(System.getProperty("tests.tribe"));
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("xpack.security.authc.password_hashing.algorithm", hasher.name())
                .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        return builder.build();
    }

    /**
     * TODO: this entire class should be removed. SecurityIntegTestCase is meant for tests, but we run against real xpack
     */
    @Override
    public void doAssertXPackIsInstalled() {
    }


    @AfterClass
    public static void tearDownExternalClusters() throws IOException {
        if (cluster2 != null) {
            try {
                cluster2.close();
            } finally {
                cluster2 = null;
            }
        }
        if (tribeNode != null) {
            try {
                tribeNode.close();
            } finally {
                tribeNode = null;
            }
        }
    }

    @After
    public void removeSecurityIndex() {
        if (client().admin().indices().prepareExists(INTERNAL_SECURITY_INDEX).get().isExists()) {
            client().admin().indices().prepareDelete(INTERNAL_SECURITY_INDEX).get();
        }
        if (cluster2.client().admin().indices().prepareExists(INTERNAL_SECURITY_INDEX).get().isExists()) {
            cluster2.client().admin().indices().prepareDelete(INTERNAL_SECURITY_INDEX).get();
        }
        securityClient(client()).prepareClearRealmCache().get();
        securityClient(cluster2.client()).prepareClearRealmCache().get();
    }

    @Override
    protected Settings externalClusterClientSettings() {
        Settings.Builder builder = Settings.builder().put(super.externalClusterClientSettings());
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, SecurityField.NAME4);
        builder.put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        return builder.build();
    }

    private ExternalTestCluster buildExternalCluster(String clusterAddresses) throws IOException {
        String[] stringAddresses = clusterAddresses.split(",");
        TransportAddress[] transportAddresses = new TransportAddress[stringAddresses.length];
        int i = 0;
        for (String stringAddress : stringAddresses) {
            URL url = new URL("http://" + stringAddress);
            InetAddress inetAddress = InetAddress.getByName(url.getHost());
            transportAddresses[i++] = new TransportAddress(new InetSocketAddress(inetAddress, url.getPort()));
        }
        return new ExternalTestCluster(createTempDir(), externalClusterClientSettings(), transportClientPlugins(), transportAddresses);
    }

    public void testThatTribeCanAuthenticateElasticUser() throws Exception {
        ClusterHealthResponse response = tribeNode.client().filterWithHeader(Collections.singletonMap("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue("elastic", BOOTSTRAP_PASSWORD)))
                .admin().cluster().prepareHealth().get();
        assertNoTimeout(response);
    }

    public void testThatTribeCanAuthenticateElasticUserWithChangedPassword() throws Exception {
        assertSecurityIndexActive();
        securityClient(client()).prepareChangePassword("elastic", "password".toCharArray(), hasher).get();

        assertTribeNodeHasAllIndices();
        ClusterHealthResponse response = tribeNode.client().filterWithHeader(Collections.singletonMap("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue("elastic", new SecureString("password".toCharArray()))))
                .admin().cluster().prepareHealth().get();
        assertNoTimeout(response);
    }

    public void testThatTribeClustersHaveDifferentPasswords() throws Exception {
        assertSecurityIndexActive();
        assertSecurityIndexActive(cluster2);
        securityClient().prepareChangePassword("elastic", "password".toCharArray(), hasher).get();
        securityClient(cluster2.client()).
            prepareChangePassword("elastic", "password2".toCharArray(), hasher).get();

        assertTribeNodeHasAllIndices();
        ClusterHealthResponse response = tribeNode.client().filterWithHeader(Collections.singletonMap("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue("elastic", new SecureString("password".toCharArray()))))
                .admin().cluster().prepareHealth().get();
        assertNoTimeout(response);
    }

    public void testUserModificationUsingTribeNodeAreDisabled() throws Exception {
        SecurityClient securityClient = securityClient(tribeNode.client());
        NotSerializableExceptionWrapper e = expectThrows(NotSerializableExceptionWrapper.class,
                () -> securityClient.preparePutUser("joe", "password".toCharArray(), hasher).get());
        assertThat(e.getMessage(), containsString("users may not be created or modified using a tribe node"));
        e = expectThrows(NotSerializableExceptionWrapper.class, () -> securityClient.prepareSetEnabled("elastic", randomBoolean()).get());
        assertThat(e.getMessage(), containsString("users may not be created or modified using a tribe node"));
        e = expectThrows(NotSerializableExceptionWrapper.class,
                () -> securityClient.prepareChangePassword("elastic", "password".toCharArray(), hasher).get());
        assertThat(e.getMessage(), containsString("users may not be created or modified using a tribe node"));
        e = expectThrows(NotSerializableExceptionWrapper.class, () -> securityClient.prepareDeleteUser("joe").get());
        assertThat(e.getMessage(), containsString("users may not be deleted using a tribe node"));
    }

    // note tribe node has tribe.on_conflict set to prefer cluster_1
    public void testRetrieveRolesOnPreferredClusterOnly() throws Exception {
        final int randomRoles = scaledRandomIntBetween(3, 8);
        List<String> shouldBeSuccessfulRoles = new ArrayList<>();

        assertSecurityIndexActive();
        for (int i = 0; i < randomRoles; i++) {
            final String rolename = "preferredClusterRole" + i;
            PutRoleResponse response = securityClient(client()).preparePutRole(rolename).cluster("monitor").get();
            assertTrue(response.isCreated());
            shouldBeSuccessfulRoles.add(rolename);
        }

        assertTribeNodeHasAllIndices();
        SecurityClient securityClient = securityClient(tribeNode.client());
        for (String rolename : shouldBeSuccessfulRoles) {
            GetRolesResponse response = securityClient.prepareGetRoles(rolename).get();
            assertTrue(response.hasRoles());
            assertEquals(1, response.roles().length);
            assertThat(response.roles()[0].getClusterPrivileges(), arrayContaining("monitor"));
        }
    }

    private void assertTribeNodeHasAllIndices() throws Exception {
        assertBusy(() -> {
            Set<String> indices = new HashSet<>();
            client().admin().cluster().prepareState().setMetaData(true).get()
                    .getState().getMetaData().getIndices().keysIt().forEachRemaining(indices::add);
            cluster2.client().admin().cluster().prepareState().setMetaData(true).get()
                    .getState().getMetaData().getIndices().keysIt().forEachRemaining(indices::add);

            ClusterState state = tribeNode.client().admin().cluster().prepareState().setRoutingTable(true)
                    .setMetaData(true).get().getState();
            StringBuilder sb = new StringBuilder();
            for (String index : indices) {
                if (sb.length() == 0) {
                    sb.append("[");
                    sb.append(index);
                } else {
                    sb.append(",");
                    sb.append(index);
                }
            }
            sb.append("]");
            Set<String> tribeIndices = new HashSet<>();
            for (ObjectCursor<IndexMetaData> cursor : state.getMetaData().getIndices().values()) {
                tribeIndices.add(cursor.value.getIndex().getName());
            }

            assertThat("cluster indices [" + indices + "] tribe indices [" + tribeIndices + "]",
                    state.getMetaData().getIndices().size(), equalTo(indices.size()));
            for (String index : indices) {
                assertTrue(state.getMetaData().hasIndex(index));
                assertTrue(state.getRoutingTable().hasIndex(index));
                assertTrue(state.getRoutingTable().index(index).allPrimaryShardsActive());
            }
        });
    }
}
