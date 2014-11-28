/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.LifecycleScope;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExternalResource;

import java.io.File;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

/**
 * Base class to run tests against a cluster with shield installed.
 * The default {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope} is {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#GLOBAL},
 * meaning that all subclasses that don't specify a different scope will share the same cluster with shield installed.
 * @see org.elasticsearch.test.ShieldSettingsSource
 */
@Ignore
@AbstractRandomizedTest.Integration
public abstract class ShieldIntegrationTest extends ElasticsearchIntegrationTest {

    private static ShieldSettingsSource SHIELD_DEFAULT_SETTINGS;

    //UnicastZen requires the number of nodes in a cluster to generate the unicast configuration.
    //The number of nodes is randomized though, but we can predict what the maximum number of nodes will be
    //and configure them all in unicast.hosts
    private static int maxNumberOfNodes() {
        ClusterScope clusterScope = ShieldIntegrationTest.class.getAnnotation(ClusterScope.class);
        if (clusterScope == null || clusterScope.scope() == Scope.GLOBAL) {
            return InternalTestCluster.DEFAULT_MAX_NUM_DATA_NODES + InternalTestCluster.DEFAULT_MAX_NUM_CLIENT_NODES;
        } else {
            if (clusterScope.numClientNodes() < 0) {
                return clusterScope.maxNumDataNodes() + InternalTestCluster.DEFAULT_MAX_NUM_CLIENT_NODES;
            } else {
                return clusterScope.maxNumDataNodes() + clusterScope.numClientNodes();
            }
        }
    }

    private static ClusterScope getAnnotation(Class<?> clazz) {
        if (clazz == Object.class || clazz == ShieldIntegrationTest.class) {
            return null;
        }
        ClusterScope annotation = clazz.getAnnotation(ClusterScope.class);
        if (annotation != null) {
            return annotation;
        }
        return getAnnotation(clazz.getSuperclass());
    }

    private Scope getCurrentClusterScope() {
        return getCurrentClusterScope(this.getClass());
    }

    private static Scope getCurrentClusterScope(Class<?> clazz) {
        ClusterScope annotation = getAnnotation(clazz);
        return annotation == null ? Scope.GLOBAL : annotation.scope();
    }

    /**
     * Settings used when the {@link org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope} is set to
     * {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#SUITE} or {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#TEST}
     * so that some of the configuration parameters can be overridden through test instance methods, similarly
     * to how {@link #nodeSettings(int)} and {@link #transportClientSettings()} work.
     */
    private CustomShieldSettingsSource customShieldSettingsSource = null;

    @BeforeClass
    public static void initDefaultSettings() {
        if (SHIELD_DEFAULT_SETTINGS == null) {
            SHIELD_DEFAULT_SETTINGS = new ShieldSettingsSource(maxNumberOfNodes(), randomBoolean(), globalTempDir(), Scope.GLOBAL);
        }
    }

    @Rule
    //Rules are the only way to have something run before the before (final) method inherited from ElasticsearchIntegrationTest
    public ExternalResource externalResource = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            Scope currentClusterScope = getCurrentClusterScope();
            switch(currentClusterScope) {
                case GLOBAL:
                    if (InternalTestCluster.DEFAULT_SETTINGS_SOURCE == SettingsSource.EMPTY) {
                        InternalTestCluster.DEFAULT_SETTINGS_SOURCE = SHIELD_DEFAULT_SETTINGS;
                    }
                    break;
                case SUITE:
                    if (customShieldSettingsSource == null) {
                        customShieldSettingsSource = new CustomShieldSettingsSource(sslTransportEnabled(), newTempDir(LifecycleScope.SUITE), currentClusterScope);
                    }
                    break;
                case TEST:
                    customShieldSettingsSource = new CustomShieldSettingsSource(sslTransportEnabled(), newTempDir(LifecycleScope.TEST), currentClusterScope);
                    break;
            }
        }
    };

    @Before
    //before methods from the superclass are run before this, which means that the current cluster is ready to go
    public void assertShieldIsInstalled() {
        NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().clear().setPlugins(true).get();
        for (NodeInfo nodeInfo : nodeInfos) {
            assertThat(ShieldPlugin.NAME + " should be the only installed plugin, found the following ones: " + nodeInfo.getPlugins().getInfos(), nodeInfo.getPlugins().getInfos().size(), equalTo(1));
            assertThat(ShieldPlugin.NAME + " should be the only installed plugin, found the following ones: " + nodeInfo.getPlugins().getInfos(), nodeInfo.getPlugins().getInfos().get(0).getName(), equalTo(ShieldPlugin.NAME));
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(customShieldSettingsSource.node(nodeOrdinal))
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        return ImmutableSettings.builder().put(super.transportClientSettings())
                .put(customShieldSettingsSource.transportClient())
                .build();
    }

    /**
     * Allows to override the users config file when the {@link org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope} is set to
     * {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#SUITE} or {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#TEST}
     */
    protected String configUsers() {
        return SHIELD_DEFAULT_SETTINGS.configUsers();
    }

    /**
     * Allows to override the users_roles config file when the {@link org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope} is set to
     * {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#SUITE} or {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#TEST}
     */
    protected String configUsersRoles() {
        return SHIELD_DEFAULT_SETTINGS.configUsersRoles();
    }

    /**
     * Allows to override the roles config file when the {@link org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope} is set to
     * {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#SUITE} or {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#TEST}
     */
    protected String configRoles() {
        return SHIELD_DEFAULT_SETTINGS.configRoles();
    }

    /**
     * Allows to override the node client username (used while sending requests to the test cluster) when the {@link org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope} is set to
     * {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#SUITE} or {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#TEST}
     */
    protected String nodeClientUsername() {
        return SHIELD_DEFAULT_SETTINGS.nodeClientUsername();
    }

    /**
     * Allows to override the node client password (used while sending requests to the test cluster) when the {@link org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope} is set to
     * {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#SUITE} or {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#TEST}
     */
    protected SecuredString nodeClientPassword() {
        return SHIELD_DEFAULT_SETTINGS.nodeClientPassword();
    }

    /**
     * Allows to override the transport client username (used while sending requests to the test cluster) when the {@link org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope} is set to
     * {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#SUITE} or {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#TEST}
     */
    protected String transportClientUsername() {
        return SHIELD_DEFAULT_SETTINGS.transportClientUsername();
    }

    /**
     * Allows to override the transport client password (used while sending requests to the test cluster) when the {@link org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope} is set to
     * {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#SUITE} or {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#TEST}
     */
    protected SecuredString transportClientPassword() {
        return SHIELD_DEFAULT_SETTINGS.transportClientPassword();
    }

    /**
     * Allows to control whether ssl is enabled or not on the transport layer when the {@link org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope} is set to
     * {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#SUITE} or {@link org.elasticsearch.test.ElasticsearchIntegrationTest.Scope#TEST}
     */
    protected boolean sslTransportEnabled() {
        return randomBoolean();
    }

    private class CustomShieldSettingsSource extends ShieldSettingsSource {
        private CustomShieldSettingsSource(boolean sslTransportEnabled, File configDir, Scope scope) {
            super(maxNumberOfNodes(), sslTransportEnabled, configDir, scope);
        }

        @Override
        protected String configUsers() {
            return ShieldIntegrationTest.this.configUsers();
        }

        @Override
        protected String configUsersRoles() {
            return ShieldIntegrationTest.this.configUsersRoles();
        }

        @Override
        protected String configRoles() {
            return ShieldIntegrationTest.this.configRoles();
        }

        @Override
        protected String nodeClientUsername() {
            return ShieldIntegrationTest.this.nodeClientUsername();
        }

        @Override
        protected SecuredString nodeClientPassword() {
            return ShieldIntegrationTest.this.nodeClientPassword();
        }


        @Override
        protected String transportClientUsername() {
            return ShieldIntegrationTest.this.transportClientUsername();
        }

        @Override
        protected SecuredString transportClientPassword() {
            return ShieldIntegrationTest.this.transportClientPassword();
        }
    }

    protected void assertGreenClusterState(Client client) {
        ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().get();
        assertNoTimeout(clusterHealthResponse);
        assertThat(clusterHealthResponse.getStatus(), is(ClusterHealthStatus.GREEN));
    }
}
