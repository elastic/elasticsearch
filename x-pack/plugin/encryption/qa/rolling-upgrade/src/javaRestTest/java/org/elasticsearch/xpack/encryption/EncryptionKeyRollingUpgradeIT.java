/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.encryption;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ParameterizedRollingUpgradeTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Upgrades a cluster that has no cluster state encryption key yet, adding the password to each node as it is upgraded. While nodes on the
 * earlier version remain, an upgraded master must neither install the key nor ask those nodes whether they can persist it: they don't
 * know the action, and a node that receives an unknown action fails an assertion. Once every node is upgraded, the key is installed.
 */
public class EncryptionKeyRollingUpgradeIT extends ParameterizedRollingUpgradeTestCase {

    private static final int FIRST_UPGRADED_NODE = 0;
    // the nodes are upgraded in order, and each one's keystore is written when it starts
    private static final MutableSettingsProvider[] keystores = new MutableSettingsProvider[] {
        new MutableSettingsProvider(),
        new MutableSettingsProvider(),
        new MutableSettingsProvider() };

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterVersion(), isOldClusterDetachedVersion())
        .nodes(NODE_NUM)
        .setting("xpack.security.enabled", "false")
        .node(0, n -> n.keystore(keystores[0]))
        .node(1, n -> n.keystore(keystores[1]))
        .node(2, n -> n.keystore(keystores[2]))
        .build();

    public EncryptionKeyRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public void testKeyIsInstalledOnceEveryNodeIsUpgraded() throws Exception {
        if (isUpgradedCluster()) {
            assertBusy(() -> {
                for (int node = 0; node < NODE_NUM; node++) {
                    ObjectPath indicator = health(node);
                    assertThat(indicator.evaluate("details.active_key_id"), notNullValue());
                    assertEquals("green", indicator.evaluate("status"));
                }
            }, 60, TimeUnit.SECONDS);
            return;
        }
        // the node upgraded next starts with a password, also if this phase fails
        int nextNode = isOldCluster() ? 0 : isFirstMixedCluster() ? 1 : 2;
        keystores[nextNode].put("cluster.state.encryption.active_password_id", "v1");
        keystores[nextNode].put("cluster.state.encryption.password.v1", "encryption-test-password");
        if (isFirstMixedCluster()) {
            // the upgraded node, which has a password, becomes the master
            String excluded = cluster.getName(1) + "," + cluster.getName(2);
            client().performRequest(new Request("POST", "/_cluster/voting_config_exclusions?node_names=" + excluded));
            try {
                assertBusy(() -> assertEquals(cluster.getName(FIRST_UPGRADED_NODE), electedMaster()), 60, TimeUnit.SECONDS);
                // it checks right away
                safeSleep(TimeValue.timeValueSeconds(5));
            } finally {
                client().performRequest(new Request("DELETE", "/_cluster/voting_config_exclusions?wait_for_removal=false"));
            }
        }
        if (isMixedCluster()) {
            Request clusterHealth = new Request("GET", "/_cluster/health");
            clusterHealth.addParameter("wait_for_nodes", Integer.toString(NODE_NUM));
            client().performRequest(clusterHealth);
            assertThat(
                "no key while nodes on the earlier version can't persist it",
                health(FIRST_UPGRADED_NODE).evaluate("details.active_key_id"),
                nullValue()
            );
        }
    }

    /** The elected master, read from the first upgraded node, since {@code _cat/master} on some earlier versions fails without one. */
    private String electedMaster() {
        try (RestClient nodeClient = nodeClient(FIRST_UPGRADED_NODE)) {
            Request request = new Request("GET", "/_cluster/state/master_node,nodes");
            ObjectPath state = ObjectPath.createFromResponse(nodeClient.performRequest(request));
            String masterId = state.evaluate("master_node");
            return masterId == null ? null : state.evaluate("nodes." + masterId + ".name");
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /** The indicator as computed by node {@code node}, which the health API does locally. */
    private ObjectPath health(int node) throws IOException {
        try (RestClient nodeClient = nodeClient(node)) {
            return new ObjectPath(
                ObjectPath.createFromResponse(nodeClient.performRequest(new Request("GET", "/_health_report/cluster_state_encryption")))
                    .evaluate("indicators.cluster_state_encryption")
            );
        }
    }

    private RestClient nodeClient(int node) throws IOException {
        return buildClient(restClientSettings(), new HttpHost[] { HttpHost.create(cluster.getHttpAddress(node)) });
    }
}
