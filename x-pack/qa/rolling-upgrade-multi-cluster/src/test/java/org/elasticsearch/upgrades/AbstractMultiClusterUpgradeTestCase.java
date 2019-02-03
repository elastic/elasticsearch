/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractMultiClusterUpgradeTestCase extends ESRestTestCase {

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    enum UpgradeState {
        NONE,
        ONE_THIRD,
        TWO_THIRD,
        ALL;

        public static UpgradeState parse(String value) {
            switch (value) {
                case "none":
                    return NONE;
                case "one_third":
                    return ONE_THIRD;
                case "two_third":
                    return TWO_THIRD;
                case "all":
                    return ALL;
                default:
                    throw new AssertionError("unknown cluster type: " + value);
            }
        }
    }

    protected final UpgradeState upgradeState = UpgradeState.parse(System.getProperty("tests.rest.upgrade_state"));

    enum ClusterName {
        LEADER,
        FOLLOWER;

        public static ClusterName parse(String value) {
            switch (value) {
                case "leader":
                    return LEADER;
                case "follower":
                    return FOLLOWER;
                default:
                    throw new AssertionError("unknown cluster type: " + value);
            }
        }
    }

    protected final ClusterName clusterName = ClusterName.parse(System.getProperty("tests.rest.cluster_name"));

    @Before
    public void configureLeaderRemoteClusters() throws IOException {
        String leaderRemoteClusterSeed = System.getProperty("tests.leader_remote_cluster_seed");
        if (leaderRemoteClusterSeed != null) {
            try  (RestClient client = buildFollowerClient()){
                logger.info("Configuring leader remote cluster [{}]", leaderRemoteClusterSeed);
                Request request = new Request("PUT", "/_cluster/settings");
                request.setJsonEntity("{\"persistent\": {\"cluster.remote.leader.seeds\": \"" + leaderRemoteClusterSeed + "\"}}");
                assertThat(client.performRequest(request).getStatusLine().getStatusCode(), equalTo(200));
            }
        } else {
            logger.info("No leader remote cluster seen found.");
        }
    }

    @Before
    public void configureFollowerRemoteClusters() throws IOException {
        String followerRemoteClusterSeed = System.getProperty("tests.follower_remote_cluster_seed");
        if (followerRemoteClusterSeed != null) {
            try  (RestClient client = buildLeaderClient()){
                logger.info("Configuring follower remote cluster [{}]", followerRemoteClusterSeed);
                Request request = new Request("PUT", "/_cluster/settings");
                request.setJsonEntity("{\"persistent\": {\"cluster.remote.follower.seeds\": \"" + followerRemoteClusterSeed + "\"}}");
                assertThat(client.performRequest(request).getStatusLine().getStatusCode(), equalTo(200));
            }
        } else {
            logger.info("No follower remote cluster seen found.");
        }
    }

    private static RestClient leaderClient;
    private static RestClient followerClient;

    @Before
    public void initClients() throws IOException {
        if (leaderClient == null) {
            leaderClient = clusterName == ClusterName.LEADER ? client() : buildLeaderClient();
        }
        if (followerClient == null) {
            followerClient = clusterName == ClusterName.FOLLOWER ? client() : buildFollowerClient();
        }
    }

    @AfterClass
    public static void destroyClients() throws IOException {
        try {
            IOUtils.close(leaderClient, followerClient);
        } finally {
            leaderClient = null;
            followerClient = null;
        }
    }

    protected static RestClient leaderClient() {
        return leaderClient;
    }

    protected static RestClient followerClient() {
        return followerClient;
    }

    private RestClient buildLeaderClient() throws IOException {
        String leaderHost = System.getProperty("tests.leader_host");
        if (leaderHost != null) {
            return buildClient(leaderHost);
        } else {
            return null;
        }
    }

    private RestClient buildFollowerClient() throws IOException {
        String followerHost = System.getProperty("tests.follower_host");
        if (followerHost != null) {
            return buildClient(followerHost);
        } else {
            return null;
        }
    }

    private RestClient buildClient(final String url) throws IOException {
        int portSeparator = url.lastIndexOf(':');
        HttpHost httpHost = new HttpHost(url.substring(0, portSeparator),
            Integer.parseInt(url.substring(portSeparator + 1)), getProtocol());
        return buildClient(restAdminSettings(), new HttpHost[]{httpHost});
    }

    protected static Map<?, ?> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

}
