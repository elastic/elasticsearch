/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
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
            return switch (value) {
                case "none" -> NONE;
                case "one_third" -> ONE_THIRD;
                case "two_third" -> TWO_THIRD;
                case "all" -> ALL;
                default -> throw new AssertionError("unknown cluster type: " + value);
            };
        }
    }

    protected final UpgradeState upgradeState = UpgradeState.parse(System.getProperty("tests.rest.upgrade_state"));

    enum ClusterName {
        LEADER,
        FOLLOWER;

        public static ClusterName parse(String value) {
            return switch (value) {
                case "leader" -> LEADER;
                case "follower" -> FOLLOWER;
                default -> throw new AssertionError("unknown cluster type: " + value);
            };
        }
    }

    protected final ClusterName clusterName = ClusterName.parse(System.getProperty("tests.rest.cluster_name"));

    protected static final Version UPGRADE_FROM_VERSION = Version.fromString(System.getProperty("tests.upgrade_from_version"));

    private static RestClient leaderClient;
    private static RestClient followerClient;
    private static boolean initialized = false;

    @Before
    public void initClientsAndConfigureClusters() throws IOException {
        String leaderHost = System.getProperty("tests.leader_host");
        if (leaderHost == null) {
            throw new AssertionError("leader host is missing");
        }

        if (initialized) {
            return;
        }

        String followerHost = System.getProperty("tests.follower_host");
        if (clusterName == ClusterName.LEADER) {
            leaderClient = buildClient(leaderHost);
            if (followerHost != null) {
                followerClient = buildClient(followerHost);
            }
        } else if (clusterName == ClusterName.FOLLOWER) {
            if (followerHost == null) {
                throw new AssertionError("follower host is missing");
            }

            leaderClient = buildClient(leaderHost);
            followerClient = buildClient(followerHost);
        } else {
            throw new AssertionError("unknown cluster name: " + clusterName);
        }
        logger.info("Leader host: {}, follower host: {}", leaderHost, followerHost);

        configureLeaderRemoteClusters();
        configureFollowerRemoteClusters();
        initialized = true;
    }

    private void configureLeaderRemoteClusters() throws IOException {
        String leaderRemoteClusterSeed = System.getProperty("tests.leader_remote_cluster_seed");
        if (leaderRemoteClusterSeed != null) {
            logger.info("Configuring leader remote cluster [{}]", leaderRemoteClusterSeed);
            Request request = new Request("PUT", "/_cluster/settings");
            request.setJsonEntity("""
                {
                  "persistent": {
                    "cluster.remote.leader.seeds": "%s"
                  }
                }""".formatted(leaderRemoteClusterSeed));
            assertThat(leaderClient.performRequest(request).getStatusLine().getStatusCode(), equalTo(200));
            if (followerClient != null) {
                assertThat(followerClient.performRequest(request).getStatusLine().getStatusCode(), equalTo(200));
            }
        } else {
            logger.info("No leader remote cluster seed found.");
        }
    }

    private void configureFollowerRemoteClusters() throws IOException {
        String followerRemoteClusterSeed = System.getProperty("tests.follower_remote_cluster_seed");
        if (followerRemoteClusterSeed != null) {
            logger.info("Configuring follower remote cluster [{}]", followerRemoteClusterSeed);
            Request request = new Request("PUT", "/_cluster/settings");
            request.setJsonEntity("""
                {
                  "persistent": {
                    "cluster.remote.follower.seeds": "%s"
                  }
                }""".formatted(followerRemoteClusterSeed));
            assertThat(leaderClient.performRequest(request).getStatusLine().getStatusCode(), equalTo(200));
            assertThat(followerClient.performRequest(request).getStatusLine().getStatusCode(), equalTo(200));
        } else {
            logger.info("No follower remote cluster seed found.");
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

    private RestClient buildClient(final String url) throws IOException {
        int portSeparator = url.lastIndexOf(':');
        HttpHost httpHost = new HttpHost(
            url.substring(0, portSeparator),
            Integer.parseInt(url.substring(portSeparator + 1)),
            getProtocol()
        );
        return buildClient(restAdminSettings(), new HttpHost[] { httpHost });
    }

    protected static Map<?, ?> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

}
