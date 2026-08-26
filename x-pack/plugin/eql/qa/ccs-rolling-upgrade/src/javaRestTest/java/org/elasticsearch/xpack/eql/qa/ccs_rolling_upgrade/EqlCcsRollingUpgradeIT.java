/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.qa.ccs_rolling_upgrade;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * This test ensures that EQL can process CCS requests correctly when the local and remote clusters
 * have different but compatible versions.
 * <p>
 * The local (querying) cluster consists of 2 nodes, both on the current version, and is never upgraded during the
 * suite. The remote cluster consists of 3 nodes, all initially on the old version, and is rolled forward one node at
 * a time across upgrade phases. Only node-0 and node-2 of the remote cluster have the {@code gateway} node attribute
 * and can accept remote connections; this creates a scenario where a query request and its fetch requests can be
 * routed through proxy nodes running different versions. The suite is run once per upgrade phase, from a fully old
 * remote cluster through to a fully upgraded one, which is why {@code build.gradle} opts out of smart retry's
 * individual test pruning.
 */
public class EqlCcsRollingUpgradeIT extends ESRestTestCase {

    private static final Logger LOGGER = LogManager.getLogger(EqlCcsRollingUpgradeIT.class);
    private static final String CLUSTER_ALIAS = "remote_cluster";

    private static final int REMOTE_NODE_NUM = 3;
    private static final String OLD_CLUSTER_VERSION = System.getProperty("tests.upgrade_from_version");
    private static final Set<Integer> upgradedRemoteNodes = new HashSet<>();

    private static final ElasticsearchCluster remoteCluster = ElasticsearchCluster.local()
        .name("remote")
        // Nodes here start on a prior Elasticsearch version, which requires the default distribution type.
        .distribution(DistributionType.DEFAULT)
        .version(OLD_CLUSTER_VERSION, isOldClusterDetachedVersion())
        .nodes(REMOTE_NODE_NUM)
        .setting("xpack.security.enabled", "false")
        .node(0, spec -> spec.setting("node.attr.gateway", "true"))
        .node(REMOTE_NODE_NUM - 1, spec -> spec.setting("node.attr.gateway", "true"))
        .build();

    private static final ElasticsearchCluster localCluster = ElasticsearchCluster.local()
        .name("local")
        // The default distribution is required for the EQL feature.
        .distribution(DistributionType.DEFAULT)
        .nodes(2)
        .setting("cluster.remote.node.attr", "gateway")
        .setting("xpack.security.enabled", "false")
        .build();

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    private final int requestedUpgradedNodes;

    public EqlCcsRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        this.requestedUpgradedNodes = upgradedNodes;
    }

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return IntStream.rangeClosed(0, REMOTE_NODE_NUM).boxed().map(n -> new Object[] { n }).toList();
    }

    @Before
    public void upgradeRemoteNode() {
        if (upgradedRemoteNodes.size() < requestedUpgradedNodes) {
            for (int n = 0; n < requestedUpgradedNodes; n++) {
                if (upgradedRemoteNodes.add(n)) {
                    LOGGER.info("Upgrading remote node {} to version {}", n, org.elasticsearch.test.cluster.util.Version.CURRENT);
                    remoteCluster.upgradeNodeToVersion(n, org.elasticsearch.test.cluster.util.Version.CURRENT);
                }
            }
        }
    }

    @AfterClass
    public static void resetUpgradedNodes() {
        upgradedRemoteNodes.clear();
    }

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Override
    protected final boolean preserveClusterUponCompletion() {
        return true;
    }

    record Node(String id, String name, Version version, String transportAddress, String httpAddress, Map<String, Object> attributes) {}

    static List<Node> getNodes(RestClient restClient) throws IOException {
        Response response = restClient.performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Map<String, Object> nodeMap = objectPath.evaluate("nodes");
        final List<Node> nodes = new ArrayList<>();
        for (String id : nodeMap.keySet()) {
            final String name = objectPath.evaluate("nodes." + id + ".name");
            final Version version = Version.fromString(objectPath.evaluate("nodes." + id + ".version"));
            final String transportAddress = objectPath.evaluate("nodes." + id + ".transport.publish_address");
            final String httpAddress = objectPath.evaluate("nodes." + id + ".http.publish_address");
            final Map<String, Object> attributes = objectPath.evaluate("nodes." + id + ".attributes");
            nodes.add(new Node(id, name, version, transportAddress, httpAddress, attributes));
        }
        return nodes;
    }

    static List<HttpHost> parseHosts(String addresses) {
        assertNotNull("cluster addresses must not be null", addresses);
        String[] stringUrls = addresses.split(",");
        List<HttpHost> hosts = new ArrayList<>(stringUrls.length);
        for (String stringUrl : stringUrls) {
            int portSeparator = stringUrl.lastIndexOf(':');
            if (portSeparator < 0) {
                throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
            }
            String host = stringUrl.substring(0, portSeparator);
            int port = Integer.parseInt(stringUrl.substring(portSeparator + 1));
            hosts.add(new HttpHost(host, port, "http"));
        }
        assertThat("cluster addresses [" + addresses + "] are empty", hosts, not(empty()));
        return hosts;
    }

    public static void configureRemoteClusters(List<Node> remoteNodes) throws Exception {
        assertThat(remoteNodes, hasSize(3));
        final String remoteClusterSettingPrefix = "cluster.remote." + CLUSTER_ALIAS + ".";
        try (RestClient localClient = newLocalClient()) {
            final Settings remoteConnectionSettings;
            if (randomBoolean()) {
                final List<String> seeds = remoteNodes.stream()
                    .filter(n -> n.attributes.containsKey("gateway"))
                    .map(n -> n.transportAddress)
                    .collect(Collectors.toList());
                assertThat(seeds, hasSize(2));
                LOGGER.info("--> use sniff mode with seed [{}], remote nodes [{}]", seeds, remoteNodes);
                remoteConnectionSettings = Settings.builder()
                    .putNull(remoteClusterSettingPrefix + "proxy_address")
                    .put(remoteClusterSettingPrefix + "mode", "sniff")
                    .putList(remoteClusterSettingPrefix + "seeds", seeds)
                    .build();
            } else {
                final Node proxyNode = randomFrom(remoteNodes);
                LOGGER.info("--> use proxy node [{}], remote nodes [{}]", proxyNode, remoteNodes);
                remoteConnectionSettings = Settings.builder()
                    .putNull(remoteClusterSettingPrefix + "seeds")
                    .put(remoteClusterSettingPrefix + "mode", "proxy")
                    .put(remoteClusterSettingPrefix + "proxy_address", proxyNode.transportAddress)
                    .build();
            }
            updateClusterSettings(localClient, remoteConnectionSettings);
            assertBusy(() -> {
                final Response resp = localClient.performRequest(new Request("GET", "/_remote/info"));
                assertOK(resp);
                final ObjectPath objectPath = ObjectPath.createFromResponse(resp);
                assertNotNull(objectPath.evaluate(CLUSTER_ALIAS));
                assertTrue(objectPath.evaluate(CLUSTER_ALIAS + ".connected"));
            }, 60, TimeUnit.SECONDS);
        }
    }

    static RestClient newLocalClient() {
        final List<HttpHost> hosts = parseHosts(localCluster.getHttpAddresses());
        final int index = random().nextInt(hosts.size());
        LOGGER.info("Using client node {}", index);
        return RestClient.builder(hosts.get(index)).build();
    }

    static RestClient newRemoteClient() {
        return RestClient.builder(randomFrom(parseHosts(remoteCluster.getHttpAddresses()))).build();
    }

    static int indexDocs(RestClient client, String index, int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            Request createDoc = new Request("POST", "/" + index + "/_doc/id_" + i);
            createDoc.setJsonEntity(Strings.format("""
                { "f": %s, "@timestamp": %s }
                """, i, i));
            assertOK(client.performRequest(createDoc));
        }

        refresh(client, index);
        return numDocs;
    }

    void verify(String localIndex, int localNumDocs, String remoteIndex, int remoteNumDocs) {
        try (RestClient localClient = newLocalClient()) {

            Request request = new Request("POST", "/" + randomFrom(remoteIndex, localIndex + "," + remoteIndex) + "/_eql/search");
            int size = between(1, 100);
            int id1 = between(0, 5);
            int id2 = between(6, Math.min(localNumDocs - 1, remoteNumDocs - 1));
            request.setJsonEntity(
                "{\"query\": \"sequence [any where f == " + id1 + "] [any where f == " + id2 + "] \", \"size\": " + size + "}"
            );
            Response response = localClient.performRequest(request);
            String responseText = EntityUtils.toString(response.getEntity());
            assertTrue(responseText.contains("\"sequences\":[{"));
            assertTrue(responseText.contains("\"_id\":\"id_" + id1 + "\""));
            assertTrue(responseText.contains("\"_id\":\"id_" + id2 + "\""));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void testSequences() throws Exception {
        String localIndex = "test_bwc_search_states_index";
        String remoteIndex = "test_bwc_search_states_remote_index";
        try (RestClient localClient = newLocalClient(); RestClient remoteClient = newRemoteClient()) {
            createIndex(
                localClient,
                localIndex,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)).build(),
                "{\"properties\": {\"@timestamp\": {\"type\": \"date\"}}}",
                null
            );
            int localNumDocs = indexDocs(localClient, localIndex, between(10, 100));
            createIndex(
                remoteClient,
                remoteIndex,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)).build(),
                "{\"properties\": {\"@timestamp\": {\"type\": \"date\"}}}",
                null
            );
            int remoteNumDocs = indexDocs(remoteClient, remoteIndex, between(10, 100));

            configureRemoteClusters(getNodes(remoteClient));
            int iterations = between(1, 20);
            for (int i = 0; i < iterations; i++) {
                verify(localIndex, localNumDocs, CLUSTER_ALIAS + ":" + remoteIndex, remoteNumDocs);
            }
            deleteIndex(localClient, localIndex);
            deleteIndex(remoteClient, remoteIndex);
        }
    }
}
