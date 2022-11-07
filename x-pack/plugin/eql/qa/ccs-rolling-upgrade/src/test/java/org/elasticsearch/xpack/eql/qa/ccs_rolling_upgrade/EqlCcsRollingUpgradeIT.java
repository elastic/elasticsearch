/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.qa.ccs_rolling_upgrade;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * This test ensures that EQL can process CCS requests correctly when the local and remote clusters
 * have different but compatible versions.
 */
@SuppressWarnings("removal")
public class EqlCcsRollingUpgradeIT extends ESRestTestCase {

    private static final Logger LOGGER = LogManager.getLogger(EqlCcsRollingUpgradeIT.class);
    private static final String CLUSTER_ALIAS = "remote_cluster";

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

    static List<HttpHost> parseHosts(String props) {
        final String address = System.getProperty(props);
        assertNotNull("[" + props + "] is not configured", address);
        String[] stringUrls = address.split(",");
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
        assertThat("[" + props + "] is empty", hosts, not(empty()));
        return hosts;
    }

    public static void configureRemoteClusters(List<Node> remoteNodes) throws Exception {
        assertThat(remoteNodes, hasSize(3));
        final String remoteClusterSettingPrefix = "cluster.remote." + CLUSTER_ALIAS + ".";
        try (RestClient localClient = newLocalClient().getLowLevelClient()) {
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

    static RestHighLevelClient newLocalClient() {
        final List<HttpHost> hosts = parseHosts("tests.rest.cluster");
        final int index = random().nextInt(hosts.size());
        LOGGER.info("Using client node {}", index);
        return new RestHighLevelClient(RestClient.builder(hosts.get(index)));
    }

    static RestHighLevelClient newRemoteClient() {
        return new RestHighLevelClient(RestClient.builder(randomFrom(parseHosts("tests.rest.remote_cluster"))));
    }

    static int indexDocs(RestHighLevelClient client, String index, int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            client.index(new IndexRequest(index).id("id_" + i).source("f", i, "@timestamp", i), RequestOptions.DEFAULT);
        }

        refresh(client.getLowLevelClient(), index);
        return numDocs;
    }

    void verify(String localIndex, int localNumDocs, String remoteIndex, int remoteNumDocs) {
        try (RestClient localClient = newLocalClient().getLowLevelClient()) {

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
        try (RestHighLevelClient localClient = newLocalClient(); RestHighLevelClient remoteClient = newRemoteClient()) {
            createIndex(
                localClient.getLowLevelClient(),
                localIndex,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)).build(),
                "{\"properties\": {\"@timestamp\": {\"type\": \"date\"}}}",
                null
            );
            int localNumDocs = indexDocs(localClient, localIndex, between(10, 100));
            createIndex(
                remoteClient.getLowLevelClient(),
                remoteIndex,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)).build(),
                "{\"properties\": {\"@timestamp\": {\"type\": \"date\"}}}",
                null
            );
            int remoteNumDocs = indexDocs(remoteClient, remoteIndex, between(10, 100));

            configureRemoteClusters(getNodes(remoteClient.getLowLevelClient()));
            int iterations = between(1, 20);
            for (int i = 0; i < iterations; i++) {
                verify(localIndex, localNumDocs, CLUSTER_ALIAS + ":" + remoteIndex, remoteNumDocs);
            }
            deleteIndex(localClient.getLowLevelClient(), localIndex);
            deleteIndex(remoteClient.getLowLevelClient(), remoteIndex);
        }
    }
}
