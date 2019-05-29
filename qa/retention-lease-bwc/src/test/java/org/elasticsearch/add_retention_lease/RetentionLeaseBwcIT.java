/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.add_retention_lease;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RetentionLeaseBwcIT extends ESRestTestCase {

    public void testRetentionLeaseBwcIT() throws IOException {
        // we have to dance like this otherwise we can not end up with a primary on the new node with a replica on the old node
        final Response getNodesResponse = client().performRequest(new Request("GET", "/_nodes"));
        final ObjectPath getNodesObjectPath = ObjectPath.createFromResponse(getNodesResponse);
        final Map<String, Object> nodesAsMap = getNodesObjectPath.evaluate("nodes");
        final List<Node> nodes = new ArrayList<>();
        for (final String id : nodesAsMap.keySet()) {
            nodes.add(new Node(
                    id,
                    getNodesObjectPath.evaluate("nodes." + id + ".name"),
                    Version.fromString(getNodesObjectPath.evaluate("nodes." + id + ".version")),
                    HttpHost.create(getNodesObjectPath.evaluate("nodes." + id + ".http.publish_address"))));
        }
        final List<Node> oldNodes =
                nodes.stream().filter(node -> node.version().before(Version.V_6_7_0)).collect(Collectors.toList());
        final Node newNode =
                nodes.stream().filter(node -> node.version().onOrAfter(Version.V_6_7_0)).findFirst().get();
        // only allow shards on the old nodes
        final Settings settings = Settings.builder()
                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(
                        "index.routing.allocation.include._name",
                        oldNodes.stream().map(Node::nodeName).collect(Collectors.joining(",")))
                .build();
        final Request createIndexRequest = new Request("PUT", "/index");
        createIndexRequest.setJsonEntity(Strings.toString(settings));
        client().performRequest(createIndexRequest);
        ensureYellow("index");
        // allow shards on all nodes
        final Request removeRoutingAllocationIncludeNameRequest = new Request("PUT", "/index/_settings");
        final Settings removeRoutingAllocationIncludeNameSettings =
                Settings.builder().putNull("index.routing.allocation.include._name").build();
        removeRoutingAllocationIncludeNameRequest.setJsonEntity(Strings.toString(removeRoutingAllocationIncludeNameSettings));
        client().performRequest(removeRoutingAllocationIncludeNameRequest);
        ensureGreen("index");
        // move the primary to the new node by excluding it on the current old node
        final List<Shard> shards = getShards("index", nodes, client());
        final Request addRoutingAllocationExcludeNameRequest = new Request("PUT", "/index/_settings");
        final Settings addRoutingAllocationExcludeNameSettings = Settings.builder()
                .put("index.routing.allocation.exclude._name", shards.stream().filter(Shard::primary).findFirst().get().node().nodeName())
                .build();
        addRoutingAllocationExcludeNameRequest.setJsonEntity(Strings.toString(addRoutingAllocationExcludeNameSettings));
        client().performRequest(addRoutingAllocationExcludeNameRequest);
        ensureGreen("index");
        try (RestClient newClient = buildClient(Settings.EMPTY, new HttpHost[]{newNode.publishAddress()})) {
            final Request addRetentionLeaseRequest = new Request("PUT", "/index/_add_retention_lease");
            addRetentionLeaseRequest.addParameter("id", "test-1");
            addRetentionLeaseRequest.addParameter("retaining_sequence_number", "-1");
            newClient.performRequest(addRetentionLeaseRequest);

            final Request statsRequest = new Request("GET", "/index/_stats");
            statsRequest.addParameter("level", "shards");
            final Response statsResponse = newClient.performRequest(statsRequest);
            final ObjectPath statsObjectPath = ObjectPath.createFromResponse(statsResponse);
            final ArrayList<Object> shardsStats = statsObjectPath.evaluate("indices.index.shards.0");
            assertThat(shardsStats, hasSize(2));
            boolean primaryFound = false;
            for (final Object shardStats : shardsStats) {
                final Map<?, ?> shardStatsAsMap = (Map<?, ?>) shardStats;
                final Map<?, ?> routing = (Map<?, ?>) shardStatsAsMap.get("routing");
                if (Boolean.FALSE.equals(routing.get("primary"))) {
                    continue;
                }
                primaryFound = true;
                final Map<?, ?> retentionLeases = (Map<?, ?>) shardStatsAsMap.get("retention_leases");
                final List<?> leases = (List<?>) retentionLeases.get("leases");
                assertThat(leases, hasSize(1));
                final Map<?, ?> lease = (Map<?, ?>) leases.get(0);
                assertThat(lease.get("id"), equalTo("test-1"));
                assertThat(lease.get("retaining_seq_no"), equalTo(0));
                assertThat(lease.get("source"), equalTo("rest"));
            }
            assertTrue(primaryFound);
        }

        final int numberOfDocuments = randomIntBetween(1, 512);
        for (int i = 0; i < numberOfDocuments; i++) {
            final Request indexingRequest = new Request("PUT", "/index/_doc/" + i);
            indexingRequest.setJsonEntity("{\"test\": \"test_" + randomAlphaOfLength(8) + "\"}");
            assertOK(client().performRequest(indexingRequest));
        }

        final Request refreshRequest = new Request("POST", "/index/_refresh");
        assertOK(client().performRequest(refreshRequest));

        assertCount("index", "_primary", numberOfDocuments);
        assertCount("index", "_replica", numberOfDocuments);
    }

    private void ensureYellow(final String index) throws IOException {
        final Request request = new Request("GET", "/_cluster/health/" + index);
        request.addParameter("wait_for_status", "yellow");
        request.addParameter("wait_for_no_relocating_shards", "true");
        request.addParameter("timeout", "30s");
        request.addParameter("level", "shards");
        client().performRequest(request);
    }

    private List<Shard> getShards(final String index, final List<Node> nodes, final RestClient client) throws IOException {
        final Request request = new Request("GET", index + "/_stats");
        request.addParameter("level", "shards");
        final Response response = client.performRequest(request);
        final List<Object> shardStats = ObjectPath.createFromResponse(response).evaluate("indices." + index + ".shards.0");
        final ArrayList<Shard> shards = new ArrayList<>();
        for (Object shard : shardStats) {
            final String nodeId = ObjectPath.evaluate(shard, "routing.node");
            final Boolean primary = ObjectPath.evaluate(shard, "routing.primary");
            final Node node = nodes.stream().filter(n -> n.id().equals(nodeId)).findFirst().get();
            shards.add(new Shard(node, primary));
        }
        return shards;
    }

    private void assertCount(final String index, final String preference, final int expectedCount) throws IOException {
        final Request request = new Request("GET", index + "/_count");
        request.addParameter("preference", preference);
        final Response response = client().performRequest(request);
        assertOK(response);
        final int actualCount = Integer.parseInt(ObjectPath.createFromResponse(response).evaluate("count").toString());
        assertThat(actualCount, equalTo(expectedCount));
    }

    final class Node {

        private final String id;

        public String id() {
            return id;
        }

        private final String nodeName;

        public String nodeName() {
            return nodeName;
        }

        private final Version version;

        public Version version() {
            return version;
        }

        private final HttpHost publishAddress;

        public HttpHost publishAddress() {
            return publishAddress;
        }

        Node(final String id, final String nodeName, final Version version, final HttpHost publishAddress) {
            this.id = id;
            this.nodeName = nodeName;
            this.version = version;
            this.publishAddress = publishAddress;
        }

        @Override
        public String toString() {
            return "Node{" +
                    "id='" + id + '\'' +
                    ", nodeName='" + nodeName + '\'' +
                    ", version=" + version +
                    ", publishAddress=" + publishAddress +
                    '}';
        }
    }

    final class Shard {

        private final Node node;

        public Node node() {
            return node;
        }

        private final boolean primary;

        public boolean primary() {
            return primary;
        }

        Shard(final Node node, final boolean primary) {
            this.node = node;
            this.primary = primary;
        }

        @Override
        public String toString() {
            return "Shard{" +
                    "node=" + node +
                    ", primary=" + primary +
                    '}';
        }

    }

}
