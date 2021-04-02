/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

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

package org.elasticsearch.upgrades;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.action.document.RestIndexAction;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;

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
 * This test ensure that we keep the search states of a CCS request correctly when the local and remote clusters
 * have different but compatible versions. See SearchService#createAndPutReaderContext
 */
public class SearchStatesIT extends ESRestTestCase {

    private static final Logger LOGGER = LogManager.getLogger(SearchStatesIT.class);
    private static final Version UPGRADE_FROM_VERSION = Version.fromString(System.getProperty("tests.upgrade_from_version"));
    private static final String CLUSTER_ALIAS = "remote_cluster";

    static class Node {
        final String id;
        final String name;
        final Version version;
        final String transportAddress;
        final String httpAddress;
        final Map<String, Object> attributes;

        Node(String id, String name, Version version, String transportAddress, String httpAddress, Map<String, Object> attributes) {
            this.id = id;
            this.name = name;
            this.version = version;
            this.transportAddress = transportAddress;
            this.httpAddress = httpAddress;
            this.attributes = attributes;
        }

        @Override
        public String toString() {
            return "Node{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", version=" + version +
                ", transportAddress='" + transportAddress + '\'' +
                ", httpAddress='" + httpAddress + '\'' +
                ", attributes=" + attributes +
                '}';
        }
    }

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
        try (RestHighLevelClient localClient = newLocalClient()) {
            final Settings remoteConnectionSettings;
            if (UPGRADE_FROM_VERSION.before(Version.V_7_6_0) || randomBoolean()) {
                final List<String> seeds = remoteNodes.stream()
                    .filter(n -> n.attributes.containsKey("gateway"))
                    .map(n -> n.transportAddress)
                    .collect(Collectors.toList());
                assertThat(seeds, hasSize(2));
                LOGGER.info("--> use sniff mode with seed [{}], remote nodes [{}]", seeds, remoteNodes);
                if (UPGRADE_FROM_VERSION.before(Version.V_7_6_0)) {
                    remoteConnectionSettings = Settings.builder()
                        .putList(remoteClusterSettingPrefix + "seeds", seeds)
                        .build();
                } else {
                    remoteConnectionSettings = Settings.builder()
                        .putNull(remoteClusterSettingPrefix + "proxy_address")
                        .put(remoteClusterSettingPrefix + "mode", "sniff")
                        .putList(remoteClusterSettingPrefix + "seeds", seeds)
                        .build();
                }
            } else {
                final Node proxyNode = randomFrom(remoteNodes);
                LOGGER.info("--> use proxy node [{}], remote nodes [{}]", proxyNode, remoteNodes);
                remoteConnectionSettings = Settings.builder()
                    .putNull(remoteClusterSettingPrefix + "seeds")
                    .put(remoteClusterSettingPrefix + "mode", "proxy")
                    .put(remoteClusterSettingPrefix + "proxy_address", proxyNode.transportAddress)
                    .build();
            }
            assertTrue(
                localClient.cluster()
                    .putSettings(new ClusterUpdateSettingsRequest().persistentSettings(remoteConnectionSettings), RequestOptions.DEFAULT)
                    .isAcknowledged()
            );
            assertBusy(() -> {
                final Response resp = localClient.getLowLevelClient().performRequest(new Request("GET", "/_remote/info"));
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
            Request indexDoc = new Request("PUT", index + "/type/" + i);
            indexDoc.setJsonEntity("{\"f\":" + i + "}");
            indexDoc.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            client.getLowLevelClient().performRequest(indexDoc);
        }
        client.indices().refresh(new RefreshRequest(index), RequestOptions.DEFAULT);
        return numDocs;
    }

    void verifySearch(String localIndex, int localNumDocs, String remoteIndex, int remoteNumDocs) {
        try (RestHighLevelClient localClient = newLocalClient()) {
            Request request = new Request("POST", "/_search");
            final int expectedDocs;
            if (randomBoolean()) {
                request.addParameter("index", remoteIndex);
                expectedDocs = remoteNumDocs;
            } else {
                request.addParameter("index", localIndex + "," + remoteIndex);
                expectedDocs = localNumDocs + remoteNumDocs;
            }
            if (UPGRADE_FROM_VERSION.onOrAfter(Version.V_7_0_0)) {
                request.addParameter("ccs_minimize_roundtrips", Boolean.toString(randomBoolean()));
            }
            int size = between(1, 100);
            request.setJsonEntity("{\"sort\": \"f\", \"size\": " + size + "}");
            Response response = localClient.getLowLevelClient().performRequest(request);
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                response.getEntity().getContent())) {
                SearchResponse searchResponse = SearchResponse.fromXContent(parser);
                ElasticsearchAssertions.assertNoFailures(searchResponse);
                ElasticsearchAssertions.assertHitCount(searchResponse, expectedDocs);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void testBWCSearchStates() throws Exception {
        String localIndex = "test_bwc_search_states_index";
        String remoteIndex = "test_bwc_search_states_remote_index";
        try (RestHighLevelClient localClient = newLocalClient();
             RestHighLevelClient remoteClient = newRemoteClient()) {
            localClient.indices().create(new CreateIndexRequest(localIndex)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))),
                RequestOptions.DEFAULT);
            int localNumDocs = indexDocs(localClient, localIndex, between(10, 100));

            remoteClient.indices().create(new CreateIndexRequest(remoteIndex)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))),
                RequestOptions.DEFAULT);
            int remoteNumDocs = indexDocs(remoteClient, remoteIndex, between(10, 100));

            configureRemoteClusters(getNodes(remoteClient.getLowLevelClient()));
            int iterations = between(1, 20);
            for (int i = 0; i < iterations; i++) {
                verifySearch(localIndex, localNumDocs, CLUSTER_ALIAS + ":" + remoteIndex, remoteNumDocs);
            }
            localClient.indices().delete(new DeleteIndexRequest(localIndex), RequestOptions.DEFAULT);
            remoteClient.indices().delete(new DeleteIndexRequest(remoteIndex), RequestOptions.DEFAULT);
        }
    }
}
