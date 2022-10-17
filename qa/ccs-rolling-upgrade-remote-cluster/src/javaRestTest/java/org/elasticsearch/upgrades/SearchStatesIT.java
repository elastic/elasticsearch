/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * =============================================================================
 *
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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

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
@SuppressWarnings("removal")
public class SearchStatesIT extends ESRestTestCase {

    private static final Logger LOGGER = LogManager.getLogger(SearchStatesIT.class);
    private static final Version UPGRADE_FROM_VERSION = Version.fromString(System.getProperty("tests.upgrade_from_version"));
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
            client.index(new IndexRequest(index).id("id_" + i).source("f", i), RequestOptions.DEFAULT);
        }

        refresh(client.getLowLevelClient(), index);
        return numDocs;
    }

    void verifySearch(String localIndex, int localNumDocs, String remoteIndex, int remoteNumDocs, Integer preFilterShardSize) {
        try (RestClient localClient = newLocalClient().getLowLevelClient()) {
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
            if (preFilterShardSize == null && randomBoolean()) {
                preFilterShardSize = randomIntBetween(1, 100);
            }
            if (preFilterShardSize != null) {
                request.addParameter("pre_filter_shard_size", Integer.toString(preFilterShardSize));
            }
            int size = between(1, 100);
            request.setJsonEntity("{\"sort\": \"f\", \"size\": " + size + "}");
            Response response = localClient.performRequest(request);
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    XContentParserConfiguration.EMPTY,
                    response.getEntity().getContent()
                )
            ) {
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
        try (RestHighLevelClient localClient = newLocalClient(); RestHighLevelClient remoteClient = newRemoteClient()) {
            createIndex(
                localClient.getLowLevelClient(),
                localIndex,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)).build()
            );
            int localNumDocs = indexDocs(localClient, localIndex, between(10, 100));

            createIndex(
                remoteClient.getLowLevelClient(),
                remoteIndex,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)).build()
            );
            int remoteNumDocs = indexDocs(remoteClient, remoteIndex, between(10, 100));

            configureRemoteClusters(getNodes(remoteClient.getLowLevelClient()));
            int iterations = between(1, 20);
            for (int i = 0; i < iterations; i++) {
                verifySearch(localIndex, localNumDocs, CLUSTER_ALIAS + ":" + remoteIndex, remoteNumDocs, null);
            }
            deleteIndex(localClient.getLowLevelClient(), localIndex);
            deleteIndex(remoteClient.getLowLevelClient(), remoteIndex);
        }
    }

    public void testCanMatch() throws Exception {
        String localIndex = "test_can_match_local_index";
        String remoteIndex = "test_can_match_remote_index";
        try (RestHighLevelClient localClient = newLocalClient(); RestHighLevelClient remoteClient = newRemoteClient()) {
            createIndex(
                localClient.getLowLevelClient(),
                localIndex,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(5, 20)).build()
            );
            int localNumDocs = indexDocs(localClient, localIndex, between(10, 100));

            createIndex(
                remoteClient.getLowLevelClient(),
                remoteIndex,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(5, 20)).build()
            );
            int remoteNumDocs = indexDocs(remoteClient, remoteIndex, between(10, 100));

            configureRemoteClusters(getNodes(remoteClient.getLowLevelClient()));
            int iterations = between(1, 10);
            for (int i = 0; i < iterations; i++) {
                verifySearch(localIndex, localNumDocs, CLUSTER_ALIAS + ":" + remoteIndex, remoteNumDocs, between(1, 10));
            }
            deleteIndex(localClient.getLowLevelClient(), localIndex);
            deleteIndex(remoteClient.getLowLevelClient(), remoteIndex);
        }
    }
}
