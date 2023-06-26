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
import org.elasticsearch.rest.action.document.RestIndexAction;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.rest.AbstractCCSRestTestCase;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.hasSize;

/**
 * This test ensure that we keep the search states of a CCS request correctly when the local and remote clusters
 * have different but compatible versions. See SearchService#createAndPutReaderContext
 */
@SuppressWarnings("removal")
public class SearchStatesIT extends AbstractCCSRestTestCase {

    private static final Logger LOGGER = LogManager.getLogger(SearchStatesIT.class);
    private static final Version UPGRADE_FROM_VERSION = Version.fromString(System.getProperty("tests.upgrade_from_version"));
    private static final String CLUSTER_ALIAS = "remote_cluster";

    protected static RestHighLevelClient newLocalClient(Logger logger) {
        final List<HttpHost> hosts = parseHosts("tests.rest.cluster");
        final int index = random().nextInt(hosts.size());
        logger.info("Using client node {}", index);
        return new RestHighLevelClient(RestClient.builder(hosts.get(index)));
    }

    protected static RestHighLevelClient newRemoteClient() {
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

    void verifySearch(String localIndex, int localNumDocs, String remoteIndex, int remoteNumDocs, Integer preFilterShardSize) {
        try (RestHighLevelClient localClient = newLocalClient(LOGGER)) {
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
            Response response = localClient.getLowLevelClient().performRequest(request);
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
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
        try (RestHighLevelClient localClient = newLocalClient(LOGGER); RestHighLevelClient remoteClient = newRemoteClient()) {
            localClient.indices()
                .create(
                    new CreateIndexRequest(localIndex).settings(
                        Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))
                    ),
                    RequestOptions.DEFAULT
                );
            int localNumDocs = indexDocs(localClient, localIndex, between(10, 100));

            remoteClient.indices()
                .create(
                    new CreateIndexRequest(remoteIndex).settings(
                        Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))
                    ),
                    RequestOptions.DEFAULT
                );
            int remoteNumDocs = indexDocs(remoteClient, remoteIndex, between(10, 100));

            List<Node> remoteNodes = getNodes(remoteClient.getLowLevelClient());
            assertThat(remoteNodes, hasSize(3));
            List<String> seeds = remoteNodes.stream()
                .filter(n -> n.attributes.containsKey("gateway"))
                .map(n -> n.transportAddress)
                .collect(Collectors.toList());
            assertThat(seeds, hasSize(2));
            configureRemoteClusters(remoteNodes, CLUSTER_ALIAS, UPGRADE_FROM_VERSION, LOGGER);
            int iterations = between(1, 20);
            for (int i = 0; i < iterations; i++) {
                verifySearch(localIndex, localNumDocs, CLUSTER_ALIAS + ":" + remoteIndex, remoteNumDocs, null);
            }
            localClient.indices().delete(new DeleteIndexRequest(localIndex), RequestOptions.DEFAULT);
            remoteClient.indices().delete(new DeleteIndexRequest(remoteIndex), RequestOptions.DEFAULT);
        }
    }

    public void testCanMatch() throws Exception {
        String localIndex = "test_can_match_local_index";
        String remoteIndex = "test_can_match_remote_index";
        try (RestHighLevelClient localClient = newLocalClient(LOGGER); RestHighLevelClient remoteClient = newRemoteClient()) {
            localClient.indices()
                .create(
                    new CreateIndexRequest(localIndex).settings(
                        Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(5, 20))
                    ),
                    RequestOptions.DEFAULT
                );
            int localNumDocs = indexDocs(localClient, localIndex, between(10, 100));

            remoteClient.indices()
                .create(
                    new CreateIndexRequest(remoteIndex).settings(
                        Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(5, 20))
                    ),
                    RequestOptions.DEFAULT
                );
            int remoteNumDocs = indexDocs(remoteClient, remoteIndex, between(10, 100));

            configureRemoteClusters(getNodes(remoteClient.getLowLevelClient()), CLUSTER_ALIAS, UPGRADE_FROM_VERSION, LOGGER);
            int iterations = between(1, 10);
            for (int i = 0; i < iterations; i++) {
                verifySearch(localIndex, localNumDocs, CLUSTER_ALIAS + ":" + remoteIndex, remoteNumDocs, between(1, 10));
            }
            localClient.indices().delete(new DeleteIndexRequest(localIndex), RequestOptions.DEFAULT);
            remoteClient.indices().delete(new DeleteIndexRequest(remoteIndex), RequestOptions.DEFAULT);
        }
    }
}
