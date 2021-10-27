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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.rest.action.document.RestIndexAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.rest.AbstractCCSRestTestCase;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * This test ensure that we emulate the "fields" option when the local cluster supports it but the remote
 * cluster is running an older compatible version.
 */
public class CCSFieldsOptionEmulationIT extends AbstractCCSRestTestCase {

    private static final Logger LOGGER = LogManager.getLogger(CCSFieldsOptionEmulationIT.class);
    private static final Version UPGRADE_FROM_VERSION = Version.fromString(System.getProperty("tests.upgrade_from_version"));
    private static final String CLUSTER_ALIAS = "remote_cluster";

    static int indexDocs(RestHighLevelClient client, String index, int numDocs, boolean expectWarnings) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            Request indexDoc = new Request("PUT", index + "/type/" + i);
            indexDoc.setJsonEntity("{\"field\": \"f" + i + "\", \"array\": [1, 2, 3] , \"obj\": { \"innerObj\" : \"foo\" } }");
            if (expectWarnings) {
                indexDoc.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            }
            client.getLowLevelClient().performRequest(indexDoc);
        }
        client.indices().refresh(new RefreshRequest(index), RequestOptions.DEFAULT);
        return numDocs;
    }

    static RestHighLevelClient newLocalClient(Logger logger) {
        final List<HttpHost> hosts = parseHosts("tests.rest.cluster");
        final int index = random().nextInt(hosts.size());
        logger.info("Using client node {}", index);
        return new RestHighLevelClient(RestClient.builder(hosts.get(index)));
    }

    static RestHighLevelClient newRemoteClient() {
        return new RestHighLevelClient(RestClient.builder(randomFrom(parseHosts("tests.rest.remote_cluster"))));
    }

    public void testFieldsOptionEmulation() throws Exception {
        String localIndex = "test_bwc_fields_index";
        String remoteIndex = "test_bwc_fields_remote_index";
        try (RestHighLevelClient localClient = newLocalClient(LOGGER); RestHighLevelClient remoteClient = newRemoteClient()) {
            localClient.indices()
                .create(
                    new CreateIndexRequest(localIndex).settings(
                        Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))
                    ),
                    RequestOptions.DEFAULT
                );
            int localNumDocs = indexDocs(localClient, localIndex, between(10, 20), true);

            Builder remoteIndexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5));
            remoteClient.indices().create(new CreateIndexRequest(remoteIndex).settings(remoteIndexSettings), RequestOptions.DEFAULT);
            boolean expectRemoteIndexWarnings = UPGRADE_FROM_VERSION.onOrAfter(Version.V_7_0_0);
            int remoteNumDocs = indexDocs(remoteClient, remoteIndex, between(10, 20), expectRemoteIndexWarnings);
            int expectedHitCount = localNumDocs + remoteNumDocs;

            List<Node> remoteNodes = getNodes(remoteClient.getLowLevelClient());
            assertThat(remoteNodes, hasSize(2));
            configureRemoteClusters(getNodes(remoteClient.getLowLevelClient()), CLUSTER_ALIAS, UPGRADE_FROM_VERSION, LOGGER);
            RestClient lowLevelClient = localClient.getLowLevelClient();
            for (String minimizeRoundTrips : new String[] { "true", "false" }) {
                Request request = new Request("POST", "/_search");
                request.addParameter("index", localIndex + "," + CLUSTER_ALIAS + ":" + remoteIndex);
                request.addParameter("ccs_minimize_roundtrips", minimizeRoundTrips);
                request.addParameter("enable_fields_emulation", "true");
                request.setJsonEntity("{\"_source\": false, \"fields\": [\"*\"] , \"size\": " + expectedHitCount + "}");
                Response response = lowLevelClient.performRequest(request);
                try (
                    XContentParser parser = JsonXContent.jsonXContent.createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        response.getEntity().getContent()
                    )
                ) {
                    SearchResponse searchResponse = SearchResponse.fromXContent(parser);
                    ElasticsearchAssertions.assertNoFailures(searchResponse);
                    ElasticsearchAssertions.assertHitCount(searchResponse, expectedHitCount);
                    SearchHit[] hits = searchResponse.getHits().getHits();
                    for (SearchHit hit : hits) {
                        assertFalse("No source in hit expected but was: " + hit.toString(), hit.hasSource());
                        Map<String, DocumentField> fields = hit.getFields();
                        assertNotNull(fields);
                        assertNotNull("Field `field` not found, hit was: " + hit.toString(), fields.get("field"));
                        if (hit.getIndex().equals(localIndex) || UPGRADE_FROM_VERSION.onOrAfter(Version.V_7_10_0)) {
                            assertNotNull("Field `field.keyword` not found, hit was: " + hit.toString(), fields.get("field.keyword"));
                        } else {
                            // we won't be able to get multi-field for remote indices below V7.10
                            assertNull(
                                "Field `field.keyword` should not be returned, hit was: " + hit.toString(),
                                fields.get("field.keyword")
                            );
                        }
                        DocumentField arrayField = fields.get("array");
                        assertNotNull("Field `array` not found, hit was: " + hit.toString(), arrayField);
                        assertEquals(3, ((List<?>) arrayField.getValues()).size());
                        assertNull("Object fields should be flattened by the fields API", fields.get("obj"));
                        assertEquals(1, fields.get("obj.innerObj").getValues().size());
                        assertEquals("foo", fields.get("obj.innerObj").getValue());
                    }
                }
            }

            // also check validation of request
            Request request = new Request("POST", "/_search");
            request.addParameter("index", localIndex + "," + CLUSTER_ALIAS + ":" + remoteIndex);
            request.addParameter("enable_fields_emulation", "true");
            request.addParameter("search_type", "dfs_query_then_fetch");
            request.setJsonEntity("{\"_source\": false, \"fields\": [\"*\"] , \"size\": " + expectedHitCount + "}");
            final ResponseException ex = expectThrows(ResponseException.class, () -> lowLevelClient.performRequest(request));
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(ex.getMessage(), containsString("Validation Failed"));
            assertThat(
                ex.getMessage(),
                containsString("[enable_fields_emulation] cannot be used with [dfs_query_then_fetch] search type.")
            );

            localClient.indices().delete(new DeleteIndexRequest(localIndex), RequestOptions.DEFAULT);
            remoteClient.indices().delete(new DeleteIndexRequest(remoteIndex), RequestOptions.DEFAULT);
        }
    }
}
