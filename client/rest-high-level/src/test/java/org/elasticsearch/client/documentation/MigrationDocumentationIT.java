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

package org.elasticsearch.client.documentation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;

/**
 * This class is used to generate the documentation for the
 * docs/java-rest/high-level/migration.asciidoc page.
 *
 * You need to wrap your code between two tags like:
 * // tag::example[]
 * // end::example[]
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{doc-tests}/MigrationDocumentationIT.java[example]
 * --------------------------------------------------
 */
public class MigrationDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testCreateIndex() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::migration-create-index
            Settings indexSettings = Settings.builder() // <1>
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .build();

            String payload = XContentFactory.jsonBuilder() // <2>
                    .startObject()
                        .startObject("settings") // <3>
                            .value(indexSettings)
                        .endObject()
                        .startObject("mappings")  // <4>
                            .startObject("doc")
                                .startObject("properties")
                                    .startObject("time")
                                        .field("type", "date")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject().string();

            HttpEntity entity = new NStringEntity(payload, ContentType.APPLICATION_JSON); // <5>

            Response response = client.getLowLevelClient().performRequest("PUT", "my-index", emptyMap(), entity); // <6>
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                // <7>
            }
            //end::migration-create-index
            assertEquals(200, response.getStatusLine().getStatusCode());
        }
    }

    public void testClusterHealth() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::migration-cluster-health
            Map<String, String> parameters = singletonMap("wait_for_status", "green");
            Response response = client.getLowLevelClient().performRequest("GET", "/_cluster/health", parameters); // <1>

            ClusterHealthStatus healthStatus;
            try (InputStream is = response.getEntity().getContent()) { // <2>
                Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true); // <3>
                healthStatus = ClusterHealthStatus.fromString((String) map.get("status")); // <4>
            }

            if (healthStatus == ClusterHealthStatus.GREEN) {
                // <5>
            }
            //end::migration-cluster-health
            assertSame(ClusterHealthStatus.GREEN, healthStatus);
        }
    }

    public void testRequests() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::migration-request-ctor
            IndexRequest request = new IndexRequest("index", "doc", "id"); // <1>
            request.source("{\"field\":\"value\"}", XContentType.JSON);
            //end::migration-request-ctor

            //tag::migration-request-ctor-execution
            IndexResponse response = client.index(request);
            //end::migration-request-ctor-execution
            assertEquals(RestStatus.CREATED, response.status());
        }
        {
            //tag::migration-request-async-execution
            DeleteRequest request = new DeleteRequest("index", "doc", "id"); // <1>
            client.deleteAsync(request, new ActionListener<DeleteResponse>() { // <2>
                @Override
                public void onResponse(DeleteResponse deleteResponse) {
                    // <3>
                }

                @Override
                public void onFailure(Exception e) {
                    // <4>
                }
            });
            //end::migration-request-async-execution
            assertBusy(() -> assertFalse(client.exists(new GetRequest("index", "doc", "id"))));
        }
        {
            //tag::migration-request-sync-execution
            DeleteRequest request = new DeleteRequest("index", "doc", "id");
            DeleteResponse response = client.delete(request); // <1>
            //end::migration-request-sync-execution
            assertEquals(RestStatus.NOT_FOUND, response.status());
        }
    }
}
