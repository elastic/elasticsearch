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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

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
    public void testClusterHealth() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::migration-cluster-health
            Request request = new Request("GET", "/_cluster/health");
            request.addParameter("wait_for_status", "green"); // <1>
            Response response = client.getLowLevelClient().performRequest(request); // <2>

            ClusterHealthStatus healthStatus;
            try (InputStream is = response.getEntity().getContent()) { // <3>
                Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true); // <4>
                healthStatus = ClusterHealthStatus.fromString((String) map.get("status")); // <5>
            }

            if (healthStatus != ClusterHealthStatus.GREEN) {
                // <6>
            }
            //end::migration-cluster-health
            assertSame(ClusterHealthStatus.GREEN, healthStatus);
        }
    }

    public void testRequests() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::migration-request-ctor
            IndexRequest request = new IndexRequest("index").id("id"); // <1>
            request.source("{\"field\":\"value\"}", XContentType.JSON);
            //end::migration-request-ctor

            //tag::migration-request-ctor-execution
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            //end::migration-request-ctor-execution
            assertEquals(RestStatus.CREATED, response.status());
        }
        {
            //tag::migration-request-async-execution
            DeleteRequest request = new DeleteRequest("index", "id"); // <1>
            client.deleteAsync(request, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>() { // <2>
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
            assertBusy(() -> assertFalse(client.exists(new GetRequest("index", "id"), RequestOptions.DEFAULT)));
        }
        {
            //tag::migration-request-sync-execution
            DeleteRequest request = new DeleteRequest("index", "id");
            DeleteResponse response = client.delete(request, RequestOptions.DEFAULT); // <1>
            //end::migration-request-sync-execution
            assertEquals(RestStatus.NOT_FOUND, response.status());
        }
    }
}
