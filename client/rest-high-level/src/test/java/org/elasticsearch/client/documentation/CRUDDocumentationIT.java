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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used to generate the Java Delete API documentation.
 * You need to wrap your code between two tags like:
 * // tag::example[]
 * // end::example[]
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts"]
 * --------------------------------------------------
 * sys2::[perl -ne 'exit if /end::example/; print if $tag; $tag = $tag || /tag::example/' \
 *     {docdir}/../../client/rest-high-level/src/test/java/org/elasticsearch/client/documentation/CRUDDocumentationIT.java]
 * --------------------------------------------------
 */
public class CRUDDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testIndex() throws IOException {
        RestHighLevelClient client = highLevelClient();

        {
            //tag::index-request-map
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("user","kimchy");
            jsonMap.put("postDate",new Date());
            jsonMap.put("message","trying out Elasticsearch");
            IndexRequest indexRequest = new IndexRequest("index", "type", "id")
                    .source(jsonMap); // <1>
            //end::index-request-map
            IndexResponse indexResponse = client.index(indexRequest);
            assertEquals(indexResponse.getResult(), DocWriteResponse.Result.CREATED);
        }
        {
            //tag::index-request-xcontent
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("user", "kimchy");
                builder.field("postDate", new Date());
                builder.field("message", "trying out Elasticsearch");
            }
            builder.endObject();
            IndexRequest indexRequest = new IndexRequest("index", "type", "id")
                    .source(builder);  // <1>
            //end::index-request-xcontent
            IndexResponse indexResponse = client.index(indexRequest);
            assertEquals(indexResponse.getResult(), DocWriteResponse.Result.UPDATED);
        }
        {
            //tag::index-request-shortcut
            IndexRequest indexRequest = new IndexRequest("index", "type", "id")
                    .source("user", "kimchy",
                            "postDate", new Date(),
                            "message", "trying out Elasticsearch"); // <1>
            //end::index-request-shortcut
            IndexResponse indexResponse = client.index(indexRequest);
            assertEquals(indexResponse.getResult(), DocWriteResponse.Result.UPDATED);
        }
        {
            //tag::index-request-string
            IndexRequest request = new IndexRequest(
                    "index", // <1>
                    "type",  // <2>
                    "id");    // <3>
            String jsonString = "{" +
                    "\"user\":\"kimchy\"," +
                    "\"postDate\":\"2013-01-30\"," +
                    "\"message\":\"trying out Elasticsearch\"" +
                    "}";
            request.source(jsonString, XContentType.JSON); //<4>
            //end::index-request-string

            // tag::index-execute
            IndexResponse indexResponse = client.index(request);
            // end::index-execute
            assertEquals(indexResponse.getResult(), DocWriteResponse.Result.UPDATED);

            // tag::index-response
            String index = indexResponse.getIndex();
            String type = indexResponse.getType();
            String id = indexResponse.getId();
            long version = indexResponse.getVersion();
            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                // <1>
            } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                // <2>
            }
            ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                // <3>
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason(); // <4>
                }
            }
            // end::index-response

            // tag::index-execute-async
            client.indexAsync(request, new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            });
            // end::index-execute-async
        }
        {
            IndexRequest request = new IndexRequest("index", "type", "id");
            // tag::index-request-routing
            request.routing("routing"); // <1>
            // end::index-request-routing
            // tag::index-request-parent
            request.parent("parent"); // <1>
            // end::index-request-parent
            // tag::index-request-timeout
            request.timeout(TimeValue.timeValueSeconds(1)); // <1>
            request.timeout("1s"); // <2>
            // end::index-request-timeout
            // tag::index-request-refresh
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL); // <1>
            request.setRefreshPolicy("wait_for");                            // <2>
            // end::index-request-refresh
            // tag::index-request-version
            request.version(2); // <1>
            // end::index-request-version
            // tag::index-request-version-type
            request.versionType(VersionType.EXTERNAL); // <1>
            // end::index-request-version-type
            // tag::index-request-op-type
            request.opType(DocWriteRequest.OpType.CREATE); // <1>
            request.opType("create"); // <2>
            // end::index-request-op-type
            // tag::index-request-pipeline
            request.setPipeline("pipeline"); // <1>
            // end::index-request-pipeline
        }
        {
            // tag::index-conflict
            IndexRequest request = new IndexRequest("index", "type", "id")
                    .source("field", "value")
                    .version(1);
            try {
                IndexResponse response = client.index(request);
            } catch(ElasticsearchException e) {
                if (e.status() == RestStatus.CONFLICT) {
                    // <1>
                }
            }
            // end::index-conflict

        }
        {
            // tag::index-optype
            IndexRequest request = new IndexRequest("index", "type", "id")
                    .source("field", "value")
                    .opType(DocWriteRequest.OpType.CREATE);
            try {
                IndexResponse response = client.index(request);
            } catch(ElasticsearchException e) {
                if (e.status() == RestStatus.CONFLICT) {
                    // <1>
                }
            }
            // end::index-optype
        }
    }

    public void testDelete() throws IOException {
        RestHighLevelClient client = highLevelClient();

        {
            IndexRequest indexRequest = new IndexRequest("index", "type", "id").source("field", "value");
            IndexResponse indexResponse = client.index(indexRequest);
            assertSame(indexResponse.status(), RestStatus.CREATED);
        }

        {
            // tag::delete-request
            DeleteRequest request = new DeleteRequest(
                    "index",    // <1>
                    "type",     // <2>
                    "id");      // <3>
            // end::delete-request

            // tag::delete-execute
            DeleteResponse deleteResponse = client.delete(request);
            // end::delete-execute
            assertSame(deleteResponse.getResult(), DocWriteResponse.Result.DELETED);

            // tag::delete-response
            String index = deleteResponse.getIndex();
            String type = deleteResponse.getType();
            String id = deleteResponse.getId();
            long version = deleteResponse.getVersion();
            ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                //<1>
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason(); // <2>
                }
            }
            // end::delete-response

            // tag::delete-execute-async
            client.deleteAsync(request, new ActionListener<DeleteResponse>() {
                @Override
                public void onResponse(DeleteResponse deleteResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            });
            // end::delete-execute-async
        }

        {
            DeleteRequest request = new DeleteRequest("index", "type", "id");
            // tag::delete-request-routing
            request.routing("routing"); // <1>
            // end::delete-request-routing
            // tag::delete-request-parent
            request.parent("parent"); // <1>
            // end::delete-request-parent
            // tag::delete-request-timeout
            request.timeout(TimeValue.timeValueMinutes(2)); // <1>
            request.timeout("2m"); // <2>
            // end::delete-request-timeout
            // tag::delete-request-refresh
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL); // <1>
            request.setRefreshPolicy("wait_for");                            // <2>
            // end::delete-request-refresh
            // tag::delete-request-version
            request.version(2); // <1>
            // end::delete-request-version
            // tag::delete-request-version-type
            request.versionType(VersionType.EXTERNAL); // <1>
            // end::delete-request-version-type
        }

        {
            // tag::delete-notfound
            DeleteRequest request = new DeleteRequest("index", "type", "does_not_exist");
            DeleteResponse deleteResponse = client.delete(request);
            if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                // <1>
            }
            // end::delete-notfound
        }

        {
            IndexResponse indexResponse = client.index(new IndexRequest("index", "type", "id").source("field", "value"));
            assertSame(indexResponse.status(), RestStatus.CREATED);

            // tag::delete-conflict
            try {
                DeleteRequest request = new DeleteRequest("index", "type", "id").version(2);
                DeleteResponse deleteResponse = client.delete(request);
            } catch (ElasticsearchException exception) {
                if (exception.status() == RestStatus.CONFLICT) {
                    // <1>
                }
            }
            // end::delete-conflict
        }
    }
}
