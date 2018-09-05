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
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * This class is used to generate the Java CRUD API documentation.
 * You need to wrap your code between two tags like:
 * // tag::example
 * // end::example
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{doc-tests}/CRUDDocumentationIT.java[example]
 * --------------------------------------------------
 *
 * The column width of the code block is 84. If the code contains a line longer
 * than 84, the line will be cut and a horizontal scroll bar will be displayed.
 * (the code indentation of the tag is not included in the width)
 */
public class CRUDDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testIndex() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            //tag::index-request-map
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("user", "kimchy");
            jsonMap.put("postDate", new Date());
            jsonMap.put("message", "trying out Elasticsearch");
            IndexRequest indexRequest = new IndexRequest("posts", "doc", "1")
                    .source(jsonMap); // <1>
            //end::index-request-map
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
        }
        {
            //tag::index-request-xcontent
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("user", "kimchy");
                builder.timeField("postDate", new Date());
                builder.field("message", "trying out Elasticsearch");
            }
            builder.endObject();
            IndexRequest indexRequest = new IndexRequest("posts", "doc", "1")
                    .source(builder);  // <1>
            //end::index-request-xcontent
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            assertEquals(DocWriteResponse.Result.UPDATED, indexResponse.getResult());
        }
        {
            //tag::index-request-shortcut
            IndexRequest indexRequest = new IndexRequest("posts", "doc", "1")
                    .source("user", "kimchy",
                            "postDate", new Date(),
                            "message", "trying out Elasticsearch"); // <1>
            //end::index-request-shortcut
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            assertEquals(DocWriteResponse.Result.UPDATED, indexResponse.getResult());
        }
        {
            //tag::index-request-string
            IndexRequest request = new IndexRequest(
                    "posts", // <1>
                    "doc",  // <2>
                    "1");   // <3>
            String jsonString = "{" +
                    "\"user\":\"kimchy\"," +
                    "\"postDate\":\"2013-01-30\"," +
                    "\"message\":\"trying out Elasticsearch\"" +
                    "}";
            request.source(jsonString, XContentType.JSON); // <4>
            //end::index-request-string

            // tag::index-execute
            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
            // end::index-execute
            assertEquals(DocWriteResponse.Result.UPDATED, indexResponse.getResult());

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
        }
        {
            IndexRequest request = new IndexRequest("posts", "doc", "1");
            // tag::index-request-routing
            request.routing("routing"); // <1>
            // end::index-request-routing
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
            IndexRequest request = new IndexRequest("posts", "doc", "1")
                    .source("field", "value")
                    .version(1);
            try {
                IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            } catch(ElasticsearchException e) {
                if (e.status() == RestStatus.CONFLICT) {
                    // <1>
                }
            }
            // end::index-conflict
        }
        {
            // tag::index-optype
            IndexRequest request = new IndexRequest("posts", "doc", "1")
                    .source("field", "value")
                    .opType(DocWriteRequest.OpType.CREATE);
            try {
                IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            } catch(ElasticsearchException e) {
                if (e.status() == RestStatus.CONFLICT) {
                    // <1>
                }
            }
            // end::index-optype
        }
        {
            IndexRequest request = new IndexRequest("posts", "doc", "async").source("field", "value");
            // tag::index-execute-listener
            ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::index-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::index-execute-async
            client.indexAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::index-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testUpdate() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            IndexRequest indexRequest = new IndexRequest("posts", "doc", "1").source("field", 0);
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            assertSame(RestStatus.CREATED, indexResponse.status());

            Request request = new Request("POST", "/_scripts/increment-field");
            request.setJsonEntity(Strings.toString(JsonXContent.contentBuilder()
                    .startObject()
                        .startObject("script")
                            .field("lang", "painless")
                            .field("code", "ctx._source.field += params.count")
                        .endObject()
                    .endObject()));
            Response response = client().performRequest(request);
            assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        }
        {
            //tag::update-request
            UpdateRequest request = new UpdateRequest(
                    "posts", // <1>
                    "doc",  // <2>
                    "1");   // <3>
            //end::update-request
            request.fetchSource(true);
            //tag::update-request-with-inline-script
            Map<String, Object> parameters = singletonMap("count", 4); // <1>

            Script inline = new Script(ScriptType.INLINE, "painless",
                    "ctx._source.field += params.count", parameters);  // <2>
            request.script(inline);  // <3>
            //end::update-request-with-inline-script
            UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
            assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());
            assertEquals(4, updateResponse.getGetResult().getSource().get("field"));

            request = new UpdateRequest("posts", "doc", "1").fetchSource(true);
            //tag::update-request-with-stored-script
            Script stored =
                    new Script(ScriptType.STORED, null, "increment-field", parameters);  // <1>
            request.script(stored);  // <2>
            //end::update-request-with-stored-script
            updateResponse = client.update(request, RequestOptions.DEFAULT);
            assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());
            assertEquals(8, updateResponse.getGetResult().getSource().get("field"));
        }
        {
            //tag::update-request-with-doc-as-map
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("updated", new Date());
            jsonMap.put("reason", "daily update");
            UpdateRequest request = new UpdateRequest("posts", "doc", "1")
                    .doc(jsonMap); // <1>
            //end::update-request-with-doc-as-map
            UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
            assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());
        }
        {
            //tag::update-request-with-doc-as-xcontent
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.timeField("updated", new Date());
                builder.field("reason", "daily update");
            }
            builder.endObject();
            UpdateRequest request = new UpdateRequest("posts", "doc", "1")
                    .doc(builder);  // <1>
            //end::update-request-with-doc-as-xcontent
            UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
            assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());
        }
        {
            //tag::update-request-shortcut
            UpdateRequest request = new UpdateRequest("posts", "doc", "1")
                    .doc("updated", new Date(),
                         "reason", "daily update"); // <1>
            //end::update-request-shortcut
            UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
            assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());
        }
        {
            //tag::update-request-with-doc-as-string
            UpdateRequest request = new UpdateRequest("posts", "doc", "1");
            String jsonString = "{" +
                    "\"updated\":\"2017-01-01\"," +
                    "\"reason\":\"daily update\"" +
                    "}";
            request.doc(jsonString, XContentType.JSON); // <1>
            //end::update-request-with-doc-as-string
            request.fetchSource(true);
            // tag::update-execute
            UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
            // end::update-execute
            assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());

            // tag::update-response
            String index = updateResponse.getIndex();
            String type = updateResponse.getType();
            String id = updateResponse.getId();
            long version = updateResponse.getVersion();
            if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
                // <1>
            } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                // <2>
            } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
                // <3>
            } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
                // <4>
            }
            // end::update-response

            // tag::update-getresult
            GetResult result = updateResponse.getGetResult(); // <1>
            if (result.isExists()) {
                String sourceAsString = result.sourceAsString(); // <2>
                Map<String, Object> sourceAsMap = result.sourceAsMap(); // <3>
                byte[] sourceAsBytes = result.source(); // <4>
            } else {
                // <5>
            }
            // end::update-getresult
            assertNotNull(result);
            assertEquals(3, result.sourceAsMap().size());
            // tag::update-failure
            ReplicationResponse.ShardInfo shardInfo = updateResponse.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                // <1>
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason(); // <2>
                }
            }
            // end::update-failure
        }
        {
            //tag::update-docnotfound
            UpdateRequest request = new UpdateRequest("posts", "type", "does_not_exist")
                    .doc("field", "value");
            try {
                UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
            } catch (ElasticsearchException e) {
                if (e.status() == RestStatus.NOT_FOUND) {
                    // <1>
                }
            }
            //end::update-docnotfound
        }
        {
            // tag::update-conflict
            UpdateRequest request = new UpdateRequest("posts", "doc", "1")
                    .doc("field", "value")
                    .version(1);
            try {
                UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
            } catch(ElasticsearchException e) {
                if (e.status() == RestStatus.CONFLICT) {
                    // <1>
                }
            }
            // end::update-conflict
        }
        {
            UpdateRequest request = new UpdateRequest("posts", "doc", "1").doc("reason", "no source");
            //tag::update-request-no-source
            request.fetchSource(true); // <1>
            //end::update-request-no-source
            UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
            assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());
            assertNotNull(updateResponse.getGetResult());
            assertEquals(3, updateResponse.getGetResult().sourceAsMap().size());
        }
        {
            UpdateRequest request = new UpdateRequest("posts", "doc", "1").doc("reason", "source includes");
            //tag::update-request-source-include
            String[] includes = new String[]{"updated", "r*"};
            String[] excludes = Strings.EMPTY_ARRAY;
            request.fetchSource(new FetchSourceContext(true, includes, excludes)); // <1>
            //end::update-request-source-include
            UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
            assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());
            Map<String, Object> sourceAsMap = updateResponse.getGetResult().sourceAsMap();
            assertEquals(2, sourceAsMap.size());
            assertEquals("source includes", sourceAsMap.get("reason"));
            assertTrue(sourceAsMap.containsKey("updated"));
        }
        {
            UpdateRequest request = new UpdateRequest("posts", "doc", "1").doc("reason", "source excludes");
            //tag::update-request-source-exclude
            String[] includes = Strings.EMPTY_ARRAY;
            String[] excludes = new String[]{"updated"};
            request.fetchSource(new FetchSourceContext(true, includes, excludes)); // <1>
            //end::update-request-source-exclude
            UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
            assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());
            Map<String, Object> sourceAsMap = updateResponse.getGetResult().sourceAsMap();
            assertEquals(2, sourceAsMap.size());
            assertEquals("source excludes", sourceAsMap.get("reason"));
            assertTrue(sourceAsMap.containsKey("field"));
        }
        {
            UpdateRequest request = new UpdateRequest("posts", "doc", "id");
            // tag::update-request-routing
            request.routing("routing"); // <1>
            // end::update-request-routing
            // tag::update-request-timeout
            request.timeout(TimeValue.timeValueSeconds(1)); // <1>
            request.timeout("1s"); // <2>
            // end::update-request-timeout
            // tag::update-request-retry
            request.retryOnConflict(3); // <1>
            // end::update-request-retry
            // tag::update-request-refresh
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL); // <1>
            request.setRefreshPolicy("wait_for");                            // <2>
            // end::update-request-refresh
            // tag::update-request-version
            request.version(2); // <1>
            // end::update-request-version
            // tag::update-request-detect-noop
            request.detectNoop(false); // <1>
            // end::update-request-detect-noop
            // tag::update-request-upsert
            String jsonString = "{\"created\":\"2017-01-01\"}";
            request.upsert(jsonString, XContentType.JSON);  // <1>
            // end::update-request-upsert
            // tag::update-request-scripted-upsert
            request.scriptedUpsert(true); // <1>
            // end::update-request-scripted-upsert
            // tag::update-request-doc-upsert
            request.docAsUpsert(true); // <1>
            // end::update-request-doc-upsert
            // tag::update-request-active-shards
            request.waitForActiveShards(2); // <1>
            request.waitForActiveShards(ActiveShardCount.ALL); // <2>
            // end::update-request-active-shards
        }
        {
            UpdateRequest request = new UpdateRequest("posts", "doc", "async").doc("reason", "async update").docAsUpsert(true);

            // tag::update-execute-listener
            ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
                @Override
                public void onResponse(UpdateResponse updateResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::update-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::update-execute-async
            client.updateAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::update-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDelete() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            IndexRequest indexRequest = new IndexRequest("posts", "doc", "1").source("field", "value");
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            assertSame(RestStatus.CREATED, indexResponse.status());
        }

        {
            // tag::delete-request
            DeleteRequest request = new DeleteRequest(
                    "posts",    // <1>
                    "doc",     // <2>
                    "1");      // <3>
            // end::delete-request

            // tag::delete-execute
            DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
            // end::delete-execute
            assertSame(DocWriteResponse.Result.DELETED, deleteResponse.getResult());

            // tag::delete-response
            String index = deleteResponse.getIndex();
            String type = deleteResponse.getType();
            String id = deleteResponse.getId();
            long version = deleteResponse.getVersion();
            ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                // <1>
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason(); // <2>
                }
            }
            // end::delete-response
        }

        {
            DeleteRequest request = new DeleteRequest("posts", "doc", "1");
            // tag::delete-request-routing
            request.routing("routing"); // <1>
            // end::delete-request-routing
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
            DeleteRequest request = new DeleteRequest("posts", "doc", "does_not_exist");
            DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
            if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                // <1>
            }
            // end::delete-notfound
        }

        {
            IndexResponse indexResponse = client.index(new IndexRequest("posts", "doc", "1").source("field", "value")
                    , RequestOptions.DEFAULT);
            assertSame(RestStatus.CREATED, indexResponse.status());

            // tag::delete-conflict
            try {
                DeleteRequest request = new DeleteRequest("posts", "doc", "1").version(2);
                DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
            } catch (ElasticsearchException exception) {
                if (exception.status() == RestStatus.CONFLICT) {
                    // <1>
                }
            }
            // end::delete-conflict
        }
        {
            IndexResponse indexResponse = client.index(new IndexRequest("posts", "doc", "async").source("field", "value"),
                    RequestOptions.DEFAULT);
            assertSame(RestStatus.CREATED, indexResponse.status());

            DeleteRequest request = new DeleteRequest("posts", "doc", "async");

            // tag::delete-execute-listener
            ActionListener<DeleteResponse> listener = new ActionListener<DeleteResponse>() {
                @Override
                public void onResponse(DeleteResponse deleteResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::delete-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-execute-async
            client.deleteAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testBulk() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            // tag::bulk-request
            BulkRequest request = new BulkRequest(); // <1>
            request.add(new IndexRequest("posts", "doc", "1")  // <2>
                    .source(XContentType.JSON,"field", "foo"));
            request.add(new IndexRequest("posts", "doc", "2")  // <3>
                    .source(XContentType.JSON,"field", "bar"));
            request.add(new IndexRequest("posts", "doc", "3")  // <4>
                    .source(XContentType.JSON,"field", "baz"));
            // end::bulk-request
            // tag::bulk-execute
            BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
            // end::bulk-execute
            assertSame(RestStatus.OK, bulkResponse.status());
            assertFalse(bulkResponse.hasFailures());
        }
        {
            // tag::bulk-request-with-mixed-operations
            BulkRequest request = new BulkRequest();
            request.add(new DeleteRequest("posts", "doc", "3")); // <1>
            request.add(new UpdateRequest("posts", "doc", "2") // <2>
                    .doc(XContentType.JSON,"other", "test"));
            request.add(new IndexRequest("posts", "doc", "4")  // <3>
                    .source(XContentType.JSON,"field", "baz"));
            // end::bulk-request-with-mixed-operations
            BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
            assertSame(RestStatus.OK, bulkResponse.status());
            assertFalse(bulkResponse.hasFailures());

            // tag::bulk-response
            for (BulkItemResponse bulkItemResponse : bulkResponse) { // <1>
                DocWriteResponse itemResponse = bulkItemResponse.getResponse(); // <2>

                if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                        || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) { // <3>
                    IndexResponse indexResponse = (IndexResponse) itemResponse;

                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) { // <4>
                    UpdateResponse updateResponse = (UpdateResponse) itemResponse;

                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) { // <5>
                    DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                }
            }
            // end::bulk-response
            // tag::bulk-has-failures
            if (bulkResponse.hasFailures()) { // <1>

            }
            // end::bulk-has-failures
            // tag::bulk-errors
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) { // <1>
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure(); // <2>

                }
            }
            // end::bulk-errors
        }
        {
            BulkRequest request = new BulkRequest();
            // tag::bulk-request-timeout
            request.timeout(TimeValue.timeValueMinutes(2)); // <1>
            request.timeout("2m"); // <2>
            // end::bulk-request-timeout
            // tag::bulk-request-refresh
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL); // <1>
            request.setRefreshPolicy("wait_for");                            // <2>
            // end::bulk-request-refresh
            // tag::bulk-request-active-shards
            request.waitForActiveShards(2); // <1>
            request.waitForActiveShards(ActiveShardCount.ALL); // <2>
            // end::bulk-request-active-shards

            // tag::bulk-execute-listener
            ActionListener<BulkResponse> listener = new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::bulk-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::bulk-execute-async
            client.bulkAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::bulk-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testReindex() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            String mapping =
                "\"doc\": {\n" +
                    "    \"properties\": {\n" +
                    "      \"user\": {\n" +
                    "        \"type\": \"text\"\n" +
                    "      },\n" +
                    "      \"field1\": {\n" +
                    "        \"type\": \"integer\"\n" +
                    "      },\n" +
                    "      \"field2\": {\n" +
                    "        \"type\": \"integer\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }";
            createIndex("source1", Settings.EMPTY, mapping);
            createIndex("source2", Settings.EMPTY, mapping);
            createPipeline("my_pipeline");
        }
        {
            // tag::reindex-request
            ReindexRequest request = new ReindexRequest(); // <1>
            request.setSourceIndices("source1", "source2"); // <2>
            request.setDestIndex("dest");  // <3>
            // end::reindex-request
            // tag::reindex-request-versionType
            request.setDestVersionType(VersionType.EXTERNAL); // <1>
            // end::reindex-request-versionType
            // tag::reindex-request-opType
            request.setDestOpType("create"); // <1>
            // end::reindex-request-opType
            // tag::reindex-request-conflicts
            request.setConflicts("proceed"); // <1>
            // end::reindex-request-conflicts
            // tag::reindex-request-typeOrQuery
            request.setSourceDocTypes("doc"); // <1>
            request.setSourceQuery(new TermQueryBuilder("user", "kimchy")); // <2>
            // end::reindex-request-typeOrQuery
            // tag::reindex-request-size
            request.setSize(10); // <1>
            // end::reindex-request-size
            // tag::reindex-request-sourceSize
            request.setSourceBatchSize(100); // <1>
            // end::reindex-request-sourceSize
            // tag::reindex-request-pipeline
            request.setDestPipeline("my_pipeline"); // <1>
            // end::reindex-request-pipeline
            // tag::reindex-request-sort
            request.addSortField("field1", SortOrder.DESC); // <1>
            request.addSortField("field2", SortOrder.ASC); // <2>
            // end::reindex-request-sort
            // tag::reindex-request-script
            request.setScript(
                new Script(
                    ScriptType.INLINE, "painless",
                    "if (ctx._source.user == 'kimchy') {ctx._source.likes++;}",
                    Collections.emptyMap())); // <1>
            // end::reindex-request-script
            // tag::reindex-request-remote
            request.setRemoteInfo(
                new RemoteInfo(
                    "https", "localhost", 9002, null, new BytesArray(new MatchAllQueryBuilder().toString()),
                    "user", "pass", Collections.emptyMap(), new TimeValue(100, TimeUnit.MILLISECONDS),
                    new TimeValue(100, TimeUnit.SECONDS)
                )
            ); // <1>
            // end::reindex-request-remote
            request.setRemoteInfo(null); // Remove it for tests
            // tag::reindex-request-timeout
            request.setTimeout(TimeValue.timeValueMinutes(2)); // <1>
            // end::reindex-request-timeout
            // tag::reindex-request-refresh
            request.setRefresh(true); // <1>
            // end::reindex-request-refresh
            // tag::reindex-request-slices
            request.setSlices(2); // <1>
            // end::reindex-request-slices
            // tag::reindex-request-scroll
            request.setScroll(TimeValue.timeValueMinutes(10)); // <1>
            // end::reindex-request-scroll


            // tag::reindex-execute
            BulkByScrollResponse bulkResponse = client.reindex(request, RequestOptions.DEFAULT);
            // end::reindex-execute
            assertSame(0, bulkResponse.getSearchFailures().size());
            assertSame(0, bulkResponse.getBulkFailures().size());
            // tag::reindex-response
            TimeValue timeTaken = bulkResponse.getTook(); // <1>
            boolean timedOut = bulkResponse.isTimedOut(); // <2>
            long totalDocs = bulkResponse.getTotal(); // <3>
            long updatedDocs = bulkResponse.getUpdated(); // <4>
            long createdDocs = bulkResponse.getCreated(); // <5>
            long deletedDocs = bulkResponse.getDeleted(); // <6>
            long batches = bulkResponse.getBatches(); // <7>
            long noops = bulkResponse.getNoops(); // <8>
            long versionConflicts = bulkResponse.getVersionConflicts(); // <9>
            long bulkRetries = bulkResponse.getBulkRetries(); // <10>
            long searchRetries = bulkResponse.getSearchRetries(); // <11>
            TimeValue throttledMillis = bulkResponse.getStatus().getThrottled(); // <12>
            TimeValue throttledUntilMillis = bulkResponse.getStatus().getThrottledUntil(); // <13>
            List<ScrollableHitSource.SearchFailure> searchFailures = bulkResponse.getSearchFailures(); // <14>
            List<BulkItemResponse.Failure> bulkFailures = bulkResponse.getBulkFailures(); // <15>
            // end::reindex-response
        }
        {
            ReindexRequest request = new ReindexRequest();
            request.setSourceIndices("source1");
            request.setDestIndex("dest");

            // tag::reindex-execute-listener
            ActionListener<BulkByScrollResponse> listener = new ActionListener<BulkByScrollResponse>() {
                @Override
                public void onResponse(BulkByScrollResponse bulkResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::reindex-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::reindex-execute-async
            client.reindexAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::reindex-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testUpdateByQuery() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            String mapping =
                "\"doc\": {\n" +
                    "    \"properties\": {\n" +
                    "      \"user\": {\n" +
                    "        \"type\": \"text\"\n" +
                    "      },\n" +
                    "      \"field1\": {\n" +
                    "        \"type\": \"integer\"\n" +
                    "      },\n" +
                    "      \"field2\": {\n" +
                    "        \"type\": \"integer\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }";
            createIndex("source1", Settings.EMPTY, mapping);
            createIndex("source2", Settings.EMPTY, mapping);
            createPipeline("my_pipeline");
        }
        {
            // tag::update-by-query-request
            UpdateByQueryRequest request = new UpdateByQueryRequest("source1", "source2"); // <1>
            // end::update-by-query-request
            // tag::update-by-query-request-conflicts
            request.setConflicts("proceed"); // <1>
            // end::update-by-query-request-conflicts
            // tag::update-by-query-request-typeOrQuery
            request.setDocTypes("doc"); // <1>
            request.setQuery(new TermQueryBuilder("user", "kimchy")); // <2>
            // end::update-by-query-request-typeOrQuery
            // tag::update-by-query-request-size
            request.setSize(10); // <1>
            // end::update-by-query-request-size
            // tag::update-by-query-request-scrollSize
            request.setBatchSize(100); // <1>
            // end::update-by-query-request-scrollSize
            // tag::update-by-query-request-pipeline
            request.setPipeline("my_pipeline"); // <1>
            // end::update-by-query-request-pipeline
            // tag::update-by-query-request-script
            request.setScript(
                new Script(
                    ScriptType.INLINE, "painless",
                    "if (ctx._source.user == 'kimchy') {ctx._source.likes++;}",
                    Collections.emptyMap())); // <1>
            // end::update-by-query-request-script
            // tag::update-by-query-request-timeout
            request.setTimeout(TimeValue.timeValueMinutes(2)); // <1>
            // end::update-by-query-request-timeout
            // tag::update-by-query-request-refresh
            request.setRefresh(true); // <1>
            // end::update-by-query-request-refresh
            // tag::update-by-query-request-slices
            request.setSlices(2); // <1>
            // end::update-by-query-request-slices
            // tag::update-by-query-request-scroll
            request.setScroll(TimeValue.timeValueMinutes(10)); // <1>
            // end::update-by-query-request-scroll
            // tag::update-by-query-request-routing
            request.setRouting("=cat"); // <1>
            // end::update-by-query-request-routing
            // tag::update-by-query-request-indicesOptions
            request.setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN); // <1>
            // end::update-by-query-request-indicesOptions

            // tag::update-by-query-execute
            BulkByScrollResponse bulkResponse = client.updateByQuery(request, RequestOptions.DEFAULT);
            // end::update-by-query-execute
            assertSame(0, bulkResponse.getSearchFailures().size());
            assertSame(0, bulkResponse.getBulkFailures().size());
            // tag::update-by-query-response
            TimeValue timeTaken = bulkResponse.getTook(); // <1>
            boolean timedOut = bulkResponse.isTimedOut(); // <2>
            long totalDocs = bulkResponse.getTotal(); // <3>
            long updatedDocs = bulkResponse.getUpdated(); // <4>
            long deletedDocs = bulkResponse.getDeleted(); // <5>
            long batches = bulkResponse.getBatches(); // <6>
            long noops = bulkResponse.getNoops(); // <7>
            long versionConflicts = bulkResponse.getVersionConflicts(); // <8>
            long bulkRetries = bulkResponse.getBulkRetries(); // <9>
            long searchRetries = bulkResponse.getSearchRetries(); // <10>
            TimeValue throttledMillis = bulkResponse.getStatus().getThrottled(); // <11>
            TimeValue throttledUntilMillis = bulkResponse.getStatus().getThrottledUntil(); // <12>
            List<ScrollableHitSource.SearchFailure> searchFailures = bulkResponse.getSearchFailures(); // <13>
            List<BulkItemResponse.Failure> bulkFailures = bulkResponse.getBulkFailures(); // <14>
            // end::update-by-query-response
        }
        {
            UpdateByQueryRequest request = new UpdateByQueryRequest();
            request.indices("source1");

            // tag::update-by-query-execute-listener
            ActionListener<BulkByScrollResponse> listener = new ActionListener<BulkByScrollResponse>() {
                @Override
                public void onResponse(BulkByScrollResponse bulkResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::update-by-query-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::update-by-query-execute-async
            client.updateByQueryAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::update-by-query-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteByQuery() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            String mapping =
                "\"doc\": {\n" +
                    "    \"properties\": {\n" +
                    "      \"user\": {\n" +
                    "        \"type\": \"text\"\n" +
                    "      },\n" +
                    "      \"field1\": {\n" +
                    "        \"type\": \"integer\"\n" +
                    "      },\n" +
                    "      \"field2\": {\n" +
                    "        \"type\": \"integer\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }";
            createIndex("source1", Settings.EMPTY, mapping);
            createIndex("source2", Settings.EMPTY, mapping);
        }
        {
            // tag::delete-by-query-request
            DeleteByQueryRequest request = new DeleteByQueryRequest("source1", "source2"); // <1>
            // end::delete-by-query-request
            // tag::delete-by-query-request-conflicts
            request.setConflicts("proceed"); // <1>
            // end::delete-by-query-request-conflicts
            // tag::delete-by-query-request-typeOrQuery
            request.setDocTypes("doc"); // <1>
            request.setQuery(new TermQueryBuilder("user", "kimchy")); // <2>
            // end::delete-by-query-request-typeOrQuery
            // tag::delete-by-query-request-size
            request.setSize(10); // <1>
            // end::delete-by-query-request-size
            // tag::delete-by-query-request-scrollSize
            request.setBatchSize(100); // <1>
            // end::delete-by-query-request-scrollSize
            // tag::delete-by-query-request-timeout
            request.setTimeout(TimeValue.timeValueMinutes(2)); // <1>
            // end::delete-by-query-request-timeout
            // tag::delete-by-query-request-refresh
            request.setRefresh(true); // <1>
            // end::delete-by-query-request-refresh
            // tag::delete-by-query-request-slices
            request.setSlices(2); // <1>
            // end::delete-by-query-request-slices
            // tag::delete-by-query-request-scroll
            request.setScroll(TimeValue.timeValueMinutes(10)); // <1>
            // end::delete-by-query-request-scroll
            // tag::delete-by-query-request-routing
            request.setRouting("=cat"); // <1>
            // end::delete-by-query-request-routing
            // tag::delete-by-query-request-indicesOptions
            request.setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN); // <1>
            // end::delete-by-query-request-indicesOptions

            // tag::delete-by-query-execute
            BulkByScrollResponse bulkResponse = client.deleteByQuery(request, RequestOptions.DEFAULT);
            // end::delete-by-query-execute
            assertSame(0, bulkResponse.getSearchFailures().size());
            assertSame(0, bulkResponse.getBulkFailures().size());
            // tag::delete-by-query-response
            TimeValue timeTaken = bulkResponse.getTook(); // <1>
            boolean timedOut = bulkResponse.isTimedOut(); // <2>
            long totalDocs = bulkResponse.getTotal(); // <3>
            long deletedDocs = bulkResponse.getDeleted(); // <4>
            long batches = bulkResponse.getBatches(); // <5>
            long noops = bulkResponse.getNoops(); // <6>
            long versionConflicts = bulkResponse.getVersionConflicts(); // <7>
            long bulkRetries = bulkResponse.getBulkRetries(); // <8>
            long searchRetries = bulkResponse.getSearchRetries(); // <9>
            TimeValue throttledMillis = bulkResponse.getStatus().getThrottled(); // <10>
            TimeValue throttledUntilMillis = bulkResponse.getStatus().getThrottledUntil(); // <11>
            List<ScrollableHitSource.SearchFailure> searchFailures = bulkResponse.getSearchFailures(); // <12>
            List<BulkItemResponse.Failure> bulkFailures = bulkResponse.getBulkFailures(); // <13>
            // end::delete-by-query-response
        }
        {
            DeleteByQueryRequest request = new DeleteByQueryRequest();
            request.indices("source1");

            // tag::delete-by-query-execute-listener
            ActionListener<BulkByScrollResponse> listener = new ActionListener<BulkByScrollResponse>() {
                @Override
                public void onResponse(BulkByScrollResponse bulkResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::delete-by-query-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-by-query-execute-async
            client.deleteByQueryAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-by-query-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGet() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            Request createIndex = new Request("PUT", "/posts");
            createIndex.setJsonEntity(
                    "{\n" +
                    "    \"mappings\" : {\n" +
                    "        \"doc\" : {\n" +
                    "            \"properties\" : {\n" +
                    "                \"message\" : {\n" +
                    "                    \"type\": \"text\",\n" +
                    "                    \"store\": true\n" +
                    "                }\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}");
            Response response = client().performRequest(createIndex);
            assertEquals(200, response.getStatusLine().getStatusCode());

            IndexRequest indexRequest = new IndexRequest("posts", "doc", "1")
                    .source("user", "kimchy",
                            "postDate", new Date(),
                            "message", "trying out Elasticsearch");
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
        }
        {
            //tag::get-request
            GetRequest getRequest = new GetRequest(
                    "posts", // <1>
                    "doc",  // <2>
                    "1");   // <3>
            //end::get-request

            //tag::get-execute
            GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
            //end::get-execute
            assertTrue(getResponse.isExists());
            assertEquals(3, getResponse.getSourceAsMap().size());
            //tag::get-response
            String index = getResponse.getIndex();
            String type = getResponse.getType();
            String id = getResponse.getId();
            if (getResponse.isExists()) {
                long version = getResponse.getVersion();
                String sourceAsString = getResponse.getSourceAsString();        // <1>
                Map<String, Object> sourceAsMap = getResponse.getSourceAsMap(); // <2>
                byte[] sourceAsBytes = getResponse.getSourceAsBytes();          // <3>
            } else {
                // <4>
            }
            //end::get-response
        }
        {
            GetRequest request = new GetRequest("posts", "doc", "1");
            //tag::get-request-no-source
            request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE); // <1>
            //end::get-request-no-source
            GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
            assertNull(getResponse.getSourceInternal());
        }
        {
            GetRequest request = new GetRequest("posts", "doc", "1");
            //tag::get-request-source-include
            String[] includes = new String[]{"message", "*Date"};
            String[] excludes = Strings.EMPTY_ARRAY;
            FetchSourceContext fetchSourceContext =
                    new FetchSourceContext(true, includes, excludes);
            request.fetchSourceContext(fetchSourceContext); // <1>
            //end::get-request-source-include
            GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            assertEquals(2, sourceAsMap.size());
            assertEquals("trying out Elasticsearch", sourceAsMap.get("message"));
            assertTrue(sourceAsMap.containsKey("postDate"));
        }
        {
            GetRequest request = new GetRequest("posts", "doc", "1");
            //tag::get-request-source-exclude
            String[] includes = Strings.EMPTY_ARRAY;
            String[] excludes = new String[]{"message"};
            FetchSourceContext fetchSourceContext =
                    new FetchSourceContext(true, includes, excludes);
            request.fetchSourceContext(fetchSourceContext); // <1>
            //end::get-request-source-exclude
            GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            assertEquals(2, sourceAsMap.size());
            assertEquals("kimchy", sourceAsMap.get("user"));
            assertTrue(sourceAsMap.containsKey("postDate"));
        }
        {
            GetRequest request = new GetRequest("posts", "doc", "1");
            //tag::get-request-stored
            request.storedFields("message"); // <1>
            GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
            String message = getResponse.getField("message").getValue(); // <2>
            //end::get-request-stored
            assertEquals("trying out Elasticsearch", message);
            assertEquals(1, getResponse.getFields().size());
            assertNull(getResponse.getSourceInternal());
        }
        {
            GetRequest request = new GetRequest("posts", "doc", "1");
            //tag::get-request-routing
            request.routing("routing"); // <1>
            //end::get-request-routing
            //tag::get-request-preference
            request.preference("preference"); // <1>
            //end::get-request-preference
            //tag::get-request-realtime
            request.realtime(false); // <1>
            //end::get-request-realtime
            //tag::get-request-refresh
            request.refresh(true); // <1>
            //end::get-request-refresh
            //tag::get-request-version
            request.version(2); // <1>
            //end::get-request-version
            //tag::get-request-version-type
            request.versionType(VersionType.EXTERNAL); // <1>
            //end::get-request-version-type
        }
        {
            GetRequest request = new GetRequest("posts", "doc", "1");

            // tag::get-execute-listener
            ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
                @Override
                public void onResponse(GetResponse getResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            //tag::get-execute-async
            client.getAsync(request, RequestOptions.DEFAULT, listener); // <1>
            //end::get-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
        {
            //tag::get-indexnotfound
            GetRequest request = new GetRequest("does_not_exist", "doc", "1");
            try {
                GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
            } catch (ElasticsearchException e) {
                if (e.status() == RestStatus.NOT_FOUND) {
                    // <1>
                }
            }
            //end::get-indexnotfound
        }
        {
            // tag::get-conflict
            try {
                GetRequest request = new GetRequest("posts", "doc", "1").version(2);
                GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
            } catch (ElasticsearchException exception) {
                if (exception.status() == RestStatus.CONFLICT) {
                    // <1>
                }
            }
            // end::get-conflict
        }
    }

    public void testExists() throws Exception {
        RestHighLevelClient client = highLevelClient();
        // tag::exists-request
        GetRequest getRequest = new GetRequest(
            "posts", // <1>
            "doc",   // <2>
            "1");    // <3>
        getRequest.fetchSourceContext(new FetchSourceContext(false)); // <4>
        getRequest.storedFields("_none_");                            // <5>
        // end::exists-request
        {
            // tag::exists-execute
            boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
            // end::exists-execute
            assertFalse(exists);
        }
        {
            // tag::exists-execute-listener
            ActionListener<Boolean> listener = new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean exists) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::exists-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::exists-execute-async
            client.existsAsync(getRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::exists-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testBulkProcessor() throws InterruptedException {
        RestHighLevelClient client = highLevelClient();
        {
            // tag::bulk-processor-init
            BulkProcessor.Listener listener = new BulkProcessor.Listener() { // <1>
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    // <2>
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request,
                        BulkResponse response) {
                    // <3>
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    // <4>
                }
            };

            BulkProcessor bulkProcessor = BulkProcessor.builder(
                            (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                            listener).build(); // <5>
            // end::bulk-processor-init
            assertNotNull(bulkProcessor);

            // tag::bulk-processor-add
            IndexRequest one = new IndexRequest("posts", "doc", "1").
                    source(XContentType.JSON, "title",
                            "In which order are my Elasticsearch queries executed?");
            IndexRequest two = new IndexRequest("posts", "doc", "2")
                    .source(XContentType.JSON, "title",
                            "Current status and upcoming changes in Elasticsearch");
            IndexRequest three = new IndexRequest("posts", "doc", "3")
                    .source(XContentType.JSON, "title",
                            "The Future of Federated Search in Elasticsearch");

            bulkProcessor.add(one);
            bulkProcessor.add(two);
            bulkProcessor.add(three);
            // end::bulk-processor-add

            // tag::bulk-processor-await
            boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS); // <1>
            // end::bulk-processor-await
            assertTrue(terminated);

            // tag::bulk-processor-close
            bulkProcessor.close();
            // end::bulk-processor-close
        }
        {
            // tag::bulk-processor-listener
            BulkProcessor.Listener listener = new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    int numberOfActions = request.numberOfActions(); // <1>
                    logger.debug("Executing bulk [{}] with {} requests",
                            executionId, numberOfActions);
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request,
                        BulkResponse response) {
                    if (response.hasFailures()) { // <2>
                        logger.warn("Bulk [{}] executed with failures", executionId);
                    } else {
                        logger.debug("Bulk [{}] completed in {} milliseconds",
                                executionId, response.getTook().getMillis());
                    }
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    logger.error("Failed to execute bulk", failure); // <3>
                }
            };
            // end::bulk-processor-listener

            // tag::bulk-processor-options
            BulkProcessor.Builder builder = BulkProcessor.builder(
                    (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener);
            builder.setBulkActions(500); // <1>
            builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB)); // <2>
            builder.setConcurrentRequests(0); // <3>
            builder.setFlushInterval(TimeValue.timeValueSeconds(10L)); // <4>
            builder.setBackoffPolicy(BackoffPolicy
                    .constantBackoff(TimeValue.timeValueSeconds(1L), 3)); // <5>
            // end::bulk-processor-options
        }
    }

    public void testMultiGet() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            Request createIndex = new Request("PUT", "/index");
            createIndex.setJsonEntity(
                    "{\n" +
                    "    \"mappings\" : {\n" +
                    "        \"type\" : {\n" +
                    "            \"properties\" : {\n" +
                    "                \"foo\" : {\n" +
                    "                    \"type\": \"text\",\n" +
                    "                    \"store\": true\n" +
                    "                }\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}");
            Response response = client().performRequest(createIndex);
            assertEquals(200, response.getStatusLine().getStatusCode());
        }

        Map<String, Object> source = new HashMap<>();
        source.put("foo", "val1");
        source.put("bar", "val2");
        source.put("baz", "val3");
        client.index(new IndexRequest("index", "type", "example_id")
            .source(source)
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);

        {
            // tag::multi-get-request
            MultiGetRequest request = new MultiGetRequest();
            request.add(new MultiGetRequest.Item(
                "index",         // <1>
                "type",          // <2>
                "example_id"));  // <3>
            request.add(new MultiGetRequest.Item("index", "type", "another_id"));  // <4>
            // end::multi-get-request

            // Add a missing index so we can test it.
            request.add(new MultiGetRequest.Item("missing_index", "type", "id"));

            // tag::multi-get-request-item-extras
            request.add(new MultiGetRequest.Item("index", "type", "with_routing")
                .routing("some_routing"));          // <1>
            request.add(new MultiGetRequest.Item("index", "type", "with_version")
                .versionType(VersionType.EXTERNAL)  // <2>
                .version(10123L));                  // <3>
            // end::multi-get-request-item-extras
            // tag::multi-get-request-top-level-extras
            request.preference("some_preference");  // <1>
            request.realtime(false);                // <2>
            request.refresh(true);                  // <3>
            // end::multi-get-request-top-level-extras

            // tag::multi-get-execute
            MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
            // end::multi-get-execute

            // tag::multi-get-response
            MultiGetItemResponse firstItem = response.getResponses()[0];
            assertNull(firstItem.getFailure());              // <1>
            GetResponse firstGet = firstItem.getResponse();  // <2>
            String index = firstItem.getIndex();
            String type = firstItem.getType();
            String id = firstItem.getId();
            if (firstGet.isExists()) {
                long version = firstGet.getVersion();
                String sourceAsString = firstGet.getSourceAsString();        // <3>
                Map<String, Object> sourceAsMap = firstGet.getSourceAsMap(); // <4>
                byte[] sourceAsBytes = firstGet.getSourceAsBytes();          // <5>
            } else {
                // <6>
            }
            // end::multi-get-response

            assertTrue(firstGet.isExists());
            assertEquals(source, firstGet.getSource());

            MultiGetItemResponse missingIndexItem = response.getResponses()[2];
            // tag::multi-get-indexnotfound
            assertNull(missingIndexItem.getResponse());                // <1>
            Exception e = missingIndexItem.getFailure().getFailure();  // <2>
            ElasticsearchException ee = (ElasticsearchException) e;    // <3>
            // TODO status is broken! fix in a followup
            // assertEquals(RestStatus.NOT_FOUND, ee.status());        // <4>
            assertThat(e.getMessage(),
                containsString("reason=no such index"));               // <5>
            // end::multi-get-indexnotfound

            // tag::multi-get-execute-listener
            ActionListener<MultiGetResponse> listener = new ActionListener<MultiGetResponse>() {
                @Override
                public void onResponse(MultiGetResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::multi-get-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::multi-get-execute-async
            client.mgetAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::multi-get-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
        {
            MultiGetRequest request = new MultiGetRequest();
            // tag::multi-get-request-no-source
            request.add(new MultiGetRequest.Item("index", "type", "example_id")
                .fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE));  // <1>
            // end::multi-get-request-no-source
            MultiGetItemResponse item = unwrapAndAssertExample(client.mget(request, RequestOptions.DEFAULT));
            assertNull(item.getResponse().getSource());
        }
        {
            MultiGetRequest request = new MultiGetRequest();
            // tag::multi-get-request-source-include
            String[] includes = new String[] {"foo", "*r"};
            String[] excludes = Strings.EMPTY_ARRAY;
            FetchSourceContext fetchSourceContext =
                    new FetchSourceContext(true, includes, excludes);
            request.add(new MultiGetRequest.Item("index", "type", "example_id")
                .fetchSourceContext(fetchSourceContext));  // <1>
            // end::multi-get-request-source-include
            MultiGetItemResponse item = unwrapAndAssertExample(client.mget(request, RequestOptions.DEFAULT));
            assertThat(item.getResponse().getSource(), hasEntry("foo", "val1"));
            assertThat(item.getResponse().getSource(), hasEntry("bar", "val2"));
            assertThat(item.getResponse().getSource(), not(hasKey("baz")));
        }
        {
            MultiGetRequest request = new MultiGetRequest();
            // tag::multi-get-request-source-exclude
            String[] includes = Strings.EMPTY_ARRAY;
            String[] excludes = new String[] {"foo", "*r"};
            FetchSourceContext fetchSourceContext =
                    new FetchSourceContext(true, includes, excludes);
            request.add(new MultiGetRequest.Item("index", "type", "example_id")
                .fetchSourceContext(fetchSourceContext));  // <1>
            // end::multi-get-request-source-exclude
            MultiGetItemResponse item = unwrapAndAssertExample(client.mget(request, RequestOptions.DEFAULT));
            assertThat(item.getResponse().getSource(), not(hasKey("foo")));
            assertThat(item.getResponse().getSource(), not(hasKey("bar")));
            assertThat(item.getResponse().getSource(), hasEntry("baz", "val3"));
        }
        {
            MultiGetRequest request = new MultiGetRequest();
            // tag::multi-get-request-stored
            request.add(new MultiGetRequest.Item("index", "type", "example_id")
                .storedFields("foo"));  // <1>
            MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
            MultiGetItemResponse item = response.getResponses()[0];
            String value = item.getResponse().getField("foo").getValue(); // <2>
            // end::multi-get-request-stored
            assertNull(item.getResponse().getSource());
            assertEquals("val1", value);
        }
        {
            // tag::multi-get-conflict
            MultiGetRequest request = new MultiGetRequest();
            request.add(new MultiGetRequest.Item("index", "type", "example_id")
                .version(1000L));
            MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
            MultiGetItemResponse item = response.getResponses()[0];
            assertNull(item.getResponse());                          // <1>
            Exception e = item.getFailure().getFailure();            // <2>
            ElasticsearchException ee = (ElasticsearchException) e;  // <3>
            // TODO status is broken! fix in a followup
            // assertEquals(RestStatus.CONFLICT, ee.status());          // <4>
            assertThat(e.getMessage(),
                containsString("version conflict, current version [1] is "
                    + "different than the one provided [1000]"));    // <5>
            // end::multi-get-conflict
        }

    }

    private MultiGetItemResponse unwrapAndAssertExample(MultiGetResponse response) {
        assertThat(response.getResponses(), arrayWithSize(1));
        MultiGetItemResponse item = response.getResponses()[0];
        assertEquals("index", item.getIndex());
        assertEquals("type", item.getType());
        assertEquals("example_id", item.getId());
        return item;
    }
}
