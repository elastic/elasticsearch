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

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.tasks.TaskSubmissionResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.RawTaskStatus;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class ReindexIT extends ESRestHighLevelClientTestCase {

    public void testReindex() throws IOException {
        final String sourceIndex = "source1";
        final String destinationIndex = "dest";
        {
            // Prepare
            Settings settings = Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build();
            createIndex(sourceIndex, settings);
            createIndex(destinationIndex, settings);
            BulkRequest bulkRequest = new BulkRequest()
                .add(new IndexRequest(sourceIndex).id("1").source(Collections.singletonMap("foo", "bar"), XContentType.JSON))
                .add(new IndexRequest(sourceIndex).id("2").source(Collections.singletonMap("foo2", "bar2"), XContentType.JSON))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            assertEquals(
                RestStatus.OK,
                highLevelClient().bulk(
                    bulkRequest,
                    RequestOptions.DEFAULT
                ).status()
            );
        }
        {
            // reindex one document with id 1 from source to destination
            ReindexRequest reindexRequest = new ReindexRequest();
            reindexRequest.setSourceIndices(sourceIndex);
            reindexRequest.setDestIndex(destinationIndex);
            reindexRequest.setSourceQuery(new IdsQueryBuilder().addIds("1"));
            reindexRequest.setRefresh(true);

            BulkByScrollResponse bulkResponse = execute(reindexRequest, highLevelClient()::reindex, highLevelClient()::reindexAsync);

            assertEquals(1, bulkResponse.getCreated());
            assertEquals(1, bulkResponse.getTotal());
            assertEquals(0, bulkResponse.getDeleted());
            assertEquals(0, bulkResponse.getNoops());
            assertEquals(0, bulkResponse.getVersionConflicts());
            assertEquals(1, bulkResponse.getBatches());
            assertTrue(bulkResponse.getTook().getMillis() > 0);
            assertEquals(1, bulkResponse.getBatches());
            assertEquals(0, bulkResponse.getBulkFailures().size());
            assertEquals(0, bulkResponse.getSearchFailures().size());
        }
    }

    public void testReindexTask() throws Exception {
        final String sourceIndex = "source123";
        final String destinationIndex = "dest2";
        {
            // Prepare
            Settings settings = Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build();
            createIndex(sourceIndex, settings);
            createIndex(destinationIndex, settings);
            BulkRequest bulkRequest = new BulkRequest()
                .add(new IndexRequest(sourceIndex).id("1").source(Collections.singletonMap("foo", "bar"), XContentType.JSON))
                .add(new IndexRequest(sourceIndex).id("2").source(Collections.singletonMap("foo2", "bar2"), XContentType.JSON))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            assertEquals(
                RestStatus.OK,
                highLevelClient().bulk(
                    bulkRequest,
                    RequestOptions.DEFAULT
                ).status()
            );
        }
        {
            // tag::submit-reindex-task
            ReindexRequest reindexRequest = new ReindexRequest(); // <1>
            reindexRequest.setSourceIndices(sourceIndex);
            reindexRequest.setDestIndex(destinationIndex);
            reindexRequest.setRefresh(true);

            TaskSubmissionResponse reindexSubmission = highLevelClient()
                .submitReindexTask(reindexRequest, RequestOptions.DEFAULT); // <2>

            String taskId = reindexSubmission.getTask(); // <3>
            // end::submit-reindex-task

            assertBusy(checkTaskCompletionStatus(client(), taskId));
        }
    }

    public void testReindexConflict() throws IOException {
        final String sourceIndex = "testreindexconflict_source";
        final String destIndex = "testreindexconflict_dest";

        final Settings settings = Settings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .build();
        createIndex(sourceIndex, settings);
        createIndex(destIndex, settings);
        final BulkRequest bulkRequest = new BulkRequest()
            .add(new IndexRequest(sourceIndex).id("1").source(Collections.singletonMap("foo", "bar"), XContentType.JSON))
            .add(new IndexRequest(sourceIndex).id("2").source(Collections.singletonMap("foo", "bar"), XContentType.JSON))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        assertThat(highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT).status(), equalTo(RestStatus.OK));

        putConflictPipeline();

        final ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(sourceIndex);
        reindexRequest.setDestIndex(destIndex);
        reindexRequest.setRefresh(true);
        reindexRequest.setDestPipeline(CONFLICT_PIPELINE_ID);
        final BulkByScrollResponse response = highLevelClient().reindex(reindexRequest, RequestOptions.DEFAULT);

        assertThat(response.getVersionConflicts(), equalTo(2L));
        assertThat(response.getSearchFailures(), empty());
        assertThat(response.getBulkFailures(), hasSize(2));
        assertThat(
            response.getBulkFailures().stream().map(BulkItemResponse.Failure::getMessage).collect(Collectors.toSet()),
            everyItem(containsString("version conflict"))
        );

        assertThat(response.getTotal(), equalTo(2L));
        assertThat(response.getCreated(), equalTo(0L));
        assertThat(response.getUpdated(), equalTo(0L));
        assertThat(response.getDeleted(), equalTo(0L));
        assertThat(response.getNoops(), equalTo(0L));
        assertThat(response.getBatches(), equalTo(1));
        assertTrue(response.getTook().getMillis() > 0);
    }

    public void testDeleteByQuery() throws Exception {
        final String sourceIndex = "source1";
        {
            // Prepare
            Settings settings = Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build();
            createIndex(sourceIndex, settings);
            assertEquals(
                RestStatus.OK,
                highLevelClient().bulk(
                    new BulkRequest()
                        .add(new IndexRequest(sourceIndex).id("1")
                            .source(Collections.singletonMap("foo", 1), XContentType.JSON))
                        .add(new IndexRequest(sourceIndex).id("2")
                            .source(Collections.singletonMap("foo", 2), XContentType.JSON))
                        .add(new IndexRequest(sourceIndex).id("3")
                            .source(Collections.singletonMap("foo", 3), XContentType.JSON))
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                    RequestOptions.DEFAULT
                ).status()
            );
        }
        {
            // test1: delete one doc
            DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();
            deleteByQueryRequest.indices(sourceIndex);
            deleteByQueryRequest.setQuery(new IdsQueryBuilder().addIds("1"));
            deleteByQueryRequest.setRefresh(true);
            BulkByScrollResponse bulkResponse =
                execute(deleteByQueryRequest, highLevelClient()::deleteByQuery, highLevelClient()::deleteByQueryAsync);
            assertEquals(1, bulkResponse.getTotal());
            assertEquals(1, bulkResponse.getDeleted());
            assertEquals(0, bulkResponse.getNoops());
            assertEquals(0, bulkResponse.getVersionConflicts());
            assertEquals(1, bulkResponse.getBatches());
            assertTrue(bulkResponse.getTook().getMillis() > 0);
            assertEquals(1, bulkResponse.getBatches());
            assertEquals(0, bulkResponse.getBulkFailures().size());
            assertEquals(0, bulkResponse.getSearchFailures().size());
            assertEquals(
                2,
                highLevelClient().search(new SearchRequest(sourceIndex), RequestOptions.DEFAULT).getHits().getTotalHits().value
            );
        }
        {
            // test delete-by-query rethrottling
            DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();
            deleteByQueryRequest.indices(sourceIndex);
            deleteByQueryRequest.setQuery(new IdsQueryBuilder().addIds("2", "3"));
            deleteByQueryRequest.setRefresh(true);

            // this following settings are supposed to halt reindexing after first document
            deleteByQueryRequest.setBatchSize(1);
            deleteByQueryRequest.setRequestsPerSecond(0.00001f);
            final CountDownLatch taskFinished = new CountDownLatch(1);
            highLevelClient().deleteByQueryAsync(deleteByQueryRequest, RequestOptions.DEFAULT, new ActionListener<BulkByScrollResponse>() {

                @Override
                public void onResponse(BulkByScrollResponse response) {
                    taskFinished.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e.toString());
                }
            });

            TaskId taskIdToRethrottle = findTaskToRethrottle(DeleteByQueryAction.NAME, deleteByQueryRequest.getDescription());
            float requestsPerSecond = 1000f;
            ListTasksResponse response = execute(new RethrottleRequest(taskIdToRethrottle, requestsPerSecond),
                highLevelClient()::deleteByQueryRethrottle, highLevelClient()::deleteByQueryRethrottleAsync);
            assertThat(response.getTasks(), hasSize(1));
            assertEquals(taskIdToRethrottle, response.getTasks().get(0).getTaskId());
            assertThat(response.getTasks().get(0).getStatus(), instanceOf(RawTaskStatus.class));
            assertEquals(Float.toString(requestsPerSecond),
                ((RawTaskStatus) response.getTasks().get(0).getStatus()).toMap().get("requests_per_second").toString());
            assertTrue(taskFinished.await(10, TimeUnit.SECONDS));

            // any rethrottling after the delete-by-query is done performed with the same taskId should result in a failure
            response = execute(new RethrottleRequest(taskIdToRethrottle, requestsPerSecond),
                highLevelClient()::deleteByQueryRethrottle, highLevelClient()::deleteByQueryRethrottleAsync);
            assertTrue(response.getTasks().isEmpty());
            assertFalse(response.getNodeFailures().isEmpty());
            assertEquals(1, response.getNodeFailures().size());
            assertEquals("Elasticsearch exception [type=resource_not_found_exception, reason=task [" + taskIdToRethrottle + "] is missing]",
                response.getNodeFailures().get(0).getCause().getMessage());
        }
    }

    public void testDeleteByQueryTask() throws Exception {
        final String sourceIndex = "source456";
        {
            // Prepare
            Settings settings = Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build();
            createIndex(sourceIndex, settings);
            assertEquals(
                RestStatus.OK,
                highLevelClient().bulk(
                    new BulkRequest()
                        .add(new IndexRequest(sourceIndex).id("1")
                            .source(Collections.singletonMap("foo", 1), XContentType.JSON))
                        .add(new IndexRequest(sourceIndex).id("2")
                            .source(Collections.singletonMap("foo", 2), XContentType.JSON))
                        .add(new IndexRequest(sourceIndex).id("3")
                            .source(Collections.singletonMap("foo", 3), XContentType.JSON))
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                    RequestOptions.DEFAULT
                ).status()
            );
        }
        {
            // tag::submit-delete_by_query-task
            DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();
            deleteByQueryRequest.indices(sourceIndex);
            deleteByQueryRequest.setQuery(new IdsQueryBuilder().addIds("1"));
            deleteByQueryRequest.setRefresh(true);

            TaskSubmissionResponse deleteByQuerySubmission = highLevelClient()
                .submitDeleteByQueryTask(deleteByQueryRequest, RequestOptions.DEFAULT);

            String taskId = deleteByQuerySubmission.getTask();
            // end::submit-delete_by_query-task

            assertBusy(checkTaskCompletionStatus(client(), taskId));
        }
    }
}
