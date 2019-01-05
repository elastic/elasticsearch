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

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.tasks.TaskSubmissionResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.function.BooleanSupplier;

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

    public void testReindexTask() throws IOException, InterruptedException {
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

            BooleanSupplier hasUpgradeCompleted = checkCompletionStatus(taskId);
            awaitBusy(hasUpgradeCompleted);
        }
    }

    private BooleanSupplier checkCompletionStatus(String taskId) {
        return () -> {
            try {
                Response response = client().performRequest(new Request("GET", "/_tasks/" + taskId));
                return (boolean) entityAsMap(response).get("completed");
            } catch (IOException e) {
                fail(e.getMessage());
                return false;
            }
        };
    }
}
