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
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequestBuilder;
import org.elasticsearch.index.reindex.RethrottleAction;
import org.elasticsearch.index.reindex.RethrottleRequestBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collections;

public class ReindexDocumentationIT extends ESIntegTestCase {

    public void reindex() {
        Client client = client();
        // tag::reindex1
        BulkByScrollResponse response = new ReindexRequestBuilder(client, ReindexAction.INSTANCE)
            .destination("target_index")
            .filter(QueryBuilders.matchQuery("category", "xzy")) // <1>
            .get();
        // end::reindex1
    }

    public void updateByQuery() {
        Client client = client();
        {
            // tag::update-by-query
            UpdateByQueryRequestBuilder updateByQuery = new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
            updateByQuery.source("source_index").abortOnVersionConflict(false);
            BulkByScrollResponse response = updateByQuery.get();
            // end::update-by-query
        }
        {
            // tag::update-by-query-filter
            UpdateByQueryRequestBuilder updateByQuery = new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
            updateByQuery.source("source_index")
                .filter(QueryBuilders.termQuery("level", "awesome"))
                .size(1000)
                .script(new Script(ScriptType.INLINE, "ctx._source.awesome = 'absolutely'", "painless", Collections.emptyMap()));
            BulkByScrollResponse response = updateByQuery.get();
            // end::update-by-query-filter
        }
        {
            // tag::update-by-query-size
            UpdateByQueryRequestBuilder updateByQuery = new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
            updateByQuery.source("source_index")
                .source().setSize(500);
            BulkByScrollResponse response = updateByQuery.get();
            // end::update-by-query-size
        }
        {
            // tag::update-by-query-sort
            UpdateByQueryRequestBuilder updateByQuery = new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
            updateByQuery.source("source_index").size(100)
                .source().addSort("cat", SortOrder.DESC);
            BulkByScrollResponse response = updateByQuery.get();
            // end::update-by-query-sort
        }
        {
            // tag::update-by-query-script
            UpdateByQueryRequestBuilder updateByQuery = new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
            updateByQuery.source("source_index")
                .script(new Script(
                    ScriptType.INLINE,
                    "if (ctx._source.awesome == 'absolutely') {"
                        + "  ctx.op='noop'"
                        + "} else if (ctx._source.awesome == 'lame') {"
                        + "  ctx.op='delete'"
                        + "} else {"
                        + "ctx._source.awesome = 'absolutely'}",
                    "painless",
                    Collections.emptyMap()));
            BulkByScrollResponse response = updateByQuery.get();
            // end::update-by-query-script
        }
        {
            // tag::update-by-query-multi-index
            UpdateByQueryRequestBuilder updateByQuery = new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
            updateByQuery.source("foo", "bar").source().setTypes("a", "b");
            BulkByScrollResponse response = updateByQuery.get();
            // end::update-by-query-multi-index
        }
        {
            // tag::update-by-query-routing
            UpdateByQueryRequestBuilder updateByQuery = new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
            updateByQuery.source().setRouting("cat");
            BulkByScrollResponse response = updateByQuery.get();
            // end::update-by-query-routing
        }
        {
            // tag::update-by-query-pipeline
            UpdateByQueryRequestBuilder updateByQuery = new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
            updateByQuery.setPipeline("hurray");
            BulkByScrollResponse response = updateByQuery.get();
            // end::update-by-query-pipeline
        }
        {
            // tag::update-by-query-list-tasks
            ListTasksResponse tasksList = client.admin().cluster().prepareListTasks()
                .setActions(UpdateByQueryAction.NAME).setDetailed(true).get();
            for (TaskInfo info: tasksList.getTasks()) {
                TaskId taskId = info.getTaskId();
                BulkByScrollTask.Status status = (BulkByScrollTask.Status) info.getStatus();
                // do stuff
            }
            // end::update-by-query-list-tasks
        }
        {
            TaskId taskId = null;
            // tag::update-by-query-get-task
            GetTaskResponse get = client.admin().cluster().prepareGetTask(taskId).get();
            // end::update-by-query-get-task
        }
        {
            TaskId taskId = null;
            // tag::update-by-query-cancel-task
            // Cancel all update-by-query requests
            client.admin().cluster().prepareCancelTasks().setActions(UpdateByQueryAction.NAME).get().getTasks();
            // Cancel a specific update-by-query request
            client.admin().cluster().prepareCancelTasks().setTaskId(taskId).get().getTasks();
            // end::update-by-query-cancel-task
        }
        {
            TaskId taskId = null;
            // tag::update-by-query-rethrottle
            new RethrottleRequestBuilder(client, RethrottleAction.INSTANCE)
                .setTaskId(taskId)
                .setRequestsPerSecond(2.0f)
                .get();
            // end::update-by-query-rethrottle
        }
    }

    public void deleteByQuery() {
        Client client = client();
        // tag::delete-by-query-sync
        BulkByScrollResponse response = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
            .filter(QueryBuilders.matchQuery("gender", "male")) // <1>
            .source("persons")                                  // <2>
            .get();                                             // <3>
        long deleted = response.getDeleted();                   // <4>
        // end::delete-by-query-sync

        // tag::delete-by-query-async
        new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
            .filter(QueryBuilders.matchQuery("gender", "male"))     // <1>
            .source("persons")                                      // <2>
            .execute(new ActionListener<BulkByScrollResponse>() {   // <3>
                @Override
                public void onResponse(BulkByScrollResponse response) {
                    long deleted = response.getDeleted();           // <4>
                }
                @Override
                public void onFailure(Exception e) {
                    // Handle the exception
                }
            });
        // end::delete-by-query-async
    }

}
