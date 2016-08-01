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

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;

public class ParentTaskAssigningClientTests extends ESTestCase {
    public void testSetsParentId() {
        TaskId[] parentTaskId = new TaskId[] {new TaskId(randomAsciiOfLength(3), randomLong())};

        // This mock will do nothing but verify that parentTaskId is set on all requests sent to it.
        NoOpClient mock = new NoOpClient(getTestName()) {
            @Override
            protected <     Request extends ActionRequest<Request>,
                            Response extends ActionResponse,
                            RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>
                        > void doExecute( Action<Request, Response, RequestBuilder> action, Request request,
                            ActionListener<Response> listener) {
                assertEquals(parentTaskId[0], request.getParentTask());
                super.doExecute(action, request, listener);
            }
        };
        try (ParentTaskAssigningClient client = new ParentTaskAssigningClient(mock, parentTaskId[0])) {
            // All of these should have the parentTaskId set
            client.bulk(new BulkRequest());
            client.search(new SearchRequest());
            client.clearScroll(new ClearScrollRequest());

            // Now lets verify that unwrapped calls don't have the parentTaskId set
            parentTaskId[0] = TaskId.EMPTY_TASK_ID;
            client.unwrap().bulk(new BulkRequest());
            client.unwrap().search(new SearchRequest());
            client.unwrap().clearScroll(new ClearScrollRequest());
        }
    }
}
