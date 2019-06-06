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

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.action.support.tasks.TasksRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Builder for the request to retrieve the list of tasks running on the specified nodes
 */
public class ListTasksRequestBuilder extends TasksRequestBuilder<ListTasksRequest, ListTasksResponse, ListTasksRequestBuilder> {

    public ListTasksRequestBuilder(ElasticsearchClient client, ListTasksAction action) {
        super(client, action, new ListTasksRequest());
    }

    /**
     * Should detailed task information be returned.
     */
    public ListTasksRequestBuilder setDetailed(boolean detailed) {
        request.setDetailed(detailed);
        return this;
    }

    /**
     * Should this request wait for all found tasks to complete?
     */
    public final ListTasksRequestBuilder setWaitForCompletion(boolean waitForCompletion) {
        request.setWaitForCompletion(waitForCompletion);
        return this;
    }
}
