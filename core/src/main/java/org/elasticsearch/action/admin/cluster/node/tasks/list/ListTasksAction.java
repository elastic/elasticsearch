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

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Action for retrieving a list of currently running tasks
 */
public class ListTasksAction extends Action<ListTasksRequest, ListTasksResponse, ListTasksRequestBuilder> {

    public static final ListTasksAction INSTANCE = new ListTasksAction();
    public static final String NAME = "cluster:monitor/tasks/lists";

    private ListTasksAction() {
        super(NAME);
    }

    @Override
    public ListTasksResponse newResponse() {
        return new ListTasksResponse();
    }

    @Override
    public ListTasksRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new ListTasksRequestBuilder(client, this);
    }
}
