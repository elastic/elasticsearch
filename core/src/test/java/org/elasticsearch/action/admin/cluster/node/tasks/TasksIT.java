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
package org.elasticsearch.action.admin.cluster.node.tasks;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Integration tests for task management API
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class TasksIT extends ESIntegTestCase {

    public void testTaskCounts() {
        // Run only on data nodes
        ListTasksResponse response = client().admin().cluster().prepareListTasks("data:true").setActions(ListTasksAction.NAME + "[n]").get();
        assertThat(response.getTasks().size(), greaterThanOrEqualTo(cluster().numDataNodes()));
    }
}
