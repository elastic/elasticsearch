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

package org.elasticsearch.transport;

import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.startsWith;

/**
 * This test verifies that all of the action names follow our defined naming conventions.
 * The identified categories are:
 * - indices:admin: apis that allow to perform administration tasks against indices
 * - indices:data: apis that are about data
 * - indices:read: apis that read data
 * - indices:write: apis that write data
 * - cluster:admin: cluster apis that allow to perform administration tasks
 * - cluster:monitor: cluster apis that allow to monitor the system
 * - internal: internal actions that are used from node to node but not directly exposed to users
 *
 * Any transport action belongs to one of the above categories and its name starts with its category, followed by a '/'
 * and the name of the api itself (e.g. cluster:admin/nodes/restart).
 * When an api exposes multiple transport handlers, some of which are invoked internally during the execution of the api,
 * we use the `[n]` suffix to identify node actions and the `[s]` suffix to identify shard actions.
 */
public class ActionNamesTests extends ElasticsearchIntegrationTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testActionNamesCategories() throws NoSuchFieldException, IllegalAccessException {
        TransportService transportService = internalCluster().getInstance(TransportService.class);
        for (String action : transportService.requestHandlers.keySet()) {
            assertThat("action doesn't belong to known category", action, either(startsWith("indices:admin")).or(startsWith("indices:monitor"))
                    .or(startsWith("indices:data/read")).or(startsWith("indices:data/write"))
                    .or(startsWith("cluster:admin")).or(startsWith("cluster:monitor"))
                    .or(startsWith("internal:")));
        }
    }
}
