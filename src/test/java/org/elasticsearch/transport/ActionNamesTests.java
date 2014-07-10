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

import org.elasticsearch.action.bench.AbortBenchmarkAction;
import org.elasticsearch.action.bench.BenchmarkAction;
import org.elasticsearch.action.bench.BenchmarkService;
import org.elasticsearch.action.bench.BenchmarkStatusAction;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.*;

public class ActionNamesTests extends ElasticsearchIntegrationTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testActionNamesCategories() throws NoSuchFieldException, IllegalAccessException {
        TransportService transportService = internalCluster().getInstance(TransportService.class);
        for (String action : transportService.serverHandlers.keySet()) {
            assertThat("action doesn't belong to known category", action, either(startsWith("indices:admin")).or(startsWith("indices:monitor"))
                    .or(startsWith("indices:data/read")).or(startsWith("indices:data/write"))
                    .or(startsWith("indices:data/benchmark"))
                    .or(startsWith("cluster:admin")).or(startsWith("cluster:monitor"))
                    .or(startsWith("internal:")));
        }
    }

    @Test
    public void testActionNamesMapping() {
        TransportService transportService = internalCluster().getInstance(TransportService.class);
        for (String action : transportService.serverHandlers.keySet()) {
            if (post_1_3_actions.contains(action)) {
                continue;
            }
            String oldName = transportService.actionNamesMapping.get(action);
            assertThat("no pre 1.4 name for action " + action, oldName, notNullValue());
            String newName = transportService.actionNamesMapping.inverse().get(oldName);
            assertThat(newName, equalTo(action));
        }
    }

    private static final Set<String> post_1_3_actions = new HashSet<>();

    static {
        //add here new actions that don't need a mapping as they weren't available prior to 1.4
        post_1_3_actions.add(BenchmarkService.STATUS_ACTION_NAME);
        post_1_3_actions.add(BenchmarkService.START_ACTION_NAME);
        post_1_3_actions.add(BenchmarkService.ABORT_ACTION_NAME);
        post_1_3_actions.add(BenchmarkAction.NAME);
        post_1_3_actions.add(BenchmarkStatusAction.NAME);
        post_1_3_actions.add(AbortBenchmarkAction.NAME);
    }
}
