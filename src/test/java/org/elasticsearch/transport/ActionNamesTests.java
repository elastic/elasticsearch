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

import com.google.common.collect.Lists;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.bench.AbortBenchmarkAction;
import org.elasticsearch.action.bench.BenchmarkAction;
import org.elasticsearch.action.bench.BenchmarkService;
import org.elasticsearch.action.bench.BenchmarkStatusAction;
import org.elasticsearch.action.exists.ExistsAction;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.*;

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
            if (post_1_4_actions.contains(action)) {
                continue;
            }
            String pre_1_4_action = ActionNames.pre_1_4_Action(action);
            assertThat("no pre 1.4 name for action " + action, pre_1_4_action, notNullValue());
            String post_1_4_action = ActionNames.post_1_4_action(pre_1_4_action);
            assertThat(post_1_4_action, equalTo(action));
        }
    }

    @Test
    public void testOutgoingAction() {
        TransportService transportService = internalCluster().getInstance(TransportService.class);
        List<String> actions = Lists.newArrayList(transportService.serverHandlers.keySet());

        int iters = iterations(10, 100);
        for (int i = 0; i < iters; i++) {
            //we rarely use a custom action since plugins might inject their own actions
            boolean customAction = rarely();
            String action;
            if (customAction) {
                do {
                    action = randomAsciiOfLength(randomInt(30));
                } while(actions.contains(action));
            } else {
                action = randomFrom(actions);
            }

            Version version = randomVersion();
            String outgoingAction = ActionNames.outgoingAction(action, version);
            if (version.onOrAfter(Version.V_1_4_0_Beta1) || customAction || post_1_4_actions.contains(action)) {
                assertThat(outgoingAction, equalTo(action));
            } else {
                assertThat(outgoingAction, not(equalTo(action)));
                assertThat(outgoingAction, equalTo(ActionNames.pre_1_4_Action(action)));
            }
        }
    }

    @Test
    public void testIncomingAction() {
        List<String> pre_1_4_names = Lists.newArrayList(ActionNames.ACTION_NAMES.inverse().keySet());
        TransportService transportService = internalCluster().getInstance(TransportService.class);
        List<String> actions = Lists.newArrayList(transportService.serverHandlers.keySet());

        Version version = randomVersion();
        int iters = iterations(10, 100);
        for (int i = 0; i < iters; i++) {
            //we rarely use a custom action since plugins might inject their own actions
            boolean customAction = rarely();
            String action;
            if (customAction) {
                do {
                    action = randomAsciiOfLength(randomInt(30));
                } while(pre_1_4_names.contains(action));
            } else {
                if (version.before(Version.V_1_4_0_Beta1)) {
                    action = randomFrom(pre_1_4_names);
                } else {
                    action = randomFrom(actions);
                }
            }

            String incomingAction = ActionNames.incomingAction  (action, version);
            if (version.onOrAfter(Version.V_1_4_0_Beta1) || customAction) {
                assertThat(incomingAction, equalTo(action));
            } else {
                assertThat(incomingAction, not(equalTo(action)));
                assertThat(incomingAction, equalTo(ActionNames.post_1_4_action(action)));
            }
        }
    }

    private static final Set<String> post_1_4_actions = new HashSet<>();

    static {
        //add here new actions that don't need a mapping as they weren't available prior to 1.4
        post_1_4_actions.add(BenchmarkService.STATUS_ACTION_NAME);
        post_1_4_actions.add(BenchmarkService.START_ACTION_NAME);
        post_1_4_actions.add(BenchmarkService.ABORT_ACTION_NAME);
        post_1_4_actions.add(BenchmarkAction.NAME);
        post_1_4_actions.add(BenchmarkStatusAction.NAME);
        post_1_4_actions.add(AbortBenchmarkAction.NAME);
        post_1_4_actions.add(ExistsAction.NAME);
        post_1_4_actions.add(ExistsAction.NAME + "[s]");
        post_1_4_actions.add(GetIndexAction.NAME);
        post_1_4_actions.add(SearchServiceTransportAction.FREE_CONTEXT_SCROLL_ACTION_NAME);
        post_1_4_actions.add(SearchServiceTransportAction.FETCH_ID_SCROLL_ACTION_NAME);
    }
}
