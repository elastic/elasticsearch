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

import org.elasticsearch.Version;
import org.elasticsearch.action.benchmark.abort.BenchmarkAbortAction;
import org.elasticsearch.action.benchmark.pause.BenchmarkPauseAction;
import org.elasticsearch.action.benchmark.resume.BenchmarkResumeAction;
import org.elasticsearch.action.benchmark.start.BenchmarkStartAction;
import org.elasticsearch.action.benchmark.status.BenchmarkStatusAction;
import org.elasticsearch.action.exists.ExistsAction;
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
        String[] actions = transportService.serverHandlers.keySet().toArray(new String[transportService.serverHandlers.keySet().size()]);

        int iters = iterations(10, 100);
        for (int i = 0; i < iters; i++) {
            boolean customAction = rarely();
            String action;
            if (customAction) {
                action = randomAsciiOfLength(randomInt(30));
            } else {
                action = randomFrom(actions);
            }

            Version version = randomVersion();
            String outgoingAction = ActionNames.outgoingAction(action, version);
            if (version.onOrAfter(Version.V_1_4_0) || customAction || post_1_4_actions.contains(action)) {
                assertThat(outgoingAction, equalTo(action));
            } else {
                assertThat(outgoingAction, not(equalTo(action)));
                assertThat(outgoingAction, equalTo(ActionNames.pre_1_4_Action(action)));
            }
        }
    }

    @Test
    public void testIncomingAction() {
        String[] pre_1_4_names = ActionNames.ACTION_NAMES.inverse().keySet().toArray(new String[ActionNames.ACTION_NAMES.inverse().keySet().size()]);
        TransportService transportService = internalCluster().getInstance(TransportService.class);
        String[] actions = transportService.serverHandlers.keySet().toArray(new String[transportService.serverHandlers.keySet().size()]);

        Version version = randomVersion();
        int iters = iterations(10, 100);
        for (int i = 0; i < iters; i++) {
            boolean customAction = rarely();
            String action;
            if (customAction) {
                action = randomAsciiOfLength(randomInt(30));
            } else {
                if (version.before(Version.V_1_4_0)) {
                    action = randomFrom(pre_1_4_names);
                } else {
                    action = randomFrom(actions);
                }
            }

            String incomingAction = ActionNames.incomingAction  (action, version);
            if (version.onOrAfter(Version.V_1_4_0) || customAction) {
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
        post_1_4_actions.add(BenchmarkStatusAction.NAME);
        post_1_4_actions.add(BenchmarkStartAction.NAME);
        post_1_4_actions.add(BenchmarkPauseAction.NAME);
        post_1_4_actions.add(BenchmarkResumeAction.NAME);
        post_1_4_actions.add(BenchmarkAbortAction.NAME);
        post_1_4_actions.add(ExistsAction.NAME);
        post_1_4_actions.add(ExistsAction.NAME + "[s]");
    }
}
