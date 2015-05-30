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
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusAction;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeAction;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeSettingsAction;
import org.elasticsearch.action.exists.ExistsAction;
import org.elasticsearch.action.fieldstats.FieldStatsAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.repositories.VerifyNodeRepositoryAction;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.*;

/**
 * This test verifies that all of the action names follow our defined naming conventions.
 * Also, it verifies the backwards compatibility mechanism that allowed us to rename the action names in a bwc manner,
 * by converting the new name to the old one when needed.
 *
 * Actions that get added after 1.4.0 don't need any renaming/mapping, thus this test needs to be instructed to
 * ignore those actions by adding them to the post_1_4_actions set, as it is fine to not have a mapping for those.
 */
public class ActionNamesTests extends ElasticsearchIntegrationTest {

    private static final Set<String> post_1_4_actions = new HashSet<>();

    static {
        //add here new actions that don't need a mapping as they weren't available prior to 1.4
        post_1_4_actions.add(ExistsAction.NAME);
        post_1_4_actions.add(ExistsAction.NAME + "[s]");
        post_1_4_actions.add(GetIndexAction.NAME);
        post_1_4_actions.add(UnicastZenPing.ACTION_NAME_GTE_1_4);
        post_1_4_actions.add(SearchServiceTransportAction.FREE_CONTEXT_SCROLL_ACTION_NAME);
        post_1_4_actions.add(SearchServiceTransportAction.FETCH_ID_SCROLL_ACTION_NAME);
        post_1_4_actions.add(VerifyRepositoryAction.NAME);
        post_1_4_actions.add(VerifyNodeRepositoryAction.ACTION_NAME);
        post_1_4_actions.add(FieldStatsAction.NAME);
        post_1_4_actions.add(FieldStatsAction.NAME + "[s]");
        post_1_4_actions.add(SyncedFlushService.IN_FLIGHT_OPS_ACTION_NAME);
        post_1_4_actions.add(SyncedFlushService.PRE_SYNCED_FLUSH_ACTION_NAME);
        post_1_4_actions.add(SyncedFlushService.SYNCED_FLUSH_ACTION_NAME);
        post_1_4_actions.add(UpgradeAction.NAME);
        post_1_4_actions.add(UpgradeAction.NAME + "[s]");
        post_1_4_actions.add(UpgradeStatusAction.NAME);
        post_1_4_actions.add(UpgradeStatusAction.NAME + "[s]");
        post_1_4_actions.add(UpgradeSettingsAction.NAME);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testActionNamesCategories() throws NoSuchFieldException, IllegalAccessException {
        TransportService transportService = internalCluster().getInstance(TransportService.class);
        for (String action : transportService.serverHandlers.keySet()) {
            assertThat("action doesn't belong to known category", action, either(startsWith("indices:admin")).or(startsWith("indices:monitor"))
                    .or(startsWith("indices:data/read")).or(startsWith("indices:data/write"))
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
                assertPost14Action(action, version, outgoingAction);
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

            String incomingAction = ActionNames.incomingAction(action, version);
            if (version.onOrAfter(Version.V_1_4_0_Beta1) || customAction) {
                assertPost14Action(action, version, incomingAction);
            } else {
                assertThat(incomingAction, not(equalTo(action)));
                assertThat(incomingAction, equalTo(ActionNames.post_1_4_action(action)));
            }
        }
    }

    private static void assertPost14Action(String originalAction, Version version, String convertedAction) {
        if (version.before(Version.V_1_5_0)) {
            if (originalAction.equals(ShardStateAction.SHARD_STARTED_ACTION_NAME)) {
                assertThat(convertedAction, equalTo(ShardStateAction.SHARD_FAILED_ACTION_NAME));
                return;
            }
            if (originalAction.equals(ShardStateAction.SHARD_FAILED_ACTION_NAME)) {
                assertThat(convertedAction, equalTo(ShardStateAction.SHARD_STARTED_ACTION_NAME));
                return;
            }
        }
        assertThat(convertedAction, equalTo(originalAction));
    }
}
