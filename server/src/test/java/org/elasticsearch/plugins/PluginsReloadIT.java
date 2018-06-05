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

package org.elasticsearch.plugins;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.admin.cluster.RestReloadSecureSettingsAction;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class PluginsReloadIT extends ESIntegTestCase {

    public void testPasswordBroadcast() throws Exception {
//        final NodeClient nodeClient = internalCluster().getInstance(NodeClient.class);
//        final RestReloadSecureSettingsAction reloadSettingsAction = new RestReloadSecureSettingsAction(Settings.EMPTY,
//                mock(RestController.class));
//        final RestRequest reloadSettingsRequest = new FakeRestRequest();
//        reloadSettingsRequest.params().put("secure_settings_password", "password");
//        final CountDownLatch reloadSettingsLatch = new CountDownLatch(1);
//        final AtomicReference<AssertionError> reloadSettingsError = new AtomicReference<>();
//        reloadSettingsAction.handleRequest(reloadSettingsRequest, new AbstractRestChannel(reloadSettingsRequest, true) {
//            @Override
//            public void sendResponse(RestResponse response) {
//                try {
//                    assertThat(response.content().utf8ToString(), not(containsString("not_used_but_this_is_a_secret")));
//                } catch (final AssertionError ex) {
//                    reloadSettingsError.set(ex);
//                }
//                reloadSettingsLatch.countDown();
//            }
//        }, nodeClient);
//        reloadSettingsLatch.await();
//        if (reloadSettingsError.get() != null) {
//            throw reloadSettingsError.get();
//        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockReloadablePlugin.class);
    }

    private static class MockReloadablePlugin extends Plugin implements ReloadablePlugin {

        public MockReloadablePlugin() {
        }

        @Override
        public void reload(Settings settings) throws Exception {
            settings.equals(null);
        }
    }
}
