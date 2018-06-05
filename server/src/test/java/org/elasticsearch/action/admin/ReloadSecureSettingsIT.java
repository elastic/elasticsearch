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

package org.elasticsearch.action.admin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.test.ESIntegTestCase;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.containsString;

public class ReloadSecureSettingsIT extends ESIntegTestCase {

    public void testPasswordBroadcast() throws Exception {
        final AtomicReference<AssertionError> reloadSettingsError = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        client().admin().cluster().prepareReloadSecureSettings().setSecureStorePassword("").execute(
                new ActionListener<NodesReloadSecureSettingsResponse>() {
                    @Override
                    public void onResponse(NodesReloadSecureSettingsResponse nodesReloadResponse) {
                        try {
                            assertThat(nodesReloadResponse, notNullValue());
                            final Map<String, NodesReloadSecureSettingsResponse.NodeResponse> nodesMap = nodesReloadResponse.getNodesMap();
                            assertThat(nodesMap.size(), equalTo(cluster().size()));
                            for (final NodesReloadSecureSettingsResponse.NodeResponse nodeResponse : nodesReloadResponse.getNodes()) {
                                assertThat(nodeResponse.reloadException(), notNullValue());
                                assertThat(nodeResponse.reloadException(), instanceOf(IllegalStateException.class));
                                assertThat(nodeResponse.reloadException().getMessage(), containsString("Keystore is missing"));
                            }
                        } catch (final AssertionError e) {
                            reloadSettingsError.set(e);
                        } finally {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail("Node request failed");
                        latch.countDown();
                    }
                });
        latch.await();
        if (reloadSettingsError.get() != null) {
            throw reloadSettingsError.get();
        }

    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockReloadablePlugin.class);
    }

    public static class MockReloadablePlugin extends Plugin implements ReloadablePlugin {

        public MockReloadablePlugin() {
        }

        @Override
        public void reload(Settings settings) throws Exception {
            settings.equals(null);
        }
    }

}
