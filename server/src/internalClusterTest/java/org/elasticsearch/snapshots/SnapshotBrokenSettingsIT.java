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

package org.elasticsearch.snapshots;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/37485")
public class SnapshotBrokenSettingsIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(BrokenSettingPlugin.class);
    }

    public void testExceptionWhenRestoringPersistentSettings() {
        logger.info("--> start 2 nodes");
        internalCluster().startNodes(2);

        Client client = client();
        Consumer<String> setSettingValue = value -> client.admin().cluster().prepareUpdateSettings().setPersistentSettings(
                Settings.builder().put(BrokenSettingPlugin.BROKEN_SETTING.getKey(), value)).execute().actionGet();

        Consumer<String> assertSettingValue = value -> assertThat(client.admin().cluster().prepareState().setRoutingTable(false)
                .setNodes(false).execute().actionGet().getState().getMetadata().persistentSettings()
                .get(BrokenSettingPlugin.BROKEN_SETTING.getKey()), equalTo(value));

        logger.info("--> set test persistent setting");
        setSettingValue.accept("new value");
        assertSettingValue.accept("new value");

        createRepository("test-repo", "fs");
        createFullSnapshot("test-repo", "test-snap");
        assertThat(getSnapshot("test-repo", "test-snap").state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> change the test persistent setting and break it");
        setSettingValue.accept("new value 2");
        assertSettingValue.accept("new value 2");
        BrokenSettingPlugin.breakSetting();

        logger.info("--> restore snapshot");
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setRestoreGlobalState(true)
                        .setWaitForCompletion(true).execute()::actionGet);
        assertEquals(BrokenSettingPlugin.EXCEPTION.getMessage(), ex.getMessage());

        assertSettingValue.accept("new value 2");
    }

    public static class BrokenSettingPlugin extends Plugin {
        private static boolean breakSetting = false;
        private static final IllegalArgumentException EXCEPTION =  new IllegalArgumentException("this setting goes boom");

        static void breakSetting() {
            BrokenSettingPlugin.breakSetting = true;
        }

        static final Setting<String> BROKEN_SETTING = new Setting<>("setting.broken", "default", s->s,
                s-> {
                    if ((s.equals("default") == false && breakSetting)) {
                        throw EXCEPTION;
                    }
                },
                Setting.Property.NodeScope, Setting.Property.Dynamic);

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(BROKEN_SETTING);
        }
    }
}
