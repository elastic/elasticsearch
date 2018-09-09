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

package org.elasticsearch.common.settings;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

public class UpgradeSettingsIT extends ESSingleNodeTestCase {

    @After
    public void cleanup() throws Exception {
                client()
                        .admin()
                        .cluster()
                        .prepareUpdateSettings()
                        .setPersistentSettings(Settings.builder().putNull("*"))
                        .setTransientSettings(Settings.builder().putNull("*"))
                        .get();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(UpgradeSettingsPlugin.class);
    }

    public static class UpgradeSettingsPlugin extends Plugin {

        static final Setting<String> oldSetting = Setting.simpleString("foo.old", Setting.Property.Dynamic, Setting.Property.NodeScope);
        static final Setting<String> newSetting = Setting.simpleString("foo.new", Setting.Property.Dynamic, Setting.Property.NodeScope);

        public UpgradeSettingsPlugin(){

        }

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(oldSetting, newSetting);
        }

        @Override
        public List<SettingUpgrader<?>> getSettingUpgraders() {
            return Collections.singletonList(new SettingUpgrader<String>() {

                @Override
                public Setting<String> getSetting() {
                    return oldSetting;
                }

                @Override
                public String getKey(final String key) {
                    return "foo.new";
                }

                @Override
                public String getValue(final String value) {
                    return "new." + value;
                }
            });
        }
    }

    public void testUpgradePersistentSettingsOnUpdate() {
        runUpgradeSettingsOnUpdateTest((settings, builder) -> builder.setPersistentSettings(settings), MetaData::persistentSettings);
    }

    public void testUpgradeTransientSettingsOnUpdate() {
        runUpgradeSettingsOnUpdateTest((settings, builder) -> builder.setTransientSettings(settings), MetaData::transientSettings);
    }

    private void runUpgradeSettingsOnUpdateTest(
            final BiConsumer<Settings, ClusterUpdateSettingsRequestBuilder> consumer,
            final Function<MetaData, Settings> settingsFunction) {
        final String value = randomAlphaOfLength(8);
        final ClusterUpdateSettingsRequestBuilder builder =
                client()
                        .admin()
                        .cluster()
                        .prepareUpdateSettings();
        consumer.accept(Settings.builder().put("foo.old", value).build(), builder);
        builder.get();

        final ClusterStateResponse response = client()
                .admin()
                .cluster()
                .prepareState()
                .clear()
                .setMetaData(true)
                .get();

        assertFalse(UpgradeSettingsPlugin.oldSetting.exists(settingsFunction.apply(response.getState().metaData())));
        assertTrue(UpgradeSettingsPlugin.newSetting.exists(settingsFunction.apply(response.getState().metaData())));
        assertThat(UpgradeSettingsPlugin.newSetting.get(settingsFunction.apply(response.getState().metaData())), equalTo("new." + value));
    }

}
