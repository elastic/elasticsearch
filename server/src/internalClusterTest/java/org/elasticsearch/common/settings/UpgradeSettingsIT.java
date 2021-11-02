/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.SniffConnectionStrategy;
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
        client().admin()
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

        public UpgradeSettingsPlugin() {

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
        runUpgradeSettingsOnUpdateTest((settings, builder) -> builder.setPersistentSettings(settings), Metadata::persistentSettings);
    }

    public void testUpgradeTransientSettingsOnUpdate() {
        runUpgradeSettingsOnUpdateTest((settings, builder) -> builder.setTransientSettings(settings), Metadata::transientSettings);
    }

    private void runUpgradeSettingsOnUpdateTest(
        final BiConsumer<Settings, ClusterUpdateSettingsRequestBuilder> consumer,
        final Function<Metadata, Settings> settingsFunction
    ) {
        final String value = randomAlphaOfLength(8);
        final ClusterUpdateSettingsRequestBuilder builder = client().admin().cluster().prepareUpdateSettings();
        consumer.accept(Settings.builder().put("foo.old", value).build(), builder);
        builder.get();

        final ClusterStateResponse response = client().admin().cluster().prepareState().clear().setMetadata(true).get();

        assertFalse(UpgradeSettingsPlugin.oldSetting.exists(settingsFunction.apply(response.getState().metadata())));
        assertTrue(UpgradeSettingsPlugin.newSetting.exists(settingsFunction.apply(response.getState().metadata())));
        assertThat(UpgradeSettingsPlugin.newSetting.get(settingsFunction.apply(response.getState().metadata())), equalTo("new." + value));
    }

    public void testUpgradeRemoteClusterSettings() {
        final boolean skipUnavailable = randomBoolean();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder()
                    .put("search.remote.foo.skip_unavailable", skipUnavailable)
                    .putList("search.remote.foo.seeds", Collections.singletonList("localhost:9200"))
                    .put("search.remote.foo.proxy", "localhost:9200")
                    .build()
            )
            .get();

        final ClusterStateResponse response = client().admin().cluster().prepareState().clear().setMetadata(true).get();

        final Settings settings = response.getState().metadata().persistentSettings();
        assertFalse(RemoteClusterService.SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo").exists(settings));
        assertTrue(RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo").exists(settings));
        assertThat(
            RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace("foo").get(settings),
            equalTo(skipUnavailable)
        );
        assertFalse(SniffConnectionStrategy.SEARCH_REMOTE_CLUSTERS_SEEDS.getConcreteSettingForNamespace("foo").exists(settings));
        assertTrue(SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace("foo").exists(settings));
        assertThat(
            SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace("foo").get(settings),
            equalTo(Collections.singletonList("localhost:9200"))
        );
        assertFalse(SniffConnectionStrategy.SEARCH_REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace("foo").exists(settings));
        assertTrue(SniffConnectionStrategy.REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace("foo").exists(settings));
        assertThat(
            SniffConnectionStrategy.REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace("foo").get(settings),
            equalTo("localhost:9200")
        );
    }

}
