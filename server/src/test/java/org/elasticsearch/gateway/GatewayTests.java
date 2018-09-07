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

package org.elasticsearch.gateway;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class GatewayTests extends ESTestCase {

    public void testUpgradePersistentSettings() {
        runUpgradeSettings(MetaData.Builder::persistentSettings, MetaData::persistentSettings);
    }

    public void testUpgradeTransientSettings() {
        runUpgradeSettings(MetaData.Builder::transientSettings, MetaData::transientSettings);
    }

    private void runUpgradeSettings(
            final BiConsumer<MetaData.Builder, Settings> applySettingsToBuilder, final Function<MetaData, Settings> metaDataSettings) {
        final Setting<?> oldSetting = Setting.simpleString("foo.old", Setting.Property.Dynamic, Setting.Property.NodeScope);
        final Setting<?> newSetting = Setting.simpleString("foo.new", Setting.Property.Dynamic, Setting.Property.NodeScope);
        final Set<Setting<?>> settingsSet =
                new HashSet<>(
                        Stream.concat(
                                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
                                Stream.of(oldSetting, newSetting))
                                .collect(Collectors.toList()));
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settingsSet);
        clusterSettings.addSettingsUpgrader(oldSetting, entry -> new AbstractMap.SimpleEntry<>("foo.new", entry.getValue()));
        final ClusterService clusterService = new ClusterService(Settings.EMPTY, clusterSettings, null);
        final Gateway gateway = new Gateway(Settings.EMPTY, clusterService, null, null);
        final MetaData.Builder builder = MetaData.builder();
        final Settings settings = Settings.builder().put("foo.old", randomAlphaOfLength(8)).build();
        applySettingsToBuilder.accept(builder, settings);
        final ClusterState state = gateway.upgradeAndArchiveUnknownOrInvalidSettings(builder).build();
        assertFalse(oldSetting.exists(metaDataSettings.apply(state.metaData())));
        assertTrue(newSetting.exists(metaDataSettings.apply(state.metaData())));
        assertThat(newSetting.get(metaDataSettings.apply(state.metaData())), equalTo(oldSetting.get(settings)));
    }

}
