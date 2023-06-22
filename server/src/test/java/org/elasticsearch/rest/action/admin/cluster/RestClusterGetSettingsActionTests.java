/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.RestClusterGetSettingsResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.DefaultSettingsFilter;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RestClusterGetSettingsActionTests extends ESTestCase {

    public void testFilterPersistentSettings() {
        runTestFilterSettingsTest(Metadata.Builder::persistentSettings, RestClusterGetSettingsResponse::getPersistentSettings);
    }

    public void testFilterTransientSettings() {
        runTestFilterSettingsTest(Metadata.Builder::transientSettings, RestClusterGetSettingsResponse::getTransientSettings);
    }

    private void runTestFilterSettingsTest(
        final BiConsumer<Metadata.Builder, Settings> md,
        final Function<RestClusterGetSettingsResponse, Settings> s
    ) {
        final Metadata.Builder mdBuilder = new Metadata.Builder();
        final Settings settings = Settings.builder().put("foo.filtered", "bar").put("foo.non_filtered", "baz").build();
        md.accept(mdBuilder, settings);
        final ClusterState.Builder builder = new ClusterState.Builder(ClusterState.EMPTY_STATE).metadata(mdBuilder);
        final SettingsFilter filter = new DefaultSettingsFilter(Collections.singleton("foo.filtered"));
        final Setting.Property[] properties = { Setting.Property.Dynamic, Setting.Property.Filtered, Setting.Property.NodeScope };
        final Set<Setting<?>> settingsSet = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.concat(
                Stream.of(Setting.simpleString("foo.filtered", properties)),
                Stream.of(Setting.simpleString("foo.non_filtered", properties))
            )
        ).collect(Collectors.toSet());
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settingsSet);
        final ClusterState clusterState = builder.build();
        final RestClusterGetSettingsResponse response = RestClusterGetSettingsAction.response(
            new ClusterGetSettingsAction.Response(
                clusterState.metadata().persistentSettings(),
                clusterState.metadata().transientSettings(),
                clusterState.metadata().settings()
            ),
            randomBoolean(),
            filter,
            clusterSettings,
            Settings.EMPTY
        );
        assertFalse(s.apply(response).hasValue("foo.filtered"));
        assertTrue(s.apply(response).hasValue("foo.non_filtered"));
    }

}
