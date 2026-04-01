/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.action;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsUpdater;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.common.settings.Setting.Property.Dynamic;
import static org.elasticsearch.common.settings.Setting.Property.NodeScope;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class ReservedClusterSettingsActionTests extends ESTestCase {

    static final Setting<String> dummySetting1 = Setting.simpleString("dummy.setting1", "default1", NodeScope, Dynamic);
    static final Setting<String> dummySetting2 = Setting.simpleString("dummy.setting2", "default2", NodeScope, Dynamic);
    static final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(dummySetting1, dummySetting2));
    static final ReservedClusterSettingsAction testAction = new ReservedClusterSettingsAction(clusterSettings);

    private TransformState processJSON(ReservedClusterSettingsAction action, TransformState prevState, String json) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return action.transform(parser.map(), prevState);
        }
    }

    public void testValidation() throws Exception {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        ReservedClusterSettingsAction action = new ReservedClusterSettingsAction(clusterSettings);

        String badPolicyJSON = """
            {
                "indices.recovery.min_bytes_per_sec": "50mb"
            }""";

        assertThat(
            expectThrows(IllegalArgumentException.class, () -> processJSON(action, prevState, badPolicyJSON)).getMessage(),
            is("persistent setting [indices.recovery.min_bytes_per_sec], not recognized")
        );
    }

    public void testSetUnsetSettings() throws Exception {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        ReservedClusterSettingsAction action = new ReservedClusterSettingsAction(clusterSettings);

        String emptyJSON = "";

        TransformState updatedState = processJSON(action, prevState, emptyJSON);
        assertThat(updatedState.keys(), empty());
        assertEquals(prevState.state(), updatedState.state());

        String settingsJSON = """
            {
                "indices.recovery.max_bytes_per_sec": "50mb",
                "cluster": {
                     "remote": {
                         "cluster_one": {
                             "seeds": [
                                 "127.0.0.1:9300"
                             ]
                         }
                     }
                }
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, settingsJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("indices.recovery.max_bytes_per_sec", "cluster.remote.cluster_one.seeds"));
        assertThat(updatedState.state().metadata().persistentSettings().get("indices.recovery.max_bytes_per_sec"), is("50mb"));
        assertThat(updatedState.state().metadata().persistentSettings().get("cluster.remote.cluster_one.seeds"), is("[127.0.0.1:9300]"));

        String oneSettingJSON = """
            {
                "indices.recovery.max_bytes_per_sec": "25mb"
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, oneSettingJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("indices.recovery.max_bytes_per_sec"));
        assertThat(updatedState.state().metadata().persistentSettings().get("indices.recovery.max_bytes_per_sec"), is("25mb"));
        assertNull(updatedState.state().metadata().persistentSettings().get("cluster.remote.cluster_one.seeds"));

        prevState = updatedState;
        updatedState = processJSON(action, prevState, emptyJSON);
        assertThat(updatedState.keys(), empty());
        assertNull(updatedState.state().metadata().persistentSettings().get("indices.recovery.max_bytes_per_sec"));
    }

    public void testIgnoreRemovedSettings() throws Exception {
        // Only setting1 remains registered; setting2 was removed from the codebase
        Setting<String> setting1 = Setting.simpleString("dummy.setting1", "default1", NodeScope, Dynamic);
        ClusterSettings limitedClusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(setting1));
        ReservedClusterSettingsAction action = new ReservedClusterSettingsAction(limitedClusterSettings);

        // Build a cluster state that has dummy.setting2 in persistent settings
        // (it was valid when originally set, but is now removed from the registry)
        ClusterState stateWithOldSetting = ClusterState.builder(new ClusterName("elasticsearch"))
            .metadata(
                Metadata.builder()
                    .persistentSettings(Settings.builder().put("dummy.setting1", "old-value1").put("dummy.setting2", "old-value2").build())
                    .build()
            )
            .build();

        // Previous reserved state tracked both settings
        TransformState prevState = new TransformState(stateWithOldSetting, Set.of("dummy.setting1", "dummy.setting2"));

        // New config only specifies dummy.setting1 (dummy.setting2 was removed from the registry)
        String json = """
            {
                "dummy": {
                    "setting1": "new-value1"
                }
            }
            """;

        // Without the fix, this would throw:
        // IllegalArgumentException: persistent setting [dummy.setting2], not recognized
        TransformState newState = processJSON(action, prevState, json);

        // dummy.setting2 should be dropped from reserved state keys
        assertThat(newState.keys(), containsInAnyOrder("dummy.setting1"));
        // dummy.setting1 should have the new value
        assertThat(newState.state().metadata().persistentSettings().get("dummy.setting1"), is("new-value1"));
        // dummy.setting2 is also absent from persistent settings: SettingsUpdater validates
        // the final settings against the registry and drops keys it does not recognize
        assertNull(newState.state().metadata().persistentSettings().get("dummy.setting2"));
    }

    public void testSettingNameNormalization() throws Exception {
        Settings prevSettings = Settings.builder().put("dummy.setting1", "a-value").build();
        var clusterState = new SettingsUpdater(clusterSettings).updateSettings(
            ClusterState.EMPTY_STATE,
            Settings.EMPTY,
            prevSettings,
            logger
        );
        TransformState prevState = new TransformState(clusterState, Set.of("dummy.setting1"));

        String json = """
            {
                "dummy": {
                    "setting1": "value1",
                    "setting2": "value2"
                }
            }
            """;

        TransformState newState = processJSON(testAction, prevState, json);
        assertThat(newState.keys(), containsInAnyOrder("dummy.setting1", "dummy.setting2"));
        assertThat(newState.state().metadata().persistentSettings().get("dummy.setting1"), is("value1"));
        assertThat(newState.state().metadata().persistentSettings().get("dummy.setting2"), is("value2"));

        String jsonRemoval = """
            {
                "dummy": {
                    "setting2": "value2"
                }
            }
            """;
        TransformState newState2 = processJSON(testAction, prevState, jsonRemoval);
        assertThat(newState2.keys(), containsInAnyOrder("dummy.setting2"));
        assertThat(newState2.state().metadata().persistentSettings().get("dummy.setting2"), is("value2"));
    }
}
