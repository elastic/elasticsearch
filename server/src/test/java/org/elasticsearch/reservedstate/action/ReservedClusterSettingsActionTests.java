/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.action;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collections;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class ReservedClusterSettingsActionTests extends ESTestCase {

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

        assertEquals(
            "persistent setting [indices.recovery.min_bytes_per_sec], not recognized",
            expectThrows(IllegalArgumentException.class, () -> processJSON(action, prevState, badPolicyJSON)).getMessage()
        );
    }

    public void testSetUnsetSettings() throws Exception {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState prevState = new TransformState(state, Collections.emptySet());
        ReservedClusterSettingsAction action = new ReservedClusterSettingsAction(clusterSettings);

        String emptyJSON = "";

        TransformState updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
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
        assertEquals("50mb", updatedState.state().metadata().persistentSettings().get("indices.recovery.max_bytes_per_sec"));
        assertEquals("[127.0.0.1:9300]", updatedState.state().metadata().persistentSettings().get("cluster.remote.cluster_one.seeds"));

        String oneSettingJSON = """
            {
                "indices.recovery.max_bytes_per_sec": "25mb"
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, oneSettingJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("indices.recovery.max_bytes_per_sec"));
        assertEquals("25mb", updatedState.state().metadata().persistentSettings().get("indices.recovery.max_bytes_per_sec"));
        assertNull(updatedState.state().metadata().persistentSettings().get("cluster.remote.cluster_one.seeds"));

        prevState = updatedState;
        updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
        assertNull(updatedState.state().metadata().persistentSettings().get("indices.recovery.max_bytes_per_sec"));
    }
}
