/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;


import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class UpdateAllocationSettingsStepTests extends ESTestCase {

    public void testModify() {
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();
        Index index = indexMetadata.getIndex();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();

        Map<String, String> include =
            randomBoolean() ? Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5)) : Collections.emptyMap();
        Map<String, String> exclude =
            randomBoolean() ? Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5)) : Collections.emptyMap();
        Map<String, String> require =
            randomBoolean() ? Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5)) : Collections.emptyMap();

        UpdateAllocationSettingsStep step = new UpdateAllocationSettingsStep(null, null, include, exclude, require);
        ClusterState newState = step.performAction(index, clusterState);
        assertThat(getRouting(index, newState, IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey()), equalTo(include));
        assertThat(getRouting(index, newState, IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey()), equalTo(exclude));
        assertThat(getRouting(index, newState, IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey()), equalTo(require));
    }

    public void testAddMissingAttr() {
        String prefix = randomFrom(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey(),
            IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey(),
            IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey());
        Map<String, String> newAttrs = Collections.singletonMap(randomAlphaOfLength(4), randomAlphaOfLength(5));
        Settings existingSettings = Settings.builder()
            .put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "box_type", "foo")
            .put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "box_type", "bar")
            .put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "box_type", "baz").build();
        Settings.Builder newSettingsBuilder = Settings.builder();
        UpdateAllocationSettingsStep.addMissingAttrs(newAttrs, prefix, existingSettings, newSettingsBuilder);

        Settings.Builder expectedSettingsBuilder = Settings.builder();
        newAttrs.forEach((k, v) -> expectedSettingsBuilder.put(prefix + k, v));
        assertThat(newSettingsBuilder.build(), equalTo(expectedSettingsBuilder.build()));
    }

    private Map<String, String> getRouting(Index index, ClusterState clusterState, String settingPrefix) {
        Settings includeSettings = clusterState.metaData().index(index).getSettings()
            .getByPrefix(settingPrefix);
        return includeSettings.keySet().stream().collect(Collectors.toMap(Function.identity(), includeSettings::get));
    }
}
