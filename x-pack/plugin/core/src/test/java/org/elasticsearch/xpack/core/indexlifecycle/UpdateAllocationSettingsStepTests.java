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
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class UpdateAllocationSettingsStepTests extends ESTestCase {

    public UpdateAllocationSettingsStep createRandomInstance() {
        StepKey stepKey = new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
        StepKey nextStepKey = new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
        Map<String, String> include = AllocateActionTests.randomMap(0, 10);
        Map<String, String> exclude = AllocateActionTests.randomMap(0, 10);
        Map<String, String> require = AllocateActionTests.randomMap(0, 10);

        return new UpdateAllocationSettingsStep(stepKey, nextStepKey, include, exclude, require);
    }

    public UpdateAllocationSettingsStep mutateInstance(UpdateAllocationSettingsStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        Map<String, String> include = instance.getInclude();
        Map<String, String> exclude = instance.getExclude();
        Map<String, String> require = instance.getRequire();

        switch (between(0, 4)) {
        case 0:
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 1:
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 2:
            include = new HashMap<>(include);
            include.put(randomAlphaOfLengthBetween(11, 15), randomAlphaOfLengthBetween(1, 20));
            break;
        case 3:
            exclude = new HashMap<>(exclude);
            exclude.put(randomAlphaOfLengthBetween(11, 15), randomAlphaOfLengthBetween(1, 20));
            break;
        case 4:
            require = new HashMap<>(require);
            require.put(randomAlphaOfLengthBetween(11, 15), randomAlphaOfLengthBetween(1, 20));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return new UpdateAllocationSettingsStep(key, nextKey, include, exclude, require);
    }

    public void testHashcodeAndEquals() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createRandomInstance(),
                instance -> new UpdateAllocationSettingsStep(instance.getKey(), instance.getNextStepKey(), instance.getInclude(),
                        instance.getExclude(), instance.getRequire()),
                this::mutateInstance);
    }

    public void testPerformAction() {
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

        UpdateAllocationSettingsStep step = createRandomInstance();
        ClusterState newState = step.performAction(index, clusterState);
        assertThat(getRouting(index, newState, IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey()), equalTo(step.getInclude()));
        assertThat(getRouting(index, newState, IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey()), equalTo(step.getExclude()));
        assertThat(getRouting(index, newState, IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey()), equalTo(step.getRequire()));
    }

    public void testPerformActionNoIndex() {
        MetaData metaData = MetaData.builder().persistentSettings(settings(Version.CURRENT).build()).build();
        Index index = new Index("invalid_index", "invalid_index_id");
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();

        UpdateAllocationSettingsStep step = createRandomInstance();
        ClusterState newState = step.performAction(index, clusterState);
        assertSame(clusterState, newState);
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

    public void testAddMissingAttrDiffenerentValue() {
        String prefix = randomFrom(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey(),
            IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey(),
            IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey());
        String newKey = randomAlphaOfLength(4);
        String newValue = randomAlphaOfLength(5);
        Map<String, String> newAttrs = Collections.singletonMap(newKey, newValue);
        Settings existingSettings = Settings.builder()
            .put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "box_type", "foo")
            .put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "box_type", "bar")
            .put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "box_type", "baz")
            .put(prefix + newKey, "1234").build();
        Settings.Builder newSettingsBuilder = Settings.builder();
        UpdateAllocationSettingsStep.addMissingAttrs(newAttrs, prefix, existingSettings, newSettingsBuilder);

        Settings.Builder expectedSettingsBuilder = Settings.builder();
        newAttrs.forEach((k, v) -> expectedSettingsBuilder.put(prefix + k, v));
        assertThat(newSettingsBuilder.build(), equalTo(expectedSettingsBuilder.build()));
    }

    public void testAddMissingAttrNoneMissing() {
        String prefix = randomFrom(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey(),
            IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey(),
            IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey());
        String newKey = randomAlphaOfLength(4);
        String newValue = randomAlphaOfLength(5);
        Map<String, String> newAttrs = Collections.singletonMap(newKey, newValue);
        Settings existingSettings = Settings.builder()
            .put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "box_type", "foo")
            .put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "box_type", "bar")
            .put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "box_type", "baz")
            .put(prefix + newKey, newValue).build();
        Settings.Builder newSettingsBuilder = Settings.builder();
        UpdateAllocationSettingsStep.addMissingAttrs(newAttrs, prefix, existingSettings, newSettingsBuilder);

        Settings.Builder expectedSettingsBuilder = Settings.builder();
        assertThat(newSettingsBuilder.build(), equalTo(expectedSettingsBuilder.build()));
    }

    private Map<String, String> getRouting(Index index, ClusterState clusterState, String settingPrefix) {
        Settings includeSettings = clusterState.metaData().index(index).getSettings()
            .getByPrefix(settingPrefix);
        return includeSettings.keySet().stream().collect(Collectors.toMap(Function.identity(), includeSettings::get));
    }
}
