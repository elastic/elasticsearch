/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class AllocateActionTests extends AbstractActionTestCase<AllocateAction> {

    @Override
    protected AllocateAction doParseInstance(XContentParser parser) {
        return AllocateAction.parse(parser);
    }

    @Override
    protected AllocateAction createTestInstance() {
        return randomInstance();
    }

    static AllocateAction randomInstance() {
        boolean hasAtLeastOneMap = false;
        Map<String, String> includes;
        if (randomBoolean()) {
            includes = randomAllocationRoutingMap(1, 100);
            hasAtLeastOneMap = true;
        } else {
            includes = randomBoolean() ? null : Collections.emptyMap();
        }
        Map<String, String> excludes;
        if (randomBoolean()) {
            hasAtLeastOneMap = true;
            excludes = randomAllocationRoutingMap(1, 100);
        } else {
            excludes = randomBoolean() ? null : Collections.emptyMap();
        }
        Map<String, String> requires;
        if (hasAtLeastOneMap == false || randomBoolean()) {
            requires = randomAllocationRoutingMap(1, 100);
        } else {
            requires = randomBoolean() ? null : Collections.emptyMap();
        }
        Integer numberOfReplicas = randomBoolean() ? null : randomIntBetween(0, 10);
        Integer totalShardsPerNode = randomBoolean() ? null : randomIntBetween(-300, 300);
        return new AllocateAction(numberOfReplicas, totalShardsPerNode, includes, excludes, requires);
    }


    @Override
    protected Reader<AllocateAction> instanceReader() {
        return AllocateAction::new;
    }

    @Override
    protected AllocateAction mutateInstance(AllocateAction instance) {
        Map<String, String> include = instance.getInclude();
        Map<String, String> exclude = instance.getExclude();
        Map<String, String> require = instance.getRequire();
        Integer numberOfReplicas = instance.getNumberOfReplicas();
        Integer totalShardsPerNode = instance.getTotalShardsPerNode();
        switch (randomIntBetween(0, 3)) {
        case 0:
            include = new HashMap<>(include);
            include.put(randomAlphaOfLengthBetween(11, 15), randomAlphaOfLengthBetween(1, 20));
            break;
        case 1:
            exclude = new HashMap<>(exclude);
            exclude.put(randomAlphaOfLengthBetween(11, 15), randomAlphaOfLengthBetween(1, 20));
            break;
        case 2:
            require = new HashMap<>(require);
            require.put(randomAlphaOfLengthBetween(11, 15), randomAlphaOfLengthBetween(1, 20));
            break;
        case 3:
            numberOfReplicas = randomIntBetween(11, 20);
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new AllocateAction(numberOfReplicas, totalShardsPerNode, include, exclude, require);
    }

    public void testAllMapsNullOrEmpty() {
        Map<String, String> include = randomBoolean() ? null : Collections.emptyMap();
        Map<String, String> exclude = randomBoolean() ? null : Collections.emptyMap();
        Map<String, String> require = randomBoolean() ? null : Collections.emptyMap();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new AllocateAction(null, null, include, exclude, require));
        assertEquals("At least one of " + AllocateAction.INCLUDE_FIELD.getPreferredName() + ", "
                + AllocateAction.EXCLUDE_FIELD.getPreferredName() + " or " + AllocateAction.REQUIRE_FIELD.getPreferredName()
                + "must contain attributes for action " + AllocateAction.NAME, exception.getMessage());
    }

    public void testInvalidNumberOfReplicas() {
        Map<String, String> include = randomAllocationRoutingMap(1, 5);
        Map<String, String> exclude = randomBoolean() ? null : Collections.emptyMap();
        Map<String, String> require = randomBoolean() ? null : Collections.emptyMap();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> new AllocateAction(randomIntBetween(-1000, -1), randomIntBetween(0, 300), include, exclude, require));
        assertEquals("[" + AllocateAction.NUMBER_OF_REPLICAS_FIELD.getPreferredName() + "] must be >= 0", exception.getMessage());
    }

    public static Map<String, String> randomAllocationRoutingMap(int minEntries, int maxEntries) {
        Map<String, String> map = new HashMap<>();
        int numIncludes = randomIntBetween(minEntries, maxEntries);
        for (int i = 0; i < numIncludes; i++) {
            String attributeName = randomValueOtherThanMany(DiscoveryNodeRole.roleNames()::contains,
                () -> randomAlphaOfLengthBetween(2, 20));
            map.put(attributeName, randomAlphaOfLengthBetween(2, 20));
        }
        return map;
    }

    public void testToSteps() {
        AllocateAction action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(2, steps.size());
        StepKey expectedFirstStepKey = new StepKey(phase, AllocateAction.NAME, AllocateAction.NAME);
        StepKey expectedSecondStepKey = new StepKey(phase, AllocateAction.NAME, AllocationRoutedStep.NAME);
        UpdateSettingsStep firstStep = (UpdateSettingsStep) steps.get(0);
        assertEquals(expectedFirstStepKey, firstStep.getKey());
        assertEquals(expectedSecondStepKey, firstStep.getNextStepKey());
        Settings.Builder expectedSettings = Settings.builder();
        if (action.getNumberOfReplicas() != null) {
            expectedSettings.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, action.getNumberOfReplicas());
        }
        action.getInclude().forEach(
            (key, value) -> expectedSettings.put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + key, value));
        action.getExclude().forEach(
            (key, value) -> expectedSettings.put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + key, value));
        action.getRequire().forEach(
            (key, value) -> expectedSettings.put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + key, value));
        if (action.getTotalShardsPerNode() != null) {
            expectedSettings.put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), action.getTotalShardsPerNode());
        }

        assertThat(firstStep.getSettings(), equalTo(expectedSettings.build()));
        AllocationRoutedStep secondStep = (AllocationRoutedStep) steps.get(1);
        assertEquals(expectedSecondStepKey, secondStep.getKey());
        assertEquals(nextStepKey, secondStep.getNextStepKey());
    }

    public void testTotalNumberOfShards() throws Exception {
        Integer totalShardsPerNode = randomIntBetween(-1000, 1000);
        Integer numberOfReplicas = randomIntBetween(0, 4);
        AllocateAction action = new AllocateAction(numberOfReplicas, totalShardsPerNode, null, null, null);
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        UpdateSettingsStep firstStep = (UpdateSettingsStep) steps.get(0);
        assertEquals(totalShardsPerNode, firstStep.getSettings().getAsInt(INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), null));

        totalShardsPerNode = null;
        action = new AllocateAction(numberOfReplicas, totalShardsPerNode, null, null, null);
        steps = action.toSteps(null, phase, nextStepKey);
        firstStep = (UpdateSettingsStep) steps.get(0);
        assertEquals(null, firstStep.getSettings().get(INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey()));
    }

}
