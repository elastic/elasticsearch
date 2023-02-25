/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class DefaultRolloverConditionsSettingTests extends ESTestCase {

    public void testDefaults() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DefaultRolloverConditionsSetting setting = DefaultRolloverConditionsSetting.create(Settings.EMPTY, clusterSettings);

        assertThat(setting.get().getMaxAge(), equalTo(TimeValue.timeValueDays(7)));
        assertThat(setting.get().getMaxPrimaryShardSize(), equalTo(ByteSizeValue.ofGb(50)));
        assertThat(setting.get().getMaxPrimaryShardDocs(), equalTo(200_000_000L));
        assertThat(setting.get().getMinDocs(), equalTo(1L));
        assertThat(setting.get().getConditions().size(), equalTo(4));
    }

    public void testUpdates() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DefaultRolloverConditionsSetting setting = DefaultRolloverConditionsSetting.create(Settings.EMPTY, clusterSettings);

        var maxSize = ByteSizeValue.ofGb(randomIntBetween(1, 100));
        var maxPrimaryShardSize = ByteSizeValue.ofGb(randomIntBetween(1, 50));
        var maxAge = TimeValue.timeValueMillis(randomMillisUpToYear9999());
        var maxDocs = randomLongBetween(1_000_000, 1_000_000_000);
        var maxPrimaryShardDocs = randomLongBetween(1_000_000, 1_000_000_000);

        var minSize = ByteSizeValue.ofGb(randomIntBetween(1, 100));
        var minPrimaryShardSize = ByteSizeValue.ofGb(randomIntBetween(1, 50));
        var minAge = TimeValue.timeValueMillis(randomMillisUpToYear9999());
        var minDocs = randomLongBetween(1_000_000, 1_000_000_000);
        var minPrimaryShardDocs = randomLongBetween(1_000_000, 1_000_000_000);
        String settingValue = "max_size="
            + maxSize.getStringRep()
            + ",max_primary_shard_size="
            + maxPrimaryShardSize.getStringRep()
            + ",max_age="
            + maxAge.getStringRep()
            + ",max_docs="
            + maxDocs
            + ",max_primary_shard_docs="
            + maxPrimaryShardDocs
            + ",min_size="
            + minSize.getStringRep()
            + ",min_primary_shard_size="
            + minPrimaryShardSize.getStringRep()
            + ",min_age="
            + minAge.getStringRep()
            + ",min_docs="
            + minDocs
            + ",min_primary_shard_docs="
            + minPrimaryShardDocs;

        Settings newSettings = Settings.builder()
            .put(DefaultRolloverConditionsSetting.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.getKey(), settingValue)
            .build();
        clusterSettings.applySettings(newSettings);

        RolloverConditions updatedSetting = setting.get();
        assertThat(updatedSetting.getMaxAge(), Matchers.equalTo(maxAge));
        assertThat(updatedSetting.getMaxPrimaryShardSize(), Matchers.equalTo(maxPrimaryShardSize));
        assertThat(updatedSetting.getMaxDocs(), Matchers.equalTo(maxDocs));
        assertThat(updatedSetting.getMaxPrimaryShardDocs(), Matchers.equalTo(maxPrimaryShardDocs));
        assertThat(updatedSetting.getMaxSize(), Matchers.equalTo(maxSize));

        assertThat(updatedSetting.getMinAge(), Matchers.equalTo(minAge));
        assertThat(updatedSetting.getMinPrimaryShardSize(), Matchers.equalTo(minPrimaryShardSize));
        assertThat(updatedSetting.getMinPrimaryShardDocs(), Matchers.equalTo(minPrimaryShardDocs));
        assertThat(updatedSetting.getMinDocs(), Matchers.equalTo(minDocs));
        assertThat(updatedSetting.getMinSize(), Matchers.equalTo(minSize));
    }

    public void testInvalidConditionsNoMax() {
        Settings settings = Settings.builder()
            .put(DefaultRolloverConditionsSetting.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1")
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DefaultRolloverConditionsSetting.create(settings, clusterSettings)
        );
        assertThat(e, hasToString(containsString("At least one max_* rollover condition must be set.")));
    }

    public void testInvalidEmptyConditions() {
        Settings settings = Settings.builder()
            .put(DefaultRolloverConditionsSetting.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.getKey(), "")
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DefaultRolloverConditionsSetting.create(settings, clusterSettings)
        );
        assertThat(e, hasToString(containsString("Invalid condition: '', format must be 'condition=value'")));
    }

    public void testInvalidInput() {
        Settings settings = Settings.builder()
            .put(DefaultRolloverConditionsSetting.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.getKey(), "something_else")
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DefaultRolloverConditionsSetting.create(settings, clusterSettings)
        );
        assertThat(e, hasToString(containsString("Invalid condition: 'something_else', format must be 'condition=value'")));
    }

    public void testUnknownCondition() {
        Settings settings = Settings.builder()
            .put(DefaultRolloverConditionsSetting.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.getKey(), "unknown=true")
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DefaultRolloverConditionsSetting.create(settings, clusterSettings)
        );
        assertThat(e, hasToString(containsString("Unknown condition: 'unknown'")));
    }
}
