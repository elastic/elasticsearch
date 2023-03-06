/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditionsTests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DataLifecycleTests extends AbstractXContentSerializingTestCase<DataLifecycle> {

    @Override
    protected Writeable.Reader<DataLifecycle> instanceReader() {
        return DataLifecycle::new;
    }

    @Override
    protected DataLifecycle createTestInstance() {
        if (randomBoolean()) {
            return new DataLifecycle();
        } else {
            return new DataLifecycle(randomMillisUpToYear9999());
        }
    }

    @Override
    protected DataLifecycle mutateInstance(DataLifecycle instance) throws IOException {
        if (instance.getDataRetention() == null) {
            return new DataLifecycle(randomMillisUpToYear9999());
        }
        return new DataLifecycle(instance.getDataRetention().millis() + randomMillisUpToYear9999());
    }

    @Override
    protected DataLifecycle doParseInstance(XContentParser parser) throws IOException {
        return DataLifecycle.fromXContent(parser);
    }

    public void testXContentSerializationWithRollover() throws IOException {
        DataLifecycle dataLifecycle = createTestInstance();
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            RolloverConditions rolloverConditions = RolloverConditionsTests.randomRolloverConditions();
            dataLifecycle.toXContent(builder, ToXContent.EMPTY_PARAMS, rolloverConditions);
            String serialized = Strings.toString(builder);
            assertThat(serialized, containsString("rollover"));
            for (String label : rolloverConditions.getConditions().keySet()) {
                assertThat(serialized, containsString(label));
            }
        }
    }

    public void testClusterSettingParsing() {
        RolloverConditions defaultSetting = RolloverConditions.parseSetting(
            "max_age=7d,max_primary_shard_size=50gb,min_docs=1,max_primary_shard_docs=200000000",
            "test-setting"
        );

        assertThat(defaultSetting.getMaxSize(), nullValue());
        assertThat(defaultSetting.getMaxPrimaryShardSize(), equalTo(ByteSizeValue.ofGb(50)));
        assertThat(defaultSetting.getMaxAge(), equalTo(TimeValue.timeValueDays(7)));
        assertThat(defaultSetting.getMaxDocs(), nullValue());
        assertThat(defaultSetting.getMaxPrimaryShardDocs(), equalTo(200_000_000L));

        assertThat(defaultSetting.getMinSize(), nullValue());
        assertThat(defaultSetting.getMinPrimaryShardSize(), nullValue());
        assertThat(defaultSetting.getMinAge(), nullValue());
        assertThat(defaultSetting.getMinDocs(), equalTo(1L));
        assertThat(defaultSetting.getMinPrimaryShardDocs(), nullValue());

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
        String setting = "max_size="
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
        RolloverConditions randomSetting = RolloverConditions.parseSetting(setting, "test2");
        assertThat(randomSetting.getMaxAge(), equalTo(maxAge));
        assertThat(randomSetting.getMaxPrimaryShardSize(), equalTo(maxPrimaryShardSize));
        assertThat(randomSetting.getMaxDocs(), equalTo(maxDocs));
        assertThat(randomSetting.getMaxPrimaryShardDocs(), equalTo(maxPrimaryShardDocs));
        assertThat(randomSetting.getMaxSize(), equalTo(maxSize));

        assertThat(randomSetting.getMinAge(), equalTo(minAge));
        assertThat(randomSetting.getMinPrimaryShardSize(), equalTo(minPrimaryShardSize));
        assertThat(randomSetting.getMinPrimaryShardDocs(), equalTo(minPrimaryShardDocs));
        assertThat(randomSetting.getMinDocs(), equalTo(minDocs));
        assertThat(randomSetting.getMinSize(), equalTo(minSize));

        SettingsException invalid = expectThrows(SettingsException.class, () -> RolloverConditions.parseSetting("", "empty-setting"));
        assertEquals("Invalid condition: '', format must be 'condition=value'", invalid.getMessage());
        SettingsException unknown = expectThrows(
            SettingsException.class,
            () -> RolloverConditions.parseSetting("unknown_condition=?", "unknown-setting")
        );
        assertEquals("Unknown condition: 'unknown_condition'", unknown.getMessage());
        SettingsException numberFormat = expectThrows(
            SettingsException.class,
            () -> RolloverConditions.parseSetting("max_docs=one", "invalid-number-setting")
        );
        assertEquals(
            "Invalid value 'one' in setting 'invalid-number-setting', the value is expected to be of type long",
            numberFormat.getMessage()
        );
    }

    public void testDefaultClusterSetting() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RolloverConditions rolloverConditions = clusterSettings.get(DataLifecycle.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING);
        assertThat(rolloverConditions.getMaxAge(), equalTo(TimeValue.timeValueDays(7)));
        assertThat(rolloverConditions.getMaxPrimaryShardSize(), equalTo(ByteSizeValue.ofGb(50)));
        assertThat(rolloverConditions.getMaxPrimaryShardDocs(), equalTo(200_000_000L));
        assertThat(rolloverConditions.getMinDocs(), equalTo(1L));
        assertThat(rolloverConditions.getMaxSize(), nullValue());
        assertThat(rolloverConditions.getMaxDocs(), nullValue());
        assertThat(rolloverConditions.getMinAge(), nullValue());
        assertThat(rolloverConditions.getMinSize(), nullValue());
        assertThat(rolloverConditions.getMinPrimaryShardSize(), nullValue());
        assertThat(rolloverConditions.getMinPrimaryShardDocs(), nullValue());

    }

    public void testInvalidClusterSetting() {
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> DataLifecycle.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.get(
                    Settings.builder().put(DataLifecycle.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.getKey(), "max_docs=1").build()
                )
            );
            assertThat(exception.getMessage(), equalTo("The min_docs rollover condition should be set and greater than 0."));
        }
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> DataLifecycle.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.get(
                    Settings.builder().put(DataLifecycle.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1").build()
                )
            );
            assertThat(exception.getMessage(), equalTo("At least one max_* rollover condition must be set."));
        }
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> DataLifecycle.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.get(
                    Settings.builder().put(DataLifecycle.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.getKey(), "min_age=1d").build()
                )
            );
            assertThat(
                exception.getMessage(),
                equalTo(
                    "The min_docs rollover condition should be set and greater than 0. At least one max_* rollover condition must be set."
                )
            );
        }
    }
}
