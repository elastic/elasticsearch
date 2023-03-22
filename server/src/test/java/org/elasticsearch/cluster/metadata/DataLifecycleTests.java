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
                    Settings.builder().put(DataLifecycle.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.getKey(), "").build()
                )
            );
            assertThat(exception.getMessage(), equalTo("The rollover conditions cannot be null or blank"));
        }
    }
}
