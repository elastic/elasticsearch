/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfigurationTests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Set;

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
            RolloverConfiguration rolloverConfiguration = RolloverConfigurationTests.randomRolloverConditions();
            dataLifecycle.toXContent(builder, ToXContent.EMPTY_PARAMS, rolloverConfiguration);
            String serialized = Strings.toString(builder);
            assertThat(serialized, containsString("rollover"));
            for (String label : rolloverConfiguration.resolveRolloverConditions(dataLifecycle.getDataRetention())
                .getConditions()
                .keySet()) {
                assertThat(serialized, containsString(label));
            }
            // Verify that max_age is marked as automatic, if it's set on auto
            if (rolloverConfiguration.getAutomaticConditions().isEmpty() == false) {
                assertThat(serialized, containsString("[automatic]"));
            }
        }
    }

    public void testLifecycleComposition() {
        // No lifecycles result to null
        {
            List<DataLifecycle> lifecycles = List.of();
            assertThat(DataLifecycle.compose(lifecycles), nullValue());
        }
        // One lifecycle results to this lifecycle as the final
        {
            DataLifecycle lifecycle = createTestInstance();
            List<DataLifecycle> lifecycles = List.of(lifecycle);
            assertThat(DataLifecycle.compose(lifecycles), equalTo(lifecycle));
        }
        // If the last lifecycle is missing a property we keep the latest from the previous ones
        {
            DataLifecycle lifecycleWithRetention = new DataLifecycle(randomMillisUpToYear9999());
            List<DataLifecycle> lifecycles = List.of(lifecycleWithRetention, new DataLifecycle());
            assertThat(DataLifecycle.compose(lifecycles).getDataRetention(), equalTo(lifecycleWithRetention.getDataRetention()));
        }
        // If both lifecycle have all properties, then the latest one overwrites all the others
        {
            DataLifecycle lifecycle1 = new DataLifecycle(randomMillisUpToYear9999());
            DataLifecycle lifecycle2 = new DataLifecycle(randomMillisUpToYear9999());
            List<DataLifecycle> lifecycles = List.of(lifecycle1, lifecycle2);
            assertThat(DataLifecycle.compose(lifecycles), equalTo(lifecycle2));
        }

    }

    public void testDefaultClusterSetting() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RolloverConfiguration rolloverConfiguration = clusterSettings.get(DataLifecycle.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING);
        assertThat(rolloverConfiguration.getAutomaticConditions(), equalTo(Set.of("max_age")));
        RolloverConditions concreteConditions = rolloverConfiguration.getConcreteConditions();
        assertThat(concreteConditions.getMaxPrimaryShardSize(), equalTo(ByteSizeValue.ofGb(50)));
        assertThat(concreteConditions.getMaxPrimaryShardDocs(), equalTo(200_000_000L));
        assertThat(concreteConditions.getMinDocs(), equalTo(1L));
        assertThat(concreteConditions.getMaxSize(), nullValue());
        assertThat(concreteConditions.getMaxDocs(), nullValue());
        assertThat(concreteConditions.getMinAge(), nullValue());
        assertThat(concreteConditions.getMinSize(), nullValue());
        assertThat(concreteConditions.getMinPrimaryShardSize(), nullValue());
        assertThat(concreteConditions.getMinPrimaryShardDocs(), nullValue());
        assertThat(concreteConditions.getMaxAge(), nullValue());
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
