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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
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
        return randomLifecycle();
    }

    @Override
    protected DataLifecycle mutateInstance(DataLifecycle instance) throws IOException {
        var retention = instance.getDataRetention();
        var downsampling = instance.getDownsampling();
        if (randomBoolean()) {
            if (retention == null || retention == DataLifecycle.Retention.NULL) {
                retention = randomValueOtherThan(retention, DataLifecycleTests::randomRetention);
            } else {
                retention = switch (randomInt(2)) {
                    case 0 -> null;
                    case 1 -> DataLifecycle.Retention.NULL;
                    default -> new DataLifecycle.Retention(
                        TimeValue.timeValueMillis(randomValueOtherThan(retention.value().millis(), ESTestCase::randomMillisUpToYear9999))
                    );
                };
            }
        } else {
            if (downsampling == null || downsampling == DataLifecycle.Downsampling.NULL) {
                downsampling = randomValueOtherThan(downsampling, DataLifecycleTests::randomDownsampling);
            } else {
                downsampling = switch (randomInt(2)) {
                    case 0 -> null;
                    case 1 -> DataLifecycle.Downsampling.NULL;
                    default -> {
                        if (downsampling.rounds().size() == 1) {
                            yield new DataLifecycle.Downsampling(
                                List.of(downsampling.rounds().get(0), nextRound(downsampling.rounds().get(0)))
                            );

                        } else {
                            var updatedRounds = new ArrayList<>(downsampling.rounds());
                            updatedRounds.remove(randomInt(downsampling.rounds().size() - 1));
                            yield new DataLifecycle.Downsampling(updatedRounds);
                        }
                    }
                };
            }
        }
        return new DataLifecycle(retention, downsampling);
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
            for (String label : rolloverConfiguration.resolveRolloverConditions(dataLifecycle.getEffectiveDataRetention())
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

    public void testDefaultClusterSetting() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RolloverConfiguration rolloverConfiguration = clusterSettings.get(DataLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING);
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
                () -> DataLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.get(
                    Settings.builder().put(DataLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getKey(), "").build()
                )
            );
            assertThat(exception.getMessage(), equalTo("The rollover conditions cannot be null or blank"));
        }
    }

    public void testInvalidDownsamplingConfiguration() {
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> new DataLifecycle.Downsampling(
                    List.of(
                        new DataLifecycle.Downsampling.Round(TimeValue.timeValueDays(10), TimeValue.timeValueHours(2)),
                        new DataLifecycle.Downsampling.Round(TimeValue.timeValueDays(3), TimeValue.timeValueHours(2))
                    )
                )
            );
            assertThat(
                exception.getMessage(),
                equalTo("A downsampling round must have a later 'after' value than the proceeding, 3d is not after 10d.")
            );
        }
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> new DataLifecycle.Downsampling(
                    List.of(
                        new DataLifecycle.Downsampling.Round(TimeValue.timeValueDays(10), TimeValue.timeValueHours(2)),
                        new DataLifecycle.Downsampling.Round(TimeValue.timeValueDays(30), TimeValue.timeValueMinutes(2))
                    )
                )
            );
            assertThat(
                exception.getMessage(),
                equalTo("A downsampling round must have a larger 'fixed_interval' value than the proceeding, 2m is not larger than 2h.")
            );
        }
        {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> new DataLifecycle.Downsampling(List.of())
            );
            assertThat(exception.getMessage(), equalTo("Downsampling configuration should have at least one round configured."));
        }
    }

    @Nullable
    public static DataLifecycle randomLifecycle() {
        return new DataLifecycle(randomRetention(), randomDownsampling());
    }

    @Nullable
    private static DataLifecycle.Retention randomRetention() {
        return switch (randomInt(2)) {
            case 0 -> null;
            case 1 -> DataLifecycle.Retention.NULL;
            default -> new DataLifecycle.Retention(TimeValue.timeValueMillis(randomMillisUpToYear9999()));
        };
    }

    @Nullable
    private static DataLifecycle.Downsampling randomDownsampling() {
        return switch (randomInt(2)) {
            case 0 -> null;
            case 1 -> DataLifecycle.Downsampling.NULL;
            default -> {
                var count = randomIntBetween(0, 10);
                List<DataLifecycle.Downsampling.Round> rounds = new ArrayList<>();
                var previous = new DataLifecycle.Downsampling.Round(
                    TimeValue.timeValueDays(randomIntBetween(1, 365)),
                    TimeValue.timeValueHours(randomIntBetween(1, 24))
                );
                rounds.add(previous);
                for (int i = 0; i < count; i++) {
                    DataLifecycle.Downsampling.Round round = nextRound(previous);
                    rounds.add(round);
                    previous = round;
                }
                yield new DataLifecycle.Downsampling(rounds);
            }
        };
    }

    private static DataLifecycle.Downsampling.Round nextRound(DataLifecycle.Downsampling.Round previous) {
        var after = TimeValue.timeValueDays(previous.after().days() + randomIntBetween(1, 10));
        var fixedInterval = TimeValue.timeValueHours(previous.after().hours() + randomIntBetween(1, 10));
        return new DataLifecycle.Downsampling.Round(after, fixedInterval);
    }
}
