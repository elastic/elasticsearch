/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RolloverConfigurationTests extends AbstractXContentSerializingTestCase<RolloverConfiguration> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(IndicesModule.getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<RolloverConfiguration> instanceReader() {
        return RolloverConfiguration::new;
    }

    @Override
    protected RolloverConfiguration createTestInstance() {
        return randomRolloverConditions();
    }

    public static RolloverConfiguration randomRolloverConditions() {
        ByteSizeValue maxSize = randomBoolean() ? randomByteSizeValue() : null;
        ByteSizeValue maxPrimaryShardSize = randomBoolean() ? randomByteSizeValue() : null;
        Long maxDocs = randomBoolean() ? randomNonNegativeLong() : null;
        Long maxPrimaryShardDocs = randomBoolean() ? randomNonNegativeLong() : null;
        ByteSizeValue minSize = randomBoolean() ? randomByteSizeValue() : null;
        ByteSizeValue minPrimaryShardSize = randomBoolean() ? randomByteSizeValue() : null;
        Long minDocs = randomBoolean() ? randomNonNegativeLong() : null;
        TimeValue minAge = randomBoolean() ? TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test") : null;
        Long minPrimaryShardDocs = randomBoolean() ? randomNonNegativeLong() : null;

        RolloverConditions.Builder concreteConditionsBuilder = RolloverConditions.newBuilder()
            .addMaxIndexSizeCondition(maxSize)
            .addMaxPrimaryShardSizeCondition(maxPrimaryShardSize)
            .addMaxIndexDocsCondition(maxDocs)
            .addMaxPrimaryShardDocsCondition(maxPrimaryShardDocs)
            .addMinIndexSizeCondition(minSize)
            .addMinPrimaryShardSizeCondition(minPrimaryShardSize)
            .addMinIndexAgeCondition(minAge)
            .addMinIndexDocsCondition(minDocs)
            .addMinPrimaryShardDocsCondition(minPrimaryShardDocs);
        Set<String> automaticConditions = new HashSet<>();
        if (randomBoolean()) {
            if (randomBoolean()) {
                concreteConditionsBuilder.addMaxIndexAgeCondition(TimeValue.timeValueMillis(randomMillisUpToYear9999()));
            } else {
                automaticConditions.add(MaxAgeCondition.NAME);
            }
        }
        return new RolloverConfiguration(concreteConditionsBuilder.build(), automaticConditions);
    }

    private static ByteSizeValue randomByteSizeValue() {
        ByteSizeUnit unit = randomFrom(ByteSizeUnit.values());
        return new ByteSizeValue(randomNonNegativeLong() / unit.toBytes(1), unit);
    }

    @Override
    protected RolloverConfiguration mutateInstance(RolloverConfiguration instance) {
        return randomValueOtherThan(instance, RolloverConfigurationTests::randomRolloverConditions);
    }

    @Override
    protected RolloverConfiguration doParseInstance(XContentParser parser) throws IOException {
        return RolloverConfiguration.fromXContent(parser);
    }

    public void testSameConditionCanOnlyBeAddedOnce() {
        RolloverConditions.Builder builder = RolloverConditions.newBuilder();
        Consumer<RolloverConditions.Builder> rolloverRequestConsumer = randomFrom(conditionsGenerator);
        rolloverRequestConsumer.accept(builder);
        expectThrows(IllegalArgumentException.class, () -> rolloverRequestConsumer.accept(builder));
    }

    public void testClusterSettingParsing() {
        RolloverConfiguration defaultSetting = RolloverConfiguration.parseSetting(
            "max_age=auto,max_primary_shard_size=50gb,min_docs=1,max_primary_shard_docs=200000000",
            "test-setting"
        );

        assertThat(defaultSetting.getConcreteConditions().getMaxSize(), nullValue());
        assertThat(defaultSetting.getConcreteConditions().getMaxPrimaryShardSize(), equalTo(ByteSizeValue.ofGb(50)));
        assertThat(defaultSetting.getConcreteConditions().getMaxAge(), nullValue());
        assertThat(defaultSetting.getAutomaticConditions(), equalTo(Set.of(MaxAgeCondition.NAME)));
        assertThat(defaultSetting.getConcreteConditions().getMaxDocs(), nullValue());
        assertThat(defaultSetting.getConcreteConditions().getMaxPrimaryShardDocs(), equalTo(200_000_000L));

        assertThat(defaultSetting.getConcreteConditions().getMinSize(), nullValue());
        assertThat(defaultSetting.getConcreteConditions().getMinPrimaryShardSize(), nullValue());
        assertThat(defaultSetting.getConcreteConditions().getMinAge(), nullValue());
        assertThat(defaultSetting.getConcreteConditions().getMinDocs(), equalTo(1L));
        assertThat(defaultSetting.getConcreteConditions().getMinPrimaryShardDocs(), nullValue());

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
        // With concrete conditions only
        {
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
            RolloverConditions randomSetting = RolloverConfiguration.parseSetting(setting, "test2").resolveRolloverConditions(null);
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
        }

        // With auto setting
        {
            String setting = "max_size="
                + maxSize.getStringRep()
                + ",max_primary_shard_size="
                + maxPrimaryShardSize.getStringRep()
                + ",max_age=auto,max_docs="
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
            RolloverConfiguration randomSettingWithAutomaticMaxAge = RolloverConfiguration.parseSetting(setting, "testAutomaticMaxAge");
            RolloverConditions concrete = randomSettingWithAutomaticMaxAge.resolveRolloverConditions(null);
            assertThat(concrete.getMaxAge(), equalTo(TimeValue.timeValueDays(30)));
            assertThat(concrete.getMaxPrimaryShardSize(), equalTo(maxPrimaryShardSize));
            assertThat(concrete.getMaxDocs(), equalTo(maxDocs));
            assertThat(concrete.getMaxPrimaryShardDocs(), equalTo(maxPrimaryShardDocs));
            assertThat(concrete.getMaxSize(), equalTo(maxSize));

            assertThat(concrete.getMinAge(), equalTo(minAge));
            assertThat(concrete.getMinPrimaryShardSize(), equalTo(minPrimaryShardSize));
            assertThat(concrete.getMinPrimaryShardDocs(), equalTo(minPrimaryShardDocs));
            assertThat(concrete.getMinDocs(), equalTo(minDocs));
            assertThat(concrete.getMinSize(), equalTo(minSize));
        }
        IllegalArgumentException invalid = expectThrows(
            IllegalArgumentException.class,
            () -> RolloverConfiguration.parseSetting("", "empty-setting")
        );
        assertEquals("The rollover conditions cannot be null or blank", invalid.getMessage());
        SettingsException unknown = expectThrows(
            SettingsException.class,
            () -> RolloverConfiguration.parseSetting("unknown_condition=?", "unknown-setting")
        );
        assertEquals("Unknown condition: 'unknown_condition'", unknown.getMessage());
        SettingsException numberFormat = expectThrows(
            SettingsException.class,
            () -> RolloverConfiguration.parseSetting("max_docs=one", "invalid-number-setting")
        );
        assertEquals(
            "Invalid value 'one' in setting 'invalid-number-setting', the value is expected to be of type long",
            numberFormat.getMessage()
        );
    }

    public void testConcreteRolloverConditionCalculation() {
        RolloverConfiguration rolloverConfiguration = new RolloverConfiguration(new RolloverConditions(), Set.of(MaxAgeCondition.NAME));
        assertThat(rolloverConfiguration.resolveRolloverConditions(null).getMaxAge(), equalTo(TimeValue.timeValueDays(30)));
        assertThat(
            rolloverConfiguration.resolveRolloverConditions(TimeValue.timeValueDays(91)).getMaxAge(),
            equalTo(TimeValue.timeValueDays(30))
        );
        assertThat(
            rolloverConfiguration.resolveRolloverConditions(TimeValue.timeValueDays(90)).getMaxAge(),
            equalTo(TimeValue.timeValueDays(7))
        );
        assertThat(
            rolloverConfiguration.resolveRolloverConditions(TimeValue.timeValueDays(1)).getMaxAge(),
            equalTo(TimeValue.timeValueDays(7))
        );
    }

    public void testXContentSerializationWithKnownDataRetention() throws IOException {
        // Test with automatic condition
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            RolloverConfiguration rolloverConfiguration = new RolloverConfiguration(new RolloverConditions(), Set.of(MaxAgeCondition.NAME));
            rolloverConfiguration.toXContent(builder, ToXContent.EMPTY_PARAMS, null);
            String serialized = Strings.toString(builder);
            assertThat(serialized, equalTo("{\"max_age\":\"30d [automatic]\"}"));
        }
        // Test without automatic condition
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            RolloverConfiguration rolloverConfiguration = new RolloverConfiguration(
                RolloverConditions.newBuilder().addMaxIndexAgeCondition(TimeValue.timeValueDays(7)).build(),
                Set.of()
            );
            rolloverConfiguration.toXContent(builder, ToXContent.EMPTY_PARAMS, null);
            String serialized = Strings.toString(builder);
            assertThat(serialized, equalTo("{\"max_age\":\"7d\"}"));
        }
    }

    private static final List<Consumer<RolloverConditions.Builder>> conditionsGenerator = Arrays.asList(
        (builder) -> builder.addMaxIndexDocsCondition(randomNonNegativeLong()),
        (builder) -> builder.addMaxIndexSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong())),
        (builder) -> builder.addMaxIndexAgeCondition(new TimeValue(randomNonNegativeLong())),
        (builder) -> builder.addMaxPrimaryShardSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong())),
        (builder) -> builder.addMaxPrimaryShardDocsCondition(randomNonNegativeLong()),
        (builder) -> builder.addMinIndexDocsCondition(randomNonNegativeLong()),
        (builder) -> builder.addMinIndexSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong())),
        (builder) -> builder.addMinIndexAgeCondition(new TimeValue(randomNonNegativeLong())),
        (builder) -> builder.addMinPrimaryShardSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong())),
        (builder) -> builder.addMinPrimaryShardDocsCondition(randomNonNegativeLong())
    );
}
