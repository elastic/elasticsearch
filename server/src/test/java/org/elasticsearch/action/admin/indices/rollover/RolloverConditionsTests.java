/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RolloverConditionsTests extends AbstractXContentSerializingTestCase<RolloverConditions> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(IndicesModule.getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<RolloverConditions> instanceReader() {
        return RolloverConditions::new;
    }

    @Override
    protected RolloverConditions createTestInstance() {
        return randomRolloverConditions();
    }

    public static RolloverConditions randomRolloverConditions() {
        ByteSizeValue maxSize = randomBoolean() ? randomByteSizeValue() : null;
        ByteSizeValue maxPrimaryShardSize = randomBoolean() ? randomByteSizeValue() : null;
        Long maxDocs = randomBoolean() ? randomNonNegativeLong() : null;
        TimeValue maxAge = randomBoolean() ? TimeValue.timeValueMillis(randomMillisUpToYear9999()) : null;
        Long maxPrimaryShardDocs = randomBoolean() ? randomNonNegativeLong() : null;
        ByteSizeValue minSize = randomBoolean() ? randomByteSizeValue() : null;
        ByteSizeValue minPrimaryShardSize = randomBoolean() ? randomByteSizeValue() : null;
        Long minDocs = randomBoolean() ? randomNonNegativeLong() : null;
        TimeValue minAge = randomBoolean() ? TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test") : null;
        Long minPrimaryShardDocs = randomBoolean() ? randomNonNegativeLong() : null;

        return RolloverConditions.newBuilder()
            .addMaxIndexSizeCondition(maxSize)
            .addMaxPrimaryShardSizeCondition(maxPrimaryShardSize)
            .addMaxIndexAgeCondition(maxAge)
            .addMaxIndexDocsCondition(maxDocs)
            .addMaxPrimaryShardDocsCondition(maxPrimaryShardDocs)
            .addMinIndexSizeCondition(minSize)
            .addMinPrimaryShardSizeCondition(minPrimaryShardSize)
            .addMinIndexAgeCondition(minAge)
            .addMinIndexDocsCondition(minDocs)
            .addMinPrimaryShardDocsCondition(minPrimaryShardDocs)
            .build();
    }

    private static ByteSizeValue randomByteSizeValue() {
        ByteSizeUnit unit = randomFrom(ByteSizeUnit.values());
        return new ByteSizeValue(randomNonNegativeLong() / unit.toBytes(1), unit);
    }

    @Override
    protected RolloverConditions mutateInstance(RolloverConditions instance) {
        ByteSizeValue maxSize = instance.getMaxSize();
        ByteSizeValue maxPrimaryShardSize = instance.getMaxPrimaryShardSize();
        TimeValue maxAge = instance.getMaxAge();
        Long maxDocs = instance.getMaxDocs();
        Long maxPrimaryShardDocs = instance.getMaxPrimaryShardDocs();
        ByteSizeValue minSize = instance.getMinSize();
        ByteSizeValue minPrimaryShardSize = instance.getMinPrimaryShardSize();
        TimeValue minAge = instance.getMinAge();
        Long minDocs = instance.getMinDocs();
        Long minPrimaryShardDocs = instance.getMinPrimaryShardDocs();
        switch (between(0, 9)) {
            case 0 -> maxSize = randomValueOtherThan(maxSize, () -> {
                ByteSizeUnit maxSizeUnit = randomFrom(ByteSizeUnit.values());
                return new ByteSizeValue(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
            });
            case 1 -> maxPrimaryShardSize = randomValueOtherThan(maxPrimaryShardSize, () -> {
                ByteSizeUnit maxPrimaryShardSizeUnit = randomFrom(ByteSizeUnit.values());
                return new ByteSizeValue(randomNonNegativeLong() / maxPrimaryShardSizeUnit.toBytes(1), maxPrimaryShardSizeUnit);
            });
            case 2 -> maxAge = randomValueOtherThan(
                maxAge,
                () -> TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test")
            );
            case 3 -> maxDocs = maxDocs == null ? randomNonNegativeLong() : maxDocs + 1;
            case 4 -> maxPrimaryShardDocs = maxPrimaryShardDocs == null ? randomNonNegativeLong() : maxPrimaryShardDocs + 1;
            case 5 -> minSize = randomValueOtherThan(minSize, () -> {
                ByteSizeUnit minSizeUnit = randomFrom(ByteSizeUnit.values());
                return new ByteSizeValue(randomNonNegativeLong() / minSizeUnit.toBytes(1), minSizeUnit);
            });
            case 6 -> minPrimaryShardSize = randomValueOtherThan(minPrimaryShardSize, () -> {
                ByteSizeUnit minPrimaryShardSizeUnit = randomFrom(ByteSizeUnit.values());
                return new ByteSizeValue(randomNonNegativeLong() / minPrimaryShardSizeUnit.toBytes(1), minPrimaryShardSizeUnit);
            });
            case 7 -> minAge = randomValueOtherThan(
                minAge,
                () -> TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test")
            );
            case 8 -> minDocs = minDocs == null ? randomNonNegativeLong() : minDocs + 1;
            case 9 -> minPrimaryShardDocs = minPrimaryShardDocs == null ? randomNonNegativeLong() : minPrimaryShardDocs + 1;
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return RolloverConditions.newBuilder()
            .addMaxIndexSizeCondition(maxSize)
            .addMaxPrimaryShardSizeCondition(maxPrimaryShardSize)
            .addMaxIndexAgeCondition(maxAge)
            .addMaxIndexDocsCondition(maxDocs)
            .addMaxPrimaryShardDocsCondition(maxPrimaryShardDocs)

            .addMinIndexSizeCondition(minSize)
            .addMinPrimaryShardSizeCondition(minPrimaryShardSize)
            .addMinIndexAgeCondition(minAge)
            .addMinIndexDocsCondition(minDocs)
            .addMinPrimaryShardDocsCondition(minPrimaryShardDocs)
            .build();
    }

    @Override
    protected RolloverConditions doParseInstance(XContentParser parser) throws IOException {
        return RolloverConditions.fromXContent(parser);
    }

    public void testSameConditionCanOnlyBeAddedOnce() {
        RolloverConditions.Builder builder = RolloverConditions.newBuilder();
        Consumer<RolloverConditions.Builder> rolloverRequestConsumer = randomFrom(conditionsGenerator);
        rolloverRequestConsumer.accept(builder);
        expectThrows(IllegalArgumentException.class, () -> rolloverRequestConsumer.accept(builder));
    }

    public void testConditionsAreMet() {
        RolloverConditions rolloverConditions = new RolloverConditions();
        assertTrue(rolloverConditions.areConditionsMet(Collections.emptyMap()));

        TimeValue age = TimeValue.timeValueSeconds(5);
        rolloverConditions = RolloverConditions.newBuilder().addMaxIndexAgeCondition(age).build();
        String maxAgeCondition = new MaxAgeCondition(age).toString();
        assertFalse(rolloverConditions.areConditionsMet(Map.of(maxAgeCondition, false)));
        assertTrue(rolloverConditions.areConditionsMet(Map.of(maxAgeCondition, true)));

        rolloverConditions = RolloverConditions.newBuilder(rolloverConditions).addMaxIndexDocsCondition(100L).build();
        String maxDocsCondition = new MaxDocsCondition(100L).toString();
        assertFalse(rolloverConditions.areConditionsMet(Map.of(maxAgeCondition, false)));
        assertTrue(rolloverConditions.areConditionsMet(Map.of(maxAgeCondition, true)));
        assertFalse(rolloverConditions.areConditionsMet(Map.of(maxDocsCondition, false)));
        assertTrue(rolloverConditions.areConditionsMet(Map.of(maxDocsCondition, true)));

        rolloverConditions = RolloverConditions.newBuilder(rolloverConditions).addMinIndexDocsCondition(1L).build();
        String minDocsCondition = new MinDocsCondition(1L).toString();
        assertFalse(rolloverConditions.areConditionsMet(Map.of(maxAgeCondition, false)));
        assertFalse(rolloverConditions.areConditionsMet(Map.of(maxAgeCondition, true)));
        assertFalse(rolloverConditions.areConditionsMet(Map.of(maxDocsCondition, false)));
        assertFalse(rolloverConditions.areConditionsMet(Map.of(maxDocsCondition, true)));
        assertFalse(rolloverConditions.areConditionsMet(Map.of(minDocsCondition, true)));
        assertTrue(rolloverConditions.areConditionsMet(Map.of(maxAgeCondition, true, minDocsCondition, true)));

        rolloverConditions = RolloverConditions.newBuilder(rolloverConditions).addMinIndexAgeCondition(age).build();
        String minAgeCondition = new MinAgeCondition(age).toString();
        assertFalse(rolloverConditions.areConditionsMet(Map.of(maxAgeCondition, true, minDocsCondition, true)));
        assertTrue(rolloverConditions.areConditionsMet(Map.of(maxAgeCondition, true, minDocsCondition, true, minAgeCondition, true)));
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

        IllegalArgumentException invalid = expectThrows(
            IllegalArgumentException.class,
            () -> RolloverConditions.parseSetting("", "empty-setting")
        );
        assertEquals("The rollover conditions cannot be null or blank", invalid.getMessage());
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
