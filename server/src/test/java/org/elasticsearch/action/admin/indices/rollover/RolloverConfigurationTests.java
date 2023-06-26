/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RolloverConfigurationTests extends AbstractWireSerializingTestCase<RolloverConfiguration> {

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

    @Override
    protected RolloverConfiguration mutateInstance(RolloverConfiguration instance) {
        return randomValueOtherThan(instance, RolloverConfigurationTests::randomRolloverConditions);
    }

    public void testConstructorValidation() {
        {
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> new RolloverConfiguration(RolloverConditions.newBuilder().build(), Set.of("max_docs"))
            );
            assertThat(
                error.getMessage(),
                equalTo("Invalid automatic configuration for [max_docs], only condition 'max_age' is supported.")
            );
        }
        {
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> new RolloverConfiguration(RolloverConditions.newBuilder().build(), Set.of("max_docs", "max_age"))
            );
            assertThat(error.getMessage(), containsString("Invalid automatic configuration for ["));
        }
        {
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> new RolloverConfiguration(
                    RolloverConditions.newBuilder().addMaxIndexAgeCondition(TimeValue.timeValueDays(1)).build(),
                    Set.of("max_age")
                )
            );
            assertThat(
                error.getMessage(),
                equalTo("Invalid configuration for 'max_age' can be either have a value or be automatic but not both.")

            );
        }
    }

    public void testSameConditionCanOnlyBeAddedOnce() {
        RolloverConfiguration.ValueParser valueParser = new RolloverConfiguration.ValueParser();
        Consumer<RolloverConfiguration.ValueParser> rolloverRequestConsumer = randomFrom(conditionsGenerator);
        rolloverRequestConsumer.accept(valueParser);
        expectThrows(IllegalArgumentException.class, () -> rolloverRequestConsumer.accept(valueParser));
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
        RolloverConfiguration autoAgeRolloverConfiguration = new RolloverConfiguration(
            new RolloverConditions(),
            Set.of(MaxAgeCondition.NAME)
        );
        assertThat(autoAgeRolloverConfiguration.resolveRolloverConditions(null).getMaxAge(), equalTo(TimeValue.timeValueDays(30)));
        RolloverConfiguration concreteAgeRolloverConfiguration = new RolloverConfiguration(
            RolloverConditions.newBuilder().addMaxIndexAgeCondition(TimeValue.timeValueHours(3)).build()
        );
        assertThat(concreteAgeRolloverConfiguration.resolveRolloverConditions(null).getMaxAge(), equalTo(TimeValue.timeValueHours(3)));

    }

    public void testAutoMaxAgeCalculation() {
        assertThat(RolloverConfiguration.evaluateMaxAgeCondition(null), equalTo(TimeValue.timeValueDays(30)));
        assertThat(RolloverConfiguration.evaluateMaxAgeCondition(TimeValue.timeValueDays(91)), equalTo(TimeValue.timeValueDays(30)));
        assertThat(RolloverConfiguration.evaluateMaxAgeCondition(TimeValue.timeValueDays(90)), equalTo(TimeValue.timeValueDays(7)));
        assertThat(RolloverConfiguration.evaluateMaxAgeCondition(TimeValue.timeValueDays(14)), equalTo(TimeValue.timeValueDays(1)));
        assertThat(RolloverConfiguration.evaluateMaxAgeCondition(TimeValue.timeValueDays(1)), equalTo(TimeValue.timeValueDays(1)));
    }

    public void testToXContent() throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint()) {
            RolloverConfiguration rolloverConfiguration = randomRolloverConditions();
            rolloverConfiguration.toXContent(builder, EMPTY_PARAMS);
            Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            RolloverConditions concreteConditions = rolloverConfiguration.getConcreteConditions();
            if (rolloverConfiguration.getAutomaticConditions().isEmpty()) {
                if (concreteConditions.getMaxAge() == null) {
                    assertThat(xContentMap.get("max_age"), nullValue());
                } else {
                    assertThat(xContentMap.get("max_age"), is(concreteConditions.getMaxAge().getStringRep()));
                }
            } else {
                assertThat(xContentMap.get("max_age"), is("auto"));
            }
            assertThat(xContentMap.get("max_docs"), is(concreteConditions.getMaxDocs()));
            if (concreteConditions.getMaxSize() == null) {
                assertThat(xContentMap.get("max_size"), nullValue());
            } else {
                assertThat(xContentMap.get("max_size"), is(concreteConditions.getMaxSize().getStringRep()));
            }
            assertThat(xContentMap.get("max_primary_shard_docs"), is(concreteConditions.getMaxPrimaryShardDocs()));
            if (concreteConditions.getMaxPrimaryShardSize() == null) {
                assertThat(xContentMap.get("max_primary_shard_size"), nullValue());
            } else {
                assertThat(xContentMap.get("max_primary_shard_size"), is(concreteConditions.getMaxPrimaryShardSize().getStringRep()));
            }
            if (concreteConditions.getMinAge() == null) {
                assertThat(xContentMap.get("min_age"), nullValue());
            } else {
                assertThat(xContentMap.get("min_age"), is(concreteConditions.getMinAge().getStringRep()));
            }
            assertThat(xContentMap.get("min_docs"), is(concreteConditions.getMinDocs()));
            if (concreteConditions.getMinSize() == null) {
                assertThat(xContentMap.get("min_size"), nullValue());
            } else {
                assertThat(xContentMap.get("min_size"), is(concreteConditions.getMinSize().getStringRep()));
            }
            assertThat(xContentMap.get("min_primary_shard_docs"), is(concreteConditions.getMinPrimaryShardDocs()));
            if (concreteConditions.getMinPrimaryShardSize() == null) {
                assertThat(xContentMap.get("min_primary_shard_size"), nullValue());
            } else {
                assertThat(xContentMap.get("min_primary_shard_size"), is(concreteConditions.getMinPrimaryShardSize().getStringRep()));
            }
        }
    }

    public void testXContentSerializationWithKnownDataRetention() throws IOException {
        // Test with automatic condition infinite retention
        try (XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint()) {
            RolloverConfiguration rolloverConfiguration = new RolloverConfiguration(new RolloverConditions(), Set.of(MaxAgeCondition.NAME));
            rolloverConfiguration.evaluateAndConvertToXContent(builder, EMPTY_PARAMS, null);
            Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            assertThat(xContentMap.get("max_age"), is("30d [automatic]"));
        }
        // Test with automatic condition with short retention
        try (XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint()) {
            RolloverConfiguration rolloverConfiguration = new RolloverConfiguration(new RolloverConditions(), Set.of(MaxAgeCondition.NAME));
            rolloverConfiguration.evaluateAndConvertToXContent(builder, EMPTY_PARAMS, TimeValue.timeValueDays(10));
            Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            assertThat(xContentMap.get("max_age"), is("1d [automatic]"));
        }
        // Test without automatic condition
        try (XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint()) {
            RolloverConfiguration rolloverConfiguration = new RolloverConfiguration(
                RolloverConditions.newBuilder().addMaxIndexAgeCondition(TimeValue.timeValueMillis(randomMillisUpToYear9999())).build()
            );
            rolloverConfiguration.evaluateAndConvertToXContent(builder, EMPTY_PARAMS, null);
            Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            assertThat(xContentMap.get("max_age"), is(rolloverConfiguration.getConcreteConditions().getMaxAge().getStringRep()));
        }
    }

    private static final List<Consumer<RolloverConfiguration.ValueParser>> conditionsGenerator = Arrays.asList(
        (builder) -> builder.addMaxIndexDocsCondition(randomNonNegativeLong()),
        (builder) -> builder.addMaxIndexSizeCondition(randomByteSizeValue().getStringRep(), "test"),
        (builder) -> builder.addMaxIndexAgeCondition(randomPositiveTimeValue(), "test"),
        (builder) -> builder.addMaxPrimaryShardSizeCondition(randomByteSizeValue().getStringRep(), "test"),
        (builder) -> builder.addMaxPrimaryShardDocsCondition(randomNonNegativeLong()),
        (builder) -> builder.addMinIndexDocsCondition(randomNonNegativeLong()),
        (builder) -> builder.addMinIndexSizeCondition(randomByteSizeValue().getStringRep(), "test"),
        (builder) -> builder.addMinIndexAgeCondition(randomPositiveTimeValue(), "test"),
        (builder) -> builder.addMinPrimaryShardSizeCondition(randomByteSizeValue().getStringRep(), "test"),
        (builder) -> builder.addMinPrimaryShardDocsCondition(randomNonNegativeLong())
    );
}
