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
        ByteSizeUnit maxSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue maxSize = randomBoolean() ? null : new ByteSizeValue(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
        ByteSizeUnit maxPrimaryShardSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue maxPrimaryShardSize = randomBoolean()
            ? null
            : new ByteSizeValue(randomNonNegativeLong() / maxPrimaryShardSizeUnit.toBytes(1), maxPrimaryShardSizeUnit);
        Long maxDocs = randomBoolean() ? null : randomNonNegativeLong();
        TimeValue maxAge = randomBoolean() ? TimeValue.timeValueMillis(randomMillisUpToYear9999()) : null;
        Long maxPrimaryShardDocs = randomBoolean() ? randomNonNegativeLong() : null;
        ByteSizeUnit minSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue minSize = randomBoolean() ? null : new ByteSizeValue(randomNonNegativeLong() / minSizeUnit.toBytes(1), minSizeUnit);
        ByteSizeUnit minPrimaryShardSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue minPrimaryShardSize = randomBoolean()
            ? null
            : new ByteSizeValue(randomNonNegativeLong() / minPrimaryShardSizeUnit.toBytes(1), minPrimaryShardSizeUnit);
        Long minDocs = randomBoolean() ? null : randomNonNegativeLong();
        TimeValue minAge = (minDocs == null || randomBoolean())
            ? TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test")
            : null;
        Long minPrimaryShardDocs = (minSize == null && minPrimaryShardSize == null && minAge == null && minDocs == null || randomBoolean())
            ? randomNonNegativeLong()
            : null;

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
