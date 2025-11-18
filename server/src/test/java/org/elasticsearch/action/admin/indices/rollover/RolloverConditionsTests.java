/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.datastreams.autosharding.AutoShardingResult;
import org.elasticsearch.action.datastreams.autosharding.AutoShardingType;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.is;

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
        TimeValue minAge = randomBoolean() ? randomPositiveTimeValue() : null;
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
                return ByteSizeValue.of(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
            });
            case 1 -> maxPrimaryShardSize = randomValueOtherThan(maxPrimaryShardSize, () -> {
                ByteSizeUnit maxPrimaryShardSizeUnit = randomFrom(ByteSizeUnit.values());
                return ByteSizeValue.of(randomNonNegativeLong() / maxPrimaryShardSizeUnit.toBytes(1), maxPrimaryShardSizeUnit);
            });
            case 2 -> maxAge = randomValueOtherThan(maxAge, () -> randomPositiveTimeValue());
            case 3 -> maxDocs = maxDocs == null ? randomNonNegativeLong() : maxDocs + 1;
            case 4 -> maxPrimaryShardDocs = maxPrimaryShardDocs == null ? randomNonNegativeLong() : maxPrimaryShardDocs + 1;
            case 5 -> minSize = randomValueOtherThan(minSize, () -> {
                ByteSizeUnit minSizeUnit = randomFrom(ByteSizeUnit.values());
                return ByteSizeValue.of(randomNonNegativeLong() / minSizeUnit.toBytes(1), minSizeUnit);
            });
            case 6 -> minPrimaryShardSize = randomValueOtherThan(minPrimaryShardSize, () -> {
                ByteSizeUnit minPrimaryShardSizeUnit = randomFrom(ByteSizeUnit.values());
                return ByteSizeValue.of(randomNonNegativeLong() / minPrimaryShardSizeUnit.toBytes(1), minPrimaryShardSizeUnit);
            });
            case 7 -> minAge = randomValueOtherThan(minAge, () -> randomPositiveTimeValue());
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

        OptimalShardCountCondition optimalShardCountCondition = new OptimalShardCountCondition(3);
        rolloverConditions = RolloverConditions.newBuilder()
            .addOptimalShardCountCondition(
                randomBoolean()
                    ? new AutoShardingResult(AutoShardingType.INCREASE_SHARDS, 1, 3, TimeValue.ZERO)
                    : new AutoShardingResult(AutoShardingType.DECREASE_SHARDS, 7, 3, TimeValue.ZERO)
            )
            .build();
        assertThat(rolloverConditions.areConditionsMet(Map.of(optimalShardCountCondition.toString(), true)), is(true));
        assertThat(rolloverConditions.areConditionsMet(Map.of(optimalShardCountCondition.toString(), false)), is(false));

        // the rollover condition must be INCREASE or DECREASE_SHARDS, any other type should be ignored
        rolloverConditions = RolloverConditions.newBuilder()
            .addOptimalShardCountCondition(
                new AutoShardingResult(
                    randomFrom(
                        AutoShardingType.COOLDOWN_PREVENTED_INCREASE,
                        AutoShardingType.COOLDOWN_PREVENTED_DECREASE,
                        AutoShardingType.NO_CHANGE_REQUIRED,
                        AutoShardingType.NOT_APPLICABLE
                    ),
                    1,
                    3,
                    TimeValue.ZERO
                )
            )
            .build();
        assertThat(rolloverConditions.getConditions().size(), is(0));
    }
}
