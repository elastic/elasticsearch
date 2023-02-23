/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RolloverConditionsTests extends AbstractXContentSerializingTestCase<RolloverConditions> {

    @Override
    protected Writeable.Reader<RolloverConditions> instanceReader() {
        return RolloverConditions::new;
    }

    @Override
    protected RolloverConditions createTestInstance() {
        return randomInstance();
    }

    public static RolloverConditions randomInstance() {
        ByteSizeUnit maxSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue maxSize = randomBoolean() ? null : new ByteSizeValue(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
        ByteSizeUnit maxPrimaryShardSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue maxPrimaryShardSize = randomBoolean()
            ? null
            : new ByteSizeValue(randomNonNegativeLong() / maxPrimaryShardSizeUnit.toBytes(1), maxPrimaryShardSizeUnit);
        Long maxDocs = randomBoolean() ? null : randomNonNegativeLong();
        TimeValue maxAge = (maxDocs == null && maxSize == null || randomBoolean())
            ? TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test")
            : null;
        Long maxPrimaryShardDocs = (maxSize == null && maxPrimaryShardSize == null && maxAge == null && maxDocs == null || randomBoolean())
            ? randomNonNegativeLong()
            : null;
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
        return new RolloverConditions(
            maxSize,
            maxPrimaryShardSize,
            maxAge,
            maxDocs,
            maxPrimaryShardDocs,
            minSize,
            minPrimaryShardSize,
            minAge,
            minDocs,
            minPrimaryShardDocs
        );
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
        return new RolloverConditions(
            maxSize,
            maxPrimaryShardSize,
            maxAge,
            maxDocs,
            maxPrimaryShardDocs,
            minSize,
            minPrimaryShardSize,
            minAge,
            minDocs,
            minPrimaryShardDocs
        );
    }

    public void testNoConditions() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new RolloverConditions(null, null, null, null, null, null, null, null, null, null)
        );
        assertEquals("At least one max_* rollover condition must be set.", exception.getMessage());
    }

    public void testBwcSerializationWithMaxPrimaryShardDocs() throws Exception {
        // In case of serializing to node with older version, replace maxPrimaryShardDocs with maxDocs.
        RolloverConditions instance = new RolloverConditions(null, null, null, null, 1L, null, null, null, null, null);
        RolloverConditions deserializedInstance = copyInstance(instance, TransportVersion.V_8_1_0);
        assertThat(deserializedInstance.getMaxPrimaryShardDocs(), nullValue());

        // But not if maxDocs is also specified:
        instance = new RolloverConditions(null, null, null, 2L, 1L, null, null, null, null, null);
        deserializedInstance = copyInstance(instance, TransportVersion.V_8_1_0);
        assertThat(deserializedInstance.getMaxPrimaryShardDocs(), nullValue());
        assertThat(deserializedInstance.getMaxDocs(), equalTo(instance.getMaxDocs()));
    }

    @Override
    protected RolloverConditions doParseInstance(XContentParser parser) throws IOException {
        return RolloverConditions.fromXContent(parser);
    }
}
