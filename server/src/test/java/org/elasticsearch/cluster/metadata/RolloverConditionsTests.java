/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticsearchParseException;
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

        ElasticsearchParseException invalid = expectThrows(
            ElasticsearchParseException.class,
            () -> RolloverConditions.parseSetting("", "empty-setting")
        );
        assertEquals("Invalid condition: '', format must be 'condition=value'", invalid.getMessage());
        ElasticsearchParseException unknown = expectThrows(
            ElasticsearchParseException.class,
            () -> RolloverConditions.parseSetting("unknown_condition=?", "unknown-setting")
        );
        assertEquals("Unknown condition: 'unknown_condition'", unknown.getMessage());
    }

    @Override
    protected RolloverConditions doParseInstance(XContentParser parser) throws IOException {
        return RolloverConditions.fromXContent(parser);
    }
}
