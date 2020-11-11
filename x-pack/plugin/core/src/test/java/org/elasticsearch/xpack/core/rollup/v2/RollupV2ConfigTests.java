/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.v2;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;


public class RollupV2ConfigTests extends AbstractSerializingTestCase<RollupV2Config> {

    String sourceIndex;

    @Before
    void setUpSourceIndex() {
        sourceIndex = randomAlphaOfLength(10);
    }

    @Override
    protected RollupV2Config createTestInstance() {
        return randomConfig(random(), sourceIndex);
    }

    public static RollupV2Config randomConfig(Random random, String sourceIndex) {
        final String rollupIndex = "rollup-" + sourceIndex;
        final TimeValue timeout = random.nextBoolean() ? null : ConfigTestHelpers.randomTimeout(random);
        final GroupConfig groupConfig = ConfigTestHelpers.randomGroupConfig(random);
        final List<MetricConfig> metricConfigs = ConfigTestHelpers.randomMetricsConfigs(random);
        return new RollupV2Config(sourceIndex, groupConfig, metricConfigs, timeout, rollupIndex);
    }

    @Override
    protected Writeable.Reader<RollupV2Config> instanceReader() {
        return RollupV2Config::new;
    }

    @Override
    protected RollupV2Config doParseInstance(final XContentParser parser) throws IOException {
        return RollupV2Config.fromXContent(parser, sourceIndex);
    }

    public void testEmptySourceIndex() {
        final RollupV2Config sample = createTestInstance();
        Exception e = expectThrows(IllegalArgumentException.class, () ->
            new RollupV2Config(randomBoolean() ? null : "", sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout(),
                sample.getRollupIndex()));
        assertThat(e.getMessage(), equalTo("The source index must be a non-null, non-empty string"));
    }

    public void testEmptyRollupIndex() {
        final RollupV2Config sample = createTestInstance();
        Exception e = expectThrows(IllegalArgumentException.class, () ->
            new RollupV2Config(sample.getSourceIndex(), sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout(),
                randomBoolean() ? null : ""));
        assertThat(e.getMessage(), equalTo("Rollup index must be a non-null, non-empty string"));
    }

    public void testEmptyGroupAndMetrics() {
        final RollupV2Config sample = createTestInstance();
        Exception e = expectThrows(IllegalArgumentException.class, () ->
            new RollupV2Config(sample.getSourceIndex(), null, randomBoolean() ? null : emptyList(), sample.getTimeout(),
                sample.getRollupIndex()));
        assertThat(e.getMessage(), equalTo("At least one grouping or metric must be configured"));
    }
}
