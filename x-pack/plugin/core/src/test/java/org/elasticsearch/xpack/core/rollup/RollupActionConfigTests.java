/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;


public class RollupActionConfigTests extends AbstractSerializingTestCase<RollupActionConfig> {

    @Override
    protected RollupActionConfig createTestInstance() {
        return randomConfig(random());
    }

    public static RollupActionConfig randomConfig(Random random) {
        final RollupActionGroupConfig groupConfig = ConfigTestHelpers.randomRollupActionGroupConfig(random);
        final List<MetricConfig> metricConfigs = ConfigTestHelpers.randomMetricsConfigs(random);
        return new RollupActionConfig(groupConfig, metricConfigs);
    }

    @Override
    protected Writeable.Reader<RollupActionConfig> instanceReader() {
        return RollupActionConfig::new;
    }

    @Override
    protected RollupActionConfig doParseInstance(final XContentParser parser) throws IOException {
        return RollupActionConfig.fromXContent(parser);
    }

    public void testEmptyGroupAndMetrics() {
        Exception e = expectThrows(IllegalArgumentException.class, () ->
            new RollupActionConfig(null, randomBoolean() ? null : emptyList()));
        assertThat(e.getMessage(), equalTo("At least one grouping or metric must be configured"));
    }

    public void testEmptyMetrics() {
        final RollupActionGroupConfig groupConfig = ConfigTestHelpers.randomRollupActionGroupConfig(random());
        Exception e = expectThrows(IllegalArgumentException.class, () ->
            new RollupActionConfig(groupConfig, randomBoolean() ? null : emptyList()));
        assertThat(e.getMessage(), equalTo("At least one metric must be configured"));
    }
}
