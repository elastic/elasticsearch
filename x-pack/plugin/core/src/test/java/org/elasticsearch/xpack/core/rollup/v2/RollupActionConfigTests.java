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
        final TimeValue timeout = random.nextBoolean() ? null : ConfigTestHelpers.randomTimeout(random);
        final GroupConfig groupConfig = ConfigTestHelpers.randomGroupConfig(random);
        final List<MetricConfig> metricConfigs = ConfigTestHelpers.randomMetricsConfigs(random);
        return new RollupActionConfig(groupConfig, metricConfigs, timeout);
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
        final RollupActionConfig sample = createTestInstance();
        Exception e = expectThrows(IllegalArgumentException.class, () ->
            new RollupActionConfig(null, randomBoolean() ? null : emptyList(), sample.getTimeout()));
        assertThat(e.getMessage(), equalTo("At least one grouping or metric must be configured"));
    }
}
