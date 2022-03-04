/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupActionConfig(null, randomBoolean() ? null : emptyList())
        );
        assertThat(e.getMessage(), equalTo("At least one grouping or metric must be configured"));
    }

    public void testEmptyMetrics() {
        final RollupActionGroupConfig groupConfig = ConfigTestHelpers.randomRollupActionGroupConfig(random());
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupActionConfig(groupConfig, randomBoolean() ? null : emptyList())
        );
        assertThat(e.getMessage(), equalTo("At least one metric must be configured"));
    }

    public void testValidateMapping() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();
        String type = getRandomType();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities myFieldCaps = mock(FieldCapabilities.class);
        when(myFieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap(type, myFieldCaps));
        responseMap.put("date_field", Collections.singletonMap("date", myFieldCaps));
        responseMap.put("group_field", Collections.singletonMap("keyword", myFieldCaps));
        responseMap.put("metric_field", Collections.singletonMap("short", myFieldCaps));

        RollupActionConfig config = new RollupActionConfig(
            new RollupActionGroupConfig(
                new RollupActionDateHistogramGroupConfig.FixedInterval("date_field", DateHistogramInterval.DAY),
                null,
                new TermsGroupConfig("group_field")
            ),
            List.of(new MetricConfig("metric_field", List.of("max")))
        );
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().size(), equalTo(0));
    }

    private String getRandomType() {
        int n = randomIntBetween(0, 8);
        if (n == 0) {
            return "keyword";
        } else if (n == 1) {
            return "text";
        } else if (n == 2) {
            return "long";
        } else if (n == 3) {
            return "integer";
        } else if (n == 4) {
            return "short";
        } else if (n == 5) {
            return "float";
        } else if (n == 6) {
            return "double";
        } else if (n == 7) {
            return "scaled_float";
        } else if (n == 8) {
            return "half_float";
        }
        return "long";
    }
}
