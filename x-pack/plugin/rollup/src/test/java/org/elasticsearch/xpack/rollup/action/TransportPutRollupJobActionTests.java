/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.PutRollupJobAction;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

public class TransportPutRollupJobActionTests extends ESTestCase {

    public void testUnsupportedMetric() {
        final List<String> metrics = new ArrayList<>(randomSubsetOf(RollupField.SUPPORTED_METRICS));
        metrics.add("_test_");

        PutRollupJobAction.Request request = new PutRollupJobAction.Request();
        request.setConfig(ConfigTestHelpers.getRollupJob("foo")
                                            .setMetricsConfig(singletonList(new MetricConfig("test", metrics)))
                                            .build());

        Exception e = expectThrows(IllegalArgumentException.class, () -> TransportPutRollupJobAction.validate(request));
        assertThat(e.getMessage(), equalTo("Unsupported metric [_test_]. Supported metrics include: [max, min, sum, avg, value_count]"));
    }
}
