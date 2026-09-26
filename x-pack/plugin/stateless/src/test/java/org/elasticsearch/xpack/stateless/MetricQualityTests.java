/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class MetricQualityTests extends ESTestCase {

    public void testIsLowerQualityThan() {
        // If this fails, we need to update the test
        assertThat(Set.of(MetricQuality.values()), equalTo(Set.of(MetricQuality.EXACT, MetricQuality.MINIMUM, MetricQuality.MISSING)));

        assertFalse(MetricQuality.EXACT.isLowerQualityThan(MetricQuality.EXACT));
        assertFalse(MetricQuality.EXACT.isLowerQualityThan(MetricQuality.MINIMUM));
        assertFalse(MetricQuality.EXACT.isLowerQualityThan(MetricQuality.MISSING));

        assertTrue(MetricQuality.MINIMUM.isLowerQualityThan(MetricQuality.EXACT));
        assertFalse(MetricQuality.MINIMUM.isLowerQualityThan(MetricQuality.MINIMUM));
        assertFalse(MetricQuality.MINIMUM.isLowerQualityThan(MetricQuality.MISSING));

        assertTrue(MetricQuality.MISSING.isLowerQualityThan(MetricQuality.EXACT));
        assertTrue(MetricQuality.MISSING.isLowerQualityThan(MetricQuality.MINIMUM));
        assertFalse(MetricQuality.MISSING.isLowerQualityThan(MetricQuality.MISSING));
    }
}
