/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ColumnIntervalsTests extends ESTestCase {

    public void testFormat() {
        assertThat(ColumnIntervals.format(new Rounding.Interval(1, "hour")), equalTo("1 hour"));
        assertThat(ColumnIntervals.format(new Rounding.Interval(90, "minute")), equalTo("90 minutes"));
        assertThat(ColumnIntervals.format(new Rounding.Interval(3, "month")), equalTo("3 months"));
    }
}
