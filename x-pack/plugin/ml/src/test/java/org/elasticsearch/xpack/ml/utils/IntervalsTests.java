/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.utils.Intervals;

import static org.hamcrest.Matchers.equalTo;

public class IntervalsTests extends ESTestCase {

    public void testAlignToFloor() {
        assertThat(Intervals.alignToFloor(0, 10), equalTo(0L));
        assertThat(Intervals.alignToFloor(10, 5), equalTo(10L));
        assertThat(Intervals.alignToFloor(6, 5), equalTo(5L));
        assertThat(Intervals.alignToFloor(36, 5), equalTo(35L));
        assertThat(Intervals.alignToFloor(10, 10), equalTo(10L));
        assertThat(Intervals.alignToFloor(11, 10), equalTo(10L));
        assertThat(Intervals.alignToFloor(19, 10), equalTo(10L));
        assertThat(Intervals.alignToFloor(20, 10), equalTo(20L));
        assertThat(Intervals.alignToFloor(25, 10), equalTo(20L));
        assertThat(Intervals.alignToFloor(-20, 10), equalTo(-20L));
        assertThat(Intervals.alignToFloor(-21, 10), equalTo(-30L));
    }

    public void testAlignToCeil() {
        assertThat(Intervals.alignToCeil(0, 10), equalTo(0L));
        assertThat(Intervals.alignToCeil(10, 5), equalTo(10L));
        assertThat(Intervals.alignToCeil(6, 5), equalTo(10L));
        assertThat(Intervals.alignToCeil(36, 5), equalTo(40L));
        assertThat(Intervals.alignToCeil(10, 10), equalTo(10L));
        assertThat(Intervals.alignToCeil(11, 10), equalTo(20L));
        assertThat(Intervals.alignToCeil(19, 10), equalTo(20L));
        assertThat(Intervals.alignToCeil(20, 10), equalTo(20L));
        assertThat(Intervals.alignToCeil(25, 10), equalTo(30L));
        assertThat(Intervals.alignToCeil(-20, 10), equalTo(-20L));
        assertThat(Intervals.alignToCeil(-21, 10), equalTo(-20L));
    }
}
