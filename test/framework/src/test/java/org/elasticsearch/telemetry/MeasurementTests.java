/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class MeasurementTests extends ESTestCase {

    public void testCombineLongs() {
        Measurement m1 = new Measurement(1L, Collections.emptyMap(), false);
        Measurement m2 = new Measurement(2L, Collections.emptyMap(), false);
        Measurement m3 = new Measurement(3L, Map.of("k", "v"), false);
        Measurement m4 = new Measurement(4L, Map.of("k", "v"), false);

        List<Measurement> combined = Measurement.combine(List.of(m1, m2, m3, m4));
        assertThat(
            combined,
            containsInAnyOrder(new Measurement(3L, Collections.emptyMap(), false), new Measurement(7L, Map.of("k", "v"), false))
        );
    }

    public void testCombineDoubles() {
        Measurement m1 = new Measurement(1.0, Collections.emptyMap(), true);
        Measurement m2 = new Measurement(2.0, Collections.emptyMap(), true);
        Measurement m3 = new Measurement(3.0, Map.of("k", "v"), true);
        Measurement m4 = new Measurement(4.0, Map.of("k", "v"), true);

        List<Measurement> combined = Measurement.combine(List.of(m1, m2, m3, m4));
        assertThat(
            combined,
            containsInAnyOrder(new Measurement(3.0, Collections.emptyMap(), true), new Measurement(7.0, Map.of("k", "v"), true))
        );
    }

}
