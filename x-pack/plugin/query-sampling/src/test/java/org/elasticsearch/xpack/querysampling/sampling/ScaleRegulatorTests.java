/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;

public class ScaleRegulatorTests extends ESTestCase {

    private final ScaleRegulator regulator = new ScaleRegulator();

    public void testSettlesAtATargetAndStaysThere() {
        regulator.targetPerHour(3600);
        double picksPerWindowAtFullScale = 120;
        for (int window = 0; window < 20; window++) {
            pick((int) Math.round(picksPerWindowAtFullScale * regulator.adjustment()));
            regulator.observe(30);
        }

        assertThat("30 seconds of 3600 picks an hour", picksPerWindowAtFullScale * regulator.adjustment(), closeTo(30, 1.5));
    }

    private void pick(int picks) {
        for (int i = 0; i < picks; i++) {
            regulator.recordPick();
        }
    }
}
