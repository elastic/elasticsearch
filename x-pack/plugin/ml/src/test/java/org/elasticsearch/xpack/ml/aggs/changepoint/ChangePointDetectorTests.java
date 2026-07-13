/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.util.Arrays;

import static org.hamcrest.Matchers.instanceOf;

public class ChangePointDetectorTests extends ESTestCase {

    /**
     * Testing the special case where values without the spike have a variance of 0.
     */
    public void testConstantWithSpike() {
        double[] values = new double[25];
        Arrays.fill(values, 51);
        values[12] = 500001;

        MlAggsHelper.DoubleBucketValues bucketValues = new MlAggsHelper.DoubleBucketValues(null, values);

        ChangeType change = ChangePointDetector.getChangeType(bucketValues);

        assertEquals(12, change.changePoint());
        assertThat(change, instanceOf(ChangeType.Spike.class));
    }

    public void testRandomWithSpike() {
        double[] values = new double[25];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomLongBetween(50, 53);
        }
        values[12] = 500001;

        MlAggsHelper.DoubleBucketValues bucketValues = new MlAggsHelper.DoubleBucketValues(null, values);

        ChangeType change = ChangePointDetector.getChangeType(bucketValues);

        assertEquals(12, change.changePoint());
        assertThat(change, instanceOf(ChangeType.Spike.class));
    }
}
