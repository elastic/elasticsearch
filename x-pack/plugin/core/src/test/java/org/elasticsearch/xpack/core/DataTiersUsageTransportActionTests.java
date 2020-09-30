/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DataTiersUsageTransportActionTests extends ESTestCase {
    public void testCalculateMAD() {
        assertThat(DataTiersUsageTransportAction.computeMedianAbsoluteDeviation(new TDigestState(10)), equalTo(0L));

        TDigestState sketch = new TDigestState(randomDoubleBetween(0, 1000, false));
        sketch.add(1);
        sketch.add(1);
        sketch.add(2);
        sketch.add(2);
        sketch.add(4);
        sketch.add(6);
        sketch.add(9);
        assertThat(DataTiersUsageTransportAction.computeMedianAbsoluteDeviation(sketch), equalTo(1L));
    }
}
