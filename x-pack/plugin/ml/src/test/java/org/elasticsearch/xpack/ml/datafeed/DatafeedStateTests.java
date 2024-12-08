/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;

public class DatafeedStateTests extends ESTestCase {

    public void testFromString() {
        assertEquals(DatafeedState.fromString("started"), DatafeedState.STARTED);
        assertEquals(DatafeedState.fromString("stopped"), DatafeedState.STOPPED);
    }

    public void testToString() {
        assertEquals("started", DatafeedState.STARTED.toString());
        assertEquals("stopped", DatafeedState.STOPPED.toString());
    }

    public void testValidOrdinals() {
        assertEquals(0, DatafeedState.STARTED.ordinal());
        assertEquals(1, DatafeedState.STOPPED.ordinal());
        assertEquals(2, DatafeedState.STARTING.ordinal());
        assertEquals(3, DatafeedState.STOPPING.ordinal());
    }
}
