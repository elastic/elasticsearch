/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.test.ESTestCase;

public class DatafeedStatusTests extends ESTestCase {

    public void testForString() {
        assertEquals(DatafeedStatus.fromString("started"), DatafeedStatus.STARTED);
        assertEquals(DatafeedStatus.fromString("stopped"), DatafeedStatus.STOPPED);
    }

    public void testValidOrdinals() {
        assertEquals(0, DatafeedStatus.STARTED.ordinal());
        assertEquals(1, DatafeedStatus.STOPPED.ordinal());
    }

}
