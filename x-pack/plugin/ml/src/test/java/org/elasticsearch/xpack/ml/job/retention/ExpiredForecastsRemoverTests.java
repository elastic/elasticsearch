/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.test.ESTestCase;

import java.util.Date;

public class ExpiredForecastsRemoverTests extends ESTestCase {

    public void testDateParsing() {
        assertEquals(Long.valueOf(1462096800000L), ExpiredForecastsRemover.parseDateField("1462096800000"));
        assertEquals(Long.valueOf(1462096800000L), ExpiredForecastsRemover.parseDateField(1462096800000L));
        assertNull(ExpiredForecastsRemover.parseDateField(new Date()));
    }
}
