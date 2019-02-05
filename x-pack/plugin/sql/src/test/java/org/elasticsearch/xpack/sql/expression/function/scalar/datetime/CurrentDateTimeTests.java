/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.test.ESTestCase;

import java.time.ZonedDateTime;

public class CurrentDateTimeTests extends ESTestCase {

    public void testNanoPrecision() throws Exception {
        ZonedDateTime zdt = ZonedDateTime.parse("2018-01-23T12:34:45.123456789Z");
        assertEquals(000_000_000, CurrentDateTime.nanoPrecision(zdt, 0).getNano());
        assertEquals(100_000_000, CurrentDateTime.nanoPrecision(zdt, 1).getNano());
        assertEquals(120_000_000, CurrentDateTime.nanoPrecision(zdt, 2).getNano());
        assertEquals(123_000_000, CurrentDateTime.nanoPrecision(zdt, 3).getNano());
        assertEquals(123_400_000, CurrentDateTime.nanoPrecision(zdt, 4).getNano());
        assertEquals(123_450_000, CurrentDateTime.nanoPrecision(zdt, 5).getNano());
        assertEquals(123_456_000, CurrentDateTime.nanoPrecision(zdt, 6).getNano());
        assertEquals(123_456_700, CurrentDateTime.nanoPrecision(zdt, 7).getNano());
        assertEquals(123_456_780, CurrentDateTime.nanoPrecision(zdt, 8).getNano());
        assertEquals(123_456_789, CurrentDateTime.nanoPrecision(zdt, 9).getNano());
    }
}
